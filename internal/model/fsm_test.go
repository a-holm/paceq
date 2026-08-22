package model_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

// runCase is one cell of the run cross table under one guard set: the state the
// machine must return, the effects it must demand, and the sentinel any refusal
// must match. A case with a nil sentinel is a transition that is allowed, and
// those cases are also the definition of which pairs are legal at all: the
// completeness test builds the legal set from this table and proves the machine
// refuses every pair the table leaves out.
type runCase struct {
	name    string
	from    model.RunState
	event   model.Event
	guards  model.Guards
	want    model.RunState
	effects []model.Effect
	err     error
}

// runTable is the run machine, written out. Changing a row means changing a
// rule, and the commit has to say why.
func runTable() []runCase {
	return []runCase{
		{
			name:    "a claim starts an available run",
			from:    model.RunQueued,
			event:   model.EvClaim,
			guards:  model.Guards{Now: now, AvailableAt: past},
			want:    model.RunRunning,
			effects: with(kinds(model.EffectBumpEpoch, model.EffectTakeLease, model.EffectSetStarted), emit("run.started")),
		},
		{
			name:    "a run available exactly now is available",
			from:    model.RunQueued,
			event:   model.EvClaim,
			guards:  model.Guards{Now: now, AvailableAt: now},
			want:    model.RunRunning,
			effects: with(kinds(model.EffectBumpEpoch, model.EffectTakeLease, model.EffectSetStarted), emit("run.started")),
		},
		{
			name:   "a claim on a deferred run is refused without moving it",
			from:   model.RunQueued,
			event:  model.EvClaim,
			guards: model.Guards{Now: now, AvailableAt: future},
			want:   model.RunQueued,
			err:    model.ErrNotAvailable,
		},
		{
			name:    "a claim on a run somebody cancelled cancels it instead",
			from:    model.RunQueued,
			event:   model.EvClaim,
			guards:  model.Guards{Now: now, AvailableAt: past, CancelRequested: true, ReasonCode: reasonCode},
			want:    model.RunCancelled,
			effects: with(kinds(model.EffectSetFinished), emit("run.cancelled")),
		},
		{
			name:    "cancellation beats a deferral",
			from:    model.RunQueued,
			event:   model.EvClaim,
			guards:  model.Guards{Now: now, AvailableAt: future, CancelRequested: true, ReasonCode: reasonCode},
			want:    model.RunCancelled,
			effects: with(kinds(model.EffectSetFinished), emit("run.cancelled")),
		},
		{
			name:   "cancelling at claim time needs a reason code",
			from:   model.RunQueued,
			event:  model.EvClaim,
			guards: model.Guards{Now: now, AvailableAt: past, CancelRequested: true},
			want:   model.RunQueued,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:    "a deferral records why and stays queued",
			from:    model.RunQueued,
			event:   model.EvDeferred,
			guards:  model.Guards{Now: now, AvailableAt: future, DeferReason: deferBecause},
			want:    model.RunQueued,
			effects: with(kinds(model.EffectSetAvailableAt), deferTo(deferBecause), emit("run.deferred")),
		},
		{
			name:   "a deferral without a reason is refused",
			from:   model.RunQueued,
			event:  model.EvDeferred,
			guards: model.Guards{Now: now, AvailableAt: future},
			want:   model.RunQueued,
			err:    model.ErrMissingDeferReason,
		},
		{
			name:    "a run whose steps all succeeded succeeds",
			from:    model.RunRunning,
			event:   model.EvAllStepsDone,
			guards:  model.Guards{Now: now, LeaseValid: true, AllStepsTerminal: true, ReasonCode: reasonCode},
			want:    model.RunSucceeded,
			effects: with(kinds(model.EffectSetFinished, model.EffectReleaseLease), emit("run.succeeded")),
		},
		{
			name:    "a run with a failed step fails",
			from:    model.RunRunning,
			event:   model.EvAllStepsDone,
			guards:  model.Guards{Now: now, LeaseValid: true, AllStepsTerminal: true, AnyStepFailed: true, ReasonCode: reasonCode},
			want:    model.RunFailed,
			effects: with(kinds(model.EffectSetFinished, model.EffectReleaseLease), emit("run.failed")),
		},
		{
			name:   "a run cannot finish while a step of it is still active",
			from:   model.RunRunning,
			event:  model.EvAllStepsDone,
			guards: model.Guards{Now: now, LeaseValid: true, ReasonCode: reasonCode},
			want:   model.RunRunning,
			err:    model.ErrStepsNotTerminal,
		},
		{
			name:   "a writer without the lease cannot finish the run",
			from:   model.RunRunning,
			event:  model.EvAllStepsDone,
			guards: model.Guards{Now: now, AllStepsTerminal: true, ReasonCode: reasonCode},
			want:   model.RunRunning,
			err:    model.ErrStaleLease,
		},
		{
			name:   "finishing a run needs a reason code",
			from:   model.RunRunning,
			event:  model.EvAllStepsDone,
			guards: model.Guards{Now: now, LeaseValid: true, AllStepsTerminal: true},
			want:   model.RunRunning,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:   "an observed cancellation kills the process group and finishes the run",
			from:   model.RunRunning,
			event:  model.EvCancelObserved,
			guards: model.Guards{Now: now, LeaseValid: true, CancelRequested: true, ReasonCode: reasonCode},
			want:   model.RunCancelled,
			effects: with(kinds(model.EffectKillProcessGroup, model.EffectSetFinished, model.EffectReleaseLease),
				emit("run.cancelled")),
		},
		{
			name:   "a writer without the lease cannot cancel the run",
			from:   model.RunRunning,
			event:  model.EvCancelObserved,
			guards: model.Guards{Now: now, CancelRequested: true, ReasonCode: reasonCode},
			want:   model.RunRunning,
			err:    model.ErrStaleLease,
		},
		{
			name:   "cancelling a running run needs a reason code",
			from:   model.RunRunning,
			event:  model.EvCancelObserved,
			guards: model.Guards{Now: now, LeaseValid: true, CancelRequested: true},
			want:   model.RunRunning,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:   "an expired lease requeues the run while the crash budget lasts",
			from:   model.RunRunning,
			event:  model.EvLeaseExpired,
			guards: model.Guards{Now: now, CrashBudgetLeft: true},
			want:   model.RunQueued,
			effects: with(kinds(model.EffectBumpEpoch, model.EffectIncCrashCount),
				deferTo(model.DeferReasonAfterCrash), emit("run.requeued")),
		},
		{
			name:    "a run out of crash budget is poisoned",
			from:    model.RunRunning,
			event:   model.EvLeaseExpired,
			guards:  model.Guards{Now: now, ReasonCode: reasonCode},
			want:    model.RunFailed,
			effects: with(kinds(model.EffectSetFinished), emit("run.poisoned")),
		},
		{
			name:   "poisoning a run needs a reason code",
			from:   model.RunRunning,
			event:  model.EvLeaseExpired,
			guards: model.Guards{Now: now},
			want:   model.RunRunning,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:   "a lease that has not expired cannot expire",
			from:   model.RunRunning,
			event:  model.EvLeaseExpired,
			guards: model.Guards{Now: now, LeaseValid: true, CrashBudgetLeft: true},
			want:   model.RunRunning,
			err:    model.ErrLeaseStillValid,
		},
		{
			name:   "a clean drain hands a claimed run back to the queue without counting a crash",
			from:   model.RunRunning,
			event:  model.EvShutdownDrain,
			guards: model.Guards{Now: now, LeaseValid: true},
			want:   model.RunQueued,
			effects: with(kinds(model.EffectBumpEpoch, model.EffectReleaseLease, model.EffectSetAvailableAt),
				deferTo(model.DeferReasonAfterShutdown), emit("run.drained")),
		},
		{
			name:   "a writer without the lease cannot drain the run",
			from:   model.RunRunning,
			event:  model.EvShutdownDrain,
			guards: model.Guards{Now: now},
			want:   model.RunRunning,
			err:    model.ErrStaleLease,
		},
		{
			name:    "an operator reopens a succeeded run",
			from:    model.RunSucceeded,
			event:   model.EvOperatorRetry,
			guards:  model.Guards{Now: now},
			want:    model.RunQueued,
			effects: with(kinds(model.EffectBumpEpoch), emit("run.reopened")),
		},
		{
			name:    "an operator reopens a failed run",
			from:    model.RunFailed,
			event:   model.EvOperatorRetry,
			guards:  model.Guards{Now: now},
			want:    model.RunQueued,
			effects: with(kinds(model.EffectBumpEpoch), emit("run.reopened")),
		},
		{
			name:    "an operator reopens a cancelled run",
			from:    model.RunCancelled,
			event:   model.EvOperatorRetry,
			guards:  model.Guards{Now: now},
			want:    model.RunQueued,
			effects: with(kinds(model.EffectBumpEpoch), emit("run.reopened")),
		},
	}
}

// stepCase is one cell of the step cross table under one guard set.
type stepCase struct {
	name    string
	from    model.StepState
	event   model.Event
	guards  model.Guards
	want    model.StepState
	effects []model.Effect
	err     error
}

// stepTable is the step machine, written out.
func stepTable() []stepCase {
	return []stepCase{
		{
			name:    "a pending step opens an attempt",
			from:    model.StepPending,
			event:   model.EvStepStarted,
			guards:  model.Guards{Now: now},
			want:    model.StepRunning,
			effects: with(kinds(model.EffectIncAttempt, model.EffectSetStarted), emit("step.started")),
		},
		{
			name:    "a running step succeeds",
			from:    model.StepRunning,
			event:   model.EvStepSucceeded,
			guards:  model.Guards{Now: now, ReasonCode: reasonCode},
			want:    model.StepSucceeded,
			effects: with(kinds(model.EffectSetFinished), emit("step.succeeded")),
		},
		{
			name:   "a step that succeeded needs a reason code too",
			from:   model.StepRunning,
			event:  model.EvStepSucceeded,
			guards: model.Guards{Now: now},
			want:   model.StepRunning,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:    "a failed attempt with retries left goes back to pending",
			from:    model.StepRunning,
			event:   model.EvStepFailed,
			guards:  model.Guards{Now: now, AttemptsLeft: true, ReasonCode: reasonCode},
			want:    model.StepPending,
			effects: with(kinds(model.EffectSetNextAttemptAt), emit("step.retry_scheduled")),
		},
		{
			name:   "scheduling a retry needs the failure's reason code",
			from:   model.StepRunning,
			event:  model.EvStepFailed,
			guards: model.Guards{Now: now, AttemptsLeft: true},
			want:   model.StepRunning,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:    "a failed attempt with no retries left fails the step",
			from:    model.StepRunning,
			event:   model.EvStepFailed,
			guards:  model.Guards{Now: now, ReasonCode: reasonCode},
			want:    model.StepFailed,
			effects: with(kinds(model.EffectSetFinished), emit("step.failed")),
		},
		{
			name:   "failing a step needs a reason code",
			from:   model.StepRunning,
			event:  model.EvStepFailed,
			guards: model.Guards{Now: now},
			want:   model.StepRunning,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:    "a failed upstream skips a pending step",
			from:    model.StepPending,
			event:   model.EvUpstreamFailed,
			guards:  model.Guards{Now: now, AnyStepFailed: true, ReasonCode: reasonCode},
			want:    model.StepSkipped,
			effects: with(kinds(model.EffectSetFinished), emit("step.skipped")),
		},
		{
			name:   "skipping a step needs a reason code",
			from:   model.StepPending,
			event:  model.EvUpstreamFailed,
			guards: model.Guards{Now: now, AnyStepFailed: true},
			want:   model.StepPending,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:    "an observed cancellation kills the process group and cancels the step",
			from:    model.StepRunning,
			event:   model.EvCancelObserved,
			guards:  model.Guards{Now: now, CancelRequested: true, ReasonCode: reasonCode},
			want:    model.StepCancelled,
			effects: with(kinds(model.EffectKillProcessGroup, model.EffectSetFinished), emit("step.cancelled")),
		},
		{
			name:   "cancelling a step needs a reason code",
			from:   model.StepRunning,
			event:  model.EvCancelObserved,
			guards: model.Guards{Now: now, CancelRequested: true},
			want:   model.StepRunning,
			err:    model.ErrMissingReasonCode,
		},
		{
			name:    "a drain puts an interrupted attempt back to pending without spending it",
			from:    model.StepRunning,
			event:   model.EvShutdownDrain,
			guards:  model.Guards{Now: now, ReasonCode: reasonCode},
			want:    model.StepPending,
			effects: with(kinds(model.EffectRestoreAttempt, model.EffectSetNextAttemptAt), emit("step.interrupted")),
		},
		{
			name:   "interrupting an attempt for the drain needs a reason code",
			from:   model.StepRunning,
			event:  model.EvShutdownDrain,
			guards: model.Guards{Now: now},
			want:   model.StepRunning,
			err:    model.ErrMissingReasonCode,
		},
	}
}

func TestRunTransitions(t *testing.T) {
	for _, tc := range runTable() {
		t.Run(tc.name, func(t *testing.T) {
			got, fx, err := model.NextRunState(tc.from, tc.event, tc.guards)
			checkOutcome(t, outcome{
				from:    tc.from,
				event:   tc.event,
				want:    tc.want,
				effects: tc.effects,
				err:     tc.err,
			}, got, fx, err)
		})
	}
}

func TestStepTransitions(t *testing.T) {
	for _, tc := range stepTable() {
		t.Run(tc.name, func(t *testing.T) {
			got, fx, err := model.NextStepState(tc.from, tc.event, tc.guards)
			checkOutcome(t, outcome{
				from:    tc.from,
				event:   tc.event,
				want:    tc.want,
				effects: tc.effects,
				err:     tc.err,
			}, got, fx, err)
		})
	}
}

// outcome is one expected result, in the shape both machines share.
type outcome struct {
	from    model.State
	event   model.Event
	want    model.State
	effects []model.Effect
	err     error
}

// checkOutcome compares one call against its row in the table, including the
// rule that a refusal changes nothing: the state comes back as it went in and
// no effect is demanded.
func checkOutcome(t *testing.T, want outcome, got model.State, fx []model.Effect, err error) {
	t.Helper()

	if got.String() != want.want.String() {
		t.Errorf("%s %q + %q gave state %q, want %q",
			want.from.Kind(), want.from, want.event, got, want.want)
	}
	if !slices.Equal(fx, want.effects) {
		t.Errorf("%s %q + %q demanded effects %v, want %v",
			want.from.Kind(), want.from, want.event, fx, want.effects)
	}
	switch {
	case want.err == nil && err != nil:
		t.Errorf("%s %q + %q is a legal transition but was refused: %v",
			want.from.Kind(), want.from, want.event, err)
	case want.err != nil && !errors.Is(err, want.err):
		t.Errorf("%s %q + %q gave error %v, want one matching %v",
			want.from.Kind(), want.from, want.event, err, want.err)
	}
	if want.err != nil {
		if got.String() != want.from.String() {
			t.Errorf("%s %q + %q was refused but moved to %q: a refusal changes nothing",
				want.from.Kind(), want.from, want.event, got)
		}
		if len(fx) != 0 {
			t.Errorf("%s %q + %q was refused but demanded effects %v: a refusal changes nothing",
				want.from.Kind(), want.from, want.event, fx)
		}
	}
}
