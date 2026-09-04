package model_test

import (
	"slices"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

// TestRunAggregateOverEveryCombinationOfUpToFourSteps is exhaustive rather than
// sampled: every list of up to four steps, 1555 of them, checked against an
// oracle written the other way round from the implementation. The invariants
// the aggregate exists to hold, I2 and I10, are asserted on every list.
func TestRunAggregateOverEveryCombinationOfUpToFourSteps(t *testing.T) {
	const maxSteps = 4

	combinations := 0
	var walk func(prefix []model.StepState)
	walk = func(prefix []model.StepState) {
		combinations++

		got := model.RunAggregate(prefix, false)
		if want := aggregateOracle(prefix); got != want {
			t.Fatalf("RunAggregate(%v) = %q, want %q", prefix, got, want)
		}
		if got == model.RunSucceeded && slices.Contains(prefix, model.StepFailed) {
			t.Fatalf("RunAggregate(%v) = %q with a failed step, which breaks I10", prefix, got)
		}
		if got == model.RunFailed && !slices.Contains(prefix, model.StepFailed) {
			t.Fatalf("RunAggregate(%v) = %q with no failed step, which breaks I10", prefix, got)
		}
		active := slices.Contains(prefix, model.StepPending) || slices.Contains(prefix, model.StepRunning)
		if active && got.IsTerminal() {
			t.Fatalf("RunAggregate(%v) = %q with an active step, which breaks I2", prefix, got)
		}
		if !slices.Contains(model.AllRunStates(), got) {
			t.Fatalf("RunAggregate(%v) = %q, which is not a run state", prefix, got)
		}

		if len(prefix) == maxSteps {
			return
		}
		for _, s := range model.AllStepStates() {
			next := make([]model.StepState, 0, len(prefix)+1)
			next = append(next, prefix...)
			walk(append(next, s))
		}
	}
	walk(nil)

	// 1 + 6 + 36 + 216 + 1296. A smaller number means the walk lost a state
	// and the sweep would be proving less than it claims.
	if want := 1555; combinations != want {
		t.Errorf("walked %d step lists, want %d", combinations, want)
	}
}

// aggregateOracle is the rule stated as membership tests rather than as one
// pass over the list, so an error in the implementation's loop does not repeat
// itself here.
func aggregateOracle(steps []model.StepState) model.RunState {
	switch {
	case slices.Contains(steps, model.StepPending), slices.Contains(steps, model.StepRunning):
		return model.RunRunning
	case slices.Contains(steps, model.StepFailed):
		return model.RunFailed
	case slices.Contains(steps, model.StepCancelled):
		return model.RunCancelled
	default:
		return model.RunSucceeded
	}
}

// TestRunAggregateOfNoStepsSucceeds pins the vacuous case. A run with no steps
// has nothing outstanding and nothing failed, so it is succeeded. The engine
// never materialises a run without steps, because a job without steps does not
// validate, and this is what the model says if one ever appears.
func TestRunAggregateOfNoStepsSucceeds(t *testing.T) {
	if got := model.RunAggregate(nil, false); got != model.RunSucceeded {
		t.Errorf("RunAggregate(nil) = %q, want %q", got, model.RunSucceeded)
	}
	if got := model.RunAggregate([]model.StepState{}, false); got != model.RunSucceeded {
		t.Errorf("RunAggregate(empty) = %q, want %q", got, model.RunSucceeded)
	}
}

// TestRunAggregateReadsSkippedStepsAsSuccess is the rule from I10 spelled out:
// a run whose steps all succeeded or were skipped succeeded. A skip is a
// decision, not a failure.
func TestRunAggregateReadsSkippedStepsAsSuccess(t *testing.T) {
	steps := []model.StepState{model.StepSucceeded, model.StepSkipped, model.StepSkipped}
	if got := model.RunAggregate(steps, false); got != model.RunSucceeded {
		t.Errorf("RunAggregate(%v) = %q, want %q", steps, got, model.RunSucceeded)
	}
}

// TestRunAggregateDoesNotTouchItsInput is what makes the function safe to hand
// a slice the caller keeps using.
func TestRunAggregateDoesNotTouchItsInput(t *testing.T) {
	steps := []model.StepState{model.StepFailed, model.StepPending, model.StepSkipped}
	before := slices.Clone(steps)

	model.RunAggregate(steps, false)

	if !slices.Equal(steps, before) {
		t.Errorf("RunAggregate rewrote its input to %v, want %v", steps, before)
	}
}

// TestTerminalVerdictMatchesRunAggregate proves the claim run_fsm.go makes: the
// machine ranks a terminal run's steps in the same order the fold does. The
// machine never reads a step row, so the two can only agree by holding the same
// order, and nothing but this test holds them to it.
//
// The comment asserted this test existed before the test did (#188).
func TestTerminalVerdictMatchesRunAggregate(t *testing.T) {
	t.Parallel()

	terminal := []model.StepState{
		model.StepSucceeded, model.StepFailed,
		model.StepCancelled, model.StepSkipped,
	}
	var walk func(prefix []model.StepState, depth int)
	walk = func(prefix []model.StepState, depth int) {
		if len(prefix) > 0 {
			want := model.RunAggregate(prefix, false)
			g := model.Guards{
				AnyStepFailed:    contains(prefix, model.StepFailed),
				AnyStepCancelled: contains(prefix, model.StepCancelled),
			}
			got, _ := model.TerminalVerdict(g)
			if got != want {
				t.Fatalf("TerminalVerdict over %v = %q, RunAggregate = %q", prefix, got, want)
			}
		}
		if depth == 0 {
			return
		}
		for _, s := range terminal {
			walk(append(prefix, s), depth-1)
		}
	}
	walk(nil, 4)
}

// TestRunLevelFailureOutranksEverySetOfSteps pins the second input: a run that
// failed for a reason no step can express is failed whatever its steps say,
// including the all-skipped shape the reaper leaves behind.
func TestRunLevelFailureOutranksEverySetOfSteps(t *testing.T) {
	t.Parallel()

	for _, steps := range [][]model.StepState{
		nil,
		{model.StepSkipped},
		{model.StepSkipped, model.StepSkipped, model.StepSkipped},
		{model.StepSucceeded, model.StepSucceeded},
		{model.StepCancelled},
		{model.StepPending},
	} {
		if got := model.RunAggregate(steps, true); got != model.RunFailed {
			t.Errorf("RunAggregate(%v, true) = %q, want %q", steps, got, model.RunFailed)
		}
	}
}

func contains(steps []model.StepState, want model.StepState) bool {
	for _, s := range steps {
		if s == want {
			return true
		}
	}
	return false
}

// TestRunStateAgreesOverEveryStoredStateAndStepList is exhaustive: every stored
// run state against every list of up to three steps, with and without a
// run-level failure, checked against an oracle stated as a set of permitted
// stored states rather than as an equality with one branch off it.
//
// Exhaustiveness is what makes this bite in both directions. A predicate that
// excused more than the one-to-many region admits a stored state the oracle's
// set does not hold; a predicate that excused less refuses one the set does.
func TestRunStateAgreesOverEveryStoredStateAndStepList(t *testing.T) {
	t.Parallel()

	const maxSteps = 3

	pairs := 0
	var walk func(prefix []model.StepState)
	walk = func(prefix []model.StepState) {
		for _, runLevelFailure := range []bool{false, true} {
			for _, have := range model.AllRunStates() {
				pairs++
				got := model.RunStateAgrees(have, prefix, runLevelFailure)
				want := runStateAgreesOracle(have, prefix, runLevelFailure)
				if got != want {
					t.Fatalf("RunStateAgrees(%q, %v, %t) = %t, want %t",
						have, prefix, runLevelFailure, got, want)
				}
			}
		}
		if len(prefix) == maxSteps {
			return
		}
		for _, s := range model.AllStepStates() {
			next := make([]model.StepState, 0, len(prefix)+1)
			next = append(next, prefix...)
			walk(append(next, s))
		}
	}
	walk(nil)

	// (1 + 6 + 36 + 216) step lists, five stored states, two flags. A
	// smaller number means the walk lost a state and the sweep proves less
	// than it claims.
	if want := 259 * 5 * 2; pairs != want {
		t.Errorf("walked %d pairs, want %d", pairs, want)
	}
}

// runStateAgreesOracle is the permitted set per step list, written as
// membership rather than as one comparison with an exception hung off it, so
// an error in the implementation's shape does not repeat itself here. It
// reads the aggregate off aggregateOracle for the same reason.
func runStateAgreesOracle(have model.RunState, steps []model.StepState, runLevelFailure bool) bool {
	var allowed []model.RunState
	switch {
	case runLevelFailure:
		allowed = []model.RunState{model.RunFailed}
	case aggregateOracle(steps) == model.RunRunning:
		allowed = []model.RunState{model.RunQueued, model.RunRunning}
	default:
		allowed = []model.RunState{aggregateOracle(steps)}
	}
	return slices.Contains(allowed, have)
}

// TestRunStateAgreesIsOneToManyOnlyForOutstandingWork counts the permitted
// stored states for every step list. Work outstanding permits exactly two,
// queued and running, because the steps cannot see whether anyone has claimed
// the run. Every other aggregate permits exactly one. The count is the claim
// the doc comment makes, asserted rather than described: a predicate that
// excuses too much raises a count, and one that excuses too little lowers the
// only count that is not one.
func TestRunStateAgreesIsOneToManyOnlyForOutstandingWork(t *testing.T) {
	t.Parallel()

	const maxSteps = 3

	var walk func(prefix []model.StepState)
	walk = func(prefix []model.StepState) {
		for _, runLevelFailure := range []bool{false, true} {
			var agreeing []model.RunState
			for _, have := range model.AllRunStates() {
				if model.RunStateAgrees(have, prefix, runLevelFailure) {
					agreeing = append(agreeing, have)
				}
			}
			want := []model.RunState{model.RunAggregate(prefix, runLevelFailure)}
			if want[0] == model.RunRunning {
				want = []model.RunState{model.RunQueued, model.RunRunning}
			}
			if !slices.Equal(agreeing, want) {
				t.Fatalf("steps %v (run-level failure %t) agree with %v, want %v",
					prefix, runLevelFailure, agreeing, want)
			}
		}
		if len(prefix) == maxSteps {
			return
		}
		for _, s := range model.AllStepStates() {
			next := make([]model.StepState, 0, len(prefix)+1)
			next = append(next, prefix...)
			walk(append(next, s))
		}
	}
	walk(nil)
}

// TestRunStateAgreesLeavesTheQueueAtRestAlone is the load-bearing half stated
// on its own, because it is the one a reader will look for. A materialised run
// nobody has claimed reads queued over pending steps, and so does every run a
// requeue puts back. That is the resting state of the whole queue, not a
// recovery window, and a checker that reported it would report the queue.
func TestRunStateAgreesLeavesTheQueueAtRestAlone(t *testing.T) {
	t.Parallel()

	for _, steps := range [][]model.StepState{
		{model.StepPending},
		{model.StepPending, model.StepPending},
		{model.StepSucceeded, model.StepPending},
		{model.StepRunning},
		{model.StepSkipped, model.StepRunning, model.StepPending},
	} {
		if !model.RunStateAgrees(model.RunQueued, steps, false) {
			t.Errorf("a queued run over %v disagrees; the queue at rest reads as drift", steps)
		}
		if !model.RunStateAgrees(model.RunRunning, steps, false) {
			t.Errorf("a running run over %v disagrees; a claimed run reads as drift", steps)
		}
	}
}

// TestRunStateAgreesRefusesEveryOtherDisagreement is the other half. Outside
// the one region, a stored state that is not the aggregate is drift and must
// be reported, including a queued row carrying a run-level failure: that flag
// outranks the steps, so the aggregate is failed and the queued exemption
// never reaches it.
func TestRunStateAgreesRefusesEveryOtherDisagreement(t *testing.T) {
	t.Parallel()

	cases := []struct {
		have            model.RunState
		steps           []model.StepState
		runLevelFailure bool
	}{
		{model.RunQueued, []model.StepState{model.StepSucceeded}, false},
		{model.RunQueued, []model.StepState{model.StepFailed}, false},
		{model.RunQueued, []model.StepState{model.StepCancelled}, false},
		{model.RunQueued, nil, false},
		{model.RunQueued, []model.StepState{model.StepPending}, true},
		{model.RunRunning, []model.StepState{model.StepSucceeded}, false},
		{model.RunRunning, []model.StepState{model.StepFailed}, false},
		{model.RunSucceeded, []model.StepState{model.StepFailed}, false},
		{model.RunSucceeded, []model.StepState{model.StepPending}, false},
		{model.RunFailed, []model.StepState{model.StepSucceeded}, false},
		{model.RunFailed, []model.StepState{model.StepPending}, false},
		{model.RunCancelled, []model.StepState{model.StepFailed}, false},
		{model.RunCancelled, []model.StepState{model.StepPending}, false},
	}
	for _, tc := range cases {
		if model.RunStateAgrees(tc.have, tc.steps, tc.runLevelFailure) {
			t.Errorf("a %s run over %v (run-level failure %t) agrees, want it reported; "+
				"the aggregate is %q",
				tc.have, tc.steps, tc.runLevelFailure,
				model.RunAggregate(tc.steps, tc.runLevelFailure))
		}
	}
}
