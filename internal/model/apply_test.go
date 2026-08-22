package model_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

// TestApplyRunFoldsTheSameWayAsCallingTheMachine runs every sequence of two
// events, under a spread of guard sets, through Apply and through a hand
// written fold. The helper exists so the property tests in M3-08 have a
// reference model to compare a real store against; a reference model that
// disagrees with the machine it references is worse than none.
func TestApplyRunFoldsTheSameWayAsCallingTheMachine(t *testing.T) {
	guards := []model.Guards{
		{Now: now, AvailableAt: past, ReasonCode: reasonCode, DeferReason: deferBecause},
		{Now: now, AvailableAt: future, LeaseValid: true, AllStepsTerminal: true, ReasonCode: reasonCode},
		{Now: now, AvailableAt: past, CancelRequested: true, CrashBudgetLeft: true, AttemptsLeft: true, ReasonCode: reasonCode},
	}

	sequences := 0
	for _, first := range model.AllEvents() {
		for _, second := range model.AllEvents() {
			for _, g := range guards {
				inputs := []model.Input{{Event: first, Guards: g}, {Event: second, Guards: g}}
				sequences++

				gotState, gotEffects, gotErr := model.ApplyRun(model.RunQueued, inputs)
				wantState, wantEffects, wantErr := foldRun(model.RunQueued, inputs)

				if gotState != wantState || !slices.Equal(gotEffects, wantEffects) || errText(gotErr) != errText(wantErr) {
					t.Fatalf("ApplyRun(queued, %q then %q) = (%q, %v, %v), want (%q, %v, %v)",
						first, second, gotState, gotEffects, gotErr, wantState, wantEffects, wantErr)
				}
			}
		}
	}
	if want := len(model.AllEvents()) * len(model.AllEvents()) * len(guards); sequences != want {
		t.Errorf("walked %d sequences, want %d", sequences, want)
	}
}

// foldRun is Apply written out at the call site, which is what Apply has to
// agree with.
func foldRun(cur model.RunState, inputs []model.Input) (model.RunState, []model.Effect, error) {
	var all []model.Effect
	for _, in := range inputs {
		next, fx, err := model.NextRunState(cur, in.Event, in.Guards)
		if err != nil {
			return cur, all, err
		}
		cur = next
		all = append(all, fx...)
	}
	return cur, all, nil
}

// TestApplyRunStopsAtTheFirstRefusal pins what a caller gets back when a
// sequence cannot be finished: the state it reached, the effects it earned on
// the way, and the refusal itself.
func TestApplyRunStopsAtTheFirstRefusal(t *testing.T) {
	g := model.Guards{Now: now, AvailableAt: past, LeaseValid: true, AllStepsTerminal: true, ReasonCode: reasonCode}
	inputs := []model.Input{
		{Event: model.EvClaim, Guards: g},
		{Event: model.EvClaim, Guards: g},
		{Event: model.EvAllStepsDone, Guards: g},
	}

	got, fx, err := model.ApplyRun(model.RunQueued, inputs)

	if got != model.RunRunning {
		t.Errorf("the sequence stopped at %q, want %q", got, model.RunRunning)
	}
	if !errors.Is(err, model.ErrIllegalTransition) {
		t.Errorf("the sequence failed with %v, want an illegal transition", err)
	}
	want := with(kinds(model.EffectBumpEpoch, model.EffectTakeLease, model.EffectSetStarted), emit("run.started"))
	if !slices.Equal(fx, want) {
		t.Errorf("the sequence earned effects %v, want %v", fx, want)
	}
}

// TestApplyRunsARunToItsEnd is the ordinary life of a run in one call: claimed,
// finished, reopened by an operator.
func TestApplyRunsARunToItsEnd(t *testing.T) {
	claim := model.Guards{Now: now, AvailableAt: past}
	finish := model.Guards{Now: now, LeaseValid: true, AllStepsTerminal: true, ReasonCode: reasonCode}
	inputs := []model.Input{
		{Event: model.EvClaim, Guards: claim},
		{Event: model.EvAllStepsDone, Guards: finish},
		{Event: model.EvOperatorRetry, Guards: finish},
	}

	got, fx, err := model.ApplyRun(model.RunQueued, inputs)
	if err != nil {
		t.Fatalf("the run could not be driven to its end: %v", err)
	}
	if got != model.RunQueued {
		t.Errorf("the run ended in %q, want %q", got, model.RunQueued)
	}
	want := slices.Concat(
		with(kinds(model.EffectBumpEpoch, model.EffectTakeLease, model.EffectSetStarted), emit("run.started")),
		with(kinds(model.EffectSetFinished, model.EffectReleaseLease), emit("run.succeeded")),
		with(kinds(model.EffectBumpEpoch), emit("run.reopened")),
	)
	if !slices.Equal(fx, want) {
		t.Errorf("the run earned effects %v, want %v", fx, want)
	}
}

// TestApplyStepFoldsTheSameWayAsCallingTheMachine is the step half of the
// reference model.
func TestApplyStepFoldsTheSameWayAsCallingTheMachine(t *testing.T) {
	guards := []model.Guards{
		{Now: now, ReasonCode: reasonCode},
		{Now: now, AttemptsLeft: true, ReasonCode: reasonCode},
		{Now: now, CancelRequested: true},
	}

	for _, first := range model.AllEvents() {
		for _, second := range model.AllEvents() {
			for _, g := range guards {
				inputs := []model.Input{{Event: first, Guards: g}, {Event: second, Guards: g}}

				gotState, gotEffects, gotErr := model.ApplyStep(model.StepPending, inputs)
				wantState, wantEffects, wantErr := foldStep(model.StepPending, inputs)

				if gotState != wantState || !slices.Equal(gotEffects, wantEffects) || errText(gotErr) != errText(wantErr) {
					t.Fatalf("ApplyStep(pending, %q then %q) = (%q, %v, %v), want (%q, %v, %v)",
						first, second, gotState, gotEffects, gotErr, wantState, wantEffects, wantErr)
				}
			}
		}
	}
}

func foldStep(cur model.StepState, inputs []model.Input) (model.StepState, []model.Effect, error) {
	var all []model.Effect
	for _, in := range inputs {
		next, fx, err := model.NextStepState(cur, in.Event, in.Guards)
		if err != nil {
			return cur, all, err
		}
		cur = next
		all = append(all, fx...)
	}
	return cur, all, nil
}

// TestApplyOfNothingChangesNothing is the empty sequence, which a caller
// replaying an empty history hands over.
func TestApplyOfNothingChangesNothing(t *testing.T) {
	state, fx, err := model.ApplyRun(model.RunRunning, nil)
	if state != model.RunRunning || len(fx) != 0 || err != nil {
		t.Errorf("ApplyRun(running, nothing) = (%q, %v, %v), want (running, no effects, no error)", state, fx, err)
	}
	step, stepFx, stepErr := model.ApplyStep(model.StepRunning, nil)
	if step != model.StepRunning || len(stepFx) != 0 || stepErr != nil {
		t.Errorf("ApplyStep(running, nothing) = (%q, %v, %v), want (running, no effects, no error)", step, stepFx, stepErr)
	}
}
