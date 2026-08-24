package model

import (
	"slices"
	"testing"
)

// M4-04: the step half of T14. A run an operator reopens carries its failed
// and skipped steps back to pending, so the claim predicate (M4-02) offers
// them again once their upstreams have succeeded. Succeeded steps stay where
// they are: their work is reused, not repeated.

func TestOperatorRetryReopensAFailedStep(t *testing.T) {
	state, effects, err := NextStepState(StepFailed, EvOperatorRetry, Guards{})
	if err != nil {
		t.Fatalf("reopen a failed step: %v", err)
	}
	if state != StepPending {
		t.Errorf("failed + operator_retry = %s, want pending", state)
	}
	if !slices.Contains(effects, act(EffectSetNextAttemptAt)) {
		t.Errorf("effects %v do not set next_attempt_at: the reopened step must be schedulable", effects)
	}
	if emitKindOf(effects) != "step.reopened" {
		t.Errorf("effects %v do not emit step.reopened", effects)
	}
}

func TestOperatorRetryReopensASkippedStep(t *testing.T) {
	state, _, err := NextStepState(StepSkipped, EvOperatorRetry, Guards{})
	if err != nil {
		t.Fatalf("reopen a skipped step: %v", err)
	}
	if state != StepPending {
		t.Errorf("skipped + operator_retry = %s, want pending", state)
	}
}

// A succeeded step must never move on an operator retry. Its output is what
// the retry is for; reopening it would spend work the run already has.
func TestOperatorRetryLeavesASucceededStepAlone(t *testing.T) {
	for _, cur := range []StepState{StepSucceeded, StepPending, StepRunning} {
		state, _, err := NextStepState(cur, EvOperatorRetry, Guards{})
		if err == nil {
			t.Errorf("%s + operator_retry moved to %s, want a refusal", cur, state)
		}
	}
}

// emitKindOf reads back the emit effect of an effect list, mirroring what the
// store's transition writer does with it.
func emitKindOf(effects []Effect) string {
	for _, fx := range effects {
		if fx.Kind == EffectEmit {
			return fx.Arg
		}
	}
	return ""
}
