package model_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

// sentinels is every refusal the package can produce. A caller matches these
// with errors.Is, which is why none of them is a bare fmt.Errorf.
func sentinels() []error {
	return []error{
		model.ErrIllegalTransition,
		model.ErrNotAvailable,
		model.ErrMissingReasonCode,
		model.ErrMissingDeferReason,
		model.ErrStepsNotTerminal,
		model.ErrStaleLease,
		model.ErrLeaseStillValid,
		model.ErrUnknownState,
	}
}

// TestEverySentinelIsReachable sweeps both cross tables and collects the
// sentinels the machines actually produce. A sentinel nothing can raise is a
// promise to callers that no code keeps.
func TestEverySentinelIsReachable(t *testing.T) {
	raised := map[error]bool{}
	for _, m := range machines() {
		for _, from := range m.states {
			for _, ev := range model.AllEvents() {
				for _, g := range guardCombos() {
					_, _, err := m.next(from, ev, g)
					for _, s := range sentinels() {
						if errors.Is(err, s) {
							raised[s] = true
						}
					}
				}
			}
		}
	}
	if _, err := model.ParseRunState("nonsense"); errors.Is(err, model.ErrUnknownState) {
		raised[model.ErrUnknownState] = true
	}

	for _, s := range sentinels() {
		if !raised[s] {
			t.Errorf("no call anywhere produces %v: either the rule is gone or the sentinel is", s)
		}
	}
}

// TestARefusalNamesTheTransitionThatWasRefused is what makes a log line and an
// explain output readable without the reader holding the code. Every refusal
// says which machine, which state and which event.
func TestARefusalNamesTheTransitionThatWasRefused(t *testing.T) {
	_, _, err := model.NextRunState(model.RunSucceeded, model.EvClaim, model.Guards{Now: now})
	if err == nil {
		t.Fatal("claiming a succeeded run was allowed")
	}
	for _, want := range []string{"run", "succeeded", "claim", "illegal transition"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not mention %q", err, want)
		}
	}

	var illegal model.IllegalTransitionError
	if !errors.As(err, &illegal) {
		t.Fatalf("the refusal %v carries no from state and event", err)
	}
	if illegal.From != model.State(model.RunSucceeded) || illegal.Event != model.EvClaim {
		t.Errorf("the refusal names (%q, %q), want (succeeded, claim)", illegal.From, illegal.Event)
	}

	_, _, stepErr := model.NextStepState(model.StepPending, model.EvClaim, model.Guards{Now: now})
	if stepErr == nil || !strings.Contains(stepErr.Error(), "step") {
		t.Errorf("the step refusal %v does not say which machine refused", stepErr)
	}
}

// TestAGuardRefusalNamesTheOutcomeItDenied is the other half: the pair is
// legal, so the useful message is which outcome the guards stood in the way of.
func TestAGuardRefusalNamesTheOutcomeItDenied(t *testing.T) {
	_, _, err := model.NextRunState(model.RunRunning, model.EvAllStepsDone,
		model.Guards{Now: now, LeaseValid: true, AllStepsTerminal: true})
	if !errors.Is(err, model.ErrMissingReasonCode) {
		t.Fatalf("finishing a run without a reason code gave %v", err)
	}

	var guard model.GuardError
	if !errors.As(err, &guard) {
		t.Fatalf("the refusal %v carries no detail", err)
	}
	if guard.From != model.State(model.RunRunning) || guard.Event != model.EvAllStepsDone {
		t.Errorf("the refusal names (%q, %q), want (running, all_steps_done)", guard.From, guard.Event)
	}
	if guard.To != model.State(model.RunSucceeded) {
		t.Errorf("the refusal denied %q, want the outcome %q", guard.To, model.RunSucceeded)
	}
	for _, want := range []string{"run", "running", "all_steps_done", "succeeded", "missing reason code"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not mention %q", err, want)
		}
	}
}

// TestAnUnknownStateSaysWhichMachineItIsNotIn keeps the parse errors as useful
// as the transition ones.
func TestAnUnknownStateSaysWhichMachineItIsNotIn(t *testing.T) {
	_, err := model.ParseStepState("queued")
	if !errors.Is(err, model.ErrUnknownState) {
		t.Fatalf("ParseStepState(\"queued\") gave %v", err)
	}
	for _, want := range []string{"step", "queued", "unknown"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not mention %q", err, want)
		}
	}
}
