package store_test

import (
	"context"
	"testing"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// #188: the finish arm writes a state its own steps have to agree with, the
// same rule the cancel arm has held since #170.
//
// A finish over a cancelled step used to be unreachable in the machine: the
// arm folded failed and cancelled into one flag, so it had two outcomes where
// the fold has three. Nothing revisits a terminal row, so whatever it wrote
// was final and fsck reported the disagreement forever.

// TestFinishRunTakesACancelledStepFromAnEarlierAttempt is the reachable route
// to that shape, and the reason a cancelled step outlives a retry:
// reopenTargetsTx puts back only failed and skipped steps, so a step cancelled
// in one generation is still cancelled in the next.
func TestFinishRunTakesACancelledStepFromAnEarlierAttempt(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "fork", forkSpec)

	// The first attempt: boom fails, the operator cancels while later is
	// still open, and later closes as cancelled. Failed outranks cancelled,
	// so the run is failed and agrees with its steps.
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("the first claim: %v", err)
	}
	first := store.LeaseRef{Owner: testOwner, Epoch: 1}
	if err := s.StartStep(ctx, runID, "boom", first); err != nil {
		t.Fatalf("start boom: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "boom", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode: ptr(1), FinishedAt: clk.Now(),
	}, first); err != nil {
		t.Fatalf("fail boom: %v", err)
	}
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop it"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	if err := s.ObserveRunCancel(ctx, runID, first, "cli:1000", reason.RUNCancelledManual); err != nil {
		t.Fatalf("ObserveRunCancel: %v", err)
	}
	assertRunMatchesItsSteps(t, ctx, s, runID, model.RunFailed, reason.RUNFailedStep)

	// The operator retries. Only boom is reopened; later keeps the
	// cancellation, which is what carries it into the finish arm.
	res, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(res.Reopened) != 1 || res.Reopened[0] != "boom" {
		t.Fatalf("reopened %v, want only boom: a cancelled step is not a reopen target", res.Reopened)
	}
	if got := mustStep(t, ctx, s, runID, "later").State; got != string(model.StepCancelled) {
		t.Fatalf("later = %s after the reopen, want cancelled", got)
	}

	// The second attempt succeeds everywhere it can, and nothing is left
	// runnable: later is terminal already.
	_, epoch, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner})
	if err != nil {
		t.Fatalf("the second claim: %v", err)
	}
	second := store.LeaseRef{Owner: testOwner, Epoch: epoch}
	if err := s.StartStep(ctx, runID, "boom", second); err != nil {
		t.Fatalf("restart boom: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "boom", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
		ExitCode: ptr(0), FinishedAt: clk.Now(),
	}, second); err != nil {
		t.Fatalf("succeed boom: %v", err)
	}

	state, err := s.FinishRun(ctx, runID, second, store.FinishReason{Code: reason.RUNCancelledManual})
	if err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	if state != string(model.RunCancelled) {
		t.Errorf("FinishRun = %s, want cancelled: succeeded and cancelled aggregate to cancelled", state)
	}
	assertRunMatchesItsSteps(t, ctx, s, runID, model.RunCancelled, reason.RUNCancelledManual)
}
