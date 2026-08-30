package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// #188: the reaper's failure arms write a state its own steps have to agree
// with. Two shapes reach those arms and they need opposite answers, which is
// why one rule cannot serve both.
//
// The reaper wins the race that produces them. Both reconcilers call
// ReapExpiredRuns before ReconcileRunStates, so a run killed between its last
// step outcome and its FinishRun is decided by the reaper, and a terminal row
// is skipped by every pass after it: whatever the reaper writes is final.

// crashTheRunUpTo drives claim-expire-reap cycles until the run has crashed n
// times, leaving it queued and due. One more crash after this poisons it.
func crashTheRunUpTo(t *testing.T, ctx context.Context, s *store.Store,
	clk interface{ Advance(time.Duration) }, runID string, n int,
) {
	t.Helper()

	for i := 0; i < n; i++ {
		if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed"}); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)
		reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
		if err != nil {
			t.Fatalf("reap %d: %v", i, err)
		}
		if len(reaped) != 1 || reaped[0].State != string(model.RunQueued) {
			t.Fatalf("crash %d took %+v, want the run requeued", i+1, reaped)
		}
		clk.Advance(store.DefaultRequeueBackoff)
	}
}

// TestReaperTakesAFinishedRunsStateFromItsSteps is the arm that used to write
// failed over work that had already succeeded. The steps are all terminal, so
// nothing is outstanding: the kill only stopped FinishRun recording what the
// run had already reached.
func TestReaperTakesAFinishedRunsStateFromItsSteps(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	crashTheRunUpTo(t, ctx, s, clk, runID, 5)

	// The sixth attempt does the work and then dies before finishing the run.
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed"}); err != nil {
		t.Fatalf("the sixth claim: %v", err)
	}
	ref := store.LeaseRef{Owner: "doomed", Epoch: 11}
	if err := s.StartStep(ctx, runID, "build", ref); err != nil {
		t.Fatalf("start build: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
		ExitCode: ptr(0), FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("succeed build: %v", err)
	}

	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)
	reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("the poison reap: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("the sweep took %+v, want the run", reaped)
	}
	if reaped[0].State != string(model.RunSucceeded) {
		t.Fatalf("the reaper took a finished run to %s, want succeeded: its only step succeeded",
			reaped[0].State)
	}

	assertRunMatchesItsSteps(t, ctx, s, runID, model.RunSucceeded, reason.RUNSucceeded)
}

// TestReaperQuarantinesARunThatNeverRanAStep is the other arm. Nothing ran, so
// every step closes as skipped and the step fold alone reads the run as a
// success. The run did fail, for a reason no step can express, and fsck must
// take that as the answer rather than reporting it forever.
func TestReaperQuarantinesARunThatNeverRanAStep(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	crashTheRunUpTo(t, ctx, s, clk, runID, 5)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed"}); err != nil {
		t.Fatalf("the sixth claim: %v", err)
	}
	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)
	reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("the poison reap: %v", err)
	}
	if len(reaped) != 1 || reaped[0].State != string(model.RunFailed) {
		t.Fatalf("the sweep took %+v, want the run quarantined", reaped)
	}
	if reaped[0].ReasonCode != string(reason.RUNPoisoned) {
		t.Errorf("quarantine reason = %q, want %q", reaped[0].ReasonCode, reason.RUNPoisoned)
	}

	if got := mustStep(t, ctx, s, runID, "build").State; got != string(model.StepSkipped) {
		t.Errorf("build = %s, want skipped: it never ran", got)
	}

	// The run is failed and its steps aggregate to succeeded. That is not a
	// mismatch, because the failure is the run's own and the steps are right
	// to say nothing happened.
	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("Fsck: %v", err)
	}
	for _, v := range violations {
		if v.Check == "I10" {
			t.Errorf("fsck I10 on %s: %s", v.Subject, v.Detail)
		}
	}
}
