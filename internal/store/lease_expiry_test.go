package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// #202. Owner, epoch and expiry are three facts about a lease, and a write
// path that reads two of them is not a weaker version of one that reads three:
// it answers differently. These tests hold every holder write path to one
// answer about the same row at the same instant.

// holderWrite is one path a lease holder writes through. admitted says whether
// the lease let the write land, which is the only fact under test here; err
// carries what the path said when it refused.
type holderWrite struct {
	name string

	// terminal seeds the run with its step already succeeded. The three run
	// level paths need a run whose steps are all terminal; RecordStepOutcome
	// needs the opposite, because a terminal step has no outcome left to
	// record. The lease question is the same either way.
	terminal bool

	write func(t *testing.T, s *store.Store, runID string, holder store.LeaseRef) (bool, error)
}

func holderWritePaths() []holderWrite {
	ctx := context.Background()
	return []holderWrite{
		{
			name:     "RecordStepOutcome",
			terminal: false,
			write: func(_ *testing.T, s *store.Store, runID string, holder store.LeaseRef) (bool, error) {
				err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
					Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
				}, holder)
				return err == nil, err
			},
		},
		{
			name:     "FinishRun",
			terminal: true,
			write: func(_ *testing.T, s *store.Store, runID string, holder store.LeaseRef) (bool, error) {
				_, err := s.FinishRun(ctx, runID, holder, store.FinishReason{Code: reason.RUNSucceeded})
				return err == nil, err
			},
		},
		{
			name:     "ObserveRunCancel",
			terminal: true,
			write: func(_ *testing.T, s *store.Store, runID string, holder store.LeaseRef) (bool, error) {
				err := s.ObserveRunCancel(ctx, runID, holder, "cli:1000", reason.RUNCancelledManual)
				return err == nil, err
			},
		},
		{
			name:     "DrainRun",
			terminal: true,
			write: func(t *testing.T, s *store.Store, runID string, holder store.LeaseRef) (bool, error) {
				// A drain that finds the row taken reports nothing owed
				// rather than an error, so handed is its admission answer.
				handed, err := s.DrainRun(ctx, runID, holder, reason.RUNInterruptedShutdown)
				return handed, err
			},
		},
	}
}

// anExpiredButOwnedLease seeds a run claimed by testOwner at epoch 1 and moves
// the clock past the ttl and the skew allowance without renewing it and
// without reaping. The row still names the owner at the token it claimed
// under; only the deadline has gone by. That is the window a stepped wall
// clock, a suspended machine or a starved renewal loop opens.
func anExpiredButOwnedLease(t *testing.T, terminal bool) (*store.Store, string, store.LeaseRef) {
	t.Helper()

	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{
		Owner: testOwner, TTL: store.DefaultRunLeaseTTL,
	}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	holder := ref(testOwner, 1)
	if err := s.StartStep(ctx, runID, "build", holder); err != nil {
		t.Fatalf("StartStep: %v", err)
	}
	if terminal {
		if err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
			ExitCode: ptr(0), FinishedAt: clk.Now(),
		}, holder); err != nil {
			t.Fatalf("succeed build: %v", err)
		}
	}
	// Every path gets the same row, including the pending request, so the
	// only difference between them is which method is called.
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)

	run := mustGetRun(t, ctx, s, runID)
	if run.LeaseOwner != testOwner || run.LeaseEpoch != 1 {
		t.Fatalf("the row reads %q at epoch %d, want the claim untouched", run.LeaseOwner, run.LeaseEpoch)
	}
	if run.LeaseExpiresAt.IsZero() || run.LeaseExpiresAt.After(clk.Now()) {
		t.Fatalf("lease_expires_at is %s, want it behind %s", run.LeaseExpiresAt, clk.Now())
	}
	return s, runID, holder
}

// TestEveryHolderWritePathAdmitsTheOwnerOfAnExpiredLease is the four answers
// table. The lease still names this writer at this token, so the fence holds
// and every path lets the write land. The deadline says when the reaper may
// take the run, not who owns it.
func TestEveryHolderWritePathAdmitsTheOwnerOfAnExpiredLease(t *testing.T) {
	for _, p := range holderWritePaths() {
		s, runID, holder := anExpiredButOwnedLease(t, p.terminal)
		admitted, err := p.write(t, s, runID, holder)
		if !admitted {
			t.Errorf("%s refused the owner of an expired lease: %v", p.name, err)
		}
	}
}

// TestEveryHolderWritePathRefusesAfterATakeover is the guarantee the fix must
// not weaken: once the epoch has moved under the holder, every path shuts the
// old writer out. Owner and epoch are the whole fence, so they had better hold
// the whole line.
func TestEveryHolderWritePathRefusesAfterATakeover(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRetryableQueuedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{
		Owner: testOwner, TTL: store.DefaultRunLeaseTTL,
	}); err != nil {
		t.Fatalf("the first claim: %v", err)
	}
	stale := ref(testOwner, 1)
	if err := s.StartStep(ctx, runID, "build", stale); err != nil {
		t.Fatalf("StartStep: %v", err)
	}
	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)
	if _, err := s.ReapExpiredRuns(ctx, store.ReapOptions{}); err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	clk.Advance(store.DefaultRequeueBackoff + time.Second)
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{
		Owner: "exec-2", TTL: store.DefaultRunLeaseTTL,
	}); err != nil {
		t.Fatalf("the takeover claim: %v", err)
	}
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	for _, p := range holderWritePaths() {
		admitted, err := p.write(t, s, runID, stale)
		if admitted {
			t.Errorf("%s admitted a writer whose lease was taken over", p.name)
			continue
		}
		if err != nil && !errors.Is(err, store.ErrLeaseLost) {
			t.Errorf("%s refused the taken-over writer with %v, want %v",
				p.name, err, store.ErrLeaseLost)
		}
	}
}

// TestFinishRunClosesARunWhoseLeaseExpiredWithoutATakeover is the whole point
// of #202 in one run: the work is done, the row still belongs to the writer,
// and the verdict lands. A refusal here leaves a running run with no live
// lease for the reaper to charge a crash for and hand to a second executor.
func TestFinishRunClosesARunWhoseLeaseExpiredWithoutATakeover(t *testing.T) {
	ctx := context.Background()
	s, runID, holder := anExpiredButOwnedLease(t, true)

	state, err := s.FinishRun(ctx, runID, holder, store.FinishReason{Code: reason.RUNSucceeded})
	if err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	if state != string(model.RunSucceeded) {
		t.Errorf("FinishRun = %s, want succeeded", state)
	}

	run := mustGetRun(t, ctx, s, runID)
	if run.State != string(model.RunSucceeded) {
		t.Errorf("the row reads %s, want succeeded", run.State)
	}
	if run.CrashCount != 0 {
		t.Errorf("crash_count = %d, want 0: nothing crashed", run.CrashCount)
	}
	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("Fsck: %v", err)
	}
	for _, v := range violations {
		t.Errorf("fsck reports %s on %s after the finish: %s", v.Check, v.Subject, v.Detail)
	}
}

// TestDrainRunHandsNothingBackToAnotherOwner holds the drain to the owner and
// the epoch at any deadline. The run belongs to exec-2; a drain from exec-1
// writes nothing, whether exec-2's lease is live or long past due.
func TestDrainRunHandsNothingBackToAnotherOwner(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{
		Owner: "exec-2", TTL: store.DefaultRunLeaseTTL,
	}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	stranger := ref(testOwner, 1)

	for _, deadline := range []string{"live", "expired"} {
		handed, err := s.DrainRun(ctx, runID, stranger, reason.RUNInterruptedShutdown)
		if err != nil && !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("DrainRun at a %s deadline: %v", deadline, err)
		}
		if handed {
			t.Errorf("DrainRun at a %s deadline handed back a run owned by exec-2", deadline)
		}
		run := mustGetRun(t, ctx, s, runID)
		if run.State != string(model.RunRunning) || run.LeaseOwner != "exec-2" || run.LeaseEpoch != 1 {
			t.Fatalf("the drain moved the row: %s owned by %q at epoch %d",
				run.State, run.LeaseOwner, run.LeaseEpoch)
		}
		clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)
	}
}
