package store

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// internalLeaseStore is a migrated store on a fake clock, for the in-package
// proofs that need the writer pool or a stated time.
func internalLeaseStore(t *testing.T) (*Store, *clock.Fake) {
	t.Helper()

	clk := clock.NewFake(time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC))
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(context.Background(), path, Options{Clock: clk})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s, clk
}

// The exhausted attempt budget is a property of the run as it was created: a
// retry origin names which attempt of the logical job this run is. The
// external API has no way to write that column, so this half of the reaper's
// decision is proved from inside the package.

// The claim and the reap sweep must both read their partial indexes, or every
// scheduler decision and every reaper tick scans the runs table instead. The
// plans are asked against the same SQL constants production uses.

func TestClaimQueryPlansThroughTheClaimIndex(t *testing.T) {
	s, _ := internalLeaseStore(t)

	// The read half: the candidate walk must ride the partial claim index.
	plan := queryPlan(t, s, fmt.Sprintf(claimCandidatesSQL, ""), int64(1000))
	if !strings.Contains(plan, "idx_runs_claim") {
		t.Fatalf("the claim candidates do not read idx_runs_claim:\n%s", plan)
	}
	if strings.Contains(plan, "SCAN runs") {
		t.Fatalf("the claim candidates scan the runs table:\n%s", plan)
	}

	// The write half: flipping the chosen rows looks them up by primary
	// key, one placeholder per chosen id.
	updatePlan := queryPlan(t, s, claimRunsSQL, "exec-1", int64(1000), int64(61_000), "01J0RUN1")
	if !strings.Contains(updatePlan, "sqlite_autoindex_runs_1") {
		t.Fatalf("the claim update does not look its rows up by id:\n%s", updatePlan)
	}
	if strings.Contains(updatePlan, "SCAN runs") {
		t.Fatalf("the claim update scans the runs table:\n%s", updatePlan)
	}
}

func TestReaperQueryPlansThroughTheReaperIndex(t *testing.T) {
	s, _ := internalLeaseStore(t)

	plan := queryPlan(t, s, reapCandidatesSQL, int64(500), 10)
	if !strings.Contains(plan, "idx_runs_reaper") {
		t.Fatalf("the reap sweep does not read idx_runs_reaper:\n%s", plan)
	}
	if strings.Contains(plan, "SCAN runs") {
		t.Fatalf("the reap sweep scans the runs table:\n%s", plan)
	}
}

func TestReaperFailsARunWhoseAttemptBudgetIsSpent(t *testing.T) {
	ctx := context.Background()
	s, clk := internalLeaseStore(t)
	runID := aRetryableQueuedRunInternal(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, LeaseInput{Owner: "doomed"}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	// The run came back for its second and final attempt; the executor died
	// holding it again.
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET attempt = 2 WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the spent budget: %v", err)
	}
	clk.Advance(DefaultRunLeaseTTL + 2*DefaultClockSkewAllowance)

	reaped, err := s.ReapExpiredRuns(ctx, ReapOptions{})
	if err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("reaped %+v, want the one run", reaped)
	}
	if reaped[0].State != string(model.RunFailed) {
		t.Fatalf("a run with no attempt budget left went to %s, want failed", reaped[0].State)
	}
	if reaped[0].ReasonCode != string(reason.RUNOrphanedReconciled) {
		t.Errorf("reason_code = %q, want %q", reaped[0].ReasonCode, reason.RUNOrphanedReconciled)
	}
	if reaped[0].LeaseEpoch != 2 {
		t.Errorf("lease_epoch = %d, want 2: the token rises even on the way to failed", reaped[0].LeaseEpoch)
	}

	detail, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.Run.FinishedAt.IsZero() {
		t.Error("a failed run without finished_at cannot be terminal")
	}
	for _, step := range detail.Steps {
		if step.State == "running" || step.State == "pending" {
			t.Errorf("the %s step survived under a terminal run (I2)", step.State)
		}
	}
}

func TestFsckNamesAnUnleasedRunningRow(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)
	runID := aRunningStepInternal(t, s)

	// A frozen moment in violation of I1: running with no lease at all.
	if _, err := s.w.ExecContext(ctx,
		`UPDATE runs SET lease_owner = NULL, lease_expires_at = NULL WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the violation: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	found := false
	for _, v := range violations {
		if v.Check == "I1" && v.Subject == "run "+runID {
			found = true
		}
	}
	if !found {
		t.Fatalf("no I1 violation named run %s: %+v", runID, violations)
	}
}

func TestFsckNamesAFallingEpoch(t *testing.T) {
	ctx := context.Background()
	s, clk := internalLeaseStore(t)
	runID := aRetryableQueuedRunInternal(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, LeaseInput{Owner: "exec-1"}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	clk.Advance(DefaultRunLeaseTTL + 2*DefaultClockSkewAllowance)
	if _, err := s.ReapExpiredRuns(ctx, ReapOptions{}); err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}

	// Rewind the token behind what the history recorded: exactly the drift
	// I11 exists to catch.
	if _, err := s.w.ExecContext(ctx,
		`UPDATE runs SET lease_epoch = 0 WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the violation: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	found := false
	for _, v := range violations {
		if v.Check == "I11" && v.Subject == "run "+runID {
			found = true
		}
	}
	if !found {
		t.Fatalf("no I11 violation named run %s: %+v", runID, violations)
	}
}

// aRetryableQueuedRunInternal seeds one manual run whose single step carries
// a retry budget.
func aRetryableQueuedRunInternal(t *testing.T, s *Store) string {
	t.Helper()

	ctx := context.Background()
	version := aJobInternal(t, s, "nightly")
	run, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "test",
		MaxAttempts:  2,
		Steps:        []NewStep{{Name: "build", MaxAttempts: 2}},
	})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	return run.ID
}
