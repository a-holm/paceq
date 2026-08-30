package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
)

// seedSucceededOneStepRun drives one seeded run's rows to a consistent
// succeeded state: run and step agree, so the sweep must stay silent.
func seedSucceededOneStepRun(t *testing.T, s *Store) string {
	t.Helper()

	runID := seedKeyedRun(t, s, "aggregatejob", "aggregatejob/default:2026-06-01T09:00:00Z")
	completeRun(t, s, runID)
	return runID
}

// TestRunAggregateMismatches is the both-directions proof of the crash
// battery's explicit consistency check: a healthy database reports nothing,
// and the planted disagreement - one step failed under a run that still says
// succeeded, exactly what an unfinished verdict leaves behind - is named by
// its run id.
func TestRunAggregateMismatches(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	t.Run("healthy_database_reports_nothing", func(t *testing.T) {
		seedSucceededOneStepRun(t, s)
		mismatches, err := s.RunAggregateMismatches(ctx)
		if err != nil {
			t.Fatalf("sweep a healthy database: %v", err)
		}
		if len(mismatches) != 0 {
			t.Fatalf("a healthy database reported %v, want none", mismatches)
		}
	})

	t.Run("planted_disagreement_is_named", func(t *testing.T) {
		// The injector plants into the first succeeded run it finds;
		// its subject names which run it chose.
		subject, err := s.InjectFailedStepUnderSucceededRun(ctx)
		if err != nil {
			t.Fatalf("plant: %v", err)
		}
		runID := strings.TrimPrefix(subject, "run ")
		runID = strings.Fields(runID)[0]
		mismatches, err := s.RunAggregateMismatches(ctx)
		if err != nil {
			t.Fatalf("sweep after planting: %v", err)
		}
		var found *AggregateMismatch
		for i := range mismatches {
			if mismatches[i].RunID == runID {
				found = &mismatches[i]
				break
			}
		}
		if found == nil {
			t.Fatalf("the sweep reported %v, want the planted mismatch on %q",
				mismatches, runID)
		}
		if found.State != "succeeded" {
			t.Errorf("the stored state reads %q, want succeeded", found.State)
		}
		if found.Aggregate != "failed" {
			t.Errorf("the steps aggregate to %q, want failed", found.Aggregate)
		}

		// The same fact must reach fsck as I10: one sweep, one truth,
		// two users.
		violations, err := s.Fsck(ctx)
		if err != nil {
			t.Fatalf("fsck after planting: %v", err)
		}
		named := false
		for _, v := range violations {
			if v.Check == "I10" && strings.Contains(v.Subject, runID) {
				named = true
				break
			}
		}
		if !named {
			t.Errorf("fsck reported %d violations, none of them I10 on %s",
				len(violations), runID)
		}
	})
}

// runLevelCodesOutsideAFailedRun sweeps every run row through the production
// predicate and returns the rows that break the implication I10 now rests on:
// a run-level failure code implies the row is failed. It calls runLevelFailure
// rather than repeating its list, so a third code added to the pair is covered
// on the day it lands.
func runLevelCodesOutsideAFailedRun(t *testing.T, s *Store) []string {
	t.Helper()

	rows, err := s.r.QueryContext(context.Background(),
		`SELECT id, state, COALESCE(reason_code, '') FROM runs`)
	if err != nil {
		t.Fatalf("sweep the run rows: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var bad []string
	for rows.Next() {
		var id, state, code string
		if err := rows.Scan(&id, &state, &code); err != nil {
			t.Fatalf("scan a run row: %v", err)
		}
		if runLevelFailure(code) && state != string(model.RunFailed) {
			bad = append(bad, fmt.Sprintf("run %s is %s carrying %s", id, state, code))
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("sweep the run rows: %v", err)
	}
	return bad
}

// TestRunLevelReasonCodesOnlyLiveOnFailedRuns guards the exclusivity that makes
// runLevelFailure a safe input to the fold. The flag outranks every step, so a
// run-level code on a row that is not failed makes fsck read that row as failed
// forever: fsck --repair has no I10 arm and the reconciler skips terminal rows.
//
// repairRequeueRunTx is the writer one UPDATE away from breaking it. It stamps
// RUN_ORPHANED_RECONCILED on its run_event while writing state = 'queued', and
// happens not to touch runs.reason_code. Adding the column there for symmetry
// with the reaper would turn every run fsck --repair requeues into a permanent
// false I10.
func TestRunLevelReasonCodesOnlyLiveOnFailedRuns(t *testing.T) {
	ctx := context.Background()
	s, runID := repairFixture(t)

	// A running run whose claim died: the I1 shape the requeue repair answers.
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET lease_expires_at = ? WHERE id = ?`,
		time.Now().Add(-time.Hour).UnixMilli(), runID); err != nil {
		t.Fatalf("expire the lease: %v", err)
	}
	if !hasCheck(mustFsck(t, s), "I1") {
		t.Fatal("the planted orphan did not trip I1")
	}
	if _, err := s.FsckRepair(ctx, nil, true); err != nil {
		t.Fatalf("repair: %v", err)
	}

	var state, code string
	if err := s.r.QueryRowContext(ctx,
		`SELECT state, COALESCE(reason_code, '') FROM runs WHERE id = ?`, runID).
		Scan(&state, &code); err != nil {
		t.Fatalf("read the repaired run back: %v", err)
	}
	if state != string(model.RunQueued) {
		t.Fatalf("the repaired run is %s, want queued", state)
	}
	if runLevelFailure(code) {
		t.Errorf("the repaired run carries %s while queued: the flag outranks its steps, "+
			"so the fold answers failed for a row that is waiting to run", code)
	}

	if bad := runLevelCodesOutsideAFailedRun(t, s); len(bad) > 0 {
		t.Errorf("run-level failure codes off a failed row: %v", bad)
	}

	// The consequence the implication exists to prevent, read off the
	// production sweep rather than a copy of it.
	for _, v := range mustFsck(t, s) {
		if v.Check == "I10" {
			t.Errorf("fsck I10 on %s after the repair: %s", v.Subject, v.Detail)
		}
	}
}

// TestRunAggregateMismatchesLeavesTheQueueAtRestAlone is the store half of the
// agreement rule, in both directions on one run. A materialised run nobody has
// claimed reads queued over pending steps, which is the resting state of every
// row in the queue and of every row a requeue puts back; the sweep must stay
// silent on it. Drive the same run's steps to succeeded and nothing legitimately
// leaves the row queued any more, so the sweep must name it and fsck must say
// I10.
func TestRunAggregateMismatchesLeavesTheQueueAtRestAlone(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	mismatches, err := s.RunAggregateMismatches(ctx)
	if err != nil {
		t.Fatalf("sweep the queue at rest: %v", err)
	}
	if len(mismatches) != 0 {
		t.Fatalf("a queued run over pending steps reported %v; that is the whole "+
			"queue at rest, not a mismatch", mismatches)
	}
	for _, v := range mustFsck(t, s) {
		if v.Check == "I10" {
			t.Fatalf("fsck I10 on %s while it waits to start: %s", v.Subject, v.Detail)
		}
	}

	// The same row with its work done: queued no longer has an excuse.
	now := time.Now().UTC().UnixMilli()
	if _, err := s.w.ExecContext(ctx, `UPDATE steps SET
state = 'succeeded', reason_code = 'STEP_SUCCEEDED', started_at = ?, finished_at = ?
WHERE run_id = ?`, now, now, runID); err != nil {
		t.Fatalf("finish the step rows: %v", err)
	}

	mismatches, err = s.RunAggregateMismatches(ctx)
	if err != nil {
		t.Fatalf("sweep a queued run with no work left: %v", err)
	}
	var found *AggregateMismatch
	for i := range mismatches {
		if mismatches[i].RunID == runID {
			found = &mismatches[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("the sweep reported %v, want %s named: it is queued with every step "+
			"succeeded", mismatches, runID)
	}
	if found.State != string(model.RunQueued) || found.Aggregate != model.RunSucceeded {
		t.Errorf("the sweep reports state %q against aggregate %q, want queued against succeeded",
			found.State, found.Aggregate)
	}
	if !hasCheck(mustFsck(t, s), "I10") {
		t.Error("the sweep named the run and fsck did not report I10")
	}
}

// TestRunAggregateMismatchesRefusesAnUnknownStoredState pins what the sweep
// does with a state the model cannot read. Comparing it would silently report a
// mismatch and invite an operator to repair a row nothing here understands, so
// the sweep stops instead. The schema's CHECK refuses the plant at write time,
// which is why it is lifted for it: fsck exists for rows that arrived around or
// before such a constraint.
func TestRunAggregateMismatchesRefusesAnUnknownStoredState(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 1`); err != nil {
		t.Fatalf("defer the check constraints: %v", err)
	}
	_, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'deferred' WHERE id = ?`, runID)
	if _, err2 := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 0`); err2 != nil {
		t.Fatalf("restore the check constraints: %v", err2)
	}
	if err != nil {
		t.Fatalf("plant the unknown state: %v", err)
	}

	mismatches, err := s.RunAggregateMismatches(ctx)
	if !errors.Is(err, model.ErrUnknownState) {
		t.Fatalf("the sweep returned (%v, %v), want an unknown state error", mismatches, err)
	}
	if !strings.Contains(err.Error(), runID) {
		t.Errorf("the refusal reads %q and does not name the run", err)
	}
}
