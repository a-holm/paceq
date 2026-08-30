package store

import (
	"context"
	"testing"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// runLevelFailureCodes is every code the catalogue marks as a run-level
// failure. The cases below read it rather than naming the codes, so a code
// added to the catalogue is driven through the store's writers on the day it
// lands instead of on the day somebody remembers this file.
func runLevelFailureCodes(t *testing.T) []reason.Code {
	t.Helper()

	var out []reason.Code
	for _, e := range reason.All() {
		if e.RunLevelFailure {
			out = append(out, e.Code)
		}
	}
	if len(out) == 0 {
		t.Fatal("the catalogue marks no run-level failure: every case below would sweep an empty list and prove nothing")
	}
	return out
}

// plantRunLevelFailureDrift leaves one run in the shape this issue is about: a
// run that failed for something no step can express, whose steps therefore all
// read skipped, and whose own state has drifted back off failed. The step fold
// alone reads such a run as a success, so a writer that folds without the
// second input repairs a quarantined run into a reported success.
func plantRunLevelFailureDrift(t *testing.T, s *Store, runID string, code reason.Code) {
	t.Helper()

	ctx := context.Background()
	if _, err := s.w.ExecContext(ctx, `UPDATE steps SET state = 'skipped',
		reason_code = ?, finished_at = 1000 WHERE run_id = ?`,
		string(reason.STEPSkippedUpstreamFailed), runID); err != nil {
		t.Fatalf("skip the steps: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'queued',
		reason_code = ?, reason_data = '{}' WHERE id = ?`, string(code), runID); err != nil {
		t.Fatalf("plant the drifted run: %v", err)
	}
}

// TestReconcileRunStatesDoesNotRepairARunLevelFailureToSuccess is the writer
// half of I10. ReconcileRunStates is not a reporter: it rewrites the run row to
// whatever the fold answers, and it is the last pass over a run nothing else
// will revisit, because every later pass skips a terminal row.
//
// So a fold that reads only the steps hands it "succeeded" for a quarantined
// run and it writes that, and the operator is told the run worked. The run's
// own reason code is the fact that outranks the steps, and the row keeps it:
// re-explaining the ending as RUN_FAILED_STEP would name a step that never
// failed, and there is none to name.
func TestReconcileRunStatesDoesNotRepairARunLevelFailureToSuccess(t *testing.T) {
	ctx := context.Background()

	for _, code := range runLevelFailureCodes(t) {
		t.Run(string(code), func(t *testing.T) {
			s, runID := plantSeededRun(t)
			plantRunLevelFailureDrift(t, s, runID, code)

			if err := s.ReconcileRunStates(ctx); err != nil {
				t.Fatalf("reconcile: %v", err)
			}

			var state, have string
			if err := s.r.QueryRowContext(ctx,
				`SELECT state, COALESCE(reason_code, '') FROM runs WHERE id = ?`, runID).
				Scan(&state, &have); err != nil {
				t.Fatalf("read the reconciled run back: %v", err)
			}
			if state == string(model.RunSucceeded) {
				t.Fatalf("the reconciler repaired a run carrying %s to succeeded: "+
					"every step reads skipped because the run ended before any of them ran, "+
					"and nothing revisits a terminal row", code)
			}
			if state != string(model.RunFailed) {
				t.Errorf("the reconciled run is %s, want failed: %s is the run's own ending", state, code)
			}
			if have != string(code) {
				t.Errorf("the reconciled run carries %s, want %s: the row already said how it ended", have, code)
			}

			// I10 asks the same fold the reconciler just wrote through, so
			// the two agreeing is the point. Only I10 is read here: the
			// planted row is drift, and the other invariants are entitled
			// to their own opinion of it.
			for _, v := range mustFsck(t, s) {
				if v.Check == "I10" {
					t.Errorf("fsck I10 on %s after the reconciler closed it: %s", v.Subject, v.Detail)
				}
			}
		})
	}
}

// TestReconcileRunStatesStillClosesAnOrdinaryRunToItsSteps is the other
// direction. Without a run-level code the steps are the whole answer, and a
// reconciler that read a failure into every row would close the crash backstop
// it exists to be.
func TestReconcileRunStatesStillClosesAnOrdinaryRunToItsSteps(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	if _, err := s.w.ExecContext(ctx, `UPDATE steps SET state = 'succeeded',
		reason_code = ?, finished_at = 1000 WHERE run_id = ?`,
		string(reason.STEPSucceeded), runID); err != nil {
		t.Fatalf("finish the steps: %v", err)
	}

	if err := s.ReconcileRunStates(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var state, have string
	if err := s.r.QueryRowContext(ctx,
		`SELECT state, COALESCE(reason_code, '') FROM runs WHERE id = ?`, runID).
		Scan(&state, &have); err != nil {
		t.Fatalf("read the reconciled run back: %v", err)
	}
	if state != string(model.RunSucceeded) || have != string(reason.RUNSucceeded) {
		t.Errorf("the reconciled run is (%s, %s), want (succeeded, %s)", state, have, reason.RUNSucceeded)
	}
}

// TestCancelGuardsRankARunLevelFailureAboveItsSteps covers the other call site
// that used to hand the fold a literal false. cancelGuards is what three of the
// reaper's arms and the queued cancellation rank their steps with, so a
// run-level code reaching it has to come out failed under the code it arrived
// with, not as a success over skipped steps and not as a step failure with no
// step to name.
func TestCancelGuardsRankARunLevelFailureAboveItsSteps(t *testing.T) {
	allSkipped := []model.StepState{model.StepSkipped, model.StepSkipped}

	for _, code := range runLevelFailureCodes(t) {
		g := cancelGuards(allSkipped, code)
		if !g.RunLevelFailure {
			t.Errorf("cancelGuards under %s left RunLevelFailure false: the machine then ranks the steps alone", code)
		}
		verdict, kind := model.TerminalVerdict(g)
		if verdict != model.RunFailed {
			t.Errorf("cancelGuards under %s rank to %s, want failed", code, verdict)
		}
		if kind != "run.failed" {
			t.Errorf("the run-level arm emits %q, want run.failed: the reaper's orphan arm already writes that name", kind)
		}
		if g.ReasonCode != string(code) {
			t.Errorf("cancelGuards under %s carry reason %s, want the code it was handed", code, g.ReasonCode)
		}
	}

	// The ordinary path is untouched: skipped steps are a decision, not a
	// failure, and a cancel that lands over them still reads success.
	g := cancelGuards(allSkipped, reason.RUNCancelledManual)
	if g.RunLevelFailure {
		t.Error("cancelGuards read RUN_CANCELLED_MANUAL as a run-level failure")
	}
	if verdict, _ := model.TerminalVerdict(g); verdict != model.RunSucceeded {
		t.Errorf("a cancel over skipped steps ranks to %s, want succeeded", verdict)
	}
	if g.ReasonCode != string(reason.RUNSucceeded) {
		t.Errorf("a cancel over skipped steps carries %s, want %s", g.ReasonCode, reason.RUNSucceeded)
	}
}

// TestFsckNamesARunLevelFailureStoredAsASuccess is the checker's other
// direction. A quarantined run whose row reads succeeded is the outcome this
// issue exists to prevent, and it is drift whichever writer produced it, so
// I10 has to name it rather than excuse every row carrying a run-level code.
func TestFsckNamesARunLevelFailureStoredAsASuccess(t *testing.T) {
	ctx := context.Background()

	for _, code := range runLevelFailureCodes(t) {
		t.Run(string(code), func(t *testing.T) {
			s, runID := plantSeededRun(t)
			plantRunLevelFailureDrift(t, s, runID, code)
			if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'succeeded',
				finished_at = 2000 WHERE id = ?`, runID); err != nil {
				t.Fatalf("report the run as a success: %v", err)
			}

			mismatches, err := s.RunAggregateMismatches(ctx)
			if err != nil {
				t.Fatalf("sweep: %v", err)
			}
			var found *AggregateMismatch
			for i := range mismatches {
				if mismatches[i].RunID == runID {
					found = &mismatches[i]
					break
				}
			}
			if found == nil {
				t.Fatalf("the sweep reported %v, want %s named: it reads succeeded carrying %s",
					mismatches, runID, code)
			}
			if found.Aggregate != model.RunFailed {
				t.Errorf("the steps and %s aggregate to %q, want failed", code, found.Aggregate)
			}
			if !hasCheck(mustFsck(t, s), "I10") {
				t.Error("the sweep named the run and fsck did not report I10")
			}
		})
	}
}
