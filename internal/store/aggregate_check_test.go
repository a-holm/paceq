package store

import (
	"context"
	"strings"
	"testing"
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
