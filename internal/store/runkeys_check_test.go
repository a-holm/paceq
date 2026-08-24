package store

import (
	"context"
	"testing"
	"time"
)

// seedKeyedRun materialises one queued run for a one step job, carrying the
// given dedup key. Both runs of the double key case need a real version and
// real steps behind them, so the rows are the shape production writes.
func seedKeyedRun(t *testing.T, s *Store, job, runKey string) string {
	t.Helper()

	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName:  job,
		SpecHash: "sha256:" + runKey + "-" + job,
		SpecJSON: `{"schema":"paceq.job.v1","name":"` + job + `",` +
			`"steps":[{"name":"only","run":["/bin/true"]}]}`,
	})
	if err != nil {
		t.Fatalf("upsert the job version: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName:      job,
		JobVersionID: version.ID,
		Origin:       "schedule",
		RunKey:       runKey,
		Steps:        []NewStep{{Name: "only"}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}
	return run.ID
}

// completeRun drives one seeded run's rows straight to succeeded. The crash
// suite plants completed states by hand for the same reason: the checker
// reads final states, not the path that produced them.
func completeRun(t *testing.T, s *Store, runID string) {
	t.Helper()

	ctx := context.Background()
	now := time.Now().UTC().UnixMilli()
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET
state = 'succeeded', reason_code = 'RUN_SUCCEEDED',
started_at = ?, finished_at = ?
WHERE id = ?`, now, now, runID); err != nil {
		t.Fatalf("complete the run row: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE steps SET
state = 'succeeded', reason_code = 'STEP_SUCCEEDED',
started_at = ?, finished_at = ?
WHERE run_id = ?`, now, now, runID); err != nil {
		t.Fatalf("complete the step rows: %v", err)
	}
}

// TestDoubleCompletedRunKeys is the negative and positive proof of the
// checker in one file: a healthy database names nothing, and two completed
// runs behind one key are named by their key.
func TestDoubleCompletedRunKeys(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	healthy := seedKeyedRun(t, s, "healthyjob", "healthyjob/default:2026-06-01T09:00:00Z")
	completeRun(t, s, healthy)

	keys, err := s.DoubleCompletedRunKeys(ctx)
	if err != nil {
		t.Fatalf("sweep a healthy database: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("a healthy database reported %v, want no keys", keys)
	}

	const duplicated = "dupjob/default:2026-06-01T09:00:00Z"
	first := seedKeyedRun(t, s, "dupjob", duplicated)
	second := seedKeyedRun(t, s, "dupjob", duplicated)
	completeRun(t, s, first)
	completeRun(t, s, second)

	keys, err = s.DoubleCompletedRunKeys(ctx)
	if err != nil {
		t.Fatalf("sweep after planting: %v", err)
	}
	if len(keys) != 1 || keys[0] != duplicated {
		t.Fatalf("the sweep reported %v, want [%q]", keys, duplicated)
	}

	// A failed twin does not count: rerunning after a failure is an
	// operator decision the invariant must keep allowing.
	third := seedKeyedRun(t, s, "failedjob", "failedjob/default:2026-06-01T09:00:00Z")
	fourth := seedKeyedRun(t, s, "failedjob", "failedjob/default:2026-06-01T09:00:00Z")
	completeRun(t, s, third)
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET
state = 'failed', reason_code = 'RUN_FAILED'
WHERE id = ?`, fourth); err != nil {
		t.Fatalf("fail the fourth run: %v", err)
	}

	keys, err = s.DoubleCompletedRunKeys(ctx)
	if err != nil {
		t.Fatalf("sweep with a failed twin: %v", err)
	}
	if len(keys) != 1 || keys[0] != duplicated {
		t.Fatalf("the sweep reported %v, want only [%q]", keys, duplicated)
	}
}
