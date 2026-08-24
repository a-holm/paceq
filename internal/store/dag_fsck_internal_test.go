package store

import (
	"context"
	"testing"
)

// TestFsckI8DetectsARunningStepWithAnUnmetNeed plants the M4-03 DAG invariant:
// a step is running while a step it needs has not succeeded. The sweep must
// name it. It lives in the internal package because planting the row needs the
// store's writer.
func TestFsckI8DetectsARunningStepWithAnUnmetNeed(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	// The smallest chain: run with steps one and two, where two needs one.
	if _, err := s.w.ExecContext(ctx,
		`INSERT INTO jobs (name, created_at, updated_at) VALUES ('i8', 1, 1)`); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	if _, err := s.w.ExecContext(ctx,
		`INSERT INTO job_versions (id, job_name, version, spec_hash, spec_json, created_at)
		VALUES ('8JOB1', 'i8', 1, 'sha256:aa', '{"steps":[]}', 1)`); err != nil {
		t.Fatalf("seed the version: %v", err)
	}
	if _, err := s.w.ExecContext(ctx,
		`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at, created_at, updated_at)
		VALUES ('8RUN1', 'i8', '8JOB1', 'manual', 'queued', 1, 1, 1)`); err != nil {
		t.Fatalf("seed the run: %v", err)
	}
	if _, err := s.w.ExecContext(ctx,
		`INSERT INTO steps (run_id, name, idx, state) VALUES ('8RUN1', 'one', 0, 'pending')`); err != nil {
		t.Fatalf("seed step one: %v", err)
	}
	if _, err := s.w.ExecContext(ctx,
		`INSERT INTO steps (run_id, name, idx, state) VALUES ('8RUN1', 'two', 1, 'pending')`); err != nil {
		t.Fatalf("seed step two: %v", err)
	}
	if _, err := s.w.ExecContext(ctx,
		`INSERT INTO step_deps (run_id, step_name, depends_on) VALUES ('8RUN1', 'two', 'one')`); err != nil {
		t.Fatalf("freeze the edge: %v", err)
	}

	// Plant the violation: two is running while one has not succeeded.
	if _, err := s.w.ExecContext(ctx,
		"UPDATE steps SET state = 'running' WHERE run_id = '8RUN1' AND name = 'two'"); err != nil {
		t.Fatalf("plant the running step: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	found := false
	for _, v := range violations {
		if v.Check == "I8" && v.Subject == "run 8RUN1 step two" {
			found = true
		}
	}
	if !found {
		t.Errorf("fsck found no I8 for the running step with an unmet need: %+v", violations)
	}
}
