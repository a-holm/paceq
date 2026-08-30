package store

import (
	"context"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/spec"
)

// TestTheParallelBudgetColumnRefusesZero is why the materialisation clamps
// instead of writing whatever the frozen document holds: zero is not a
// smaller budget, it is a run no step can ever be claimed for, and the column
// refuses it outright. A path that wrote it would lose the whole
// materialisation transaction rather than start a slow run.
func TestTheParallelBudgetColumnRefusesZero(t *testing.T) {
	ctx := context.Background()
	s, runID, _ := claimableRun(t)

	var versionID string
	if err := s.w.QueryRowContext(ctx,
		`SELECT job_version_id FROM runs WHERE id = ?`, runID).Scan(&versionID); err != nil {
		t.Fatalf("read the run's version: %v", err)
	}
	_, err := s.w.ExecContext(ctx, `INSERT INTO runs
(id, job_name, job_version_id, origin, state, available_at, created_at, updated_at, max_parallel)
VALUES ('zero-budget', 'diamond', ?, 'manual', 'queued', 0, 1, 1, 0)`, versionID)
	if err == nil {
		t.Fatal("a run was born with a parallel budget of zero: the CHECK is not there")
	}
	if !strings.Contains(err.Error(), "CHECK") && !strings.Contains(err.Error(), "constraint") {
		t.Fatalf("insert with a zero budget failed with %v, want the CHECK constraint", err)
	}

	if got := runMaxParallel(&spec.Job{MaxParallel: 0}); got != spec.DefaultMaxParallel {
		t.Errorf("a spec carrying no budget materialises %d, want the default %d",
			got, spec.DefaultMaxParallel)
	}
}
