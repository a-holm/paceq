package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The batch heartbeat is one transaction per tick no matter how many runs the
// owner holds, which is what keeps the write budget flat as the workload
// grows. Counting real commits is the only honest way to say that, so this
// test uses the package private commit counter.

func TestRenewRunLeasesCommitsOneTransactionForAllHeldRuns(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC))
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(ctx, path, Options{Clock: clk})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	version, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:nightly",
		SpecJSON: `{"max_concurrent":1,"name":"nightly","schema":"paceq.job.v1",` +
			`"steps":[{"name":"build","run":["/bin/true"],"shell":false}],"timeout_ms":3600000}`,
	})
	if err != nil {
		t.Fatalf("record the job: %v", err)
	}

	const runs = 100
	for i := 0; i < runs; i++ {
		out, err := s.MaterializeManualTrigger(ctx, ManualTriggerInput{JobName: "nightly"})
		if err != nil {
			t.Fatalf("materialise run %d: %v", i, err)
		}
		if _, _, err := s.ClaimRun(ctx, out.Run.ID,
			LeaseInput{Owner: "exec-1", TTL: 30 * time.Second}); err != nil {
			t.Fatalf("claim run %d: %v", i, err)
		}
	}
	_ = version

	var commits int
	s.onCommit = func() { commits++ }
	defer func() { s.onCommit = nil }()

	renewed, err := s.RenewRunLeases(ctx, "exec-1", 30*time.Second)
	if err != nil {
		t.Fatalf("RenewRunLeases: %v", err)
	}
	if len(renewed) != runs {
		t.Fatalf("the renewal answered for %d runs, want %d", len(renewed), runs)
	}
	if commits != 1 {
		t.Fatalf("%d runs renewed in %d transactions, want exactly 1", runs, commits)
	}

	clk.Advance(time.Second)
	if _, err := s.RenewRunLeases(ctx, "exec-1", 30*time.Second); err != nil {
		t.Fatalf("second RenewRunLeases: %v", err)
	}
	if commits != 2 {
		t.Fatalf("two ticks cost %d transactions, want exactly 2", commits)
	}
}
