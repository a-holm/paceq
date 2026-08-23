package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// The load-bearing proof of #68: fifty goroutines admit runs for one job with
// a ceiling of one, under -race, and the invariant holds after every single
// operation. No interleaving may push two runs of the job into running, no
// attempt may come back SQLITE_BUSY, and the queue must not deadlock: with
// overlap queue every attempt either runs now or defers, never disappears.
//
// The work is bounded by attempt counts, not by a wall clock, so the test is
// deterministic in what it proves however loaded the machine is; the race
// detector is what makes the interleavings hostile.

func TestFiftyConcurrentAdmissionsNeverBreakTheLimit(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 1)
	sched := admitSchedule(t, s, "build", "queue")

	const (
		workers  = 50
		attempts = 10
	)
	base := admitFire(0)
	total := workers * attempts

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	violations := make(chan string, workers)

	// runningNow reads the invariant straight from a fresh read pool
	// connection: it is the same question fsck I12 asks, asked after every
	// operation from outside the deciding transaction.
	runningNow := func() (int, error) {
		var n int
		err := s.withRead(context.Background(), func(ctx context.Context, r reader) error {
			return r.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM runs WHERE job_name = 'build' AND state = 'running'`).Scan(&n)
		})
		return n, err
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < attempts; i++ {
				n := w*attempts + i
				fire := base.Add(time.Duration(n) * time.Millisecond)
				res, err := s.MaterializeTick(ctx, TickInput{
					Schedule:       sched,
					ScheduledFor:   fire,
					Outcome:        OutcomeTriggered,
					RunKey:         fmt.Sprintf("build/nightly:%d", n),
					NextTickAt:     fire.Add(time.Hour),
					UpdateProgress: true,
					Actor:          "scheduler",
				})
				if err != nil {
					errs <- fmt.Errorf("worker %d attempt %d: %w", w, i, err)
					return
				}
				if !res.Claimed {
					errs <- fmt.Errorf("worker %d attempt %d: the fire-time was not claimed", w, i)
					return
				}
				n2, err := runningNow()
				if err != nil {
					errs <- fmt.Errorf("worker %d attempt %d: read the invariant: %w", w, i, err)
					return
				}
				if n2 > 1 {
					violations <- fmt.Sprintf("worker %d attempt %d: %d running runs of build, ceiling is 1", w, i, n2)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	close(violations)

	for err := range errs {
		if strings.Contains(err.Error(), "SQLITE_BUSY") || strings.Contains(err.Error(), "database is locked") {
			t.Errorf("a concurrent attempt failed with contention: %v", err)
			continue
		}
		t.Errorf("a concurrent attempt failed: %v", err)
	}
	for v := range violations {
		t.Error(v)
	}

	// The queue never deadlocked and never dropped work: every attempt is a
	// run, exactly one of them took the slot, the rest wait deferred.
	runs, err := s.ListRuns(context.Background(), RunFilter{Limit: listLimitMax})
	if err != nil {
		t.Fatalf("list the runs: %v", err)
	}
	if len(runs) != total {
		t.Fatalf("%d runs exist, want one per attempt (%d)", len(runs), total)
	}
	// Exactly one attempt took the slot as a due-now run; every later
	// attempt deferred instead of dropping. By the time the storm ends the
	// deferred backoffs have long passed, so they read as queued-due
	// backlog, which is the healthy shape of a queue under load and not a
	// violation: the ceiling governs what RUNS, and nothing ran twice.
	var tookSlot int
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE job_name = 'build' AND state = 'queued' AND available_at <= created_at`,
	).Scan(&tookSlot); err != nil {
		t.Fatalf("count the slot takers: %v", err)
	}
	if tookSlot != 1 {
		t.Fatalf("%d runs were admitted due now, want exactly one", tookSlot)
	}
	var waiting int
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE job_name = 'build' AND state = 'queued' AND available_at > created_at`,
	).Scan(&waiting); err != nil {
		t.Fatalf("count the deferred: %v", err)
	}
	if waiting != total-1 {
		t.Fatalf("%d runs deferred, want %d (every attempt but the first)", waiting, total-1)
	}
}
