package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
)

// Coalescing: repeated identical skipped evaluations fold into the previous
// row's repeat_count instead of growing history without bound. Errors,
// triggers and outcome changes never coalesce.

// coalesceStore opens a store on a fake clock so every evaluation lands on
// its own distinct millisecond: "latest row" must never be decided by a tie.
func coalesceStore(t *testing.T) (*Store, ScheduleRow, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC))
	path := t.TempDir() + "/state.db"
	s, err := Open(context.Background(), path, Options{Clock: clk})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	seedScheduleJob(t, s)
	row, err := s.UpsertSchedule(context.Background(), ScheduleInput{
		JobName:    "nightly",
		Name:       "default",
		Kind:       "cron",
		Expr:       "*/1 * * * *",
		Timezone:   "UTC",
		NextTickAt: clk.Now(),
	})
	if err != nil {
		t.Fatalf("seed the schedule: %v", err)
	}
	return s, row, clk
}

func skipInput(sched ScheduleRow, at time.Time, code reason.Code) TickInput {
	in := triggeredInput(sched, at)
	in.Outcome = OutcomeSkipped
	in.ReasonCode = code
	return in
}

func TestIdenticalSkipsCoalesceIntoOneRow(t *testing.T) {
	s, sched, clk := coalesceStore(t)
	ctx := context.Background()

	base := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 2400; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		res, err := s.MaterializeTick(ctx, skipInput(sched, at, reason.TICKSkippedPaused))
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if !res.Claimed {
			t.Fatalf("iteration %d was refused outright", i)
		}
		if !res.Coalesced && i > 0 {
			t.Fatalf("iteration %d did not fold into the previous row", i)
		}
		clk.Advance(time.Minute)
	}

	var rows, repeat int
	var lastStarted *int64
	if err := s.r.QueryRowContext(ctx,
		`SELECT count(*), max(repeat_count), max(last_started_at) FROM ticks
		   WHERE source_kind = 'schedule' AND source_name = ?`,
		sched.JobName+"/"+sched.Name).Scan(&rows, &repeat, &lastStarted); err != nil {
		t.Fatalf("read the coalesced row: %v", err)
	}
	if rows != 1 {
		t.Fatalf("2400 identical skips produced %d rows, want 1", rows)
	}
	if repeat != 2400 {
		t.Fatalf("repeat_count is %d, want 2400", repeat)
	}
	if lastStarted == nil || *lastStarted == base.UnixMilli() {
		t.Errorf("last_started_at never moved: %v", lastStarted)
	}
}

func TestCoalescingNeverTouchesRunsOrDifferentReasons(t *testing.T) {
	s, sched, clk := coalesceStore(t)
	ctx := context.Background()
	base := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

	// A run happened at T0.
	if _, err := s.MaterializeTick(ctx, triggeredInput(sched, base)); err != nil {
		t.Fatalf("triggered evaluation: %v", err)
	}
	clk.Advance(time.Minute)
	// The next minute is skipped for one reason.
	if _, err := s.MaterializeTick(ctx, skipInput(sched, base.Add(time.Minute),
		reason.TICKSkippedCatchupDisabled)); err != nil {
		t.Fatalf("first skip: %v", err)
	}
	clk.Advance(time.Minute)
	// And then a run again at T2.
	if _, err := s.MaterializeTick(ctx, triggeredInput(sched, base.Add(2*time.Minute))); err != nil {
		t.Fatalf("second trigger: %v", err)
	}
	clk.Advance(time.Minute)
	// A paused flip at T3: the latest row is now a triggered one, so this
	// starts its own row instead of absorbing anywhere.
	if _, err := s.MaterializeTick(ctx, skipInput(sched, base.Add(3*time.Minute),
		reason.TICKSkippedPaused)); err != nil {
		t.Fatalf("paused skip: %v", err)
	}
	clk.Advance(time.Minute)
	// A second disabled skip at T4 must not absorb into the paused row nor
	// resurrect the older disabled row: it becomes its own row.
	if _, err := s.MaterializeTick(ctx, skipInput(sched, base.Add(4*time.Minute),
		reason.TICKSkippedCatchupDisabled)); err != nil {
		t.Fatalf("second disabled skip: %v", err)
	}
	clk.Advance(time.Minute)

	// A skipped row that somehow carries triggers (a foreign writer, a
	// future sensor bug) must never absorb anything: the childless
	// predicate is load bearing even when outcomes look identical.
	if _, err := s.w.ExecContext(ctx,
		`UPDATE ticks SET trigger_count = 3 WHERE source_name = ? AND outcome = 'skipped'`,
		sched.JobName+"/"+sched.Name); err != nil {
		t.Fatalf("plant the trigger count: %v", err)
	}
	if _, err := s.MaterializeTick(ctx, skipInput(sched, base.Add(45*time.Minute),
		reason.TICKSkippedCatchupDisabled)); err != nil {
		t.Fatalf("skip against a fertile latest row: %v", err)
	}

	// The fertile row refused to absorb: a fresh row exists beside it.
	var rows2 int
	if err := s.r.QueryRowContext(ctx, `SELECT count(*) FROM ticks WHERE source_name = ?`,
		sched.JobName+"/"+sched.Name).Scan(&rows2); err != nil || rows2 != 6 {
		t.Fatalf("%d tick rows after the fertile-row probe, want 6 (2 triggers, paused, 3 disabled) (%v)", rows2, err)
	}
	var freshRepeat int
	if err := s.r.QueryRowContext(ctx,
		`SELECT max(repeat_count) FROM ticks WHERE source_name=? AND outcome='skipped' AND reason_code='TICK_SKIPPED_CATCHUP_DISABLED'`,
		sched.JobName+"/"+sched.Name).Scan(&freshRepeat); err != nil || freshRepeat > 1 {
		t.Fatalf("a skip was absorbed into a row carrying triggers (repeat_count %d, %v)", freshRepeat, err)
	}
	var runs int
	if err := s.r.QueryRowContext(ctx, `SELECT count(*) FROM runs`).Scan(&runs); err != nil || runs != 2 {
		t.Fatalf("coalescing disturbed the runs table: %d runs (%v)", runs, err)
	}
}
