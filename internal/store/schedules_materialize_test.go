package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// The tick transaction: one fire-time in, at most one tick + trigger +
// run_keys + run + event + progress out, all or nothing. The UNIQUE index on
// (source_kind, source_name, scheduled_for) IS the idempotency; nothing here
// checks first.

func tickTestStore(t *testing.T) (*Store, ScheduleRow) {
	t.Helper()
	s := migratedStore(t)
	seedScheduleJob(t, s)
	row, err := s.UpsertSchedule(context.Background(), ScheduleInput{
		JobName:    "nightly",
		Name:       "default",
		Kind:       "cron",
		Expr:       "*/5 * * * *",
		Timezone:   "UTC",
		NextTickAt: time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("seed the schedule: %v", err)
	}
	return s, row
}

func triggeredInput(sched ScheduleRow, at time.Time) TickInput {
	return TickInput{
		Schedule:       sched,
		ScheduledFor:   at,
		Outcome:        OutcomeTriggered,
		RunKey:         "nightly/default:" + at.UTC().Format(time.RFC3339),
		NextTickAt:     at.Add(5 * time.Minute),
		UpdateProgress: true,
		Actor:          "scheduler",
	}
}

func TestMaterializeTickTurnsOneFireTimeIntoTheWholeChain(t *testing.T) {
	ctx := context.Background()
	s, sched := tickTestStore(t)
	at := time.Date(2026, 3, 15, 12, 5, 0, 0, time.UTC)

	res, err := s.MaterializeTick(ctx, triggeredInput(sched, at))
	if err != nil {
		t.Fatalf("MaterializeTick: %v", err)
	}
	if !res.Claimed {
		t.Fatal("the first claim of a fresh fire-time was refused")
	}
	if res.Run.ID == "" || res.Run.State != "queued" || res.Run.Origin != "schedule" {
		t.Fatalf("the chain produced no queued scheduled run: %+v", res.Run)
	}
	if res.Run.ScheduledFor.UTC() != at {
		t.Errorf("run.scheduled_for is %v, want %v", res.Run.ScheduledFor, at)
	}
	var stepCount int
	if err := s.r.QueryRowContext(ctx,
		`SELECT count(*) FROM steps WHERE run_id = ?`, res.Run.ID).Scan(&stepCount); err != nil || stepCount != 1 {
		t.Fatalf("the run was not materialised from the frozen spec: %d build steps (%v)", stepCount, err)
	}

	// The whole story is readable back through the public surface.
	events, err := s.RunEvents(ctx, res.Run.ID)
	if err != nil || len(events) != 1 || events[0].ToState != "queued" || events[0].Actor != "scheduler" {
		t.Fatalf("run_events did not record exactly one queued transition by the scheduler: %+v (%v)", events, err)
	}

	var tickCount, trigCount, keyCount, runCount int
	if err := s.r.QueryRowContext(ctx,
		`SELECT (SELECT count(*) FROM ticks), (SELECT count(*) FROM triggers),
		        (SELECT count(*) FROM run_keys), (SELECT count(*) FROM runs)`).
		Scan(&tickCount, &trigCount, &keyCount, &runCount); err != nil {
		t.Fatalf("count the chain: %v", err)
	}
	if tickCount != 1 || trigCount != 1 || keyCount != 1 || runCount != 1 {
		t.Fatalf("chain counts tick=%d trigger=%d run_key=%d run=%d, want 1 everywhere",
			tickCount, trigCount, keyCount, runCount)
	}

	var lastTick, nextTick *int64
	if err := s.r.QueryRowContext(ctx,
		`SELECT last_tick_at, next_tick_at FROM schedules WHERE id = ?`, sched.ID).
		Scan(&lastTick, &nextTick); err != nil {
		t.Fatalf("read progress: %v", err)
	}
	if lastTick == nil || *lastTick != at.UnixMilli() {
		t.Errorf("last_tick_at is %v, want %d", lastTick, at.UnixMilli())
	}
	if nextTick == nil || *nextTick != at.Add(5*time.Minute).UnixMilli() {
		t.Errorf("next_tick_at is %v, want %d", nextTick, at.Add(5*time.Minute).UnixMilli())
	}
}

func TestMaterializeTickSecondClaimIsSilent(t *testing.T) {
	ctx := context.Background()
	s, sched := tickTestStore(t)
	at := time.Date(2026, 3, 15, 12, 5, 0, 0, time.UTC)

	first, err := s.MaterializeTick(ctx, triggeredInput(sched, at))
	if err != nil || !first.Claimed {
		t.Fatalf("first claim: claimed=%v err=%v", first.Claimed, err)
	}

	second, err := s.MaterializeTick(ctx, triggeredInput(sched, at))
	if err != nil {
		t.Fatalf("second claim errored: %v", err)
	}
	if second.Claimed {
		t.Fatal("the second claim of the same fire-time was taken, the gate failed")
	}
	if second.Run.ID != "" {
		t.Fatalf("a refused claim wrote a run anyway: %+v", second.Run)
	}

	var tickCount, trigCount, runCount int
	if err := s.r.QueryRowContext(ctx,
		`SELECT (SELECT count(*) FROM ticks), (SELECT count(*) FROM triggers),
		        (SELECT count(*) FROM runs)`).
		Scan(&tickCount, &trigCount, &runCount); err != nil {
		t.Fatalf("count again: %v", err)
	}
	if tickCount != 1 || trigCount != 1 || runCount != 1 {
		t.Fatalf("after the refused claim counts are tick=%d trigger=%d run=%d, want 1 everywhere",
			tickCount, trigCount, runCount)
	}
}

func TestMaterializeTickRecordsSkipsAndErrorsWithoutTriggers(t *testing.T) {
	ctx := context.Background()
	s, sched := tickTestStore(t)
	at := time.Date(2026, 3, 15, 12, 5, 0, 0, time.UTC)

	skipIn := triggeredInput(sched, at)
	skipIn.Outcome = OutcomeSkipped
	skipIn.ReasonCode = reason.TICKSkippedCatchupDisabled
	res, err := s.MaterializeTick(ctx, skipIn)
	if err != nil || !res.Claimed {
		t.Fatalf("skip claim: claimed=%v err=%v", res.Claimed, err)
	}

	errIn := triggeredInput(sched, at.Add(time.Minute))
	errIn.Outcome = OutcomeError
	errIn.ReasonCode = reason.TICKErrorConfig
	errIn.UpdateProgress = false // a config failure retries after the fix
	res, err = s.MaterializeTick(ctx, errIn)
	if err != nil || !res.Claimed {
		t.Fatalf("error claim: claimed=%v err=%v", res.Claimed, err)
	}

	var rows int
	if err := s.r.QueryRowContext(ctx,
		`SELECT count(*) FROM runs`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("skips and errors wrote %d runs, want 0 (%v)", rows, err)
	}
	var badReason int
	if err := s.r.QueryRowContext(ctx,
		`SELECT count(*) FROM ticks WHERE outcome IN ('skipped','error')
		   AND (reason_code IS NULL OR reason_code = 'UNKNOWN' OR reason_code = '')`).
		Scan(&badReason); err != nil || badReason != 0 {
		t.Fatalf("%d skip/error rows carry no usable reason code (%v)", badReason, err)
	}

	// The skip advanced the cursor to its own fire-time; the config error,
	// told not to, left it there.
	var lastTick *int64
	if err := s.r.QueryRowContext(ctx,
		`SELECT last_tick_at FROM schedules WHERE id = ?`, sched.ID).
		Scan(&lastTick); err != nil {
		t.Fatalf("read the cursor: %v", err)
	}
	if lastTick == nil || *lastTick != at.UnixMilli() {
		t.Errorf("the skipped evaluation did not advance the cursor to %d: %v", at.UnixMilli(), lastTick)
	}
}

func TestMaterializeTickPausedFlipWritesPausedSkipNeverARun(t *testing.T) {
	ctx := context.Background()
	s, sched := tickTestStore(t)
	at := time.Date(2026, 3, 15, 12, 5, 0, 0, time.UTC)

	// Pause behind the loop's back: between discovery and this call.
	flag := 1
	if _, err := s.w.ExecContext(ctx,
		`UPDATE schedules SET paused = ? WHERE id = ?`, flag, sched.ID); err != nil {
		t.Fatalf("pause the schedule: %v", err)
	}

	in := triggeredInput(sched, at)
	res, err := s.MaterializeTick(ctx, in)
	if err != nil {
		t.Fatalf("MaterializeTick under pause: %v", err)
	}
	if !res.Claimed {
		t.Fatal("the paused evaluation was dropped without a row")
	}
	if res.Run.ID != "" {
		t.Fatalf("a paused schedule produced a run: %+v", res.Run)
	}
	var outcome string
	var code *string
	if err := s.r.QueryRowContext(ctx,
		`SELECT outcome, reason_code FROM ticks WHERE source_name = ?`,
		sched.JobName+"/"+sched.Name).Scan(&outcome, &code); err != nil {
		t.Fatalf("read the tick back: %v", err)
	}
	if outcome != "skipped" || code == nil || *code != "TICK_SKIPPED_PAUSED" {
		t.Fatalf("paused flip wrote outcome=%s code=%v, want skipped/TICK_SKIPPED_PAUSED", outcome, code)
	}
}

func TestMaterializeTickRollbackLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	s, sched := tickTestStore(t)
	at := time.Date(2026, 3, 15, 12, 5, 0, 0, time.UTC)

	// Sabotage statement three of the chain. If the first statement's effect
	// survives the failure, the writes were not really one transaction.
	if _, err := s.w.ExecContext(ctx, `CREATE TRIGGER paceq_test_abort_on_trigger
AFTER INSERT ON triggers BEGIN SELECT RAISE(ABORT, 'injected refusal'); END`); err != nil {
		t.Fatalf("plant the sabotage trigger: %v", err)
	}

	if _, err := s.MaterializeTick(ctx, triggeredInput(sched, at)); err == nil {
		t.Fatal("MaterializeTick sailed past the injected refusal")
	}

	var tickCount, runCount int
	var lastTick, nextTick *int64
	if err := s.r.QueryRowContext(ctx, `SELECT count(*) FROM ticks`).Scan(&tickCount); err != nil || tickCount != 0 {
		t.Fatalf("the tick survived its own transaction's rollback: %d rows (%v)", tickCount, err)
	}
	if err := s.r.QueryRowContext(ctx, `SELECT count(*) FROM runs`).Scan(&runCount); err != nil || runCount != 0 {
		t.Fatalf("the run survived the rollback: %d rows (%v)", runCount, err)
	}
	if err := s.r.QueryRowContext(ctx,
		`SELECT last_tick_at, next_tick_at FROM schedules WHERE id = ?`, sched.ID).
		Scan(&lastTick, &nextTick); err != nil {
		t.Fatalf("read the untouched cursor: %v", err)
	}
	if lastTick != nil {
		t.Errorf("the cursor moved during a rolled back transaction: %v", *lastTick)
	}
}
