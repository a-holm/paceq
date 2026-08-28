package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// The disk-guard's admission gate (#44). The floor is the cheapest and
// strongest condition admission knows, so these tests hold it to the same
// standard as the concurrency gates: the refusal is a real row with a real
// code, decided inside the transaction that owns the tick, and lifting the
// hold admits again without a restart.

// diskHold is the gate the daemon installs, reduced to fixed numbers so the
// assertions can name the measurements it stores.
func diskHold(free, total, floor int64) RunHoldFunc {
	return func() *RunHold {
		return &RunHold{
			Code: reason.RUNRejectedDiskLow,
			Text: "the filesystem holding the state is under its free-space floor",
			Data: map[string]any{
				"free_bytes":     free,
				"total_bytes":    total,
				"min_free_bytes": floor,
			},
		}
	}
}

func TestTheDiskHoldStandsTheTickDownWithItsCode(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 1)
	sched := admitSchedule(t, s, "build", "")
	s.SetRunHold(diskHold(500<<20, 50<<30, 1<<30))

	res := admitTick(t, s, sched, 0)
	if res.Run.ID != "" {
		t.Fatalf("a held disk materialised run %s", res.Run.ID)
	}

	row := readTickRow(t, s, "build/nightly", 0)
	if row.outcome != OutcomeSkipped {
		t.Fatalf("the hold recorded outcome %q, want skipped", row.outcome)
	}
	if row.reasonCode != "RUN_REJECTED_DISK_LOW" {
		t.Fatalf("the hold names %q, want RUN_REJECTED_DISK_LOW", row.reasonCode)
	}
	if free, _ := row.reasonData["free_bytes"].(float64); int64(free) != 500<<20 {
		t.Errorf("reason_data.free_bytes is %v, want the guard's measurement", row.reasonData["free_bytes"])
	}
	if total, _ := row.reasonData["total_bytes"].(float64); int64(total) != 50<<30 {
		t.Errorf("reason_data.total_bytes is %v, want the guard's measurement", row.reasonData["total_bytes"])
	}
	if floor, _ := row.reasonData["min_free_bytes"].(float64); int64(floor) != 1<<30 {
		t.Errorf("reason_data.min_free_bytes is %v, want the configured floor", row.reasonData["min_free_bytes"])
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM runs`); got != 0 {
		t.Errorf("the hold left %d run rows, want 0", got)
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM triggers`); got != 0 {
		t.Errorf("the hold wrote %d trigger rows, want 0: only a real trigger admits", got)
	}
	// The cursor still moved: the stand-down was decided, not left due to be
	// re-decided forever by every later pass.
	last, _, err := s.ScheduleCursor(context.Background(), "build", "nightly")
	if err != nil || last == nil {
		t.Fatalf("the cursor did not move past the stood-down fire-time: %v", err)
	}

	// Lifting the hold admits again, with no restart and no second code path.
	s.SetRunHold(nil)
	res2 := admitTick(t, s, sched, 1)
	if res2.Run.ID == "" {
		t.Fatalf("a healthy disk still refuses runs: %+v", res2)
	}
}

func TestTheDiskHoldRefusesAManualRunAndRecordsIt(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 1)
	s.SetRunHold(diskHold(500<<20, 50<<30, 1<<30))

	_, err := s.MaterializeManualTrigger(context.Background(), ManualTriggerInput{
		JobName: "build",
		Actor:   "operator",
	})
	if !errors.Is(err, ErrRunsHeld) {
		t.Fatalf("a manual run under the floor returned %v, want ErrRunsHeld", err)
	}
	var held *HeldError
	if !errors.As(err, &held) || held.Hold.Code != reason.RUNRejectedDiskLow {
		t.Fatalf("the refusal does not carry the hold's code: %v", err)
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM runs`); got != 0 {
		t.Errorf("the refused manual run left %d run rows, want 0", got)
	}
}

func TestMarkLogShardPrunedClearsOnlyTheRemovedShard(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	admitJob(t, s, "build", 1)

	// One run with three logged steps: two in the shard that will be
	// removed, one in a shard that stays, carrying the columns the
	// clearing must leave alone.
	var versionID string
	if err := s.r.QueryRowContext(ctx,
		`SELECT current_version_id FROM jobs WHERE name = ?`, "build").Scan(&versionID); err != nil {
		t.Fatalf("read the current version: %v", err)
	}
	now := time.Now().UnixMilli()
	if _, err := s.w.ExecContext(ctx,
		`INSERT INTO runs (id, job_name, job_version_id, origin, state, reason_code, available_at, created_at, updated_at)
VALUES ('01J0LOGRUN1', 'build', ?, 'schedule', 'failed', 'RUN_FAILED_STEP', ?, ?, ?)`,
		versionID, now, now, now); err != nil {
		t.Fatalf("seed the run: %v", err)
	}
	step := func(name, logPath string) {
		t.Helper()
		var tail sql.NullString
		if logPath != "" {
			tail = sql.NullString{String: "panic: the real error", Valid: true}
		}
		if _, err := s.w.ExecContext(ctx,
			`INSERT INTO steps (run_id, name, idx, state, reason_code, attempt, max_attempts, log_path, log_bytes, log_truncated, error_tail)
VALUES (?, ?, 0, 'failed', 'STEP_FAILED_NONZERO_EXIT', 1, 1, ?, 4096, 1, ?)`,
			"01J0LOGRUN1", name, nullIfEmpty(logPath), tail); err != nil {
			t.Fatalf("seed step %s: %v", name, err)
		}
	}
	step("gone-a", "2026-01-01/01J0LOGRUN1/gone-a.1.ndjson")
	step("gone-b", "2026-01-01/01J0LOGRUN1/gone-b.1.ndjson")
	step("kept", "2026-01-02/01J0LOGRUN1/kept.1.ndjson")
	step("never-logged", "")

	n, err := s.MarkLogShardPruned(ctx, "2026-01-01")
	if err != nil {
		t.Fatalf("mark the shard pruned: %v", err)
	}
	if n != 2 {
		t.Fatalf("the clearing touched %d rows, want the 2 under the removed shard", n)
	}

	var goneA, kept string
	var logBytes int
	var tail sql.NullString
	if err := s.r.QueryRowContext(ctx,
		`SELECT COALESCE(log_path,''), log_bytes, error_tail FROM steps WHERE name = 'gone-a'`).
		Scan(&goneA, &logBytes, &tail); err != nil {
		t.Fatalf("read the cleared step: %v", err)
	}
	if goneA != "" {
		t.Errorf("a step under the removed shard still names %q", goneA)
	}
	if logBytes != 4096 || !tail.Valid || tail.String != "panic: the real error" {
		t.Errorf("the clearing lost the surviving evidence: bytes=%d tail=%v", logBytes, tail)
	}
	if err := s.r.QueryRowContext(ctx,
		`SELECT COALESCE(log_path,'') FROM steps WHERE name = 'kept'`).Scan(&kept); err != nil {
		t.Fatalf("read the kept step: %v", err)
	}
	if kept != "2026-01-02/01J0LOGRUN1/kept.1.ndjson" {
		t.Errorf("the clearing crossed shards: %q lost its log path", kept)
	}
	if err := s.r.QueryRowContext(ctx,
		`SELECT COALESCE(log_path,'') FROM steps WHERE name = 'never-logged'`).Scan(&kept); err != nil {
		t.Fatalf("read the null step: %v", err)
	}
	if kept != "" {
		t.Errorf("a step that never logged now names %q", kept)
	}
}
