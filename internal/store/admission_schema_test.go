package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The schema half of admission control (#68): a schedule carries its overlap
// policy, a deferred queued row is legal exactly when it says why, and
// deferred never became a state of its own.

// TestSchedulesCarryAnOverlapPolicy proves the column end to end: the default
// is skip, queue is accepted, anything else is refused by the database, and
// the due query hands the policy back to the loop.
func TestSchedulesCarryAnOverlapPolicy(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	if _, err := s.w.ExecContext(ctx, `INSERT INTO jobs (name, created_at, updated_at)
VALUES ('nightly', 1000, 1000)`); err != nil {
		t.Fatalf("seed the job: %v", err)
	}

	row, err := s.UpsertSchedule(ctx, ScheduleInput{
		JobName:    "nightly",
		Name:       "default-policy",
		Kind:       "cron",
		Expr:       "* * * * *",
		NextTickAt: time.UnixMilli(2000),
	})
	if err != nil {
		t.Fatalf("upsert a schedule with no overlap given: %v", err)
	}
	if row.Overlap != "skip" {
		t.Errorf("a schedule that says nothing overlaps with %q, want %q", row.Overlap, "skip")
	}

	row, err = s.UpsertSchedule(ctx, ScheduleInput{
		JobName:    "nightly",
		Name:       "queued",
		Kind:       "cron",
		Expr:       "* * * * *",
		Overlap:    "queue",
		NextTickAt: time.UnixMilli(2000),
	})
	if err != nil {
		t.Fatalf("upsert a queue schedule: %v", err)
	}
	if row.Overlap != "queue" {
		t.Errorf("the queue schedule reads back overlap %q, want %q", row.Overlap, "queue")
	}

	due, err := s.DueSchedules(ctx, 9000, 10)
	if err != nil {
		t.Fatalf("list due schedules: %v", err)
	}
	seen := map[string]string{}
	for _, d := range due {
		seen[d.Name] = d.Overlap
	}
	if seen["default-policy"] != "skip" || seen["queued"] != "queue" {
		t.Errorf("the due query returned %v, want both policies carried", seen)
	}

	if _, err := s.w.ExecContext(ctx, `INSERT INTO schedules
(id, job_name, name, kind, expr, overlap, next_tick_at, created_at, updated_at)
VALUES ('01J0SCHOV', 'nightly', 'bad', 'cron', '* * * * *', 'replace', 2000, 1000, 1000)`); err == nil {
		t.Fatal("the database accepted overlap 'replace', want a CHECK refusal")
	} else if !strings.Contains(err.Error(), "overlap IN ('skip', 'queue')") {
		t.Fatalf("the refusal names the wrong rule: %v", err)
	}
}

// TestDeferCheckAcceptsADeferredQueuedRun is the accepting side of the double
// enforcement: available_at ahead of created_at is legal for a queued row if,
// and only if, defer_reason rides along. The refusing side lives in
// TestCoreSchemaRejectsInvalidRows.
func TestDeferCheckAcceptsADeferredQueuedRun(t *testing.T) {
	s := seededStore(t)
	ctx := context.Background()

	deferred := `INSERT INTO runs (id, job_name, job_version_id, origin, state,
			available_at, defer_reason, created_at, updated_at)
		VALUES ('01J0RUND1', 'nightly', '01J0VER1', 'schedule', 'queued',
			5000, 'concurrency', 2001, 2001)`
	if _, err := s.w.ExecContext(ctx, deferred); err != nil {
		t.Fatalf("the database refused a deferred row that says why: %v", err)
	}

	dueNow := `INSERT INTO runs (id, job_name, job_version_id, origin, state,
			available_at, created_at, updated_at)
		VALUES ('01J0RUNN1', 'nightly', '01J0VER1', 'schedule', 'queued',
			2001, 2001, 2001)`
	if _, err := s.w.ExecContext(ctx, dueNow); err != nil {
		t.Fatalf("the database refused a run that is due as it was born: %v", err)
	}
}

// TestRunsStateEnumNeverGrowsADeferredState pins the design decision that
// "utsatt" is computed, not stored: the state CHECK lists exactly the five
// states, and the deferral facts live in available_at and defer_reason.
func TestRunsStateEnumNeverGrowsADeferredState(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()

	var sql string
	if err := s.r.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'runs'`).Scan(&sql); err != nil {
		t.Fatalf("read the runs table definition: %v", err)
	}
	if strings.Contains(sql, "'deferred'") {
		t.Error("the runs table mentions 'deferred'; deferred is not a state, it is queued with available_at ahead and a defer_reason")
	}
	want := "CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled'))"
	if !strings.Contains(sql, want) {
		t.Errorf("the state CHECK drifted away from %s", want)
	}
	wantDefer := "defer_reason IS NOT NULL OR available_at <= created_at OR state <> 'queued'"
	if !strings.Contains(sql, wantDefer) {
		t.Errorf("the defer CHECK drifted away from %s", wantDefer)
	}
}
