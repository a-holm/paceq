package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The due discovery query is the loop's first read every wake. These tests
// hold its contract: only unpaused schedules whose next tick has arrived come
// back, oldest next tick first, at most max of them.

func scheduleSeed(name string, next time.Time) ScheduleInput {
	return ScheduleInput{
		JobName:    "nightly",
		Name:       name,
		Kind:       "cron",
		Expr:       "*/5 * * * *",
		Timezone:   "UTC",
		NextTickAt: next,
	}
}

// seedScheduleJob creates the job a schedule belongs to; the schema refuses a
// schedule without one. Its ceiling is deliberately generous (#68): these
// tests exercise discovery, cursors and coalescing, not admission control,
// and the default ceiling of one would stand later fire-times down before
// coalescing ever got involved.
func seedScheduleJob(t *testing.T, s *Store) {
	t.Helper()
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:       "nightly",
		SpecHash:      "sha256:seed",
		SpecJSON:      `{"schema":"paceq.job.v1","name":"nightly","max_concurrent":200,"steps":[{"name":"build","run":["true"]}]}`,
		MaxConcurrent: 200,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
}

func TestDueSchedulesReturnsOnlyDueUnpausedRows(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	seedScheduleJob(t, s)

	due1 := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	due2 := due1.Add(-2 * time.Minute)
	future := due1.Add(time.Hour)

	for _, in := range []ScheduleInput{
		scheduleSeed("due-late", due2),
		scheduleSeed("due-early", due1),
		scheduleSeed("asleep", future),
		func() ScheduleInput { in := scheduleSeed("resting", due1); in.Paused = true; return in }(),
	} {
		if _, err := s.UpsertSchedule(ctx, in); err != nil {
			t.Fatalf("seed %s: %v", in.Name, err)
		}
	}

	got, err := s.DueSchedules(ctx, due1.Add(time.Minute).UnixMilli(), 100)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	var names []string
	for _, row := range got {
		names = append(names, row.Name)
	}
	if len(names) != 2 || names[0] != "due-late" || names[1] != "due-early" {
		t.Fatalf("DueSchedules returned %v, want [due-late due-early] ordered by next_tick_at", names)
	}
	row := got[0]
	if row.JobName != "nightly" || row.Kind != "cron" || row.Expr != "*/5 * * * *" ||
		row.Timezone != "UTC" || row.SpringForward != "skip" || row.FallBack != "first" ||
		row.Catchup != "skip" || row.CatchupLimit != 10 || row.CatchupWindowMS != 86400000 ||
		row.Paused {
		t.Errorf("the due row lost its columns: %+v", row)
	}
	if row.LastTickAt != nil {
		t.Errorf("a fresh schedule carries no last_tick_at, got %v", row.LastTickAt)
	}
	if !row.NextTickAt.Equal(due2) {
		t.Errorf("next_tick_at is %v, want %v", row.NextTickAt, due2)
	}
}

func TestUpsertScheduleReplacesTheDefinitionAndKeepsTheRow(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	seedScheduleJob(t, s)

	first := scheduleSeed("default", time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC))
	created, err := s.UpsertSchedule(ctx, first)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID == "" {
		t.Fatal("UpsertSchedule minted no id")
	}

	again := first
	again.Catchup = "last"
	again.CatchupLimit = 3
	again.CatchupWindowMS = 60_000
	again.SpringForward = "shift"
	again.FallBack = "both"
	again.NextTickAt = first.NextTickAt.Add(time.Minute)

	updated, err := s.UpsertSchedule(ctx, again)
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if updated.ID != created.ID {
		t.Fatalf("re-apply minted a new id %s, the row should be replaced in place (%s)", updated.ID, created.ID)
	}
	if updated.Catchup != "last" || updated.CatchupLimit != 3 || updated.CatchupWindowMS != 60_000 ||
		updated.SpringForward != "shift" || updated.FallBack != "both" {
		t.Errorf("re-apply lost its columns: %+v", updated)
	}
}

func TestDueSchedulesHonoursTheLimitAndTheBoundary(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	seedScheduleJob(t, s)

	base := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		in := scheduleSeed("s"+string(rune('a'+i)), base.Add(time.Duration(i)*time.Minute))
		if _, err := s.UpsertSchedule(ctx, in); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// Exactly at the boundary the schedule is due; one millisecond before it is not.
	now := base.Add(2 * time.Minute)
	got, err := s.DueSchedules(ctx, now.Add(-time.Millisecond).UnixMilli(), 100)
	if err != nil {
		t.Fatalf("DueSchedules just before: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("just before the boundary %d rows are due, want 2", len(got))
	}
	got, err = s.DueSchedules(ctx, now.UnixMilli(), 100)
	if err != nil {
		t.Fatalf("DueSchedules at the boundary: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("at the boundary %d rows are due, want 3", len(got))
	}

	got, err = s.DueSchedules(ctx, now.Add(time.Hour).UnixMilli(), 2)
	if err != nil {
		t.Fatalf("DueSchedules limited: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("LIMIT was ignored: %d rows came back, want 2", len(got))
	}
}

// AC9: the discovery query must run through the partial index
// idx_schedules_due (next_tick_at) WHERE paused = 0, or a big schedules table
// turns every wake into a scan.
//
// The plan is taken over the same constant production executes, never a
// lookalike string written for this test.
func TestTheDueQueryPlanUsesThePartialIndex(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	rows, err := s.r.QueryContext(ctx, "EXPLAIN QUERY PLAN "+dueSchedulesSQL, 0, 10)
	if err != nil {
		t.Fatalf("explain the due query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var plan []string
	for rows.Next() {
		var id, parent int
		var notused, detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan a plan row: %v", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the plan: %v", err)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "idx_schedules_due") {
		t.Fatalf("the due query does not use idx_schedules_due:\n%s", joined)
	}
}
