package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// Shadow-mode store behaviour (#32). Package-internal because the assertions
// read raw row counts: the whole point is proving what was NOT written
// alongside what was.

func shadowTestStore(t *testing.T) *Store {
	t.Helper()
	s := openTestStore(t, Options{})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// seedShadowWorld plants job backup (max_concurrent 1 - the ceiling the
// overlap simulation leans on) with its five-minute schedule in row-level
// shadow mode.
func seedShadowWorld(t *testing.T, ctx context.Context, s *Store) ScheduleRow {
	t.Helper()
	if _, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName:  "backup",
		SpecHash: "sha256:shadowworld",
		SpecJSON: `{"schema":"paceq.job.v1","name":"backup","max_concurrent":1,"steps":[{"name":"dump","run":["/usr/bin/backup.sh"]}],"schedules":[{"name":"five","cron":"*/5 * * * *"}]}`,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	row, err := s.UpsertSchedule(ctx, ScheduleInput{
		JobName:    "backup",
		Name:       "five",
		Kind:       "cron",
		Expr:       "*/5 * * * *",
		Timezone:   "UTC",
		Catchup:    "all",
		Shadow:     true,
		NextTickAt: time.Date(2026, 8, 20, 11, 50, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("seed the schedule: %v", err)
	}
	return row
}

func countShadowRows(t *testing.T, s *Store, table string) int {
	t.Helper()
	return countRows(t, s, table)
}

// seedTimedHistoryRun plants one succeeded run of job backup with a fixed
// duration, the history the shadow duration estimate reads. Plain SQL here:
// there is no production path that mints finished runs without executing.
func seedTimedHistoryRun(t *testing.T, s *Store, id string, started time.Time, dur time.Duration) {
	t.Helper()
	ctx := context.Background()
	var versionID string
	if err := s.r.QueryRow(`SELECT current_version_id FROM jobs WHERE name = ?`,
		"backup").Scan(&versionID); err != nil {
		t.Fatalf("read the current version: %v", err)
	}
	finished := started.Add(dur)
	if _, err := s.w.ExecContext(ctx, `INSERT INTO runs
(id, job_name, job_version_id, origin, run_key, state, available_at,
 reason_code, scheduled_for, params_json, attempt, max_attempts, created_at, updated_at,
 concurrency_key, started_at, finished_at, duration_ms)
VALUES (?, 'backup', ?, 'schedule', ?, 'succeeded', ?, 'OK', ?, '{}', 1, 1, ?, ?, NULL, ?, ?, ?)`,
		id, versionID, "hist:"+id, started.UnixMilli(), started.UnixMilli(),
		started.UnixMilli(), started.UnixMilli(), started.UnixMilli(),
		finished.UnixMilli(), dur.Milliseconds()); err != nil {
		t.Fatalf("seed a history run: %v", err)
	}
}

func countShadowTicks(t *testing.T, s *Store) int64 {
	t.Helper()
	var n int64
	if err := s.w.QueryRow(`SELECT COUNT(*) FROM ticks WHERE outcome = ?`,
		OutcomeShadowTriggered).Scan(&n); err != nil {
		t.Fatalf("count shadow ticks: %v", err)
	}
	return n
}

func TestShadowMaterialisesTheDecisionAndNothingElse(t *testing.T) {
	ctx := context.Background()
	s := shadowTestStore(t)
	row := seedShadowWorld(t, ctx, s)

	res, err := s.MaterializeTick(ctx, TickInput{
		Schedule:       row,
		ScheduledFor:   time.Date(2026, 8, 20, 11, 55, 0, 0, time.UTC),
		Outcome:        OutcomeTriggered,
		RunKey:         "backup/five:2026-08-20T11:55:00Z",
		NextTickAt:     time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC),
		UpdateProgress: true,
		Actor:          "scheduler",
		Shadow:         true,
	})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	if !res.Claimed || res.Run.ID != "" || res.Deferred {
		t.Fatalf("a claimed shadow fire returned %+v: no run shape may exist", res)
	}
	if got := countShadowTicks(t, s); got != 1 {
		t.Fatalf("ticks carry %d shadow_triggered rows, want 1", got)
	}
	var triggerCount int64
	if err := s.w.QueryRow(`SELECT trigger_count FROM ticks WHERE outcome = ?`,
		OutcomeShadowTriggered).Scan(&triggerCount); err != nil || triggerCount != 0 {
		t.Fatalf("a shadow marker carries trigger_count %d (err=%v)", triggerCount, err)
	}
	for _, table := range []string{"runs", "triggers", "run_keys"} {
		if n := countShadowRows(t, s, table); n != 0 {
			t.Fatalf("%s holds %d rows in shadow mode", table, n)
		}
	}
	last, _, err := s.ScheduleCursor(ctx, "backup", "five")
	if err != nil {
		t.Fatalf("read the cursor: %v", err)
	}
	want := time.Date(2026, 8, 20, 11, 55, 0, 0, time.UTC)
	if last == nil || last.Compare(want) != 0 {
		t.Fatalf("cursor did not advance through the shadow claim: last=%v want=%v", last, want)
	}
	var finished *int64
	if err := s.w.QueryRow(`SELECT finished_at FROM ticks WHERE outcome = ?`,
		OutcomeShadowTriggered).Scan(&finished); err != nil || finished == nil {
		t.Fatalf("a would-run closes at the decision itself: finished=%v err=%v", finished, err)
	}
}

func TestShadowOverlapSimulationUsesRunHistory(t *testing.T) {
	ctx := context.Background()
	s := shadowTestStore(t)
	row := seedShadowWorld(t, ctx, s)

	// History says this job takes about eleven minutes per attempt.
	base := time.Date(2026, 8, 19, 9, 0, 0, 0, time.UTC)
	for i := range 3 {
		started := base.Add(time.Duration(i) * time.Hour)
		seedTimedHistoryRun(t, s, fmt.Sprintf("01HIST%02d0", i), started, 11*time.Minute)
	}

	fireOne := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	fireTwo := fireOne.Add(5 * time.Minute)
	before := countShadowRows(t, s, "runs")
	for i, when := range []time.Time{fireOne, fireTwo} {
		if _, err := s.MaterializeTick(ctx, TickInput{
			Schedule:       row,
			ScheduledFor:   when,
			Outcome:        OutcomeTriggered,
			RunKey:         "fire" + string(rune('A'+i)),
			NextTickAt:     when.Add(5 * time.Minute),
			UpdateProgress: true,
			Shadow:         true,
		}); err != nil {
			t.Fatalf("fire %d: %v", i+1, err)
		}
	}
	var overlapRepeats int64
	if err := s.w.QueryRow(`SELECT repeat_count FROM ticks
WHERE outcome='skipped' AND reason_code=?`, string(reason.TICKSkippedOverlap)).Scan(&overlapRepeats); err != nil {
		t.Fatalf("no TICK_SKIPPED_OVERLAP came out of the simulation: %v", err)
	}
	if overlapRepeats != 1 {
		t.Fatalf("overlap skip repeats %d times, want exactly the second fire", overlapRepeats)
	}
	if after := countShadowRows(t, s, "runs"); after != before {
		t.Fatalf("simulating admission still wrote %d new runs (%d -> %d)", after-before, before, after)
	}
	// The first fire stays a clean would-run; only the collision converted.
	if got := countShadowTicks(t, s); got != 1 {
		t.Fatalf("%d would-runs survived, want exactly the first fire", got)
	}
}

func TestShadowObservationsDedupeOnReread(t *testing.T) {
	ctx := context.Background()
	s := shadowTestStore(t)
	at := time.Date(2026, 8, 20, 6, 0, 1, 0, time.UTC)
	o := ShadowObservation{
		ObservedAt: at,
		Source:     ObsSourceFile,
		Raw:        "Aug 20 06:00:01 host cron[99]: (johan) CMD (/usr/bin/backup.sh)",
		Command:    "/usr/bin/backup.sh",
		CronUser:   "johan",
	}
	first, err := s.InsertShadowObservation(ctx, o)
	if err != nil || !first {
		t.Fatalf("first insert: ok=%v err=%v", first, err)
	}
	second, err := s.InsertShadowObservation(ctx, o)
	if err != nil || second {
		t.Fatalf("the UNIQUE gate folded nothing: ok=%v err=%v", second, err)
	}
	rows, err := s.ListShadowObservations(ctx, at.Add(-time.Minute).UnixMilli(), "")
	if err != nil || len(rows) != 1 {
		t.Fatalf("read back %d observations (err=%v)", len(rows), err)
	}
	if rows[0].ObservedAt.Compare(at) != 0 || rows[0].CronUser != "johan" {
		t.Fatalf("the round trip lost facts: %+v", rows[0])
	}
}

func TestShadowRuntimeMetaRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := shadowTestStore(t)
	if err := s.SetShadowRuntime(ctx, true, ObsSourceJournald); err != nil {
		t.Fatalf("set: %v", err)
	}
	info, err := s.ShadowRuntime(ctx)
	if err != nil || !info.Running || info.Observe != ObsSourceJournald {
		t.Fatalf("running marker missing: %+v err=%v", info, err)
	}
	// A normal start clears it: status must not whisper shadows afterwards.
	if err := s.SetShadowRuntime(ctx, false, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}
	info, err = s.ShadowRuntime(ctx)
	if err != nil || info.Running {
		t.Fatalf("marker survived a normal start: %+v err=%v", info, err)
	}
}
