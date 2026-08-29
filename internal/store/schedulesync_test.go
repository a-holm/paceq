package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/spec"
)

// The schedule sync tests walk the real SQLite file a migratedStore opens, and
// they assert the whole row rather than the column under test: the point of the
// cursor rule is what an apply leaves ALONE, and a test that read only the
// column it changed would never see a re-apply resetting the calendar.

func scheduleFixture(name, cron string) spec.Schedule {
	return spec.Schedule{
		Name:     name,
		Cron:     cron,
		Timezone: spec.DefaultTimezone,
		Overlap:  spec.DefaultOverlap,
	}
}

// applyWithSchedules applies one job whose spec declares the given schedules.
// The hash is what decides whether the job version is new, so a caller that
// changes the schedules changes the hash too, the way a changed file would.
func applyWithSchedules(t *testing.T, s *Store, job, hash string, schedules ...spec.Schedule) JobApplyResult {
	t.Helper()

	results, err := s.ApplyJobs(context.Background(), []JobVersionInput{{
		JobName:   job,
		SpecHash:  "sha256:" + hash,
		SpecJSON:  `{"schema":"paceq.job.v1","name":"` + job + `"}`,
		Schedules: schedules,
	}})
	if err != nil {
		t.Fatalf("apply %s: %v", job, err)
	}
	if len(results) != 1 {
		t.Fatalf("apply %s returned %d results, want 1", job, len(results))
	}
	return results[0]
}

// scheduleSnapshotSQL reads every column of one schedule as one quoted string,
// so a test can say "nothing moved" about the whole row in one comparison.
// quote() renders NULL as the text NULL instead of poisoning the concatenation.
const scheduleSnapshotSQL = `SELECT quote(id) || ' ' || quote(job_name) || ' ' || quote(name) ||
       ' ' || quote(kind) || ' ' || quote(expr) || ' ' || quote(timezone) ||
       ' ' || quote(spring_forward) || ' ' || quote(fall_back) || ' ' || quote(catchup) ||
       ' ' || quote(catchup_limit) || ' ' || quote(catchup_window_ms) ||
       ' ' || quote(overlap) || ' ' || quote(shadow) || ' ' || quote(paused) ||
       ' ' || quote(last_tick_at) || ' ' || quote(next_tick_at) ||
       ' ' || quote(created_at) || ' ' || quote(updated_at)
  FROM schedules WHERE job_name = ? AND name = ?`

func scheduleSnapshot(t *testing.T, s *Store, job, name string) string {
	t.Helper()

	var row string
	if err := s.r.QueryRow(scheduleSnapshotSQL, job, name).Scan(&row); err != nil {
		t.Fatalf("read schedule %s/%s: %v", job, name, err)
	}
	return row
}

// tickTheSchedule moves the cursor the way a scheduler pass would, so the next
// apply has real drift state to leave alone.
func tickTheSchedule(t *testing.T, s *Store, job, name string, fired, next time.Time) {
	t.Helper()

	if _, err := s.w.Exec(`UPDATE schedules SET last_tick_at = ?, next_tick_at = ?
WHERE job_name = ? AND name = ?`, fired.UnixMilli(), next.UnixMilli(), job, name); err != nil {
		t.Fatalf("advance the cursor of %s/%s: %v", job, name, err)
	}
}

func TestApplyMaterialisesAScheduleThatIsDueAtOnce(t *testing.T) {
	s := migratedStore(t)

	res := applyWithSchedules(t, s, "reports", "a1", scheduleFixture("nightly", "0 3 * * *"))
	if len(res.Schedules.Created) != 1 || res.Schedules.Created[0] != "nightly" {
		t.Fatalf("the apply result does not carry the schedule sync: %+v", res.Schedules)
	}

	var kind, expr, timezone, overlap string
	var shadow, paused int
	var lastTick *int64
	var nextTick, createdAt int64
	err := s.r.QueryRow(`SELECT kind, expr, timezone, overlap, shadow, paused,
       last_tick_at, next_tick_at, created_at
  FROM schedules WHERE job_name = 'reports' AND name = 'nightly'`).
		Scan(&kind, &expr, &timezone, &overlap, &shadow, &paused, &lastTick, &nextTick, &createdAt)
	if err != nil {
		t.Fatalf("read the schedule row: %v", err)
	}
	if kind != "cron" || expr != "0 3 * * *" || timezone != "UTC" || overlap != "skip" {
		t.Errorf("the definition columns are kind=%q expr=%q timezone=%q overlap=%q",
			kind, expr, timezone, overlap)
	}
	if shadow != 0 || paused != 0 {
		t.Errorf("a fresh schedule starts with shadow=%d paused=%d", shadow, paused)
	}
	if lastTick != nil {
		t.Errorf("a fresh schedule has already ticked at %d", *lastTick)
	}
	if nextTick != createdAt {
		t.Errorf("next_tick_at is %d and created_at is %d: a new schedule is due at once",
			nextTick, createdAt)
	}

	// The point of the row is that the loop finds it. Nothing else in this
	// test proves the daemon would ever see the schedule.
	due, err := s.DueSchedules(context.Background(), createdAt, 10)
	if err != nil {
		t.Fatalf("DueSchedules: %v", err)
	}
	if len(due) != 1 || due[0].Name != "nightly" {
		t.Fatalf("the freshly applied schedule is not due: %+v", due)
	}
}

func TestApplyingTheSameScheduleTwiceMovesNothing(t *testing.T) {
	s := migratedStore(t)

	applyWithSchedules(t, s, "reports", "a1", scheduleFixture("nightly", "0 3 * * *"))
	fired := time.UnixMilli(1_700_000_000_000).UTC()
	tickTheSchedule(t, s, "reports", "nightly", fired, fired.Add(24*time.Hour))
	before := scheduleSnapshot(t, s, "reports", "nightly")

	res := applyWithSchedules(t, s, "reports", "a1", scheduleFixture("nightly", "0 3 * * *"))
	if len(res.Schedules.Unchanged) != 1 || res.Schedules.Unchanged[0] != "nightly" {
		t.Fatalf("the second apply reports %+v, want one unchanged", res.Schedules)
	}
	if len(res.Schedules.Created)+len(res.Schedules.Updated)+len(res.Schedules.Removed) != 0 {
		t.Errorf("the second apply reports work it did not do: %+v", res.Schedules)
	}

	if after := scheduleSnapshot(t, s, "reports", "nightly"); after != before {
		t.Errorf("a re-apply of an unchanged spec moved the row:\nbefore %s\nafter  %s", before, after)
	}
}

func TestApplyReplacesAChangedDefinitionAndKeepsTheCursor(t *testing.T) {
	s := migratedStore(t)

	applyWithSchedules(t, s, "reports", "a1", scheduleFixture("nightly", "0 3 * * *"))
	fired := time.UnixMilli(1_700_000_000_000).UTC()
	next := fired.Add(24 * time.Hour)
	tickTheSchedule(t, s, "reports", "nightly", fired, next)

	var id string
	var createdAt int64
	if err := s.r.QueryRow(`SELECT id, created_at FROM schedules
WHERE job_name = 'reports' AND name = 'nightly'`).Scan(&id, &createdAt); err != nil {
		t.Fatalf("read the schedule identity: %v", err)
	}

	res := applyWithSchedules(t, s, "reports", "a2", scheduleFixture("nightly", "15 4 * * *"))
	if len(res.Schedules.Updated) != 1 || res.Schedules.Updated[0] != "nightly" {
		t.Fatalf("the changed apply reports %+v, want one updated", res.Schedules)
	}

	var gotID, expr string
	var gotCreated, lastTick, nextTick int64
	err := s.r.QueryRow(`SELECT id, expr, created_at, last_tick_at, next_tick_at
  FROM schedules WHERE job_name = 'reports' AND name = 'nightly'`).
		Scan(&gotID, &expr, &gotCreated, &lastTick, &nextTick)
	if err != nil {
		t.Fatalf("read the schedule row: %v", err)
	}
	if expr != "15 4 * * *" {
		t.Errorf("the expression is %q, want the one the file now says", expr)
	}
	if gotID != id || gotCreated != createdAt {
		t.Errorf("the row lost its identity: id %q -> %q, created_at %d -> %d",
			id, gotID, createdAt, gotCreated)
	}
	if lastTick != fired.UnixMilli() || nextTick != next.UnixMilli() {
		t.Errorf("the cursor moved to last_tick_at=%d next_tick_at=%d: the file does not own it",
			lastTick, nextTick)
	}
}

func TestApplyAddsAScheduleWithoutDisturbingTheOthers(t *testing.T) {
	s := migratedStore(t)

	applyWithSchedules(t, s, "reports", "a1", scheduleFixture("nightly", "0 3 * * *"))
	fired := time.UnixMilli(1_700_000_000_000).UTC()
	tickTheSchedule(t, s, "reports", "nightly", fired, fired.Add(24*time.Hour))
	before := scheduleSnapshot(t, s, "reports", "nightly")

	res := applyWithSchedules(t, s, "reports", "a2",
		scheduleFixture("nightly", "0 3 * * *"),
		scheduleFixture("weekly", "0 5 * * 1"))
	if len(res.Schedules.Created) != 1 || res.Schedules.Created[0] != "weekly" {
		t.Fatalf("the widened apply reports %+v, want weekly created", res.Schedules)
	}

	var count int
	if err := s.r.QueryRow(`SELECT COUNT(*) FROM schedules WHERE job_name = 'reports'`).
		Scan(&count); err != nil {
		t.Fatalf("count the schedules: %v", err)
	}
	if count != 2 {
		t.Errorf("the job holds %d schedules, want 2", count)
	}
	if after := scheduleSnapshot(t, s, "reports", "nightly"); after != before {
		t.Errorf("adding a schedule moved the one already there:\nbefore %s\nafter  %s", before, after)
	}
}

func TestApplyRemovesADroppedScheduleAndKeepsItsTicks(t *testing.T) {
	s := migratedStore(t)

	applyWithSchedules(t, s, "reports", "a1",
		scheduleFixture("nightly", "0 3 * * *"),
		scheduleFixture("weekly", "0 5 * * 1"))

	if _, err := s.w.Exec(`INSERT INTO ticks
(id, source_kind, source_name, scheduled_for, started_at, last_started_at, outcome)
VALUES ('01TICKWEEKLY', 'schedule', 'reports/weekly', 1000, 1000, 1000, 'triggered')`); err != nil {
		t.Fatalf("record a tick for the schedule about to be dropped: %v", err)
	}

	res := applyWithSchedules(t, s, "reports", "a2", scheduleFixture("nightly", "0 3 * * *"))
	if len(res.Schedules.Removed) != 1 || res.Schedules.Removed[0] != "weekly" {
		t.Fatalf("the narrowed apply reports %+v, want weekly removed", res.Schedules)
	}

	var rows int
	if err := s.r.QueryRow(`SELECT COUNT(*) FROM schedules
WHERE job_name = 'reports' AND name = 'weekly'`).Scan(&rows); err != nil {
		t.Fatalf("count the dropped schedule: %v", err)
	}
	if rows != 0 {
		t.Errorf("the dropped schedule is still there")
	}

	var ticks int
	if err := s.r.QueryRow(`SELECT COUNT(*) FROM ticks
WHERE source_kind = 'schedule' AND source_name = 'reports/weekly'`).Scan(&ticks); err != nil {
		t.Fatalf("count the ticks of the dropped schedule: %v", err)
	}
	if ticks != 1 {
		t.Errorf("the dropped schedule took %d of its ticks with it", 1-ticks)
	}

	// The schedule that stayed is untouched by its neighbour leaving.
	var kept int
	if err := s.r.QueryRow(`SELECT COUNT(*) FROM schedules
WHERE job_name = 'reports' AND name = 'nightly'`).Scan(&kept); err != nil {
		t.Fatalf("count the kept schedule: %v", err)
	}
	if kept != 1 {
		t.Errorf("dropping one schedule took the other with it")
	}
}

// TestSchedulePlanReadsEveryDefinitionField keeps the digest honest: a field
// the comparison forgets is a file change that never reaches the database.
func TestSchedulePlanReadsEveryDefinitionField(t *testing.T) {
	base := scheduleFixture("nightly", "0 3 * * *")
	existing := map[string]string{"nightly": scheduleDefOf(base).digest()}

	elsewhere := base
	elsewhere.Timezone = "Europe/Oslo"
	queued := base
	queued.Overlap = spec.OverlapQueue
	watched := base
	watched.Shadow = true

	cases := []struct {
		what     string
		schedule spec.Schedule
		want     string
	}{
		{"an unchanged spec", base, ""},
		{"a changed expression", scheduleFixture("nightly", "15 4 * * *"), lifecycleSpecChanged},
		{"a changed timezone", elsewhere, lifecycleSpecChanged},
		{"a changed overlap", queued, lifecycleSpecChanged},
		{"a changed shadow flag", watched, lifecycleSpecChanged},
	}
	for _, c := range cases {
		t.Run(c.what, func(t *testing.T) {
			plan := buildSchedulePlan([]spec.Schedule{c.schedule}, existing)
			if len(plan) != 1 {
				t.Fatalf("the plan holds %d items, want 1", len(plan))
			}
			if plan[0].kind != c.want {
				t.Errorf("%s plans %q, want %q", c.what, plan[0].kind, c.want)
			}
		})
	}
}

// TestASparseScheduleDigestsLikeAnExplicitOne: FromIR leaves timezone and
// overlap out at their defaults, so a job version read back from the database
// must not look like a changed file.
func TestASparseScheduleDigestsLikeAnExplicitOne(t *testing.T) {
	sparse := spec.Schedule{Name: "nightly", Cron: "0 3 * * *"}
	explicit := scheduleFixture("nightly", "0 3 * * *")

	if scheduleDefOf(sparse).digest() != scheduleDefOf(explicit).digest() {
		t.Errorf("a schedule that says nothing digests differently from one that spells the defaults out:\n%s\n%s",
			scheduleDefOf(sparse).digest(), scheduleDefOf(explicit).digest())
	}
}

// TestApplyingAJobWithNoSchedulesRemovesTheRowsItOwned covers the destructive
// edge of the sync contract: a file with no schedules[] block is a file that
// declares none, so every schedule row of that job goes, whichever seam put it
// there. Removed comes out of a map, so only the set is asserted.
func TestApplyingAJobWithNoSchedulesRemovesTheRowsItOwned(t *testing.T) {
	s := migratedStore(t)

	applyWithSchedules(t, s, "reports", "a1",
		scheduleFixture("nightly", "0 3 * * *"),
		scheduleFixture("weekly", "0 5 * * 1"))

	res := applyWithSchedules(t, s, "reports", "a2")
	gone := map[string]bool{}
	for _, name := range res.Schedules.Removed {
		gone[name] = true
	}
	if len(res.Schedules.Removed) != 2 || !gone["nightly"] || !gone["weekly"] {
		t.Fatalf("the emptied apply reports %+v, want both schedules removed", res.Schedules)
	}

	var left int
	if err := s.r.QueryRow(`SELECT COUNT(*) FROM schedules WHERE job_name = 'reports'`).
		Scan(&left); err != nil {
		t.Fatalf("count the schedules: %v", err)
	}
	if left != 0 {
		t.Errorf("the job keeps %d schedules after declaring none", left)
	}
}

// TestApplyLeavesAPausedScheduleStopped: paused is an operator decision, and a
// changed definition is still a file. Standing a schedule back up because
// somebody edited its cron expression is the one way a re-apply could start
// work nobody asked for.
func TestApplyLeavesAPausedScheduleStopped(t *testing.T) {
	s := migratedStore(t)

	applyWithSchedules(t, s, "reports", "a1", scheduleFixture("nightly", "0 3 * * *"))
	if _, err := s.w.Exec(`UPDATE schedules SET paused = 1
WHERE job_name = 'reports' AND name = 'nightly'`); err != nil {
		t.Fatalf("pause the schedule: %v", err)
	}

	applyWithSchedules(t, s, "reports", "a2", scheduleFixture("nightly", "15 4 * * *"))

	var paused int
	var expr string
	if err := s.r.QueryRow(`SELECT paused, expr FROM schedules
WHERE job_name = 'reports' AND name = 'nightly'`).Scan(&paused, &expr); err != nil {
		t.Fatalf("read the schedule row: %v", err)
	}
	if expr != "15 4 * * *" {
		t.Errorf("the expression is %q, want the one the file now says", expr)
	}
	if paused != 1 {
		t.Error("a re-apply resumed a schedule an operator paused")
	}
}
