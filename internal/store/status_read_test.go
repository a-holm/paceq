package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The status read path (#30): seven statements for the whole overview,
// however many jobs exist. These tests pin three things at once: every plan
// stays off the history tables, the statement count does not grow with the
// job count, and the newest-finished-run join answers what the tables
// actually hold.

// statusCatalogTables are the tables a SCAN is allowed to name: one row per
// operator declaration, bounded by what the project defines, never by what
// ran. Every other table is history; reading all of it per status call is
// exactly the regression the query-plan guard exists to catch.
var statusCatalogTables = map[string]bool{
	"jobs":            true,
	"schedules":       true,
	"sensors":         true,
	"daemon_sessions": true,
}

// TestStatusQueryPlans walks EXPLAIN QUERY PLAN over the exact statement
// texts production executes. A plan line that scans a non-catalogue table
// fails, and the per-job run lookup must ride idx_runs_job_finished - the
// partial index 0012 added for precisely that seek.
func TestStatusQueryPlans(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()

	for _, q := range StatusQueries() {
		rows, err := s.r.QueryContext(ctx, "EXPLAIN QUERY PLAN "+q, int64(1))
		if err != nil {
			t.Fatalf("plan %q: %v", q, err)
		}
		var plans []string
		for rows.Next() {
			var a, b, c any
			var detail string
			if err := rows.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatalf("scan a plan line of %q: %v", q, err)
			}
			plans = append(plans, detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read the plan of %q: %v", q, err)
		}
		rows.Close()

		for _, line := range plans {
			scanStart := strings.Index(line, "SCAN ")
			if scanStart < 0 {
				continue
			}
			object := strings.Fields(line[scanStart+len("SCAN "):])
			if len(object) == 0 {
				continue
			}
			table := strings.Trim(object[0], "`")
			if statusCatalogTables[table] {
				continue // a catalogue read: bounded by declarations
			}
			if strings.Contains(line, "INDEX") {
				continue // an index walk, not a raw table read
			}
			t.Errorf("a status query reads %s by full table scan:\n%s\nplan line: %s",
				table, q, line)
		}
	}

	// The join that decides "what did this job last do" must use the index
	// built for it. If a refactor drops or renames the index, SQLite falls
	// back silently - so assert on the chosen plan, not on the schema.
	jobsPlan := queryPlanText(t, s, ctx, StatusQueries()[0])
	if !strings.Contains(jobsPlan, "idx_runs_job_finished") {
		t.Errorf("the per-job last-run lookup no longer rides idx_runs_job_finished:\n%s", jobsPlan)
	}
	if strings.Contains(jobsPlan, "TEMP B-TREE FOR RIGHT PART OF ORDER BY") {
		t.Errorf("the per-job last-run lookup sorts instead of walking the index:\n%s", jobsPlan)
	}
}

func queryPlanText(t *testing.T, s *Store, ctx context.Context, q string) string {
	t.Helper()
	rows, err := s.r.QueryContext(ctx, "EXPLAIN QUERY PLAN "+q)
	if err != nil {
		t.Fatalf("plan %q: %v", q, err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var a, b, c any
		var detail string
		if err := rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatalf("scan a plan line of %q: %v", q, err)
		}
		lines = append(lines, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the plan of %q: %v", q, err)
	}
	return strings.Join(lines, "\n")
}

// plantStatusJob records a job row through the real upsert, so foreign keys
// hold for everything planted under it.
func plantStatusJob(t *testing.T, s *Store, name string, paused bool) {
	t.Helper()
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:  name,
		SpecHash: "sha256:" + name,
		SpecJSON: `{"schema":"paceq.job.v1","name":"` + name + `","steps":[{"name":"work","run":["/bin/true"]}]}`,
	}); err != nil {
		t.Fatalf("plant job %s: %v", name, err)
	}
	if paused {
		if _, err := s.w.ExecContext(context.Background(),
			`UPDATE jobs SET paused = 1 WHERE name = ?`, name); err != nil {
			t.Fatalf("pause job %s: %v", name, err)
		}
	}
}

// plantFinishedRun inserts a terminal run row directly, the way the engine's
// transitions would have left it: finished_at set, duration recorded, reason
// code carried as the schema demands of every terminal row.
func plantFinishedRun(t *testing.T, s *Store, job, id, state, reasonCode string, started, finished time.Time) {
	t.Helper()
	ctx := context.Background()
	var versionID string
	if err := s.r.QueryRowContext(ctx,
		`SELECT current_version_id FROM jobs WHERE name = ?`, job).Scan(&versionID); err != nil {
		t.Fatalf("read the version of %s: %v", job, err)
	}
	stmt := `INSERT INTO runs (id, job_name, job_version_id, origin, state,
		reason_code, available_at, created_at, started_at, finished_at, duration_ms, updated_at)
		VALUES (?, ?, ?, 'schedule', ?, ?, ?, ?, ?, ?, ?, ?)`
	now := finished.UnixMilli()
	if _, err := s.w.ExecContext(ctx, stmt, id, job, versionID, state, reasonCode,
		started.UnixMilli(), started.UnixMilli(), started.UnixMilli(),
		finished.UnixMilli(), finished.Sub(started).Milliseconds(), now); err != nil {
		t.Fatalf("plant a %s run on %s: %v", state, job, err)
	}
}

// plantActiveRun inserts an unfinished run row, optionally with an expired
// lease: the shape a lost executor leaves behind.
func plantActiveRun(t *testing.T, s *Store, job, id, state string, leaseExpires time.Time) {
	t.Helper()
	ctx := context.Background()
	var versionID string
	if err := s.r.QueryRowContext(ctx,
		`SELECT current_version_id FROM jobs WHERE name = ?`, job).Scan(&versionID); err != nil {
		t.Fatalf("read the version of %s: %v", job, err)
	}
	now := time.Now().UnixMilli()
	stmt := `INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,
		lease_owner, lease_epoch, lease_expires_at, created_at, updated_at)
		VALUES (?, ?, ?, 'schedule', ?, ?, 'test-owner', 1, ?, ?, ?)`
	if _, err := s.w.ExecContext(ctx, stmt, id, job, versionID, state,
		now, leaseExpires.UnixMilli(), now, now); err != nil {
		t.Fatalf("plant a %s run on %s: %v", state, job, err)
	}
}

func TestStatusJobsReadsNewestFinishedRun(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()

	base := time.Date(2026, 12, 9, 3, 0, 0, 0, time.UTC)

	// healthy: succeeded newest. The failed run underneath it was confirmed
	// by the later success, so the newest finished run is the answer.
	plantStatusJob(t, s, "healthy", false)
	plantFinishedRun(t, s, "healthy", "01A_HEALTH_F", "failed", "RUN_FAILED_STEP",
		base.Add(-2*time.Hour), base.Add(-2*time.Hour+time.Minute))
	plantFinishedRun(t, s, "healthy", "01A_HEALTH_S", "succeeded", "RUN_SUCCEEDED",
		base.Add(-time.Hour), base.Add(-time.Hour+4*time.Minute))

	// broken: failed newest, nothing after it confirms recovery.
	plantStatusJob(t, s, "broken", false)
	plantFinishedRun(t, s, "broken", "01A_BROKEN_F", "failed", "RUN_FAILED_STEP",
		base.Add(-30*time.Minute), base.Add(-29*time.Minute))

	// fresh: the newest run is still running, so the newest FINISHED one -
	// yesterday's success - is what the table shows, while the running run
	// counts toward the running total elsewhere.
	plantStatusJob(t, s, "fresh", false)
	plantFinishedRun(t, s, "fresh", "01A_FRESH_S", "succeeded", "RUN_SUCCEEDED",
		base.Add(-24*time.Hour), base.Add(-24*time.Hour))
	plantActiveRun(t, s, "fresh", "01A_FRESH_R", "running", base.Add(DefaultRunLeaseTTL))

	// never: applied but never finished anything.
	plantStatusJob(t, s, "never", false)

	// resting: paused by an operator, whatever its runs did.
	plantStatusJob(t, s, "resting", true)
	plantFinishedRun(t, s, "resting", "01A_REST_F", "failed", "RUN_FAILED_STEP",
		base.Add(-time.Hour), base.Add(-59*time.Minute))

	rows, err := s.StatusJobs(ctx, "")
	if err != nil {
		t.Fatalf("StatusJobs: %v", err)
	}
	got := make(map[string]StatusJobRow, len(rows))
	for _, row := range rows {
		got[row.JobName] = row
	}
	if len(rows) != 5 {
		t.Fatalf("StatusJobs returned %d rows, want 5: %+v", len(rows), rows)
	}

	if r := got["healthy"]; r.RunID != "01A_HEALTH_S" || r.RunState != "succeeded" {
		t.Errorf("healthy = %s/%s, want the newest finished run 01A_HEALTH_S/succeeded", r.RunID, r.RunState)
	}
	if r := got["broken"]; r.RunID != "01A_BROKEN_F" || r.ReasonCode != "RUN_FAILED_STEP" {
		t.Errorf("broken = %s/%s, want 01A_BROKEN_F with its reason code", r.RunID, r.ReasonCode)
	}
	if r := got["fresh"]; r.RunID != "01A_FRESH_S" {
		t.Errorf("fresh = %s/%s, want the newest FINISHED run, not the running one", r.RunID, r.RunState)
	}
	if r := got["never"]; r.RunID != "" || r.RunState != "" {
		t.Errorf("never = %+v, want an empty run side", r)
	}
	if r := got["resting"]; !r.Paused {
		t.Errorf("resting = %+v, want paused carried beside its runs", r)
	}
}

// TestStatusQueriesConstantInJobCount is the N+1 guard: the same statements,
// counted at the driver boundary, whether the project holds 10 jobs or 60.
// The hook is a test seam on the store layer, exactly where the count lives.
func TestStatusQueriesConstantInJobCount(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	now := time.Date(2026, 12, 9, 8, 0, 0, 0, time.UTC)

	countStatements := func() int {
		calls := 0
		queryTraceHook = func(string) { calls++ }
		defer func() { queryTraceHook = nil }()
		readAll := func() error {
			if _, err := s.StatusJobs(ctx, ""); err != nil {
				return err
			}
			if _, err := s.StatusNextTicks(ctx); err != nil {
				return err
			}
			if _, err := s.StatusSensorIntervals(ctx); err != nil {
				return err
			}
			if _, _, err := s.StatusStateCounts(ctx); err != nil {
				return err
			}
			if _, err := s.StatusStuckCounts(ctx, now); err != nil {
				return err
			}
			if _, err := s.StatusSensorErrorCounts(ctx); err != nil {
				return err
			}
			if _, _, _, err := s.StatusOpenSession(ctx); err != nil {
				return err
			}
			return nil
		}
		if err := readAll(); err != nil {
			t.Fatalf("the status reads failed: %v", err)
		}
		return calls
	}

	for i := 0; i < 10; i++ {
		plantStatusJob(t, s, fmt.Sprintf("job-%02d", i), false)
	}
	small := countStatements()

	for i := 10; i < 60; i++ {
		plantStatusJob(t, s, fmt.Sprintf("job-%02d", i), false)
		plantFinishedRun(t, s, fmt.Sprintf("job-%02d", i),
			fmt.Sprintf("01A_JOB_%02d", i), "succeeded", "RUN_SUCCEEDED",
			now.Add(-time.Duration(i)*time.Minute), now.Add(-time.Duration(i)*time.Minute+time.Second))
	}
	large := countStatements()

	want := len(StatusQueries())
	if small != want {
		t.Errorf("%d jobs took %d statements, want %d", 10, small, want)
	}
	if large != want {
		t.Errorf("%d jobs took %d statements, want %d (an N+1 crept in)", 60, large, want)
	}
}

func TestStatusStuckNextTicksAndSensorErrors(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	now := time.Date(2026, 12, 9, 8, 0, 0, 0, time.UTC)

	plantStatusJob(t, s, "haunted", false)
	plantStatusJob(t, s, "calm", false)

	// haunted holds a running run whose lease died mid-flight.
	plantActiveRun(t, s, "haunted", "01A_HAUNT_R", "running", now.Add(-time.Minute))
	// calm's running run still owns a live lease.
	plantActiveRun(t, s, "calm", "01A_CALM_R", "running", now.Add(time.Minute))

	stuck, err := s.StatusStuckCounts(ctx, now)
	if err != nil {
		t.Fatalf("StatusStuckCounts: %v", err)
	}
	if stuck["haunted"] != 1 {
		t.Errorf("stuck = %v, want haunted: 1 (expired lease)", stuck)
	}
	if _, ok := stuck["calm"]; ok {
		t.Errorf("calm appears stuck with a live lease: %v", stuck)
	}

	queued, running, err := s.StatusStateCounts(ctx)
	if err != nil {
		t.Fatalf("StatusStateCounts: %v", err)
	}
	if queued != 0 || running != 2 {
		t.Errorf("counts = queued %d running %d, want 0 and 2", queued, running)
	}

	// A standing schedule makes the next tick readable pre-materialised;
	// a paused schedule never does.
	next := now.Add(time.Hour).Truncate(time.Minute)
	if _, err := s.UpsertSchedule(ctx, ScheduleInput{
		JobName: "calm", Name: "hourly", Kind: "cron", Expr: "0 * * * *",
		Timezone: "UTC", NextTickAt: next,
	}); err != nil {
		t.Fatalf("plant schedule: %v", err)
	}
	if _, err := s.UpsertSchedule(ctx, ScheduleInput{
		JobName: "calm", Name: "resting", Kind: "cron", Expr: "0 2 * * *",
		Timezone: "UTC", NextTickAt: next, Paused: true,
	}); err != nil {
		t.Fatalf("plant paused schedule: %v", err)
	}
	ticks, err := s.StatusNextTicks(ctx)
	if err != nil {
		t.Fatalf("StatusNextTicks: %v", err)
	}
	if got := ticks["calm"]; got != next.UnixMilli() {
		t.Errorf("next tick for calm = %d, want %d from the standing schedule only", got, next.UnixMilli())
	}
	if _, ok := ticks["haunted"]; ok {
		t.Errorf("haunted has no schedule but appears in the next-tick map: %v", ticks)
	}

	// A sensor with consecutive failures lands its job in the error map.
	plantStatusJob(t, s, "watched", false)
	if err := s.UpsertSensor(ctx, SensorSeedInput{
		Name: "dropzone", JobName: "watched", ExecJSON: `["/bin/echo","{}"]`,
	}); err != nil {
		t.Fatalf("plant sensor: %v", err)
	}
	if _, err := s.w.ExecContext(ctx,
		`UPDATE sensors SET consecutive_failures = 2 WHERE name = 'dropzone'`); err != nil {
		t.Fatalf("record failures: %v", err)
	}
	errors, err := s.StatusSensorErrorCounts(ctx)
	if err != nil {
		t.Fatalf("StatusSensorErrorCounts: %v", err)
	}
	if errors["watched"] != 1 {
		t.Errorf("sensor errors = %v, want watched: 1", errors)
	}
	intervals, err := s.StatusSensorIntervals(ctx)
	if err != nil {
		t.Fatalf("StatusSensorIntervals: %v", err)
	}
	if intervals["watched"] != 60000 {
		t.Errorf("sensor interval = %d, want the seeded 60000", intervals["watched"])
	}
}

func TestStatusOpenSession(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()

	version, since, found, err := s.StatusOpenSession(ctx)
	if err != nil {
		t.Fatalf("StatusOpenSession on an empty database: %v", err)
	}
	if found {
		t.Errorf("no daemon ever ran, but a session came back: %s@%s", version, since)
	}

	started := time.Date(2026, 12, 3, 4, 2, 55, 0, time.UTC)
	if _, err := s.w.ExecContext(ctx, `INSERT INTO daemon_sessions
(id, version, pid, started_at, last_seen_at)
VALUES ('01A_SESSION_X', '0.1.0', 42, ?, ?)`,
		started.UnixMilli(), started.UnixMilli()); err != nil {
		t.Fatalf("open a session: %v", err)
	}

	version, since, found, err = s.StatusOpenSession(ctx)
	if err != nil || !found {
		t.Fatalf("StatusOpenSession = %s@%s found=%t err=%v, want the open session",
			version, since, found, err)
	}
	if version != "0.1.0" || !since.Equal(started) {
		t.Errorf("session = %s@%s, want 0.1.0@%s", version, since, started)
	}

	// A closed session never answers for the daemon, however recent.
	if _, err := s.w.ExecContext(ctx,
		`UPDATE daemon_sessions SET stopped_at = ?, stop_reason = 'clean' WHERE id = '01A_SESSION_X'`,
		started.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatalf("close the session: %v", err)
	}
	if _, _, found, _ = s.StatusOpenSession(ctx); found {
		t.Errorf("a closed session answered as open")
	}
}
