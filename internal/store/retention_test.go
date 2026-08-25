package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Retention needs history to delete. The seeders here insert straight through
// the writer connection: they are fixtures, not production paths, and the
// production insert paths have their own tests.

// mustVersion gives a run a job version to point at.
func mustVersion(t *testing.T, s *Store, job string) string {
	t.Helper()
	v, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:  job,
		SpecHash: "sha256:" + job,
		SpecJSON: `{"steps":[{"name":"build","run":"true"}]}`,
	})
	if err != nil {
		t.Fatalf("upsert job version for %s: %v", job, err)
	}
	return v.ID
}

// seedFinishedRun inserts one terminal run with one of each child row kind:
// a step, a step_deps edge, a run_event and an artifact.
func seedFinishedRun(t *testing.T, s *Store, job, versionID, runID string, finished time.Time) {
	t.Helper()
	ctx := context.Background()
	ms := finished.UnixMilli()
	reason := "OK"
	if _, err := s.w.ExecContext(ctx, `
INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,
                  reason_code, created_at, started_at, finished_at, updated_at)
VALUES (?, ?, ?, 'schedule', 'succeeded', ?, ?, ?, ?, ?, ?)`,
		runID, job, versionID, ms-2000, reason, ms-2000, ms-1000, ms, ms); err != nil {
		t.Fatalf("seed run %s: %v", runID, err)
	}
	for _, stmt := range []struct {
		q    string
		args []any
	}{
		{`INSERT INTO steps (run_id, name, idx, state, attempt, max_attempts, reason_code)
		  VALUES (?, 'build', 0, 'succeeded', 1, 1, 'OK')`, []any{runID}},
		{`INSERT INTO step_deps (run_id, step_name, depends_on) VALUES (?, 'build', 'prep')`, []any{runID}},
		{`INSERT INTO run_events (run_id, at, kind, actor) VALUES (?, ?, 'run_succeeded', 'test')`, []any{runID, ms}},
		{fmt.Sprintf(`INSERT INTO artifacts (id, run_id, name, uri, created_at)
		  VALUES ('art-%s', ?, 'out', 'logs/x', ?)`, runID), []any{runID, ms}},
	} {
		if _, err := s.w.ExecContext(ctx, stmt.q, stmt.args...); err != nil {
			t.Fatalf("seed child of %s: %v", runID, err)
		}
	}
}

func retentionCountRows(t *testing.T, s *Store, table string) int64 {
	t.Helper()
	var n int64
	if err := s.w.QueryRowContext(context.Background(),
		"SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// drainRuns loops PruneRunsBatch until it reports nothing left, the way the
// janitor does.
func drainRuns(t *testing.T, s *Store, cutoff time.Time, keepMin int) (deleted int64) {
	t.Helper()
	for {
		n, err := s.PruneRunsBatch(context.Background(), cutoff, keepMin)
		if err != nil {
			t.Fatalf("prune runs batch: %v", err)
		}
		deleted += n
		if n == 0 {
			return deleted
		}
	}
}

func drainTicks(t *testing.T, s *Store, cutoff time.Time, keepMin int) (deleted int64) {
	t.Helper()
	for {
		n, err := s.PruneSkippedTicksBatch(context.Background(), cutoff)
		if err != nil {
			t.Fatalf("prune skipped ticks: %v", err)
		}
		deleted += n
		if n == 0 {
			break
		}
	}
	for {
		n, err := s.PruneTicksBatch(context.Background(), cutoff, keepMin)
		if err != nil {
			t.Fatalf("prune ticks: %v", err)
		}
		deleted += n
		if n == 0 {
			return deleted
		}
	}
}

// TestRetentionKeepsThreeOldRunsOfAQuarterlyJob is the double criterion's
// reason to exist: a job that ran three times two years ago keeps all three
// runs. Without the minimum guarantee the core promise fails exactly for the
// jobs that are hardest to debug (06 section 9.4).
func TestRetentionKeepsThreeOldRunsOfAQuarterlyJob(t *testing.T) {
	s := migratedStore(t)
	versionID := mustVersion(t, s, "quarterly")
	now := time.Now().UTC()
	old := now.AddDate(-2, 0, 0)

	for i := range 3 {
		seedFinishedRun(t, s, "quarterly", versionID, fmt.Sprintf("r-old-%d", i), old.Add(time.Duration(i)*time.Hour))
	}

	deleted := drainRuns(t, s, now.AddDate(0, 0, -90), DefaultPolicies().RunsKeepMin)
	if deleted != 0 {
		t.Fatalf("deleted %d runs of a three-run job, want 0 (keep-minimum protects all three)", deleted)
	}
	if got := retentionCountRows(t, s, "runs"); got != 3 {
		t.Fatalf("runs table holds %d rows, want the 3 old ones", got)
	}
}

// TestRetentionKeepsExactlyFiftyOfFiveHundredOldRuns pins the other side of
// the guarantee: protection is a minimum, not an amnesty. A job with five
// hundred ancient finished runs keeps its newest fifty and loses the rest.
func TestRetentionKeepsExactlyFiftyOfFiveHundredOldRuns(t *testing.T) {
	s := migratedStore(t)
	versionID := mustVersion(t, s, "bulk")
	now := time.Now().UTC()
	old := now.AddDate(-1, 0, 0)

	for i := range 500 {
		finished := old.Add(time.Duration(i) * time.Minute)
		seedFinishedRun(t, s, "bulk", versionID, fmt.Sprintf("r-bulk-%03d", i), finished)
	}

	deleted := drainRuns(t, s, now.AddDate(0, 0, -90), 50)
	if deleted != 450 {
		t.Fatalf("deleted %d runs, want exactly 450 (500 minus the newest 50)", deleted)
	}
	left := retentionCountRows(t, s, "runs")
	if left != 50 {
		t.Fatalf("%d runs left, want 50", left)
	}
}

// TestRetentionBatchesAtLimit proves one call deletes at most
// PruneBatchLimit rows even when far more qualify: the batch size is what
// bounds the write-lock window.
func TestRetentionBatchesAtLimit(t *testing.T) {
	s := migratedStore(t)
	versionID := mustVersion(t, s, "batched")
	now := time.Now().UTC()
	old := now.AddDate(-1, 0, 0)
	for i := range 1200 {
		seedFinishedRun(t, s, "batched", versionID, fmt.Sprintf("r-b%04d", i), old.Add(time.Duration(i)*time.Second))
	}

	n, err := s.PruneRunsBatch(context.Background(), now.AddDate(0, 0, -90), 0)
	if err != nil {
		t.Fatalf("one batch: %v", err)
	}
	if n != PruneBatchLimit {
		t.Fatalf("first batch deleted %d rows, want the limit %d", n, PruneBatchLimit)
	}
}

// TestRetentionCascadesToEveryChildTable counts each child table before and
// after, then asserts no orphans survived - the same shape fsck asks later.
func TestRetentionCascadesToEveryChildTable(t *testing.T) {
	s := migratedStore(t)
	versionID := mustVersion(t, s, "cascade")
	now := time.Now().UTC()
	old := now.AddDate(-1, 0, 0)

	seedFinishedRun(t, s, "cascade", versionID, "r-gone", old)
	seedFinishedRun(t, s, "cascade", versionID, "r-stays", now.Add(-time.Hour))

	before := map[string]int64{
		"steps":      int64(retentionCountRows(t, s, "steps")),
		"step_deps":  int64(retentionCountRows(t, s, "step_deps")),
		"run_events": int64(retentionCountRows(t, s, "run_events")),
		"artifacts":  int64(retentionCountRows(t, s, "artifacts")),
	}
	if before["steps"] != 2 || before["run_events"] != 2 {
		t.Fatalf("fixture did not seed children: %+v", before)
	}

	// keepMin 0 disables the floor: this test is about cascades, not about
	// protection, and a two-run job would sit entirely inside any floor.
	if deleted := drainRuns(t, s, now.AddDate(0, 0, -90), 0); deleted != 1 {
		t.Fatalf("deleted %d runs, want 1", deleted)
	}

	for _, table := range []string{"steps", "step_deps", "run_events", "artifacts"} {
		if got := int64(retentionCountRows(t, s, table)); got != before[table]-1 {
			t.Fatalf("%s went from %d to %d rows, want exactly one cascade deletion", table, before[table], got)
		}
	}

	var orphans int64
	if err := s.w.QueryRowContext(context.Background(), `
SELECT (SELECT count(*) FROM steps WHERE run_id NOT IN (SELECT id FROM runs))
     + (SELECT count(*) FROM step_deps WHERE run_id NOT IN (SELECT id FROM runs))
     + (SELECT count(*) FROM run_events WHERE run_id NOT IN (SELECT id FROM runs))
     + (SELECT count(*) FROM artifacts WHERE run_id NOT IN (SELECT id FROM runs))`).Scan(&orphans); err != nil {
		t.Fatalf("count orphans: %v", err)
	}
	if orphans != 0 {
		t.Fatalf("%d orphan child rows survived retention", orphans)
	}
}

// TestRunningAndQueuedRunsAreNeverDeleted draws the line retention may not
// cross: only terminal runs are candidates, whatever their age.
func TestRunningAndQueuedRunsAreNeverDeleted(t *testing.T) {
	s := migratedStore(t)
	versionID := mustVersion(t, s, "live")
	now := time.Now().UTC()
	ancient := now.AddDate(-3, 0, 0)

	seedFinishedRun(t, s, "live", versionID, "r-done", ancient)
	for _, state := range []string{"queued", "running"} {
		q := `
INSERT INTO runs (id, job_name, job_version_id, origin, state,
                  available_at, created_at, updated_at)
VALUES (?, 'live', ?, 'manual', ?, ?, ?, ?)`
		if _, err := s.w.ExecContext(context.Background(), q,
			"r-"+state, versionID, state,
			ancient.UnixMilli(), ancient.UnixMilli(), ancient.UnixMilli()); err != nil {
			t.Fatalf("seed %s run: %v", state, err)
		}
	}

	if deleted := drainRuns(t, s, ancient.Add(time.Second), 0); deleted != 1 {
		t.Fatalf("deleted %d runs, want only the terminal one", deleted)
	}
	for _, id := range []string{"r-queued", "r-running"} {
		var n int64
		if err := s.w.QueryRowContext(context.Background(),
			"SELECT count(*) FROM runs WHERE id = ?", id).Scan(&n); err != nil || n != 1 {
			t.Fatalf("run %s vanished or lookup failed (n=%d, err=%v)", id, n, err)
		}
	}
}

// seedTick plants one tick row; skipped ticks carry a skip reason code, which
// the schema demands of every outcome nobody acted on.
func seedRetentionTick(t *testing.T, s *Store, id, kind, name, outcome string, started time.Time, repeat int) {
	t.Helper()
	var reason any
	if outcome == "skipped" || outcome == "error" || outcome == "missed" {
		reason = "WINDOW_MISSED"
	}
	// Sensors tick without a logical slot: their scheduled_for stays NULL,
	// which is what makes repeated evaluations legal under
	// UNIQUE (source_kind, source_name, scheduled_for).
	var schedFor any
	if kind == "schedule" {
		schedFor = started.UnixMilli()
	}
	if _, err := s.w.ExecContext(context.Background(), `
INSERT INTO ticks (id, source_kind, source_name, scheduled_for, started_at, last_started_at,
                   finished_at, repeat_count, outcome, reason_code)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, kind, name, schedFor, started.UnixMilli(), started.UnixMilli(),
		started.UnixMilli(), repeat, outcome, reason); err != nil {
		t.Fatalf("seed tick %s: %v", id, err)
	}
}

// TestSkippedTicksGoInSevenDaysOthersInNinetyWithTwoHundredPerSource covers
// both tick rules in one database: skips have no floor, everything else keeps
// its newest 200 per source.
func TestSkippedTicksGoInSevenDaysOthersInNinetyWithTwoHundredPerSource(t *testing.T) {
	s := migratedStore(t)
	now := time.Now().UTC()

	// Ten thousand ancient skips: all of them go, no floor applies.
	for i := range 10_000 {
		seedRetentionTick(t, s, fmt.Sprintf("t-skip-%05d", i), "sensor", "poll", "skipped", now.AddDate(-1, 0, 0), 3)
	}
	// An old triggered tick per source: protected by the 200 floor.
	for _, src := range [][2]string{{"schedule", "nightly"}, {"sensor", "poll"}} {
		for i := range 210 {
			seedRetentionTick(t, s, fmt.Sprintf("t-%s-%s-%03d", src[0], src[1], i), src[0], src[1], "triggered",
				now.AddDate(-1, 0, 0).Add(time.Duration(i)*time.Minute), 1)
		}
	}

	deleted := drainTicks(t, s, now.AddDate(0, 0, -90), DefaultPolicies().TicksKeepMin)

	// 10_000 skips + (210-200)+(210-200) old non-skipped ticks = 10_020.
	if deleted != 10_020 {
		t.Fatalf("deleted %d ticks, want 10020 (all skips plus the two sources' overflow)", deleted)
	}
	var skipped int64
	if err := s.w.QueryRowContext(context.Background(),
		"SELECT count(*) FROM ticks WHERE outcome = 'skipped'").Scan(&skipped); err != nil || skipped != 0 {
		t.Fatalf("skipped ticks survived (n=%d, err=%v)", skipped, err)
	}
	for _, src := range [][2]string{{"schedule", "nightly"}, {"sensor", "poll"}} {
		var n int64
		if err := s.w.QueryRowContext(context.Background(),
			"SELECT count(*) FROM ticks WHERE source_kind = ? AND source_name = ?", src[0], src[1]).Scan(&n); err != nil {
			t.Fatalf("count ticks of %v: %v", src, err)
		}
		if n != 200 {
			t.Fatalf("source %v kept %d ticks, want exactly the 200-floor", src, n)
		}
	}
}

// TestRecentSkippedTicksStayUntilTheirHorizon shows the seven-day rule cuts
// both ways: a week-old skip is still evidence, a month-old one is noise.
func TestRecentSkippedTicksStayUntilTheirHorizon(t *testing.T) {
	s := migratedStore(t)
	now := time.Now().UTC()
	seedRetentionTick(t, s, "t-week", "sensor", "poll", "skipped", now.AddDate(0, 0, -6), 1)
	seedRetentionTick(t, s, "t-month", "sensor", "poll", "skipped", now.AddDate(0, 0, -30), 1)

	if deleted := drainTicks(t, s, now.AddDate(0, 0, -7), 0); deleted != 1 {
		t.Fatalf("deleted %d ticks, want only the month-old skip", deleted)
	}
	var n int64
	if err := s.w.QueryRowContext(context.Background(),
		"SELECT count(*) FROM ticks WHERE id = 't-week'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("the six-day-old skip did not survive (n=%d, err=%v)", n, err)
	}
}

// TestRunKeysPruneByAge checks the longest horizon and the tuple delete.
func TestRunKeysPruneByAge(t *testing.T) {
	s := migratedStore(t)
	now := time.Now().UTC()
	rows := []struct {
		key  string
		seen time.Time
	}{
		{"k-old", now.AddDate(-2, 0, 0)},
		{"k-edge", now.AddDate(0, 0, -366)},
		{"k-new", now.AddDate(0, 0, -300)},
	}
	for _, r := range rows {
		if _, err := s.w.ExecContext(context.Background(), `
INSERT INTO run_keys (source_id, epoch, run_key, first_seen_at) VALUES ('sched', 0, ?, ?)`,
			r.key, r.seen.UnixMilli()); err != nil {
			t.Fatalf("seed run key %s: %v", r.key, err)
		}
	}

	deleted := int64(0)
	for {
		n, err := s.PruneRunKeysBatch(context.Background(), now.AddDate(0, 0, -365))
		if err != nil {
			t.Fatalf("prune run keys: %v", err)
		}
		deleted += n
		if n == 0 {
			break
		}
	}
	if deleted != 2 {
		t.Fatalf("deleted %d run keys, want the two past the horizon", deleted)
	}
	var left int64
	if err := s.w.QueryRowContext(context.Background(),
		"SELECT count(*) FROM run_keys WHERE run_key = 'k-new'").Scan(&left); err != nil || left != 1 {
		t.Fatalf("the in-horizon key did not survive (n=%d, err=%v)", left, err)
	}
}

// TestSessionsPruneProtectsOutageWitnesses covers the three session rules at
// once: age, the outage references, and the newest-fifty floor.
func TestSessionsPruneProtectsOutageWitnesses(t *testing.T) {
	s := migratedStore(t)
	now := time.Now().UTC()
	old := now.AddDate(-1, 0, 0)

	insertSession := func(id string, started time.Time, stopped bool) {
		t.Helper()
		var stopCol any
		if stopped {
			stopCol = started.Add(time.Hour).UnixMilli()
		}
		if _, err := s.w.ExecContext(context.Background(), `
INSERT INTO daemon_sessions (id, version, pid, started_at, last_seen_at, stopped_at, stop_reason)
VALUES (?, '0.1.0', 1, ?, ?, ?, 'clean')`,
			id, started.UnixMilli(), started.UnixMilli(), stopCol); err != nil {
			t.Fatalf("seed session %s: %v", id, err)
		}
	}

	// The witness: ancient but cited by an outage, so it stays.
	insertSession("sess-witness", old, true)
	if _, err := s.w.ExecContext(context.Background(), `
INSERT INTO outages (from_ts, to_ts, detected_at, kind, prev_session)
VALUES (?, ?, ?, 'crash', 'sess-witness')`,
		old.UnixMilli(), old.Add(time.Hour).UnixMilli(), old.Add(2*time.Hour).UnixMilli()); err != nil {
		t.Fatalf("seed outage: %v", err)
	}
	// Ancient and uncited: goes.
	insertSession("sess-gone", old.Add(-24*time.Hour), true)
	// Still open: never swept, however old.
	insertSession("sess-open", old.Add(-48*time.Hour), false)

	deleted := int64(0)
	for {
		n, err := s.PruneSessionsBatch(context.Background(), now.AddDate(0, 0, -90), 1)
		if err != nil {
			t.Fatalf("prune sessions: %v", err)
		}
		deleted += n
		if n == 0 {
			break
		}
	}
	if deleted != 1 {
		t.Fatalf("deleted %d sessions, want only the uncited stopped one", deleted)
	}
	for _, id := range []string{"sess-witness", "sess-open"} {
		var n int64
		if err := s.w.QueryRowContext(context.Background(),
			"SELECT count(*) FROM daemon_sessions WHERE id = ?", id).Scan(&n); err != nil || n != 1 {
			t.Fatalf("session %s should have survived (n=%d, err=%v)", id, n, err)
		}
	}
}

// TestEstimateMatchesExecution is the dry-run contract: the numbers shown are
// the numbers a real pass deletes.
func TestEstimateMatchesExecution(t *testing.T) {
	s := migratedStore(t)
	versionID := mustVersion(t, s, "plan")
	now := time.Now().UTC()
	old := now.AddDate(-1, 0, 0)

	pol := DefaultPolicies()
	for i := range 60 {
		seedFinishedRun(t, s, "plan", versionID, fmt.Sprintf("r-p%02d", i), old)
	}
	for i := range 30 {
		seedRetentionTick(t, s, fmt.Sprintf("t-e%02d", i), "sensor", "pol", "skipped", now.AddDate(0, 0, -20), 1)
	}
	seedRetentionTick(t, s, "t-keep0", "schedule", "n", "triggered", old, 1)
	seedRetentionTick(t, s, "t-keep1", "schedule", "n", "error", old.Add(-time.Hour), 1)

	plan, err := s.EstimateRetention(context.Background(), pol, now)
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	// 60 ancient runs, the newest 50 of them protected by the floor: the
	// estimate must already show the protection, not just the age rule.
	if plan.Runs != 10 {
		t.Fatalf("estimate runs = %d, want 10 (60 old runs minus the newest-50 floor)", plan.Runs)
	}
	if plan.SkippedTicks != 30 {
		t.Fatalf("estimate skipped ticks = %d, want 30", plan.SkippedTicks)
	}
	if plan.Ticks != 0 {
		t.Fatalf("estimate ticks = %d, want 0 (two ticks are below the 200 floor)", plan.Ticks)
	}

	drainRuns(t, s, runsCutoff(now, pol), pol.RunsKeepMin)
	drainTicks(t, s, now.AddDate(0, 0, -7), pol.TicksKeepMin)

	if left := retentionCountRows(t, s, "runs"); left != 50 {
		t.Fatalf("%d runs survived a full pass, want the floor-protected 50", left)
	}
	if left := retentionCountRows(t, s, "ticks"); left != 2 {
		t.Fatalf("%d ticks survived, want the two floor-protected ones", left)
	}
}
