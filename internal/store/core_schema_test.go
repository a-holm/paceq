package store

import (
	"context"
	"strings"
	"testing"
)

// coreSeed is one consistent decision-to-execution graph: a job with a version,
// a schedule and a sensor on it, a due evaluation that produced an accepted
// trigger, the run that trigger created, its steps, the edge between them, the
// queued event and an artifact.
//
// It is both the fixture the rejection cases sit on and the proof that the
// schema accepts what it is meant to accept. A schema that refused everything
// would pass a rejection table on its own.
var coreSeed = []string{
	`INSERT INTO jobs (name, description, source_path, created_at, updated_at)
		VALUES ('nightly', 'the nightly report', 'jobs/nightly.yaml', 1000, 1000)`,
	`INSERT INTO job_versions (id, job_name, version, spec_hash, spec_json, source_path, created_at)
		VALUES ('01J0VER1', 'nightly', 1, 'sha256:aa', '{"steps":[]}', 'jobs/nightly.yaml', 1000)`,
	`UPDATE jobs SET current_version_id = '01J0VER1' WHERE name = 'nightly'`,
	`INSERT INTO schedules (id, job_name, name, kind, expr, timezone, next_tick_at, created_at, updated_at)
		VALUES ('01J0SCH1', 'nightly', 'default', 'cron', '0 3 * * *', 'Europe/Oslo', 2000, 1000, 1000)`,
	`INSERT INTO sensors (name, job_name, exec_json, interval_ms, next_eval_at, created_at, updated_at)
		VALUES ('inbox', 'nightly', '["ls","/inbox"]', 30000, 2000, 1000, 1000)`,
	`INSERT INTO ticks (id, source_kind, source_name, scheduled_for, started_at, last_started_at,
			finished_at, duration_ms, outcome, trigger_count)
		VALUES ('01J0TICK1', 'schedule', 'nightly', 2000, 2001, 2001, 2002, 1, 'triggered', 1)`,
	`INSERT INTO triggers (id, tick_id, job_name, run_key, created_at, outcome)
		VALUES ('01J0TRG1', '01J0TICK1', 'nightly', 'file-a', 2001, 'accepted')`,
	`INSERT INTO run_keys (source_id, epoch, run_key, first_seen_at, run_id)
		VALUES ('inbox', 0, 'file-a', 2001, '01J0RUN1')`,
	`INSERT INTO runs (id, job_name, job_version_id, trigger_id, origin, state,
			available_at, scheduled_for, concurrency_key, created_at, updated_at)
		VALUES ('01J0RUN1', 'nightly', '01J0VER1', '01J0TRG1', 'schedule', 'queued',
			2001, 2000, 'nightly', 2001, 2001)`,
	`UPDATE triggers SET run_id = '01J0RUN1' WHERE id = '01J0TRG1'`,
	`INSERT INTO steps (run_id, name, idx, state) VALUES ('01J0RUN1', 'build', 0, 'pending')`,
	`INSERT INTO steps (run_id, name, idx, state) VALUES ('01J0RUN1', 'test', 1, 'pending')`,
	`INSERT INTO step_deps (run_id, step_name, depends_on) VALUES ('01J0RUN1', 'test', 'build')`,
	`INSERT INTO run_events (run_id, at, kind, to_state, actor)
		VALUES ('01J0RUN1', 2001, 'run.queued', 'queued', 'system')`,
	`INSERT INTO artifacts (id, run_id, step_name, name, uri, size_bytes, checksum, created_at)
		VALUES ('01J0ART1', '01J0RUN1', 'build', 'binary', 'file:///tmp/a', 12, 'sha256:bb', 2002)`,
}

// seededStore is a migrated store holding coreSeed.
func seededStore(t *testing.T) *Store {
	t.Helper()

	s := migratedStore(t)
	ctx := context.Background()
	for _, stmt := range coreSeed {
		if _, err := s.w.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed the core graph: %v\n%s", err, stmt)
		}
	}
	return s
}

func TestCoreSchemaAcceptsTheWholeGraph(t *testing.T) {
	seededStore(t)
}

// TestCoreSchemaRejectsInvalidRows is the double enforcement plan 07 section 7
// asks for: the state machines live in internal/model, and the database refuses
// the same values on its own. Every CHECK and every UNIQUE in the core schema
// has a case here, and want is the text SQLite puts in the error, so dropping a
// constraint makes the case fail naming what it expected.
func TestCoreSchemaRejectsInvalidRows(t *testing.T) {
	cases := []struct {
		name string
		want string
		stmt string
	}{
		// 0002_definitions.
		{
			"job paused outside the flag",
			"CHECK constraint failed: paused IN (0, 1)",
			`INSERT INTO jobs (name, paused, created_at, updated_at) VALUES ('other', 2, 1000, 1000)`,
		},
		{
			"job concurrency below one",
			"CHECK constraint failed: max_concurrent > 0",
			`INSERT INTO jobs (name, max_concurrent, created_at, updated_at) VALUES ('other', 0, 1000, 1000)`,
		},
		{
			"job version reuses a version number",
			"UNIQUE constraint failed: job_versions.job_name, job_versions.version",
			`INSERT INTO job_versions (id, job_name, version, spec_hash, spec_json, created_at)
				VALUES ('01J0VER2', 'nightly', 1, 'sha256:cc', '{}', 1100)`,
		},
		{
			"job version reuses a spec hash",
			"UNIQUE constraint failed: job_versions.job_name, job_versions.spec_hash",
			`INSERT INTO job_versions (id, job_name, version, spec_hash, spec_json, created_at)
				VALUES ('01J0VER2', 'nightly', 2, 'sha256:aa', '{}', 1100)`,
		},
		{
			"job version of a job that does not exist",
			"FOREIGN KEY constraint failed",
			`INSERT INTO job_versions (id, job_name, version, spec_hash, spec_json, created_at)
				VALUES ('01J0VER2', 'no-such-job', 1, 'sha256:cc', '{}', 1100)`,
		},

		// 0003_decisions.
		{
			"unknown schedule kind",
			"CHECK constraint failed: kind IN ('cron', 'interval')",
			`INSERT INTO schedules (id, job_name, name, kind, expr, next_tick_at, created_at, updated_at)
				VALUES ('01J0SCH2', 'nightly', 'other', 'calendar', '@yearly', 2000, 1000, 1000)`,
		},
		{
			"unknown spring forward policy",
			"CHECK constraint failed: spring_forward IN ('skip', 'shift')",
			`INSERT INTO schedules (id, job_name, name, kind, expr, spring_forward, next_tick_at, created_at, updated_at)
				VALUES ('01J0SCH2', 'nightly', 'other', 'cron', '0 3 * * *', 'guess', 2000, 1000, 1000)`,
		},
		{
			"unknown fall back policy",
			"CHECK constraint failed: fall_back IN ('first', 'both')",
			`INSERT INTO schedules (id, job_name, name, kind, expr, fall_back, next_tick_at, created_at, updated_at)
				VALUES ('01J0SCH2', 'nightly', 'other', 'cron', '0 3 * * *', 'second', 2000, 1000, 1000)`,
		},
		{
			"unknown catchup policy",
			"CHECK constraint failed: catchup IN ('skip', 'last', 'all')",
			`INSERT INTO schedules (id, job_name, name, kind, expr, catchup, next_tick_at, created_at, updated_at)
				VALUES ('01J0SCH2', 'nightly', 'other', 'cron', '0 3 * * *', 'some', 2000, 1000, 1000)`,
		},
		{
			"negative catchup limit",
			"CHECK constraint failed: catchup_limit >= 0",
			`INSERT INTO schedules (id, job_name, name, kind, expr, catchup_limit, next_tick_at, created_at, updated_at)
				VALUES ('01J0SCH2', 'nightly', 'other', 'cron', '0 3 * * *', -1, 2000, 1000, 1000)`,
		},
		{
			"schedule paused outside the flag",
			"CHECK constraint failed: paused IN (0, 1)",
			`INSERT INTO schedules (id, job_name, name, kind, expr, paused, next_tick_at, created_at, updated_at)
				VALUES ('01J0SCH2', 'nightly', 'other', 'cron', '0 3 * * *', 2, 2000, 1000, 1000)`,
		},
		{
			"two schedules with one name on one job",
			"UNIQUE constraint failed: schedules.job_name, schedules.name",
			`INSERT INTO schedules (id, job_name, name, kind, expr, next_tick_at, created_at, updated_at)
				VALUES ('01J0SCH2', 'nightly', 'default', 'interval', '15m', 2000, 1000, 1000)`,
		},
		{
			"sensor kind beyond exec",
			"CHECK constraint failed: kind = 'exec'",
			`INSERT INTO sensors (name, job_name, kind, exec_json, interval_ms, next_eval_at, created_at, updated_at)
				VALUES ('web', 'nightly', 'http', '[]', 30000, 2000, 1000, 1000)`,
		},
		{
			"sensor polling faster than a second",
			"CHECK constraint failed: interval_ms >= 1000",
			`INSERT INTO sensors (name, job_name, exec_json, interval_ms, next_eval_at, created_at, updated_at)
				VALUES ('fast', 'nightly', '[]', 100, 2000, 1000, 1000)`,
		},
		{
			"sensor timeout of zero",
			"CHECK constraint failed: timeout_ms > 0",
			`INSERT INTO sensors (name, job_name, exec_json, interval_ms, timeout_ms, next_eval_at, created_at, updated_at)
				VALUES ('slow', 'nightly', '[]', 30000, 0, 2000, 1000, 1000)`,
		},
		{
			"sensor that may emit no triggers",
			"CHECK constraint failed: max_triggers_per_tick > 0",
			`INSERT INTO sensors (name, job_name, exec_json, interval_ms, max_triggers_per_tick, next_eval_at, created_at, updated_at)
				VALUES ('mute', 'nightly', '[]', 30000, 0, 2000, 1000, 1000)`,
		},
		{
			"sensor paused outside the flag",
			"CHECK constraint failed: paused IN (0, 1)",
			`INSERT INTO sensors (name, job_name, exec_json, interval_ms, paused, next_eval_at, created_at, updated_at)
				VALUES ('odd', 'nightly', '[]', 30000, 2, 2000, 1000, 1000)`,
		},
		{
			"tick from a source kind nobody evaluates",
			"CHECK constraint failed: source_kind IN ('schedule', 'sensor', 'manual')",
			`INSERT INTO ticks (id, source_kind, source_name, started_at, last_started_at, outcome)
				VALUES ('01J0TICK2', 'webhook', 'nightly', 2001, 2001, 'running')`,
		},
		{
			"tick that coalesced fewer than one evaluation",
			"CHECK constraint failed: repeat_count >= 1",
			`INSERT INTO ticks (id, source_kind, source_name, started_at, last_started_at, repeat_count, outcome)
				VALUES ('01J0TICK2', 'sensor', 'inbox', 2001, 2001, 0, 'running')`,
		},
		{
			"unknown tick outcome",
			"CHECK constraint failed: outcome IN ('running', 'triggered', 'skipped',\n                                       'error', 'missed', 'shadow_triggered')",
			`INSERT INTO ticks (id, source_kind, source_name, started_at, last_started_at, outcome)
				VALUES ('01J0TICK2', 'sensor', 'inbox', 2001, 2001, 'maybe')`,
		},
		{
			"skipped tick without a reason code",
			"CHECK constraint failed: outcome IN ('running', 'triggered', 'shadow_triggered')\n         OR reason_code IS NOT NULL",
			`INSERT INTO ticks (id, source_kind, source_name, started_at, last_started_at, outcome)
				VALUES ('01J0TICK2', 'sensor', 'inbox', 2001, 2001, 'skipped')`,
		},
		{
			"two ticks for one schedule slot",
			"UNIQUE constraint failed: ticks.source_kind, ticks.source_name, ticks.scheduled_for",
			`INSERT INTO ticks (id, source_kind, source_name, scheduled_for, started_at, last_started_at, outcome)
				VALUES ('01J0TICK2', 'schedule', 'nightly', 2000, 2100, 2100, 'running')`,
		},
		{
			"unknown trigger outcome",
			"CHECK constraint failed: outcome IN ('accepted', 'deduped', 'rejected')",
			`INSERT INTO triggers (id, tick_id, job_name, created_at, outcome)
				VALUES ('01J0TRG2', '01J0TICK1', 'nightly', 2001, 'maybe')`,
		},
		{
			"deduped trigger without a reason code",
			"CHECK constraint failed: outcome = 'accepted' OR reason_code IS NOT NULL",
			`INSERT INTO triggers (id, tick_id, job_name, created_at, outcome)
				VALUES ('01J0TRG2', '01J0TICK1', 'nightly', 2001, 'deduped')`,
		},
		{
			"trigger from a tick that never happened",
			"FOREIGN KEY constraint failed",
			`INSERT INTO triggers (id, tick_id, job_name, created_at, outcome)
				VALUES ('01J0TRG2', 'no-such-tick', 'nightly', 2001, 'accepted')`,
		},
		{
			"one run key seen twice in one epoch",
			"UNIQUE constraint failed: run_keys.source_id, run_keys.epoch, run_keys.run_key",
			`INSERT INTO run_keys (source_id, epoch, run_key, first_seen_at)
				VALUES ('inbox', 0, 'file-a', 3000)`,
		},

		// 0004_execution.
		{
			"run from an origin nothing produces",
			"CHECK constraint failed: origin IN ('schedule', 'sensor', 'manual', 'retry', 'replay', 'backfill')",
			`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at, created_at, updated_at)
				VALUES ('01J0RUN2', 'nightly', '01J0VER1', 'webhook', 'queued', 2001, 2001, 2001)`,
		},
		{
			"deferred is not a run state",
			"CHECK constraint failed: state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
			`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at, created_at, updated_at)
				VALUES ('01J0RUN2', 'nightly', '01J0VER1', 'manual', 'deferred', 2001, 2001, 2001)`,
		},
		{
			"a run held back without saying why",
			"CHECK constraint failed: defer_reason IS NOT NULL OR available_at <= created_at OR state <> 'queued'",
			`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at, created_at, updated_at)
				VALUES ('01J0RUN2', 'nightly', '01J0VER1', 'manual', 'queued', 9000, 2001, 2001)`,
		},
		{
			"terminal run without a reason code",
			"CHECK constraint failed: state IN ('queued', 'running') OR reason_code IS NOT NULL",
			`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at, created_at, updated_at)
				VALUES ('01J0RUN2', 'nightly', '01J0VER1', 'manual', 'succeeded', 2001, 2001, 2001)`,
		},
		{
			"run that finished before it started",
			"CHECK constraint failed: finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at",
			`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,
					started_at, finished_at, reason_code, created_at, updated_at)
				VALUES ('01J0RUN2', 'nightly', '01J0VER1', 'manual', 'failed', 2001,
					3000, 2500, 'RUN_FAILED_STEP', 2001, 2001)`,
		},
		{
			"run that may never be attempted",
			"CHECK constraint failed: max_attempts >= 1",
			`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,
					max_attempts, created_at, updated_at)
				VALUES ('01J0RUN2', 'nightly', '01J0VER1', 'manual', 'queued', 2001, 0, 2001, 2001)`,
		},
		{
			"run of a version that does not exist",
			"FOREIGN KEY constraint failed",
			`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at, created_at, updated_at)
				VALUES ('01J0RUN2', 'nightly', 'no-such-version', 'manual', 'queued', 2001, 2001, 2001)`,
		},
		{
			"second active run on one concurrency key",
			"UNIQUE constraint failed: runs.concurrency_key",
			`INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,
					concurrency_key, created_at, updated_at)
				VALUES ('01J0RUN2', 'nightly', '01J0VER1', 'manual', 'queued', 2001, 'nightly', 2001, 2001)`,
		},
		{
			"unknown step state",
			"CHECK constraint failed: state IN ('pending', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')",
			`INSERT INTO steps (run_id, name, idx, state) VALUES ('01J0RUN1', 'deploy', 2, 'upstream_failed')`,
		},
		{
			"step that may never be attempted",
			"CHECK constraint failed: max_attempts >= 1",
			`INSERT INTO steps (run_id, name, idx, state, max_attempts)
				VALUES ('01J0RUN1', 'deploy', 2, 'pending', 0)`,
		},
		{
			"step truncation outside the flag",
			"CHECK constraint failed: log_truncated IN (0, 1)",
			`INSERT INTO steps (run_id, name, idx, state, log_truncated)
				VALUES ('01J0RUN1', 'deploy', 2, 'pending', 2)`,
		},
		{
			"terminal step without a reason code",
			"CHECK constraint failed: state IN ('pending', 'running') OR reason_code IS NOT NULL",
			`INSERT INTO steps (run_id, name, idx, state) VALUES ('01J0RUN1', 'deploy', 2, 'succeeded')`,
		},
		{
			"one step name twice in one run",
			"UNIQUE constraint failed: steps.run_id, steps.name",
			`INSERT INTO steps (run_id, name, idx, state) VALUES ('01J0RUN1', 'build', 2, 'pending')`,
		},
		{
			"step of a run that does not exist",
			"FOREIGN KEY constraint failed",
			`INSERT INTO steps (run_id, name, idx, state) VALUES ('no-such-run', 'build', 0, 'pending')`,
		},
		{
			"one frozen edge twice",
			"UNIQUE constraint failed: step_deps.run_id, step_deps.step_name, step_deps.depends_on",
			`INSERT INTO step_deps (run_id, step_name, depends_on) VALUES ('01J0RUN1', 'test', 'build')`,
		},
		{
			"one artifact name twice in one run",
			"UNIQUE constraint failed: artifacts.run_id, artifacts.name",
			`INSERT INTO artifacts (id, run_id, name, uri, created_at)
				VALUES ('01J0ART2', '01J0RUN1', 'binary', 'file:///tmp/b', 2003)`,
		},
		{
			"event on a run that does not exist",
			"FOREIGN KEY constraint failed",
			`INSERT INTO run_events (run_id, at, kind) VALUES ('no-such-run', 2001, 'run.queued')`,
		},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := seededStore(t)
			_, err := s.w.ExecContext(ctx, tc.stmt)
			if err == nil {
				t.Fatalf("the database accepted the row, want %q:\n%s", tc.want, tc.stmt)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the database refused the row with %q, want it to name %q:\n%s",
					err, tc.want, tc.stmt)
			}
		})
	}
}

// TestTicksAreUniquePerScheduleSlotAndFreeForSensors is the NULL trick from plan
// 07 section 3.3. One constraint carries two semantics: a schedule slot is a
// value and can be claimed once, a sensor evaluation is NULL and NULL is
// distinct from NULL in a unique index, so a sensor may tick without limit.
func TestTicksAreUniquePerScheduleSlotAndFreeForSensors(t *testing.T) {
	ctx := context.Background()
	s := seededStore(t)

	_, err := s.w.ExecContext(ctx,
		`INSERT INTO ticks (id, source_kind, source_name, scheduled_for, started_at, last_started_at, outcome)
			VALUES ('01J0TICK2', 'schedule', 'nightly', 2000, 5000, 5000, 'running')`)
	if err == nil {
		t.Fatal("the second tick for one schedule slot was accepted, want a unique constraint error")
	}

	for _, tickID := range []string{"01J0TICK3", "01J0TICK4"} {
		if _, err := s.w.ExecContext(ctx,
			`INSERT INTO ticks (id, source_kind, source_name, started_at, last_started_at, outcome)
				VALUES (?, 'sensor', 'inbox', 5000, 5000, 'running')`, tickID); err != nil {
			t.Fatalf("sensor tick %s rejected: %v", tickID, err)
		}
	}
}

// TestRunKeysSurviveTheirEpoch pins the dedup table's shape: WITHOUT ROWID, a
// three part key, and an epoch bump that makes the same key a new key. The
// epoch is what lets a sensor cursor be reset without the reset silently
// swallowing every trigger it produces.
func TestRunKeysSurviveTheirEpoch(t *testing.T) {
	ctx := context.Background()
	s := seededStore(t)

	var withoutRowid int
	if err := s.w.QueryRowContext(ctx,
		"SELECT wr FROM pragma_table_list WHERE name = 'run_keys'").Scan(&withoutRowid); err != nil {
		t.Fatalf("read the table list: %v", err)
	}
	if withoutRowid != 1 {
		t.Error("run_keys has a rowid: it is a pure key lookup table and pays for an index level it never uses")
	}

	if _, err := s.w.ExecContext(ctx,
		`INSERT INTO run_keys (source_id, epoch, run_key, first_seen_at) VALUES ('inbox', 1, 'file-a', 3000)`,
	); err != nil {
		t.Fatalf("the same run key in a new epoch was rejected: %v", err)
	}

	var count int
	if err := s.w.QueryRowContext(ctx,
		"SELECT count(*) FROM run_keys WHERE source_id = 'inbox' AND run_key = 'file-a'").Scan(&count); err != nil {
		t.Fatalf("count run keys: %v", err)
	}
	if count != 2 {
		t.Errorf("the two epochs hold %d rows for one run key, want 2", count)
	}
}

// TestConcurrencyKeyBlocksOnlyActiveRuns is the enforcement itself: the partial
// unique index, not an application check. A finished run leaves the key, so the
// next run takes it without anyone deleting anything.
func TestConcurrencyKeyBlocksOnlyActiveRuns(t *testing.T) {
	ctx := context.Background()
	s := seededStore(t)

	newRun := `INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,
			concurrency_key, reason_code, created_at, updated_at)
		VALUES (?, 'nightly', '01J0VER1', 'manual', ?, 2001, 'nightly', ?, 2001, 2001)`

	if _, err := s.w.ExecContext(ctx, newRun, "01J0RUN2", "running", nil); err == nil {
		t.Fatal("a second active run took the concurrency key, want a unique constraint error")
	}

	if _, err := s.w.ExecContext(ctx,
		"UPDATE runs SET state = 'succeeded', reason_code = 'RUN_SUCCEEDED' WHERE id = '01J0RUN1'"); err != nil {
		t.Fatalf("finish the first run: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, newRun, "01J0RUN2", "queued", nil); err != nil {
		t.Fatalf("a finished run still blocks the concurrency key: %v", err)
	}
}

// TestHotQueriesUseTheirIndexes pins the query plans the daemon depends on. A
// plan that degenerates to a table scan is invisible until the history is large
// enough to matter, which is exactly when nobody wants to find out.
func TestHotQueriesUseTheirIndexes(t *testing.T) {
	cases := []struct {
		name  string
		index string
		query string
	}{
		{
			"claim", "idx_runs_claim",
			`SELECT id FROM runs WHERE state = 'queued' AND available_at <= 5000 ORDER BY available_at, id LIMIT 10`,
		},
		{
			"reaper", "idx_runs_reaper",
			`SELECT id FROM runs WHERE state = 'running' AND lease_expires_at < 5000`,
		},
		{
			// Admission control reads the whole occupancy map in one query, so
			// this is the shape that matters. A count for one job is answered
			// from idx_runs_history instead, which is a lookup either way.
			"concurrency count", "idx_runs_active",
			`SELECT job_name, count(*) FROM runs WHERE state IN ('queued', 'running') GROUP BY job_name`,
		},
		{
			"history listing", "idx_runs_history",
			`SELECT id FROM runs WHERE job_name = 'nightly' ORDER BY id DESC LIMIT 50`,
		},
		{
			"retention sweep", "idx_runs_finished",
			`SELECT id FROM runs WHERE finished_at IS NOT NULL AND finished_at < 5000 ORDER BY finished_at LIMIT 500`,
		},
		{
			"due schedules", "idx_schedules_due",
			`SELECT id FROM schedules WHERE paused = 0 AND next_tick_at <= 5000 ORDER BY next_tick_at`,
		},
		{
			"due sensors", "idx_sensors_due",
			`SELECT name FROM sensors WHERE paused = 0 AND next_eval_at <= 5000 ORDER BY next_eval_at`,
		},
		{
			"next step of a run", "idx_steps_claimable",
			`SELECT name FROM steps WHERE run_id = '01J0RUN1' AND state = 'pending' ORDER BY idx LIMIT 1`,
		},
		{
			"steps waiting for a retry", "idx_steps_retry",
			`SELECT run_id, name FROM steps WHERE state = 'pending' AND next_attempt_at <= 5000`,
		},
		{
			"the events of one run", "idx_run_events_run",
			`SELECT kind FROM run_events WHERE run_id = '01J0RUN1' ORDER BY id`,
		},
		{
			"the triggers of one tick", "idx_triggers_tick",
			`SELECT id FROM triggers WHERE tick_id = '01J0TICK1'`,
		},
		{
			// Naming a run by an id prefix is a range scan on the primary key,
			// the way a git object is named. It must never read a row that
			// cannot match, however long the history is.
			"run lookup by id prefix", "sqlite_autoindex_runs_1",
			`SELECT id FROM runs WHERE id >= '01J0RUN0' AND id < '01J0RUN2' ORDER BY id LIMIT 2`,
		},
		{
			"the versions of one job", "idx_job_versions_job",
			`SELECT id FROM job_versions WHERE job_name = 'nightly' ORDER BY version DESC LIMIT 1`,
		},
		{
			"what a step feeds", "idx_step_deps_upstream",
			`SELECT step_name FROM step_deps WHERE run_id = '01J0RUN1' AND depends_on = 'build'`,
		},
	}

	s := seededStore(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := queryPlan(t, s, tc.query)
			if !strings.Contains(plan, tc.index) {
				t.Fatalf("the %s query does not use %s:\n%s", tc.name, tc.index, plan)
			}
		})
	}
}

// TestDeletingARunTakesItsChildrenAndLeavesTheDedupKeys is what retention rests
// on. The children go with the run through ON DELETE CASCADE, so no sweep has
// to know they exist, and run_keys stays: it outlives runs by design, because
// deleting a key lets an old trigger fire again.
func TestDeletingARunTakesItsChildrenAndLeavesTheDedupKeys(t *testing.T) {
	ctx := context.Background()
	s := seededStore(t)

	if _, err := s.w.ExecContext(ctx, "DELETE FROM runs WHERE id = '01J0RUN1'"); err != nil {
		t.Fatalf("delete the run: %v", err)
	}

	for _, table := range []string{"steps", "step_deps", "run_events", "artifacts"} {
		var count int
		if err := s.w.QueryRowContext(ctx,
			"SELECT count(*) FROM "+table+" WHERE run_id = '01J0RUN1'").Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("%s holds %d rows for the deleted run, want none", table, count)
		}
	}

	assertForeignKeysHold(t, s)

	var keys int
	if err := s.w.QueryRowContext(ctx, "SELECT count(*) FROM run_keys").Scan(&keys); err != nil {
		t.Fatalf("count run_keys: %v", err)
	}
	if keys != 1 {
		t.Errorf("run_keys holds %d rows after the run was deleted, want 1", keys)
	}

	// The trigger keeps its row and loses its pointer, so the decision history
	// still says a run was created and explain can say it has been swept.
	var runID any
	if err := s.w.QueryRowContext(ctx,
		"SELECT run_id FROM triggers WHERE id = '01J0TRG1'").Scan(&runID); err != nil {
		t.Fatalf("read the trigger: %v", err)
	}
	if runID != nil {
		t.Errorf("the trigger still points at %v, want NULL", runID)
	}
}

// TestForeignKeysHoldAfterASeededMigration is the criterion stated as a query:
// no row in the database points at a parent that is not there.
func TestForeignKeysHoldAfterASeededMigration(t *testing.T) {
	assertForeignKeysHold(t, seededStore(t))
}

// assertForeignKeysHold fails naming every row that points at a missing parent.
func assertForeignKeysHold(t *testing.T, s *Store) {
	t.Helper()

	ctx := context.Background()
	rows, err := s.w.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("foreign key check: %v", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var table, parent any
		var rowid, constraint any
		if err := rows.Scan(&table, &rowid, &parent, &constraint); err != nil {
			t.Fatalf("scan the foreign key check: %v", err)
		}
		t.Errorf("table %v holds a row pointing at a missing %v", table, parent)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("foreign key check: %v", err)
	}
}

// queryPlan is the plan SQLite would run, joined into one string per row so a
// failure prints what it actually chose.
func queryPlan(t *testing.T, s *Store, query string, args ...any) string {
	t.Helper()

	rows, err := s.w.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("explain %s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	var plan strings.Builder
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		plan.WriteString(detail + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read plan: %v", err)
	}
	return plan.String()
}
