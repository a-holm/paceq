package store

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// The store half of the atomic sensor commit (issue #4, M3-03). These tests
// hold the guarantee G4: the cursor never advances without every trigger and
// run derived from it being committed in the same transaction, and a crash
// anywhere in that transaction leaves the database as if it never ran.
//
// The seam is the two methods M3-02's evaluator runtime calls: BeginSensorTick
// (the intention row) and CommitSensorTick (the whole commit). The evaluator
// itself lives in internal/sensor and is built by the M3-02 issue, which is
// why these tests speak only to the store.

const (
	sensorJob  = "polling-job"
	sensorName = "finder"
)

// seedSensorJob creates the job a sensor row points at; the schema refuses a
// sensor without one.
func seedSensorJob(t *testing.T, s *Store) {
	t.Helper()
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:       sensorJob,
		SpecHash:      "sha256:sensor-seed",
		SpecJSON:      `{"schema":"paceq.job.v1","name":"polling-job","steps":[{"name":"collect","run":["true"]}]}`,
		MaxConcurrent: 10,
	}); err != nil {
		t.Fatalf("seed the sensor job: %v", err)
	}
}

// seedSensor inserts one sensor row directly. There is no store method for it
// yet: the apply path that materialises sensor rows belongs to M3-01, which is
// not merged, so these tests seed the exact columns CommitSensorTick reads.
func seedSensor(t *testing.T, s *Store, name, cursor string, version int64) {
	t.Helper()
	_, err := s.w.Exec(`INSERT INTO sensors
(name, job_name, kind, exec_json, interval_ms, min_interval_ms, timeout_ms,
 max_triggers_per_tick, cursor, cursor_version, next_eval_at, created_at, updated_at)
VALUES (?, ?, 'exec', '["cat"]', 60000, 1000, 30000, 100, ?, ?, 0, 1, 1)`,
		name, sensorJob, nullString(cursor), version)
	if err != nil {
		t.Fatalf("seed sensor %s: %v", name, err)
	}
}

// readSensor returns the cursor and cursor_version columns of one sensor, for
// assertions that the cursor moved exactly when it should have.
func readSensor(t *testing.T, s *Store, name string) (cursor string, version int64) {
	t.Helper()
	ctx := context.Background()
	var c sql.NullString
	if err := s.r.QueryRowContext(ctx, "SELECT cursor, cursor_version FROM sensors WHERE name = ?",
		name).Scan(&c, &version); err != nil {
		t.Fatalf("read sensor %s: %v", name, err)
	}
	return c.String, version
}

// sensorCursorStamped reports whether cursor_updated_at is set. A seeded row
// leaves it NULL, so a stamp means the commit under test wrote one.
func sensorCursorStamped(t *testing.T, s *Store, name string) bool {
	t.Helper()
	var at sql.NullInt64
	if err := s.r.QueryRowContext(context.Background(),
		"SELECT cursor_updated_at FROM sensors WHERE name = ?", name).Scan(&at); err != nil {
		t.Fatalf("read cursor_updated_at of %s: %v", name, err)
	}
	return at.Valid
}

// tickOutcome reads the outcome, cursor_after, trigger_count and deduped_count
// of one tick.
func tickOutcome(t *testing.T, s *Store, tickID string) (outcome, cursorAfter string, triggers, deduped int) {
	t.Helper()
	ctx := context.Background()
	var ca sql.NullString
	if err := s.r.QueryRowContext(ctx, "SELECT outcome, cursor_after, trigger_count, deduped_count FROM ticks WHERE id = ?",
		tickID).Scan(&outcome, &ca, &triggers, &deduped); err != nil {
		t.Fatalf("read tick %s: %v", tickID, err)
	}
	return outcome, ca.String, triggers, deduped
}

// countSensorRuns counts runs with origin 'sensor' for the job, regardless of
// run key.
func countSensorRuns(t *testing.T, s *Store) int {
	t.Helper()
	ctx := context.Background()
	var n int
	if err := s.r.QueryRowContext(ctx, "SELECT COUNT(*) FROM runs WHERE origin = 'sensor'").Scan(&n); err != nil {
		t.Fatalf("count sensor runs: %v", err)
	}
	return n
}

// A basic triggered commit fixture: one sensor at cursor "a", one run key
// "file:1". Returns the tick id and the next cursor_version for reuse.
func commitOne(t *testing.T, s *Store, tickID, runKey string, cursorVersion int64) SensorTickCommitResult {
	t.Helper()
	out, err := s.CommitSensorTick(context.Background(), SensorTickCommitInput{
		TickID:        tickID,
		SensorName:    sensorName,
		JobName:       sensorJob,
		CursorVersion: cursorVersion,
		CursorAfter:   "b",
		DedupEpoch:    0,
		Triggers:      []SensorTrigger{{RunKey: runKey}},
		Outcome:       OutcomeTriggered,
		NextEvalAt:    60000,
		DurationMs:    12,
	})
	if err != nil {
		t.Fatalf("commit sensor tick: %v", err)
	}
	return out
}

// TestSensorCommitWritesAllRowsInOneTransaction is the happy path: a
// triggered evaluation lands its tick outcome, its trigger, its accepted run,
// its run_key and its queued event together, and the cursor moves.
func TestSensorCommitWritesAllRowsInOneTransaction(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)
	ctx := context.Background()

	begin, err := s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName: sensorName, CursorBefore: "a",
	})
	if err != nil {
		t.Fatalf("begin sensor tick: %v", err)
	}
	if begin.TickID == "" || begin.CursorVersion != 0 {
		t.Fatalf("begin returned tick %q version %d, want a tick and version 0", begin.TickID, begin.CursorVersion)
	}

	out := commitOne(t, s, begin.TickID, "file:1", 0)

	if out.Fenced || out.Accepted != 1 || out.Deduped != 0 || len(out.RunIDs) != 1 {
		t.Fatalf("commit result = accepted %d deduped %d fenced %v runs %d, want 1/0/false/1",
			out.Accepted, out.Deduped, out.Fenced, len(out.RunIDs))
	}

	outcome, cursorAfter, triggers, deduped := tickOutcome(t, s, begin.TickID)
	if outcome != OutcomeTriggered || cursorAfter != "b" || triggers != 1 || deduped != 0 {
		t.Errorf("tick = %s/%s/%d/%d, want triggered/b/1/0", outcome, cursorAfter, triggers, deduped)
	}

	if curs, version := readSensor(t, s, sensorName); curs != "b" || version != 1 {
		t.Errorf("sensor cursor = %q version %d, want b/1", curs, version)
	}
	if got := countSensorRuns(t, s); got != 1 {
		t.Errorf("sensor runs = %d, want 1", got)
	}
	assertOneQueuedEvent(t, s, out.RunIDs[0])
}

// TestSensorCommitCursorMovesOnlyOnATriggeredEvaluation pins the two writers of
// one fact to one answer. sensors.cursor and ticks.cursor_after are written by
// the same commit, so a row that holds a cursor no tick recorded is work that
// nothing will ever evaluate: the window between the two cursors is skipped
// silently, and the tick says the cursor stood still. The case that opens it is
// a sensor reporting a watermark while it skips, which is what the out contract
// invites a sensor to do.
func TestSensorCommitCursorMovesOnlyOnATriggeredEvaluation(t *testing.T) {
	cases := []struct {
		name       string
		outcome    string
		code       reason.Code
		reported   string
		triggers   []SensorTrigger
		wantCursor string
		wantRuns   int
	}{
		{
			name:       "triggered with a cursor moves it",
			outcome:    OutcomeTriggered,
			reported:   "b",
			triggers:   []SensorTrigger{{RunKey: "file:1"}},
			wantCursor: "b",
			wantRuns:   1,
		},
		{
			name:       "triggered without a cursor keeps the old one",
			outcome:    OutcomeTriggered,
			triggers:   []SensorTrigger{{RunKey: "file:1"}},
			wantCursor: "a",
			wantRuns:   1,
		},
		{
			name:       "skipped with a reported watermark keeps the old cursor",
			outcome:    OutcomeSkipped,
			code:       reason.TICKSkippedSensor,
			reported:   "zzz-new-cursor",
			wantCursor: "a",
		},
		{
			name:       "skipped without a cursor keeps the old one",
			outcome:    OutcomeSkipped,
			code:       reason.TICKSkippedSensor,
			wantCursor: "a",
		},
		{
			name:       "errored keeps the old cursor whatever it carries",
			outcome:    OutcomeError,
			code:       reason.TICKErrorSensorFailed,
			reported:   "zzz-new-cursor",
			wantCursor: "a",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := migratedStore(t)
			seedSensorJob(t, s)
			seedSensor(t, s, sensorName, "a", 0)
			ctx := context.Background()

			begin, err := s.BeginSensorTick(ctx, BeginSensorTickInput{
				SensorName: sensorName, CursorBefore: "a",
			})
			if err != nil {
				t.Fatalf("begin sensor tick: %v", err)
			}
			out, err := s.CommitSensorTick(ctx, SensorTickCommitInput{
				TickID:        begin.TickID,
				SensorName:    sensorName,
				JobName:       sensorJob,
				CursorVersion: begin.CursorVersion,
				CursorAfter:   tc.reported,
				Triggers:      tc.triggers,
				Outcome:       tc.outcome,
				ReasonCode:    tc.code,
				ReasonText:    "nothing new",
				NextEvalAt:    60000,
			})
			if err != nil {
				t.Fatalf("commit sensor tick: %v", err)
			}
			if out.Fenced {
				t.Fatalf("a fresh commit was fenced; there is no concurrent writer")
			}

			cursor, version := readSensor(t, s, sensorName)
			outcome, cursorAfter, _, _ := tickOutcome(t, s, begin.TickID)

			// The agreement, whichever way the rule goes: a tick that
			// recorded a move names the cursor the row now holds, and a tick
			// that recorded none left the row where it was.
			switch {
			case cursorAfter == "" && cursor != "a":
				t.Errorf("sensors.cursor = %q but the tick recorded no move", cursor)
			case cursorAfter != "" && cursorAfter != cursor:
				t.Errorf("ticks.cursor_after = %q but sensors.cursor = %q", cursorAfter, cursor)
			}

			// Which way it goes: only a triggered evaluation moves it.
			if cursor != tc.wantCursor {
				t.Errorf("sensors.cursor = %q, want %q", cursor, tc.wantCursor)
			}
			if outcome != tc.outcome {
				t.Errorf("ticks.outcome = %q, want %q", outcome, tc.outcome)
			}
			// The fence is unconditional: every decided evaluation bumps it.
			if version != 1 {
				t.Errorf("sensors.cursor_version = %d, want 1", version)
			}
			// The timestamp mirrors the cursor, never the commit.
			if stamped := sensorCursorStamped(t, s, sensorName); stamped != (cursor != "a") {
				t.Errorf("cursor_updated_at stamped = %v, want %v", stamped, cursor != "a")
			}
			if got := countSensorRuns(t, s); got != tc.wantRuns {
				t.Errorf("sensor runs = %d, want %d", got, tc.wantRuns)
			}
		})
	}
}

// TestSensorCommitCASRejectsAStaleEvaluation covers the cursor CAS guard: a
// commit carrying a cursor_version the sensor has already moved past is
// refused, writes no runs, and the tick records TICK_MISSED_LEASE_LOST.
func TestSensorCommitCASRejectsAStaleEvaluation(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)
	ctx := context.Background()

	// First evaluation reads version 0 and commits.
	begin, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: sensorName, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out := commitOne(t, s, begin.TickID, "file:1", 0); out.Fenced {
		t.Fatal("first commit was fenced unexpectedly")
	}

	// A second, supposedly concurrent evaluation started from the SAME
	// version 0 that is now stale; its commit must be refused.
	stale, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: sensorName, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin the stale evaluation: %v", err)
	}
	out, err := s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        stale.TickID,
		SensorName:    sensorName,
		JobName:       sensorJob,
		CursorVersion: 0, // stale: the sensor is already at 1
		CursorAfter:   "z",
		Triggers:      []SensorTrigger{{RunKey: "file:2"}},
		Outcome:       OutcomeTriggered,
		NextEvalAt:    60000,
	})
	if err != nil {
		t.Fatalf("commit the stale evaluation: %v", err)
	}
	if !out.Fenced {
		t.Fatal("stale evaluation was not fenced")
	}
	if out.RunIDs != nil || out.Accepted != 0 {
		t.Fatalf("fenced commit returned runs %v accepted %d, want none", out.RunIDs, out.Accepted)
	}

	if curs, version := readSensor(t, s, sensorName); curs != "b" || version != 1 {
		t.Errorf("sensor cursor = %q version %d, want b/1 (the winner, not the stale commit)", curs, version)
	}
	if got := countSensorRuns(t, s); got != 1 {
		t.Errorf("sensor runs = %d, want 1 (only the winner's run)", got)
	}

	outcome, _, _, _ := tickOutcome(t, s, stale.TickID)
	if outcome != OutcomeError {
		t.Errorf("stale tick outcome = %q, want error (TICK_MISSED_LEASE_LOST)", outcome)
	}
}

// TestSensorCommitReplayConvergesToExactlyOneRun is the crash replay proof at
// the API level: committing the same logical evaluation twice (what a restart
// after a mid-commit kill does) creates exactly one run, never a duplicate.
func TestSensorCommitReplayConvergesToExactlyOneRun(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)
	ctx := context.Background()

	first, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: sensorName, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out := commitOne(t, s, first.TickID, "file:1", 0); out.Accepted != 1 {
		t.Fatalf("first commit accepted %d, want 1", out.Accepted)
	}

	// A replayed evaluation (as after a restart from the old cursor) folds
	// into the run that already exists.
	replay, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: sensorName, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin replay: %v", err)
	}
	out, err := s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        replay.TickID,
		SensorName:    sensorName,
		JobName:       sensorJob,
		CursorVersion: 1,
		CursorAfter:   "b",
		DedupEpoch:    0,
		Triggers:      []SensorTrigger{{RunKey: "file:1"}},
		Outcome:       OutcomeTriggered,
		NextEvalAt:    60000,
	})
	if err != nil {
		t.Fatalf("commit replay: %v", err)
	}
	if out.Accepted != 0 || out.Deduped != 1 || out.RunIDs != nil {
		t.Fatalf("replay = accepted %d deduped %d runs %v, want 0/1/none", out.Accepted, out.Deduped, out.RunIDs)
	}
	if got := countSensorRuns(t, s); got != 1 {
		t.Errorf("sensor runs after replay = %d, want exactly 1 (no duplicate)", got)
	}

	outcome, _, triggers, deduped := tickOutcome(t, s, replay.TickID)
	if outcome != OutcomeTriggered || triggers != 0 || deduped != 1 {
		t.Errorf("replay tick = %s (triggers %d deduped %d), want triggered with 0 accepted and 1 deduped",
			outcome, triggers, deduped)
	}
}

// assertOneQueuedEvent verifies exactly one run.queued event exists for a run,
// written in the same transaction as the run (G10).
func assertOneQueuedEvent(t *testing.T, s *Store, runID string) {
	t.Helper()
	var n int
	if err := s.r.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM run_events WHERE run_id = ? AND kind = 'run.queued'",
		runID).Scan(&n); err != nil {
		t.Fatalf("count events for run %s: %v", runID, err)
	}
	if n != 1 {
		t.Fatalf("run %s has %d run.queued events, want exactly 1", runID, n)
	}
}

// TestSensorCommitCrashLeavesNoPartialState kills a real subprocess mid
// CommitSensorTick (after the runs landed, before COMMIT) and proves the
// transaction rolled back whole: integrity holds, no run, trigger, run_key or
// cursor change survives, and a replay after restart converges to exactly one
// run. This is the SIGKILL-mid-commit guarantee the milestone exit criterion
// is measured on.
func TestSensorCommitCrashLeavesNoPartialState(t *testing.T) {
	if os.Getenv("PACEQ_SENSOR_CRASH_DB") != "" {
		t.Skip("child process, driven by the parent test")
	}

	path := filepath.Join(t.TempDir(), "state.db")
	cmd := exec.Command(os.Args[0], "-test.run=TestSensorCommitCrashChild", "-test.count=1")
	cmd.Env = append(os.Environ(), "PACEQ_SENSOR_CRASH_DB="+path)
	out, err := cmd.CombinedOutput()

	var status *exec.ExitError
	if !asExitError(err, &status) || status.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child exited with %v, want death by SIGKILL\n%s", err, out)
	}

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("reopen the crashed database: %v", err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	var integrity string
	if err := s.w.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity_check reports %q, want ok", integrity)
	}

	if got := countSensorRuns(t, s); got != 0 {
		t.Errorf("sensor runs survive the mid-commit crash: %d, want 0 (partial run)", got)
	}
	if curs, version := readSensor(t, s, sensorName); curs != "a" || version != 0 {
		t.Errorf("sensor cursor after crash = %q version %d, want a/0 (never advanced)", curs, version)
	}
	// The run key gate must be empty too: a rolled back commit registers no key.
	var keys int
	if err := s.r.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_keys WHERE source_id = ?", sensorName).Scan(&keys); err != nil {
		t.Fatalf("count run_keys: %v", err)
	}
	if keys != 0 {
		t.Errorf("run_keys survive the crash: %d, want 0", keys)
	}

	// The replay after restart must converge to exactly one run, never zero
	// (the crashed commit lost nothing) and never two (it duplicated nothing).
	seedSensorJob(t, s)
	begin, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: sensorName, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin after crash: %v", err)
	}
	outResult := commitOne(t, s, begin.TickID, "file:1", 0)
	if outResult.Accepted != 1 {
		t.Fatalf("replay after crash accepted %d, want 1", outResult.Accepted)
	}
	if got := countSensorRuns(t, s); got != 1 {
		t.Errorf("sensor runs after crash replay = %d, want exactly 1", got)
	}
	if curs, version := readSensor(t, s, sensorName); curs != "b" || version != 1 {
		t.Errorf("sensor cursor after replay = %q version %d, want b/1", curs, version)
	}
}

// TestSensorCommitCrashChild is the child half of the crash test. It runs only
// under the parent, which sets the crash database path and then expects a
// SIGKILL on the hook set below.
func TestSensorCommitCrashChild(t *testing.T) {
	path := os.Getenv("PACEQ_SENSOR_CRASH_DB")
	if path == "" {
		t.Skip("driven by TestSensorCommitCrashLeavesNoPartialState")
	}

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)

	begin, err := s.BeginSensorTick(context.Background(), BeginSensorTickInput{
		SensorName: sensorName, CursorBefore: "a", Now: time.UnixMilli(1000),
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Arm the kill: the next CommitSensorTick dies after the runs land,
	// before COMMIT, with the transaction still open.
	sensorCommitHook = func() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) }
	_, _ = s.CommitSensorTick(context.Background(), SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    sensorName,
		JobName:       sensorJob,
		CursorVersion: 0,
		CursorAfter:   "b",
		Triggers:      []SensorTrigger{{RunKey: "file:1"}},
		Outcome:       OutcomeTriggered,
		NextEvalAt:    60000,
		Now:           time.UnixMilli(2000),
	})
	t.Fatal("the child survived its own SIGKILL")
}

// TestSensorCommitIntentionRowSurvives is the reconciliation seam: after a
// crash before any commit, the intention tick that BeginSensorTick wrote is
// still there in 'running', so the restart can see an evaluation was in flight.
func TestSensorCommitIntentionRowSurvives(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)

	begin, err := s.BeginSensorTick(context.Background(), BeginSensorTickInput{
		SensorName: sensorName, CursorBefore: "a",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	outcome, _, _, _ := tickOutcome(t, s, begin.TickID)
	if outcome != "running" {
		t.Errorf("intention tick outcome = %q, want running", outcome)
	}
}
