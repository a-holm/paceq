package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// The status read path (M5-03, issue #30). Like the explain reads, everything
// here is a fixed set of single statements against the read pool: no
// transaction, no OFFSET, and above all no query whose count grows with the
// number of jobs. Status is the command an MOTD script runs every minute, so
// the whole overview costs seven driver-level statements however many jobs
// the project holds.
//
// These methods exist so the CLI never writes SQL: SQL lives in internal/store
// alone, and internal/status is a presentation layer over these rows.
//
// Every statement is exposed as a constant or function so the query-plan test
// can run EXPLAIN QUERY PLAN on exactly the text production executes.

// queryTraceHook is a test seam: store tests set it to count driver-level
// statements while a read path runs. It is nil in production, where the call
// is one nil check per query.
var queryTraceHook func(statement string)

// traceQuery reports one statement to the hook when a test installed one.
func traceQuery(statement string) {
	if queryTraceHook != nil {
		queryTraceHook(statement)
	}
}

// StatusJobRow is what the overview says about one job: its newest finished
// run beside it. A job that has never finished a run carries an empty RunID;
// a paused job says so through Paused rather than by disappearing.
type StatusJobRow struct {
	JobName string
	Paused  bool

	RunID      string
	RunState   string // terminal state of the newest finished run
	ReasonCode string
	StartedAt  time.Time
	FinishedAt time.Time

	// DurationMS is the duration the run wrote at finish time. HasDuration
	// distinguishes "took zero milliseconds" from "no duration recorded",
	// which matters to a table cell.
	DurationMS  int64
	HasDuration bool
}

// statusJobsCoreSQL is the join every per-job read shares; the exported
// statements hang their WHERE and ORDER BY off it so the plan test sees the
// exact text either way.
//
// The correlated subquery is one seek per job on idx_runs_job_finished
// (job_name, finished_at DESC, id DESC): SQLite resolves it inside this
// single statement, which is why the driver-level query count stays at one
// however many jobs exist. Ordering by finished_at DESC, id DESC comes
// straight off the index; the id tail makes ties between same-millisecond
// finishes deterministic, which a frozen test clock produces constantly.
const statusJobsCoreSQL = `SELECT j.name, j.paused,
r.id, r.state, COALESCE(r.reason_code, ''),
r.started_at, r.finished_at, r.duration_ms
FROM jobs j
LEFT JOIN runs r ON r.id = (
	SELECT r2.id FROM runs r2
	WHERE r2.job_name = j.name AND r2.finished_at IS NOT NULL
	ORDER BY r2.finished_at DESC, r2.id DESC
	LIMIT 1)`

// statusJobsSQL reads one row per job with its newest finished run beside it.
const statusJobsSQL = statusJobsCoreSQL + "\nORDER BY j.name"

// statusOneJobSQL narrows the same answer to one job.
const statusOneJobSQL = statusJobsCoreSQL + "\nWHERE j.name = ?\nORDER BY j.name"

// StatusJobs reads the per-job rows the overview renders. An empty jobName
// reads every job; a name narrows the answer to one. Jobs come back in name
// order whether or not they ever finished a run.
func (s *Store) StatusJobs(ctx context.Context, jobName string) ([]StatusJobRow, error) {
	statement, args := statusJobsSQL, []any{}
	if jobName != "" {
		statement, args = statusOneJobSQL, []any{jobName}
	}
	traceQuery(statement)

	rows, err := s.r.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("read the status of the jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]StatusJobRow, 0)
	for rows.Next() {
		var (
			row               StatusJobRow
			paused            int
			runID, runState   sql.NullString
			reasonCode        sql.NullString
			started, finished sql.NullInt64
			duration          sql.NullInt64
		)
		if err := rows.Scan(&row.JobName, &paused, &runID, &runState,
			&reasonCode, &started, &finished, &duration); err != nil {
			return nil, fmt.Errorf("scan a job's status row: %w", err)
		}
		row.Paused = paused != 0
		row.RunID = runID.String
		row.RunState = runState.String
		row.ReasonCode = reasonCode.String
		if started.Valid {
			row.StartedAt = time.UnixMilli(started.Int64).UTC()
		}
		if finished.Valid {
			row.FinishedAt = time.UnixMilli(finished.Int64).UTC()
		}
		if duration.Valid {
			row.DurationMS = duration.Int64
			row.HasDuration = true
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the status of the jobs: %w", err)
	}
	return out, nil
}

// statusNextTicksSQL reads the next due moment of every standing schedule,
// already materialised by the scheduler (M2-05): status recomputes nothing.
// The schedules table holds one row per declared schedule, so this is a
// catalogue read, never a history walk.
const statusNextTicksSQL = `SELECT job_name, MIN(next_tick_at)
FROM schedules
WHERE paused = 0
GROUP BY job_name`

// StatusNextTicks reads the earliest pending fire-time per job, in
// milliseconds since the epoch. A job with no standing schedule is absent.
func (s *Store) StatusNextTicks(ctx context.Context) (map[string]int64, error) {
	traceQuery(statusNextTicksSQL)
	rows, err := s.r.QueryContext(ctx, statusNextTicksSQL)
	if err != nil {
		return nil, fmt.Errorf("read the next scheduled runs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]int64)
	for rows.Next() {
		var (
			job  string
			next int64
		)
		if err := rows.Scan(&job, &next); err != nil {
			return nil, fmt.Errorf("scan a next-run time: %w", err)
		}
		out[job] = next
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the next scheduled runs: %w", err)
	}
	return out, nil
}

// statusStateCountsSQL counts active work off the claim-side partial index:
// queued and running are the two states a monitor asks about, and the index
// holds nothing else.
const statusStateCountsSQL = `SELECT state, COUNT(*)
FROM runs
WHERE state IN ('queued', 'running')
GROUP BY state`

// StatusStateCounts reads how many runs are queued and how many are running
// right now. Absent states count zero.
func (s *Store) StatusStateCounts(ctx context.Context) (queued, running int, err error) {
	traceQuery(statusStateCountsSQL)
	rows, err := s.r.QueryContext(ctx, statusStateCountsSQL)
	if err != nil {
		return 0, 0, fmt.Errorf("read the active run counts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			state string
			n     int
		)
		if err := rows.Scan(&state, &n); err != nil {
			return 0, 0, fmt.Errorf("scan an active run count: %w", err)
		}
		switch state {
		case "queued":
			queued = n
		case "running":
			running = n
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("read the active run counts: %w", err)
	}
	return queued, running, nil
}

// statusStuckSQL finds the runs the executor lost: still marked running, but
// their lease ran out, so nobody is coming back to finish them. The reaper
// collects these eventually; until it does, status names them, because a run
// that died mid-flight is exactly what the morning view exists to surface.
const statusStuckSQL = `SELECT job_name, COUNT(*)
FROM runs
WHERE state = 'running'
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at < ?
GROUP BY job_name`

// StatusStuckCounts reads, per job, how many running runs hold an expired
// lease as of now. A job absent from the map has nothing stuck.
func (s *Store) StatusStuckCounts(ctx context.Context, now time.Time) (map[string]int, error) {
	traceQuery(statusStuckSQL)
	rows, err := s.r.QueryContext(ctx, statusStuckSQL, now.UTC().UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("read the stuck runs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]int)
	for rows.Next() {
		var (
			job string
			n   int
		)
		if err := rows.Scan(&job, &n); err != nil {
			return nil, fmt.Errorf("scan a stuck-run count: %w", err)
		}
		out[job] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the stuck runs: %w", err)
	}
	return out, nil
}

// statusSensorErrorsSQL counts sensors currently in the failure path, per
// job: consecutive failures without a success, or a breaker that tripped.
// Sensors are a catalogue table, so this is bounded by declarations, never by
// evaluation history.
const statusSensorErrorsSQL = `SELECT job_name, COUNT(*)
FROM sensors
WHERE consecutive_failures > 0 OR breaker_opened_at IS NOT NULL
GROUP BY job_name`

// StatusSensorErrorCounts reads, per job, how many of its sensors are in
// error. A job absent from the map has healthy sensors (or none).
func (s *Store) StatusSensorErrorCounts(ctx context.Context) (map[string]int, error) {
	traceQuery(statusSensorErrorsSQL)
	rows, err := s.r.QueryContext(ctx, statusSensorErrorsSQL)
	if err != nil {
		return nil, fmt.Errorf("read the sensor failures: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]int)
	for rows.Next() {
		var (
			job string
			n   int
		)
		if err := rows.Scan(&job, &n); err != nil {
			return nil, fmt.Errorf("scan a sensor-failure count: %w", err)
		}
		out[job] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the sensor failures: %w", err)
	}
	return out, nil
}

// statusSensorIntervalsSQL reads the evaluation cadence of each sensor-driven
// job's standing sensors. The overview's NEXT column shows "sensor every 60s"
// for a job with no schedule but a live sensor: its next chance to run is the
// sensor's next evaluation.
const statusSensorIntervalsSQL = `SELECT job_name, MIN(interval_ms)
FROM sensors
WHERE paused = 0
GROUP BY job_name`

// StatusSensorIntervals reads the shortest live evaluation interval per
// sensor-driven job, in milliseconds. A job with no unpaused sensor is
// absent.
func (s *Store) StatusSensorIntervals(ctx context.Context) (map[string]int64, error) {
	traceQuery(statusSensorIntervalsSQL)
	rows, err := s.r.QueryContext(ctx, statusSensorIntervalsSQL)
	if err != nil {
		return nil, fmt.Errorf("read the sensor intervals: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]int64)
	for rows.Next() {
		var (
			job      string
			interval int64
		)
		if err := rows.Scan(&job, &interval); err != nil {
			return nil, fmt.Errorf("scan a sensor interval: %w", err)
		}
		out[job] = interval
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the sensor intervals: %w", err)
	}
	return out, nil
}

// statusOpenSessionSQL reads the newest session the daemon has not closed.
// started_at is the uptime anchor and version answers "which binary".
const statusOpenSessionSQL = `SELECT version, started_at
FROM daemon_sessions
WHERE stopped_at IS NULL
ORDER BY started_at DESC
LIMIT 1`

// StatusOpenSession reads the daemon's newest open session. found is false
// when no session is open, which is the database's half of the daemon-down
// answer; the live socket dial is the caller's half.
func (s *Store) StatusOpenSession(ctx context.Context) (version string, since time.Time, found bool, err error) {
	traceQuery(statusOpenSessionSQL)
	var (
		versionNull sql.NullString
		startedAt   int64
	)
	scanErr := s.r.QueryRowContext(ctx, statusOpenSessionSQL).Scan(&versionNull, &startedAt)
	if scanErr != nil {
		if scanErr == sql.ErrNoRows {
			return "", time.Time{}, false, nil
		}
		return "", time.Time{}, false, fmt.Errorf("read the open daemon session: %w", scanErr)
	}
	return versionNull.String, time.UnixMilli(startedAt).UTC(), true, nil
}

// StatusQueries lists the exact statement texts the status overview executes.
// The query-plan test walks this list and refuses any plan that scans a
// history table, so an index regression fails a test instead of an operator's
// morning.
func StatusQueries() []string {
	return []string{
		statusJobsSQL,
		statusNextTicksSQL,
		statusSensorIntervalsSQL,
		statusStateCountsSQL,
		statusStuckSQL,
		statusSensorErrorsSQL,
		statusOpenSessionSQL,
	}
}
