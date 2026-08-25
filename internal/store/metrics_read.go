package store

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/a-holm/paceq/internal/spec"
)

// The /metrics read side (#40). Every method here answers exactly one metric
// family with exactly one query, so a scrape costs a fixed number of
// statements however many jobs, schedules and sensors are configured - the
// same N+1 discipline the status work runs under. All of them read through
// the read-only pool via withRead, never through an explicit transaction: a
// scrape every fifteen seconds that held a read snapshot open would be the
// long lived reader that starves WAL checkpointing (07 section 6.4).
//
// The families follow the two-source rule (06 section 6.1): cumulative event
// counts live in memory and reach the scraper through MetricsHook; everything
// here is state that is true right now, read fresh at scrape time so it is
// correct immediately after a restart without any rebuild.

// MetaBackupLastSuccessAtMs is the meta key carrying the unix-millisecond
// stamp of the last successful backup. Nothing writes it yet - the backup
// machinery itself is M6 work - and the metrics side exposes the family only
// while the key exists, the same no-series-without-truth rule expected_within
// follows.
const MetaBackupLastSuccessAtMs = "backup_last_success_at_ms"

// MetaBackupVerifiedOk is the meta key carrying whether the last backup
// passed its verification ("1") or failed it ("0").
const MetaBackupVerifiedOk = "backup_verified_ok"

// MetaGCLastSuccessAtMs is the meta key carrying the unix-millisecond stamp
// of the last completed retention sweep.
const MetaGCLastSuccessAtMs = "gc_last_success_at_ms"

// MetricsJobStateCount is one cell of the runs grid: how many runs of one job
// sit in one state right now.
type MetricsJobStateCount struct {
	Job   string
	State string
	Count int64
}

// MetricsRunsByStates returns every non-empty cell of the runs grid in one
// grouped scan. Terminal states ride along so the run totals come from the
// same statement instead of a second pass.
func (s *Store) MetricsRunsByStates(ctx context.Context) ([]MetricsJobStateCount, error) {
	var out []MetricsJobStateCount
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT job_name, state, COUNT(*)
  FROM runs
 GROUP BY job_name, state`)
		if err != nil {
			return fmt.Errorf("count runs by job and state: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var c MetricsJobStateCount
			if err := rows.Scan(&c.Job, &c.State, &c.Count); err != nil {
				return fmt.Errorf("scan a runs-by-state count: %w", err)
			}
			out = append(out, c)
		}
		return rows.Err()
	})
	return out, err
}

// MetricsJobStamp is one timestamped fact about one job.
type MetricsJobStamp struct {
	Job string
	At  time.Time
}

// MetricsLastSuccesses returns the newest succeeded finished_at per job, in
// one grouped scan. A job without a success has no row: absence is the truth,
// and a fabricated zero would alarm on every healthy newcomer.
func (s *Store) MetricsLastSuccesses(ctx context.Context) ([]MetricsJobStamp, error) {
	var out []MetricsJobStamp
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT job_name, MAX(finished_at)
  FROM runs
 WHERE state = 'succeeded'
 GROUP BY job_name`)
		if err != nil {
			return fmt.Errorf("read the last success per job: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name string
			var milli int64
			if err := rows.Scan(&name, &milli); err != nil {
				return fmt.Errorf("scan a last-success stamp: %w", err)
			}
			out = append(out, MetricsJobStamp{Job: name, At: time.UnixMilli(milli)})
		}
		return rows.Err()
	})
	return out, err
}

// MetricsJobSLA is one job's declared freshness expectation, read back out of
// the frozen current version rather than mirrored into a column of its own:
// the versioned spec is the one place the expectation lives.
type MetricsJobSLA struct {
	Job     string
	Within  time.Duration
	Version string
}

// MetricsJobSLAs returns the freshness expectation of every current job
// version that declares one, in one joined scan. Jobs without the field have
// no row, which is what "no series" needs; the extraction reads one top-level
// key instead of paying for the whole FromIR decode at scrape time.
func (s *Store) MetricsJobSLAs(ctx context.Context) ([]MetricsJobSLA, error) {
	var out []MetricsJobSLA
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT j.name, j.current_version_id, v.spec_json
  FROM jobs j
  JOIN job_versions v ON v.id = j.current_version_id`)
		if err != nil {
			return fmt.Errorf("list current job versions for their SLA: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name, versionID, specJSON string
			if err := rows.Scan(&name, &versionID, &specJSON); err != nil {
				return fmt.Errorf("scan a job version for its SLA: %w", err)
			}
			within, ok, err := spec.ExpectedWithinFromIR([]byte(specJSON))
			if err != nil {
				// A version whose bytes cannot be read back was already
				// refused at apply; a scrape must not fail because history
				// surprised us. Skip the series rather than hang the scrape.
				continue
			}
			if !ok {
				continue
			}
			out = append(out, MetricsJobSLA{Job: name, Within: within, Version: versionID})
		}
		return rows.Err()
	})
	return out, err
}

// MetricsInstigatorState is the scrape-time state of one schedule or sensor
// row: what monitoring calls an instigator. Name is the row's own name (the
// schedule name within its job, or the sensor name), matching the label the
// tick counters use.
type MetricsInstigatorState struct {
	Name                string
	Paused              bool
	Cadence             time.Duration // sensors: their interval; schedules: filled by the caller
	CadenceKnown        bool
	LastTick            time.Time // zero when the row never ticked
	NextTick            time.Time
	CursorUpdatedAt     time.Time // sensors only; zero when never moved
	ConsecutiveFailures int64     // sensors only
}

// MetricsScheduleStates returns the state of every schedule row in one scan.
// Cadence stays unknown here: a cron expression has no stored interval, and
// computing one belongs to the collector, which owns the cron parser.
func (s *Store) MetricsScheduleStates(ctx context.Context) ([]MetricsInstigatorState, error) {
	var out []MetricsInstigatorState
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT name, paused, next_tick_at
  FROM schedules`)
		if err != nil {
			return fmt.Errorf("list schedule states: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var st MetricsInstigatorState
			var paused int
			var nextMilli int64
			if err := rows.Scan(&st.Name, &paused, &nextMilli); err != nil {
				return fmt.Errorf("scan a schedule state: %w", err)
			}
			st.Paused = paused == 1
			st.NextTick = time.UnixMilli(nextMilli)
			out = append(out, st)
		}
		return rows.Err()
	})
	return out, err
}

// MetricsSensorStates returns the state of every sensor row in one scan.
func (s *Store) MetricsSensorStates(ctx context.Context) ([]MetricsInstigatorState, error) {
	var out []MetricsInstigatorState
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT name, paused, interval_ms, next_eval_at,
       cursor_updated_at, consecutive_failures
  FROM sensors`)
		if err != nil {
			return fmt.Errorf("list sensor states: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var st MetricsInstigatorState
			var paused int
			var intervalMilli, nextMilli int64
			var cursorMilli *int64
			if err := rows.Scan(&st.Name, &paused, &intervalMilli, &nextMilli,
				&cursorMilli, &st.ConsecutiveFailures); err != nil {
				return fmt.Errorf("scan a sensor state: %w", err)
			}
			st.Paused = paused == 1
			st.Cadence = time.Duration(intervalMilli) * time.Millisecond
			st.CadenceKnown = true
			st.NextTick = time.UnixMilli(nextMilli)
			if cursorMilli != nil {
				st.CursorUpdatedAt = time.UnixMilli(*cursorMilli)
			}
			out = append(out, st)
		}
		return rows.Err()
	})
	return out, err
}

// MetricsSourceLag is how late the newest decided evaluation of one source
// fired relative to the fire-time it was claiming.
type MetricsSourceLag struct {
	Kind      string // "schedule" or "sensor"
	Name      string
	LagMillis int64
}

// MetricsTickLags returns, per source, the scheduled-for gap of its newest
// decided tick in one windowed scan. Running intention rows carry no decision
// yet and are excluded. The schedule rows store their source as
// "<job>/<schedule>"; both halves come from names the parser refuses "/" in,
// so the first slash splits them losslessly.
const metricsTickLagSQL = `SELECT source_kind, source_name, last_started_at - scheduled_for AS lag_ms
  FROM (SELECT source_kind, source_name, last_started_at, scheduled_for,
               ROW_NUMBER() OVER (PARTITION BY source_kind, source_name
                                  ORDER BY started_at DESC, id DESC) AS rn
          FROM ticks
         WHERE scheduled_for IS NOT NULL AND outcome <> 'running')
 WHERE rn = 1`

func (s *Store) MetricsTickLags(ctx context.Context) ([]MetricsSourceLag, error) {
	var out []MetricsSourceLag
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, metricsTickLagSQL)
		if err != nil {
			return fmt.Errorf("read the newest tick lag per source: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var l MetricsSourceLag
			if err := rows.Scan(&l.Kind, &l.Name, &l.LagMillis); err != nil {
				return fmt.Errorf("scan a tick lag: %w", err)
			}
			l.Name = metricsSourceName(l.Kind, l.Name)
			out = append(out, l)
		}
		return rows.Err()
	})
	return out, err
}

// MetricsSourceStamp is the newest activity stamp of one source.
type MetricsSourceStamp struct {
	Kind string
	Name string
	At   time.Time
}

// MetricsLastTicks returns the newest last_started_at per source in one
// grouped scan. This is the single source for last-tick stamps: the schedules
// table carries its own copy of the same instant, and exposing both would be
// the double bookkeeping this endpoint refuses.
func (s *Store) MetricsLastTicks(ctx context.Context) ([]MetricsSourceStamp, error) {
	var out []MetricsSourceStamp
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT source_kind, source_name, MAX(last_started_at)
  FROM ticks
 GROUP BY source_kind, source_name`)
		if err != nil {
			return fmt.Errorf("read the last tick per source: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var st MetricsSourceStamp
			var milli int64
			if err := rows.Scan(&st.Kind, &st.Name, &milli); err != nil {
				return fmt.Errorf("scan a last-tick stamp: %w", err)
			}
			st.At = time.UnixMilli(milli)
			st.Name = metricsSourceName(st.Kind, st.Name)
			out = append(out, st)
		}
		return rows.Err()
	})
	return out, err
}

// metricsSourceName strips the job prefix off a schedule's composite tick
// source name. Sensor ticks carry the bare sensor name and pass through; the
// parser refuses "/" inside any name, so the first slash is always the seam.
func metricsSourceName(kind, source string) string {
	const scheduleKind = "schedule"
	if kind != scheduleKind {
		return source
	}
	for i := 0; i < len(source); i++ {
		if source[i] == '/' {
			return source[i+1:]
		}
	}
	return source
}

// MetricsOutageSeconds returns the provable downtime total: the summed span
// of every recorded outage, in seconds. Reading it from the outages ledger at
// scrape time makes it exact immediately after a restart, which an in-memory
// copy of the same number would not be.
func (s *Store) MetricsOutageSeconds(ctx context.Context) (float64, error) {
	var seconds float64
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		return r.QueryRowContext(ctx, `SELECT COALESCE(SUM(to_ts - from_ts), 0) / 1000.0
  FROM outages`).Scan(&seconds)
	})
	if err != nil {
		return 0, fmt.Errorf("sum the outage ledger: %w", err)
	}
	return seconds, nil
}

// MetricsMetaValues returns whichever of the backup/GC meta keys exist. The
// map simply lacks the absent ones, so the collector can apply the
// no-series-without-truth rule without a second round trip per key.
func (s *Store) MetricsMetaValues(ctx context.Context) (map[string]string, error) {
	out := make(map[string]string, 3)
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT key, value FROM meta
 WHERE key IN (?, ?, ?)`,
			MetaBackupLastSuccessAtMs, MetaBackupVerifiedOk, MetaGCLastSuccessAtMs)
		if err != nil {
			return fmt.Errorf("read the backup and GC meta keys: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				return fmt.Errorf("scan a meta value: %w", err)
			}
			out[k] = v
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MetricsDBBytes returns the size of the main database file in bytes, measured
// through SQLite's own page accounting, and the size of its WAL companion as
// the filesystem sees it. Both feed the canary alerts: a WAL that only ever
// grows is the long lived reader that blocks checkpointing.
func (s *Store) MetricsDBBytes(ctx context.Context) (int64, int64, error) {
	var pages, pageSize int64
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		if err := r.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err != nil {
			return fmt.Errorf("read page_count: %w", err)
		}
		return r.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize)
	})
	if err != nil {
		return 0, 0, err
	}
	wal := int64(0)
	if info, err := os.Stat(s.path + "-wal"); err == nil {
		wal = info.Size()
	}
	return pages * pageSize, wal, nil
}
