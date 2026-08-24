package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// The read half of M3-06 (issue #11): the sensors CLI group. A read command
// (list, show) opens the RO pool and calls one of these methods, which is why
// they all run against s.r and never write. The write side of the group
// (pause, resume, reset, cursor set, tick) lands on the store methods already
// here (PauseSensor, ResumeSensor, ResetSensor, SetSensorCursor, and the tick
// transaction in sensor_tick.go). This file is the single owner of the read
// SQL, mirroring how schedules.go owns the schedule reads.

// SensorSummary is one sensor as the CLI lists it: the definition columns a
// pause/resume decision needs and the state an operator wants at a glance.
type SensorSummary struct {
	Name                string
	JobName             string
	Kind                string
	ExecJSON            string
	IntervalMS          int64
	MinIntervalMS       int64
	TimeoutMS           int64
	MaxTriggersPerTick  int
	Paused              bool
	PausedReason        string
	Cursor              *string
	CursorVersion       int64
	DedupEpoch          int64
	ConsecutiveFailures int
	NextEvalAt          int64
	LastTickAt          *int64
	LastOutcome         string
}

// The read columns of a sensor row plus its coalesced last tick. last_tick_at
// and last_outcome come from the same sub-select on ticks, so a sensor that
// never ticked shows them as nil/empty rather than as a broken join.
const sensorSummarySelect = `SELECT s.name, s.job_name, s.kind, s.exec_json, s.interval_ms,
       s.min_interval_ms, s.timeout_ms, s.max_triggers_per_tick,
       s.paused, COALESCE(s.paused_reason, ''), s.cursor, s.cursor_version,
       s.dedup_epoch, s.consecutive_failures, s.next_eval_at,
              t.last_started_at, COALESCE(t.outcome, '')
       FROM sensors s
LEFT JOIN (
    SELECT source_name, MAX(last_started_at) AS last_started_at, outcome
    FROM ticks
    WHERE source_kind = 'sensor'
    GROUP BY source_name
) t ON t.source_name = s.name`

// scanSensorSummary scans one row back into a SensorSummary. paused comes back
// as an integer, cursor and last_tick_at as nullable columns.
func scanSensorSummary(row interface{ Scan(...any) error }, out *SensorSummary) error {
	var pausedRaw int
	var cursor sql.NullString
	var lastTick sql.NullInt64
	if err := row.Scan(
		&out.Name, &out.JobName, &out.Kind, &out.ExecJSON, &out.IntervalMS,
		&out.MinIntervalMS, &out.TimeoutMS, &out.MaxTriggersPerTick,
		&pausedRaw, &out.PausedReason, &cursor, &out.CursorVersion,
		&out.DedupEpoch, &out.ConsecutiveFailures, &out.NextEvalAt,
		&lastTick, &out.LastOutcome,
	); err != nil {
		return err
	}
	out.Paused = pausedRaw != 0
	if cursor.Valid {
		out.Cursor = &cursor.String
	}
	if lastTick.Valid {
		out.LastTickAt = &lastTick.Int64
	}
	return nil
}

// GetSensor reads one sensor row with its last tick, or ErrNotFound.
func (s *Store) GetSensor(ctx context.Context, name string) (SensorSummary, error) {
	var out SensorSummary
	err := scanSensorSummary(s.r.QueryRowContext(ctx, sensorSummarySelect+
		` WHERE s.name = ?`, name), &out)
	if errors.Is(err, sql.ErrNoRows) {
		return SensorSummary{}, fmt.Errorf("find sensor %s: %w", name, ErrNotFound)
	}
	if err != nil {
		return SensorSummary{}, fmt.Errorf("find sensor %s: %w", name, err)
	}
	return out, nil
}

// ListSensors reads every sensor row with its last tick, in name order.
func (s *Store) ListSensors(ctx context.Context) ([]SensorSummary, error) {
	rows, err := s.r.QueryContext(ctx, sensorSummarySelect+
		` ORDER BY s.name`)
	if err != nil {
		return nil, fmt.Errorf("list sensors: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SensorSummary
	for rows.Next() {
		var row SensorSummary
		if err := scanSensorSummary(rows, &row); err != nil {
			return nil, fmt.Errorf("scan a sensor row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list sensors: %w", err)
	}
	return out, nil
}

// SensorTickView is one recorded sensor tick as the CLI shows it: what
// happened, when, and how many runs it produced.
type SensorTickView struct {
	StartedAt    time.Time
	Outcome      string
	ReasonCode   string
	TriggerCount int
	DedupedCount int
}

// SensorTicks lists the last N ticks of one sensor, newest first. This is the
// coalesced history `sensors show --limit N` reads; ticks were coalesced onto
// one row at write time, so repetition shows as a grow of repeat_count rather
// than a second row.
func (s *Store) SensorTicks(ctx context.Context, name string, limit int) ([]SensorTickView, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.r.QueryContext(ctx, `SELECT started_at, outcome,
COALESCE(reason_code, ''), trigger_count, deduped_count
FROM ticks WHERE source_kind = 'sensor' AND source_name = ?
ORDER BY started_at DESC LIMIT ?`, name, limit)
	if err != nil {
		return nil, fmt.Errorf("list ticks of sensor %s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()

	var out []SensorTickView
	for rows.Next() {
		var v SensorTickView
		var at int64
		if err := rows.Scan(&at, &v.Outcome, &v.ReasonCode, &v.TriggerCount, &v.DedupedCount); err != nil {
			return nil, fmt.Errorf("scan a sensor tick for %s: %w", name, err)
		}
		v.StartedAt = time.UnixMilli(at).UTC()
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list ticks of sensor %s: %w", name, err)
	}
	return out, nil
}

// DedupVerdict is the dry-run answer for one run key: does this key already
// have a run in the sensor's current dedup epoch, and which run is it?
type DedupVerdict struct {
	RunKey string
	Seen   bool
	RunID  string
}

// PeekDedup is the only check-first (read-then-decide WITHOUT a following
// write) operation in the system, and it is legal for exactly that reason: it
// feeds `sensors test`, which writes nothing. It must never be copied into a
// write path (plan 11 section 5.2). It answers, for each run key a dry-run
// evaluation produced, whether the key already has a run in the current epoch
// and which run that is, so the CLI can show the dedup verdict per trigger.
func (s *Store) PeekDedup(ctx context.Context, name string, epoch int64, runKeys []string) ([]DedupVerdict, error) {
	out := make([]DedupVerdict, 0, len(runKeys))
	for _, key := range runKeys {
		v := DedupVerdict{RunKey: key}
		var runID sql.NullString
		err := s.r.QueryRowContext(ctx,
			`SELECT run_id FROM run_keys WHERE source_id = ? AND epoch = ? AND run_key = ?`,
			name, epoch, key).Scan(&runID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("peek the dedup gate for key %s: %w", key, err)
		}
		if runID.Valid {
			v.Seen = true
			v.RunID = runID.String
		}
		out = append(out, v)
	}
	return out, nil
}

// PauseSensor marks a sensor paused with the given reason (or "operator" when
// none is given). Idempotent: pausing an already paused sensor succeeds.
func (s *Store) PauseSensor(ctx context.Context, name, reason string) error {
	if reason == "" {
		reason = "operator"
	}
	now := s.clk.Now().UTC().UnixMilli()
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE sensors
SET paused = 1, paused_reason = ?, updated_at = ?
WHERE name = ? AND paused = 0`, reason, now, name)
		if err != nil {
			return fmt.Errorf("pause sensor %s: %w", name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("pause sensor %s: %w", name, err)
		}
		if n == 0 {
			// Either already paused (fine) or missing (not found). Disambiguate.
			var exists int
			if err := tx.QueryRow(`SELECT 1 FROM sensors WHERE name = ?`, name).Scan(&exists); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("pause sensor %s: %w", name, ErrNotFound)
				}
				return fmt.Errorf("pause sensor %s: %w", name, err)
			}
			// Already paused: keep the existing reason, nothing changes.
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// ResumeSensor clears a sensor's paused state, its reason, and its
// consecutive-failure count (the breaker state an operator reset by resuming).
func (s *Store) ResumeSensor(ctx context.Context, name string) error {
	now := s.clk.Now().UTC().UnixMilli()
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE sensors
SET paused = 0, paused_reason = NULL, consecutive_failures = 0, updated_at = ?
WHERE name = ?`, now, name)
		if err != nil {
			return fmt.Errorf("resume sensor %s: %w", name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("resume sensor %s: %w", name, err)
		}
		if n == 0 {
			var exists int
			if err := tx.QueryRow(`SELECT 1 FROM sensors WHERE name = ?`, name).Scan(&exists); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("resume sensor %s: %w", name, ErrNotFound)
				}
				return fmt.Errorf("resume sensor %s: %w", name, err)
			}
			// Already resumed: nothing to clear.
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// SetSensorDue makes a sensor due for evaluation now. It is the store-half of
// `sensors tick` through the daemon: the CLI asks the daemon, the daemon moves
// next_eval_at to now, and the evaluator runtime picks the sensor up on its
// next wake. The runtime wake itself is the M3 emitter's seam; this method
// only records the operator's request.
func (s *Store) SetSensorDue(ctx context.Context, name string) error {
	now := s.clk.Now().UTC().UnixMilli()
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE sensors
SET next_eval_at = ?, updated_at = ?
WHERE name = ?`, now, now, name)
		if err != nil {
			return fmt.Errorf("set sensor %s due: %w", name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("set sensor %s due: %w", name, err)
		}
		if n == 0 {
			return fmt.Errorf("set sensor %s due: %w", name, ErrNotFound)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// SensorSeedInput is the definition columns a sensor row needs, for the seed
// path that materialises a sensor before the apply seam (M3-01) owns it. It is
// deliberately minimal: the CLI test harness and early fixtures use it to get
// rows into the table, and it doubles as the single-insert building block the
// apply path can reuse. Drift state (cursor, dedup_epoch, failures, paused) is
// left alone on a re-seed, matching the apply guarantee.
type SensorSeedInput struct {
	Name     string
	JobName  string
	ExecJSON string
	Paused   bool
}

// UpsertSensor inserts a sensor definition row, or replaces just the
// definition columns on a re-seed without touching drift state.
func (s *Store) UpsertSensor(ctx context.Context, in SensorSeedInput) error {
	if in.Name == "" {
		return fmt.Errorf("seed sensor: %w", errors.New("empty sensor name"))
	}
	if in.JobName == "" {
		return fmt.Errorf("seed sensor %s: %w", in.Name, errors.New("empty job name"))
	}
	if in.ExecJSON == "" {
		return fmt.Errorf("seed sensor %s: %w", in.Name, errors.New("empty exec spec"))
	}
	now := s.clk.Now().UTC().UnixMilli()
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO sensors
(name, job_name, kind, exec_json, interval_ms, min_interval_ms, timeout_ms,
 max_triggers_per_tick, paused, next_eval_at, created_at, updated_at)
VALUES (?, ?, 'exec', ?, 60000, 1000, 30000, 100, ?, ?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    job_name = excluded.job_name, exec_json = excluded.exec_json,
    interval_ms = excluded.interval_ms, min_interval_ms = excluded.min_interval_ms,
    timeout_ms = excluded.timeout_ms, max_triggers_per_tick = excluded.max_triggers_per_tick,
    next_eval_at = excluded.next_eval_at, updated_at = excluded.updated_at`,
			in.Name, in.JobName, in.ExecJSON, boolToInt(in.Paused), now, now, now)
		if err != nil {
			return fmt.Errorf("seed sensor %s: %w", in.Name, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// boolToInt renders a bool as the integer the schema stores.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
