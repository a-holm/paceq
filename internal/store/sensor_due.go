package store

import (
	"context"
	"fmt"
)

// The due-sensors reader: the discovery query the daemon's evaluator loop runs
// every wake, and the sensor twin of DueSchedules in schedules.go. It reads
// through the RO pool like every other read, so a wake never queues behind the
// single writer connection.

// dueSensorsSQL is the statement the evaluator loop runs every wake. It is a
// package constant so a query-plan test proves THE statement that ships, not a
// lookalike written for the test. The partial index idx_sensors_due
// (next_eval_at) WHERE paused = 0 serves both the predicate and the ordering;
// idx_ticks_source serves the newest-tick lookups per candidate row.
//
// The min_interval_ms term is the hot-loop guard. `sensors tick` and a cursor
// reset both drag next_eval_at to now from outside the evaluator, so the
// reader is the only place that can hold a sensor to the lower bound between
// two starts its spec declared.
//
// The last start is the newest tick's last_started_at, the same column
// sensorSummarySelect reads for the same field: started_at is where a tick
// first ran and last_started_at is where it ran last, so a coalesced tick
// moves the second and leaves the first. started_at orders the lookup because
// idx_ticks_source is built on it.
//
// The column order is scanSensorSummary's. The two tail columns are the newest
// tick, which sensorSummarySelect takes from a join grouped over every sensor
// tick; a per-wake query takes them per candidate row instead, so the whole
// statement stays index-served however long the tick history grows.
const dueSensorsSQL = `SELECT s.name, s.job_name, s.kind, s.exec_json, s.interval_ms,
       s.min_interval_ms, s.timeout_ms, s.max_triggers_per_tick,
       s.paused, COALESCE(s.paused_reason, ''), s.cursor, s.cursor_version,
       s.dedup_epoch, s.consecutive_failures, s.next_eval_at,
       (SELECT t.last_started_at FROM ticks t
         WHERE t.source_kind = 'sensor' AND t.source_name = s.name
         ORDER BY t.started_at DESC LIMIT 1),
       COALESCE((SELECT t.outcome FROM ticks t
                  WHERE t.source_kind = 'sensor' AND t.source_name = s.name
                  ORDER BY t.started_at DESC LIMIT 1), '')
  FROM sensors s
 WHERE s.paused = 0
   AND s.next_eval_at <= ?
   AND COALESCE((SELECT t.last_started_at FROM ticks t
                  WHERE t.source_kind = 'sensor' AND t.source_name = s.name
                  ORDER BY t.started_at DESC LIMIT 1), 0)
       + s.min_interval_ms <= ?
 ORDER BY s.next_eval_at
 LIMIT ?`

// DueSensors returns up to limit unpaused sensors that are due at or before
// nowMilli and past their min_interval floor, most overdue first.
func (s *Store) DueSensors(ctx context.Context, nowMilli int64, limit int) ([]SensorSummary, error) {
	rows, err := s.r.QueryContext(ctx, dueSensorsSQL, nowMilli, nowMilli, limit)
	if err != nil {
		return nil, fmt.Errorf("list due sensors at %d: %w", nowMilli, err)
	}
	defer func() { _ = rows.Close() }()

	var out []SensorSummary
	for rows.Next() {
		var row SensorSummary
		if err := scanSensorSummary(rows, &row); err != nil {
			return nil, fmt.Errorf("scan a due sensor: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list due sensors at %d: %w", nowMilli, err)
	}
	return out, nil
}
