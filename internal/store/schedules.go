package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/id"
)

// Schedules are the automatic firing rules the scheduler loop reads. This file
// is the single owner of their SQL: the due discovery query and the upsert the
// apply path and tests use to put a schedule in the database.

// ScheduleRow is one schedule as the due query returns it. Every column the
// loop needs to decide what fired comes back at once, so processing one
// schedule costs one read.
type ScheduleRow struct {
	ID              string
	JobName         string
	Name            string
	Kind            string
	Expr            string
	Timezone        string
	SpringForward   string
	FallBack        string
	Catchup         string
	CatchupLimit    int
	CatchupWindowMS int64
	Paused          bool
	LastTickAt      *time.Time
	NextTickAt      time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time

	// Scan primitives, filled by scanTargets and folded into the exported
	// fields by finalize.
	nulls       scheduleNulls
	pausedRaw   int
	nextTickRaw int64
	createdRaw  int64
	updatedRaw  int64
}

// ScheduleInput names a schedule to create or replace. Fields left empty take
// the schema defaults.
type ScheduleInput struct {
	JobName         string
	Name            string
	Kind            string
	Expr            string
	Timezone        string
	SpringForward   string
	FallBack        string
	Catchup         string
	CatchupLimit    int
	CatchupWindowMS int64
	Paused          bool
	NextTickAt      time.Time
}

// The column defaults, mirrored in Go so the conflict branch of the upsert
// writes the same values the insert branch would have defaulted to. A re-apply
// of a sparse definition must not replace defaults with empty strings.
func (in ScheduleInput) normalized() ScheduleInput {
	out := in
	if out.Kind == "" {
		out.Kind = "cron"
	}
	if out.Timezone == "" {
		out.Timezone = "UTC"
	}
	if out.SpringForward == "" {
		out.SpringForward = "skip"
	}
	if out.FallBack == "" {
		out.FallBack = "first"
	}
	if out.Catchup == "" {
		out.Catchup = "skip"
	}
	if out.CatchupWindowMS == 0 {
		out.CatchupWindowMS = 86_400_000
	}
	if out.CatchupLimit == 0 {
		out.CatchupLimit = 10
	}
	return out
}

// upsertScheduleSQL inserts a schedule or replaces its definition in place.
// The id and created_at survive a re-apply: history keeps pointing at the row
// it always pointed at. RETURNING yields the post-image either way, so one
// statement both writes and reads back.
const upsertScheduleSQL = `INSERT INTO schedules
(id, job_name, name, kind, expr, timezone, spring_forward, fall_back, catchup,
 catchup_limit, catchup_window_ms, paused, next_tick_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_name, name) DO UPDATE SET
    kind = excluded.kind, expr = excluded.expr, timezone = excluded.timezone,
    spring_forward = excluded.spring_forward, fall_back = excluded.fall_back,
    catchup = excluded.catchup, catchup_limit = excluded.catchup_limit,
    catchup_window_ms = excluded.catchup_window_ms, paused = excluded.paused,
    next_tick_at = excluded.next_tick_at, updated_at = excluded.updated_at
RETURNING id, job_name, name, kind, expr, timezone,
       spring_forward, fall_back, catchup, catchup_limit, catchup_window_ms,
       paused, last_tick_at, next_tick_at, created_at, updated_at`

// UpsertSchedule inserts a schedule or replaces its definition in place,
// keeping id and timestamps stable across re-apply.
func (s *Store) UpsertSchedule(ctx context.Context, in ScheduleInput) (ScheduleRow, error) {
	in = in.normalized()
	now := s.clk.Now().UTC()
	rowID, err := id.New(now)
	if err != nil {
		return ScheduleRow{}, fmt.Errorf("mint a schedule id: %w", err)
	}
	at := now.UnixMilli()
	paused := 0
	if in.Paused {
		paused = 1
	}
	var row ScheduleRow
	err = s.w.QueryRowContext(ctx, upsertScheduleSQL,
		rowID, in.JobName, in.Name, in.Kind, in.Expr, in.Timezone,
		in.SpringForward, in.FallBack, in.Catchup,
		in.CatchupLimit, in.CatchupWindowMS, paused, in.NextTickAt.UnixMilli(), at, at,
	).Scan(row.scanTargets()...)
	if err != nil {
		return ScheduleRow{}, fmt.Errorf("upsert schedule %s/%s: %w", in.JobName, in.Name, err)
	}
	row.finalize()
	return row, nil
}

// dueSchedulesSQL is the discovery query the scheduler loop runs every wake.
// It is a package constant so the query plan test proves THE statement that
// ships, not a lookalike written for the test. The partial index
// idx_schedules_due (next_tick_at) WHERE paused = 0 serves it.
const dueSchedulesSQL = `SELECT id, job_name, name, kind, expr, timezone,
       spring_forward, fall_back, catchup, catchup_limit, catchup_window_ms,
       paused, last_tick_at, next_tick_at, created_at, updated_at
  FROM schedules
 WHERE paused = 0 AND next_tick_at <= ?
 ORDER BY next_tick_at
 LIMIT ?`

// DueSchedules returns up to max unpaused schedules whose next tick is due at
// or before nowMilli, oldest next tick first.
func (s *Store) DueSchedules(ctx context.Context, nowMilli int64, max int) ([]ScheduleRow, error) {
	rows, err := s.r.QueryContext(ctx, dueSchedulesSQL, nowMilli, max)
	if err != nil {
		return nil, fmt.Errorf("list due schedules at %d: %w", nowMilli, err)
	}
	defer func() { _ = rows.Close() }()

	var out []ScheduleRow
	for rows.Next() {
		var row ScheduleRow
		if err := rows.Scan(row.scanTargets()...); err != nil {
			return nil, fmt.Errorf("scan a due schedule: %w", err)
		}
		row.finalize()
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list due schedules at %d: %w", nowMilli, err)
	}
	return out, nil
}

// scanTargets and finalize turn one row into one ScheduleRow. last_tick_at is
// the only nullable column; everything else is NOT NULL by schema.
func (r *ScheduleRow) scanTargets() []any {
	r.nulls.lastTick = new(sql.NullInt64)
	return []any{
		&r.ID, &r.JobName, &r.Name, &r.Kind, &r.Expr, &r.Timezone,
		&r.SpringForward, &r.FallBack, &r.Catchup, &r.CatchupLimit, &r.CatchupWindowMS,
		&r.pausedRaw, r.nulls.lastTick, &r.nextTickRaw, &r.createdRaw, &r.updatedRaw,
	}
}

type scheduleNulls struct {
	lastTick *sql.NullInt64
}

// finalize moves the scanned primitives into their exported shapes.
func (r *ScheduleRow) finalize() {
	r.Paused = r.pausedRaw == 1
	if r.nulls.lastTick.Valid {
		t := time.UnixMilli(r.nulls.lastTick.Int64).UTC()
		r.LastTickAt = &t
	}
	r.NextTickAt = time.UnixMilli(r.nextTickRaw).UTC()
	r.CreatedAt = time.UnixMilli(r.createdRaw).UTC()
	r.UpdatedAt = time.UnixMilli(r.updatedRaw).UTC()
}
