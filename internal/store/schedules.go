package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
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

// claimTickSQL is the idempotency gate. The UNIQUE index on
// (source_kind, source_name, scheduled_for) is the whole decision: the insert
// lands when this pass is the first to claim the fire-time, and affects zero
// rows otherwise. There is no SELECT first, so there is no window between
// seeing a free fire-time and taking it. RowsAffected zero is the follower
// answer, not an error. A triggered evaluation carries its trigger count from
// birth; skips and errors stay childless.
const claimTickSQL = `INSERT INTO ticks
(id, source_kind, source_name, scheduled_for, started_at, last_started_at,
 outcome, reason_code, reason_text, reason_data, trigger_count)
VALUES (?, 'schedule', ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source_kind, source_name, scheduled_for) DO NOTHING`

// coalesceSkipSQL folds one more identical skipped evaluation into the
// latest row for this source, but only when that row really is a childless
// skip with the same reason: a triggered or errored latest row never absorbs,
// and a status change starts its own row. Empty RETURNING means nothing
// identical was there to fold into.
const coalesceSkipSQL = `UPDATE ticks
   SET repeat_count = repeat_count + 1, last_started_at = ?, finished_at = ?
 WHERE id = (SELECT id FROM ticks
              WHERE source_kind = 'schedule' AND source_name = ?
              ORDER BY started_at DESC LIMIT 1)
   AND outcome = 'skipped' AND reason_code = ? AND trigger_count = 0
RETURNING id`

// updateProgressSQL moves the cursor to the fire-time just claimed and names
// the moment after it as the next due instant. The predicate keeps the cursor
// monotone: a stale pass from before a handover must never drag it back over
// ground a newer pass already covered.
const updateProgressSQL = `UPDATE schedules
   SET last_tick_at = ?, next_tick_at = ?, updated_at = ?
 WHERE id = ? AND (last_tick_at IS NULL OR last_tick_at < ?)`

// Tick outcomes as stored in ticks.outcome.
const (
	OutcomeTriggered = "triggered"
	OutcomeSkipped   = "skipped"
	OutcomeError     = "error"
)

// TickInput is one decided evaluation: the loop has already computed what
// happened at this fire-time and why; this transaction only records it.
type TickInput struct {
	// Schedule names the row the evaluation belongs to.
	Schedule ScheduleRow

	// ScheduledFor is the fire-time in UTC. Together with source_kind
	// 'schedule' and the schedule's name it IS the idempotency key.
	ScheduledFor time.Time

	// Outcome is OutcomeTriggered, OutcomeSkipped or OutcomeError.
	Outcome string

	// ReasonCode explains every outcome that produced no run.
	ReasonCode reason.Code

	// ReasonText and ReasonData carry the human line and its JSON detail.
	ReasonText string
	ReasonData string

	// RunKey is required when Outcome is triggered.
	RunKey string

	// NextTickAt is where progress should move to when UpdateProgress holds.
	NextTickAt time.Time

	// UpdateProgress moves last_tick_at/next_tick_at to this fire-time. A
	// config failure sets it false: the schedule must stay due so a fixed
	// definition picks the work back up instead of skipping past it.
	UpdateProgress bool

	// Actor lands on the queued run event. Empty becomes "system".
	Actor string
}

// TickResult says what one decided evaluation became.
type TickResult struct {
	// Claimed is true when this call recorded the evaluation: a new tick
	// row, or an identical earlier skip absorbing it. False means the
	// fire-time was already materialised by someone else; nothing was
	// written and no error is reported.
	Claimed bool

	// Coalesced is true when an identical previous skip took this
	// evaluation in as another repeat_count step instead of a new row.
	Coalesced bool

	// Run describes the queued run when Claimed and the outcome triggered.
	Run Run
}

// triggerCountOf says whether the claimed evaluation owns a trigger: only a
// triggered outcome does.
func triggerCountOf(outcome string) int {
	if outcome == OutcomeTriggered {
		return 1
	}
	return 0
}

// materializeTick claims one fire-time end to end. The order inside the
// transaction is the write model's: claim first, then the consequences, then
// progress, and every consequence aborts together when any statement refuses.
func (s *Store) MaterializeTick(ctx context.Context, in TickInput) (TickResult, error) {
	// Ids and stamps are minted before the transaction opens: each costs a
	// read of the entropy source, and the write model forbids that while the
	// write lock is held.
	now := s.clk.Now().UTC()
	at := now.UnixMilli()
	fireAt := in.ScheduledFor.UTC()
	tickID, err := id.New(now)
	if err != nil {
		return TickResult{}, fmt.Errorf("mint a tick id: %w", err)
	}

	actor := in.Actor
	if actor == "" {
		actor = "system"
	}
	sourceName := in.Schedule.JobName + "/" + in.Schedule.Name

	out := TickResult{}
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		// Pause is re-read INSIDE the transaction. A pause applied between
		// discovery and this instant must turn the evaluation into a
		// recorded stand-down here, never into a run.
		var paused int
		if err := tx.QueryRow(`SELECT paused FROM schedules WHERE id = ?`,
			in.Schedule.ID).Scan(&paused); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("materialise tick for schedule %s: %w", sourceName, ErrNotFound)
			}
			return fmt.Errorf("read the pause flag of schedule %s: %w", sourceName, err)
		}

		outcome := in.Outcome
		reasonCode := in.ReasonCode
		if paused == 1 && outcome == OutcomeTriggered {
			outcome = OutcomeSkipped
			reasonCode = reason.TICKSkippedPaused
			out.Run = Run{}
		}

		faults.Point("M2:tick:before_insert")

		// Identical skips coalesce before anything else: the latest row for
		// this source, if it is an identical childless skip, absorbs this
		// evaluation as another repeat. Errors and triggers never take this
		// path, so history only ever folds where nothing happened.
		//
		// SEAM (M3 sensors): coalesce_skips is configurable there; schedules
		// always coalesce, which is the documented default.
		if outcome == OutcomeSkipped {
			var absorbed string
			err := tx.QueryRow(coalesceSkipSQL,
				at, at, sourceName, string(reasonCode)).Scan(&absorbed)
			switch {
			case err == nil:
				out.Claimed = true
				out.Coalesced = true
				if in.UpdateProgress {
					faults.Point("M2:tick:after_run_before_progress")
					if _, err := tx.Exec(updateProgressSQL,
						fireAt.UnixMilli(), in.NextTickAt.UnixMilli(), at, in.Schedule.ID, fireAt.UnixMilli()); err != nil {
						return fmt.Errorf("advance the cursor of schedule %s: %w", sourceName, err)
					}
				}
				return nil
			case errors.Is(err, sql.ErrNoRows):
				// Nothing identical to fold into: fall through to the gate.
			default:
				return fmt.Errorf("coalesce the skip of schedule %s: %w", sourceName, err)
			}
		}

		res, err := tx.Exec(claimTickSQL,
			tickID, sourceName, fireAt.UnixMilli(), at, at, outcome,
			nullIfEmpty(string(reasonCode)), nullIfEmpty(in.ReasonText), nullIfEmpty(in.ReasonData),
			triggerCountOf(outcome),
		)
		if err != nil {
			return fmt.Errorf("claim the fire-time %s for schedule %s: %w",
				fireAt.Format(time.RFC3339), sourceName, err)
		}
		faults.Point("M2:tick:after_tick_before_run")
		written, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("read the claim result of tick %s: %w", sourceName, err)
		}
		if written == 0 {
			// Someone already made this exact fire-time: another pass, a
			// restart replaying the same window, or a rival holder mid
			// handover. Abort silently; nothing about this fire-time is ours.
			out.Claimed = false
			return nil
		}
		out.Claimed = true

		switch outcome {
		case OutcomeTriggered:
			run, err := s.materializeTickRun(tx, in, now, at, tickID, sourceName, fireAt, actor)
			if err != nil {
				return err
			}
			out.Run = run
		default:
			// A skip or an error closes its own evaluation: started,
			// finished, explained. No trigger, no run.
			if _, err := tx.Exec(`UPDATE ticks SET finished_at = ?, duration_ms = 0
WHERE id = ?`, at, tickID); err != nil {
				return fmt.Errorf("close the skipped tick for %s: %w", sourceName, err)
			}
		}

		if in.UpdateProgress {
			faults.Point("M2:tick:after_run_before_progress")
			// Progress moves forward only. A stale pass from before a lease
			// handover must never drag the cursor back over ground a newer
			// pass already covered.
			if _, err := tx.Exec(updateProgressSQL,
				fireAt.UnixMilli(), in.NextTickAt.UnixMilli(), at, in.Schedule.ID, fireAt.UnixMilli()); err != nil {
				return fmt.Errorf("advance the cursor of schedule %s: %w", sourceName, err)
			}
		}
		return nil
	})
	if err != nil {
		return TickResult{}, err
	}
	faults.Point("M2:tick:after_commit")
	return out, nil
}

// materializeTickRun writes the trigger, the dedup registration, the queued
// run frozen to the job's current version, its steps, and the queued event.
//
// SEAM (#60, M2-06): the run is born queued with no lease columns. Run leases
// attach when an executor CLAIMS the run, which is #60's claim statement, not
// this insert. Nothing here claims, fences or heartbeats; when #60 lands, its
// ClaimRuns takes over from the dispatcher exactly as it does for manual runs
// today.
func (s *Store) materializeTickRun(tx *sql.Tx, in TickInput, now time.Time, at int64,
	tickID, sourceName string, fireAt time.Time, actor string,
) (Run, error) {
	runID, err := id.New(now)
	if err != nil {
		return Run{}, fmt.Errorf("mint a run id: %w", err)
	}
	triggerID, err := id.New(now)
	if err != nil {
		return Run{}, fmt.Errorf("mint a trigger id: %w", err)
	}

	// One trigger per claimed tick, accepted by construction: the loop only
	// asks for a run after the policy said yes.
	if _, err := tx.Exec(`INSERT INTO triggers
(id, tick_id, job_name, run_key, params_json, created_at, outcome, reason_code, run_id)
VALUES (?, ?, ?, ?, '{}', ?, 'accepted', ?, NULL)`,
		triggerID, tickID, in.Schedule.JobName, in.RunKey, at,
		string(reason.TRIGGERAccepted)); err != nil {
		return Run{}, fmt.Errorf("record the trigger for %s at %s: %w",
			sourceName, fireAt.Format(time.RFC3339), err)
	}

	// Dedup registration, epoch 0: schedules have no takeover epochs. A crash
	// after this commit followed by a recomputed window lands on the same key
	// and deduplicates without application logic.
	if _, err := tx.Exec(`INSERT INTO run_keys (source_id, epoch, run_key, first_seen_at, run_id)
VALUES (?, 0, ?, ?, ?)
ON CONFLICT(source_id, epoch, run_key) DO NOTHING`,
		sourceName, in.RunKey, at, runID); err != nil {
		return Run{}, fmt.Errorf("register the run key of %s: %w", sourceName, err)
	}

	// The version is chosen inside the transaction, so an apply racing the
	// loop still freezes one whole version rather than a mix of two.
	var versionID, specJSON string
	if err := tx.QueryRow(`SELECT j.current_version_id, v.spec_json
FROM jobs j JOIN job_versions v ON v.id = j.current_version_id
WHERE j.name = ?`, in.Schedule.JobName).Scan(&versionID, &specJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Run{}, fmt.Errorf("materialise %s: no job version is current: %w", sourceName, ErrNotFound)
		}
		return Run{}, fmt.Errorf("find the current version of job %s: %w", in.Schedule.JobName, err)
	}
	job, err := spec.FromIR([]byte(specJSON))
	if err != nil {
		return Run{}, fmt.Errorf("read the frozen spec of job %s (version %s): %w",
			in.Schedule.JobName, versionID, err)
	}

	run := Run{
		ID:           runID,
		JobName:      in.Schedule.JobName,
		JobVersionID: versionID,
		TriggerID:    triggerID,
		Origin:       "schedule",
		State:        "queued",
		AvailableAt:  now,
		ScheduledFor: fireAt,
		ParamsJSON:   "{}",
		MaxAttempts:  1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if _, err := tx.Exec(`INSERT INTO runs
(id, job_name, job_version_id, trigger_id, origin, run_key, state, available_at,
 scheduled_for, params_json, attempt, max_attempts, created_at, updated_at)
VALUES (?, ?, ?, ?, 'schedule', ?, 'queued', ?, ?, '{}', 0, 1, ?, ?)`,
		run.ID, run.JobName, run.JobVersionID, nullIfEmpty(run.TriggerID), in.RunKey,
		run.AvailableAt.UnixMilli(), fireAt.UnixMilli(), at, at); err != nil {
		return Run{}, fmt.Errorf("create the run of schedule %s: %w", sourceName, err)
	}

	if err := insertSteps(tx, run.ID, job.Steps); err != nil {
		return Run{}, err
	}
	faults.Point("M2:tick:after_run")

	if err := appendRunEvent(tx, RunEvent{
		RunID:   run.ID,
		At:      now,
		Kind:    "run.queued",
		ToState: "queued",
		Actor:   actor,
	}); err != nil {
		return Run{}, err
	}
	return run, nil
}

// RunIDByRunKey names the run a registered dedup key points at. The crash
// harness uses it to find the run behind an already-committed tick decision;
// explain will use it to answer "which run did this fire-time become".
func (s *Store) RunIDByRunKey(ctx context.Context, sourceID, runKey string) (string, error) {
	var runID sql.NullString
	err := s.r.QueryRowContext(ctx,
		`SELECT run_id FROM run_keys WHERE source_id = ? AND epoch = ? AND run_key = ?`,
		sourceID, 0, runKey).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("find the run behind key %s/%s: %w", sourceID, runKey, ErrNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("find the run behind key %s/%s: %w", sourceID, runKey, err)
	}
	return runID.String, nil
}

// TickView is one recorded schedule tick as a reader wants it: what fired,
// what it became, and why it did not.
type TickView struct {
	ScheduledFor time.Time // zero when the tick carries no fire-time
	Outcome      string
	ReasonCode   string
	RepeatCount  int
	TriggerCount int
}

// ScheduleTicks lists one schedule's recorded ticks, oldest fire-time first.
// This is the read side of everything MaterializeTick writes, and the table
// explain (M5-01) will read.
func (s *Store) ScheduleTicks(ctx context.Context, jobName, name string) ([]TickView, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT scheduled_for, outcome,
COALESCE(reason_code, ''), repeat_count, trigger_count
FROM ticks WHERE source_kind = 'schedule' AND source_name = ?
ORDER BY scheduled_for`, jobName+"/"+name)
	if err != nil {
		return nil, fmt.Errorf("list ticks of schedule %s/%s: %w", jobName, name, err)
	}
	defer func() { _ = rows.Close() }()

	var out []TickView
	for rows.Next() {
		var v TickView
		var fire *int64
		if err := rows.Scan(&fire, &v.Outcome, &v.ReasonCode, &v.RepeatCount, &v.TriggerCount); err != nil {
			return nil, fmt.Errorf("scan a tick of schedule %s/%s: %w", jobName, name, err)
		}
		if fire != nil {
			v.ScheduledFor = time.UnixMilli(*fire).UTC()
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list ticks of schedule %s/%s: %w", jobName, name, err)
	}
	return out, nil
}

// ScheduleCursor returns a schedule's progress stamps: last_tick_at (nil when
// the schedule has never been evaluated) and next_tick_at.
func (s *Store) ScheduleCursor(ctx context.Context, jobName, name string) (*time.Time, time.Time, error) {
	var lastRaw sql.NullInt64
	var next int64
	err := s.r.QueryRowContext(ctx,
		`SELECT last_tick_at, next_tick_at FROM schedules WHERE job_name = ? AND name = ?`,
		jobName, name).Scan(&lastRaw, &next)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, time.Time{}, fmt.Errorf("read the cursor of schedule %s/%s: %w", jobName, name, ErrNotFound)
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("read the cursor of schedule %s/%s: %w", jobName, name, err)
	}
	var last *time.Time
	if lastRaw.Valid {
		t := time.UnixMilli(lastRaw.Int64).UTC()
		last = &t
	}
	return last, time.UnixMilli(next).UTC(), nil
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
