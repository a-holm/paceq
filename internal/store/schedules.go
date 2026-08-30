package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
)

// Schedules are the automatic firing rules the scheduler loop reads. This file
// owns the SQL the loop runs: the due discovery query, the tick transaction and
// the upsert tests seed a schedule with. The write that materialises a declared
// schedule at apply lives in schedulesync.go.

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

	// Overlap is what a tick does when the job's max_concurrent is already
	// held: "skip" stands down and records why, "queue" materialises the run
	// deferred into the future with a defer_reason. Empty means skip.
	Overlap string

	// Shadow marks the schedule as running in shadow mode (#32): every
	// evaluation materialises its tick exactly as it would normally, but no
	// run is created and nothing executes. A global --shadow on serve
	// shadows every schedule regardless of this flag.
	Shadow bool

	Paused     bool
	LastTickAt *time.Time
	NextTickAt time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time

	// Scan primitives, filled by scanTargets and folded into the exported
	// fields by finalize.
	nulls       scheduleNulls
	pausedRaw   int
	shadowRaw   int
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

	// Overlap is the policy for ticks that fire while the job's concurrency
	// limit is held: "skip" (default) or "queue".
	Overlap string

	// Shadow puts the schedule into shadow mode on apply: ticks are
	// recorded, nothing executes (#32).
	Shadow bool

	Paused     bool
	NextTickAt time.Time
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
	if out.Overlap == "" {
		out.Overlap = "skip"
	}
	return out
}

// upsertScheduleSQL inserts a schedule or replaces its definition in place.
// The id and created_at survive a re-apply: history keeps pointing at the row
// it always pointed at. RETURNING yields the post-image either way, so one
// statement both writes and reads back.
const upsertScheduleSQL = `INSERT INTO schedules
(id, job_name, name, kind, expr, timezone, spring_forward, fall_back, catchup,
 catchup_limit, catchup_window_ms, paused, next_tick_at, created_at, updated_at, overlap, shadow)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_name, name) DO UPDATE SET
    kind = excluded.kind, expr = excluded.expr, timezone = excluded.timezone,
    spring_forward = excluded.spring_forward, fall_back = excluded.fall_back,
    catchup = excluded.catchup, catchup_limit = excluded.catchup_limit,
    catchup_window_ms = excluded.catchup_window_ms, paused = excluded.paused,
    next_tick_at = excluded.next_tick_at, updated_at = excluded.updated_at,
    overlap = excluded.overlap, shadow = excluded.shadow
RETURNING id, job_name, name, kind, expr, timezone,
       spring_forward, fall_back, catchup, catchup_limit, catchup_window_ms,
       overlap, paused, last_tick_at, next_tick_at, created_at, updated_at, shadow`

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
	shadow := 0
	if in.Shadow {
		shadow = 1
	}
	var row ScheduleRow
	err = s.w.QueryRowContext(ctx, upsertScheduleSQL,
		rowID, in.JobName, in.Name, in.Kind, in.Expr, in.Timezone,
		in.SpringForward, in.FallBack, in.Catchup,
		in.CatchupLimit, in.CatchupWindowMS, paused, in.NextTickAt.UnixMilli(), at, at,
		in.Overlap, shadow,
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
       overlap, paused, last_tick_at, next_tick_at, created_at, updated_at, shadow
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

// replayMissedTickSQL converts the gap walk's explanation of one fire-time
// into this pass's claim, so a catch-up replay is not refused by evidence
// that nothing ran. The predicate carries the whole safety argument: only a
// row that says nobody evaluated the slot, and that owns no trigger, can be
// taken over, so a real evaluation and a rival holder's row both keep theirs.
// reason_data survives the conversion, because the outage it names is still
// why this run is late.
const replayMissedTickSQL = `UPDATE ticks
   SET started_at = ?, last_started_at = ?, finished_at = NULL, duration_ms = NULL,
       outcome = ?, reason_code = ?, reason_text = ?, trigger_count = ?
 WHERE source_kind = 'schedule' AND source_name = ? AND scheduled_for = ?
   AND outcome = 'missed' AND trigger_count = 0
RETURNING id`

// holderOfTickSQL names what owns a fire-time this pass could not claim. It
// runs only on the loser path, so the extra read costs nothing on the way
// that matters.
const holderOfTickSQL = `SELECT outcome FROM ticks
 WHERE source_kind = 'schedule' AND source_name = ? AND scheduled_for = ?`

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

	// OutcomeMissed is the gap walk's synthetic explanation of a fire-time
	// nobody was alive to evaluate. It is written by startup reconciliation,
	// never by the loop, and it carries no run: it is the one outcome a
	// catch-up attempt is allowed to take the slot back from.
	OutcomeMissed = "missed"

	// OutcomeShadowTriggered records that a fire-time WOULD have started a
	// run (#32): the decision chain ran to its triggered verdict, but shadow
	// mode replaced materialisation with this marker. No trigger row, no run
	// key, no run, no process ever follows it.
	OutcomeShadowTriggered = "shadow_triggered"
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

	// Shadow runs this evaluation under shadow mode (#32): the tick row is
	// claimed exactly as normally - same UNIQUE gate, same pause re-read,
	// same coalescing for skips, same cursor advance - but a would-trigger
	// stores outcome 'shadow_triggered' instead of creating any run.
	// Admission is still simulated against real occupancy plus the virtual
	// occupancy earlier shadow fire-times hold while their estimated
	// duration lasts, so overlap stand-downs are recorded with their real
	// reason codes. This flag is read ONLY inside MaterializeTick; the loop
	// fills it from the process-level shadow switch or the schedule row.
	Shadow bool
}

// Loss names what already owned a fire-time a pass could not claim. Losing
// to a rival holder is the ordinary follower answer and says nothing; losing
// to the gap walk's evidence means a decision was overruled by a row that
// only records that nobody was there, and a caller may want to say so.
type Loss string

const (
	// LossNone is the zero value: this call lost nothing.
	LossNone Loss = ""

	// LossEvaluation says a real evaluation holds the fire-time, made by an
	// earlier pass of this loop or by a rival holder mid handover.
	LossEvaluation Loss = "evaluation"

	// LossMissed says the gap walk's synthetic explanation holds the
	// fire-time and would not give it up.
	LossMissed Loss = "missed"
)

// TickResult says what one decided evaluation became.
type TickResult struct {
	// Claimed is true when this call recorded the evaluation: a new tick
	// row, an identical earlier skip absorbing it, or a gap row replayed
	// into it. False means the fire-time was already materialised by
	// someone else; nothing was written and no error is reported.
	Claimed bool

	// LostTo names what held the fire-time when Claimed is false. It is
	// LossNone whenever Claimed is true.
	LostTo Loss

	// Replayed is true when this evaluation took its fire-time back from a
	// gap-detection 'missed' row rather than claiming a free slot: the
	// outage is still recorded, and the run it owed exists after all.
	Replayed bool

	// Coalesced is true when an identical previous skip took this
	// evaluation in as another repeat_count step instead of a new row.
	Coalesced bool

	// Deferred is true when the run was materialised under overlap: queue
	// with every concurrency slot held: queued, but available_at points
	// into the future and defer_reason says why.
	Deferred bool

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

// replayMissedTick lets a catch-up attempt take a fire-time back from the gap
// walk. Startup reconciliation fills every slot of an outage with a 'missed'
// row before any loop runs, and those rows sit on the same slot uniqueness the
// loop claims through, so without this an advertised catch-up would be refused
// by the evidence that it was needed.
//
// Only an evaluation that would really start something may convert a row: a
// skip explains why nothing ran, which is what 'missed' already says, and says
// it better, because it names the outage. An empty id means the slot stands.
func replayMissedTick(tx *sql.Tx, in TickInput, outcome string, reasonCode reason.Code,
	sourceName string, fireAt time.Time, at int64,
) (string, error) {
	if outcome != OutcomeTriggered && outcome != OutcomeShadowTriggered {
		return "", nil
	}
	var replayID string
	err := tx.QueryRow(replayMissedTickSQL,
		at, at, outcome, nullIfEmpty(string(reasonCode)), nullIfEmpty(in.ReasonText),
		triggerCountOf(outcome), sourceName, fireAt.UnixMilli(),
	).Scan(&replayID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("replay the missed fire-time %s of schedule %s: %w",
			fireAt.Format(time.RFC3339), sourceName, err)
	}
	return replayID, nil
}

// holderOfTick names what owns a fire-time a pass could not claim, so the
// caller can tell an overruled decision from a normal leader handover.
func holderOfTick(tx *sql.Tx, sourceName string, fireAt time.Time) (Loss, error) {
	var outcome string
	err := tx.QueryRow(holderOfTickSQL, sourceName, fireAt.UnixMilli()).Scan(&outcome)
	if errors.Is(err, sql.ErrNoRows) {
		// Nothing there to name any more; treat it as the ordinary answer.
		return LossEvaluation, nil
	}
	if err != nil {
		return LossNone, fmt.Errorf("read what holds the fire-time %s of schedule %s: %w",
			fireAt.Format(time.RFC3339), sourceName, err)
	}
	if outcome == OutcomeMissed {
		return LossMissed, nil
	}
	return LossEvaluation, nil
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
	// The verdict that actually lands in ticks.outcome, decided inside the
	// transaction below and observed after it commits (#40).
	var finalOutcome, finalReason string
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		// withTx runs this again on a busy snapshot, so every attempt starts
		// from nothing: a verdict reached by an attempt that rolled back must
		// never survive into the one that commits.
		out = TickResult{}

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

		// fo/fr follow the verdict that actually lands in ticks.outcome: the
		// pause re-read and the admission gate can both rewrite a triggered
		// evaluation into a stand-down inside the transaction, and the counter
		// must record what committed, not what was asked for (#40).
		outcome := in.Outcome
		reasonCode := in.ReasonCode
		if paused == 1 && outcome == OutcomeTriggered {
			outcome = OutcomeSkipped
			reasonCode = reason.TICKSkippedPaused
			out.Run = Run{}
		} else if in.Shadow && outcome == OutcomeTriggered {
			// Shadow mode (#32) replaces only the materialisation step: the
			// claim below stores a marker that nothing will execute. The
			// decision chain is identical up to this point by construction.
			outcome = OutcomeShadowTriggered
			reasonCode = ""
		}
		finalOutcome, finalReason = outcome, string(reasonCode)

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
			// restart replaying the same window, a rival holder mid
			// handover, or the gap walk explaining an outage. Only the last
			// of those can be overruled, and only by an attempt that would
			// really start something.
			replayID, err := replayMissedTick(tx, in, outcome, reasonCode, sourceName, fireAt, at)
			if err != nil {
				return err
			}
			if replayID == "" {
				lost, err := holderOfTick(tx, sourceName, fireAt)
				if err != nil {
					return err
				}
				out.Claimed = false
				out.LostTo = lost
				return nil
			}
			// The rest of the transaction works on the row that was the
			// outage's evidence a statement ago.
			tickID = replayID
			out.Replayed = true
		}
		out.Claimed = true

		switch outcome {
		case OutcomeTriggered:
			// A zero Run back from admitTickRun is a stand-down only:
			// it has already converted the claimed tick row into the
			// skipped row with its reason inside this same transaction,
			// so one row per fire-time stays the idempotency gate's
			// promise. The verdict it left behind is what the counter
			// records.
			run, verdict, code, err := s.admitTickRun(tx, in, now, at, tickID, sourceName, fireAt, actor)
			if err != nil {
				return err
			}
			out.Run = run
			out.Deferred = run.DeferReason != ""
			finalOutcome, finalReason = verdict, string(code)
		case OutcomeShadowTriggered:
			// Simulated admission answers what WOULD have happened (#32),
			// against real occupancy plus virtual occupancy from earlier
			// shadow fire-times of the same job. A refused fire-time is
			// rewritten into the same skipped stand-down with the same reason
			// code the real path writes; nothing else diverges, and nothing -
			// no trigger row, no run key, no run, no process - is created.
			verdict, code, text, err := resolveShadowOutcomeTx(tx, in, at, tickID, sourceName, fireAt)
			if err != nil {
				return err
			}
			finalOutcome = verdict
			if code != "" {
				finalReason = string(code)
			} else {
				finalReason = ""
			}
			if text != "" && in.ReasonText == "" {
				// The annotation rides on the committed row as prose only;
				// the outcome stays shadow_triggered for queue-shaped refusals.
				if _, err := tx.Exec(`UPDATE ticks SET reason_text = ? WHERE id = ?`, text, tickID); err != nil {
					return fmt.Errorf("annotate the shadow tick for %s: %w", sourceName, err)
				}
			}
			if verdict == OutcomeShadowTriggered {
				// Nothing will ever start for this fire-time, so nobody's
				// completion can fill the row in later: it closes here, at
				// the decision itself.
				if _, err := tx.Exec(`UPDATE ticks SET finished_at = ?, duration_ms = 0
			WHERE id = ?`, at, tickID); err != nil {
					return fmt.Errorf("close the shadow tick for %s: %w", sourceName, err)
				}
			}
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
	if out.Claimed {
		// Only a claimed fire-time was an event: a follower pass that lost
		// the UNIQUE gate committed nothing and counts nowhere (#40).
		s.observeTick("schedule", in.Schedule.Name, finalOutcome, finalReason)
	}
	return out, nil
}

// admitTickRun is the admission decision (#68) and the materialisation it
// leads to, all inside the caller's one BEGIN IMMEDIATE transaction: read the
// frozen spec, read the occupancy, decide, write. The occupancy read goes
// through the transaction handle, so the numbers it sees are the numbers no
// other writer could have moved: the writer pool has one connection and the
// transaction is IMMEDIATE.
//
// Three outcomes:
//
//   - A slot is free: the run is queued due now, exactly as before #68.
//   - The ceiling is held and the schedule's overlap policy is queue: the run
//     is still materialised, but deferred, available_at a backoff into the
//     future and defer_reason naming the hold. A deferred run is not active,
//     so it never blocks its own release.
//   - The ceiling is held and the policy is skip (the default): nothing is
//     materialised. The claimed tick row is converted to a stand-down with
//     the reason code, and the caller records that in the same transaction.
func (s *Store) admitTickRun(tx *sql.Tx, in TickInput, now time.Time, at int64,
	tickID, sourceName string, fireAt time.Time, actor string,
) (Run, string, reason.Code, error) {
	// The disk floor is admission's cheapest and strongest condition (#44):
	// one atomic load inside the transaction that already owns the tick. A
	// held evaluation stands down here, in the same IMMEDIATE transaction
	// that claimed it, so the decision and its record cannot disagree, and
	// no run row is ever born for a fire-time the disk cannot afford.
	if hold, held := s.heldRun(); held {
		if err := s.standDownTickTx(tx, tickID, at, hold); err != nil {
			return Run{}, "", "", err
		}
		return Run{}, OutcomeSkipped, hold.Code, nil
	}
	// The version is chosen inside the transaction, so an apply racing the
	// loop still freezes one whole version rather than a mix of two.
	var versionID, specJSON string
	if err := tx.QueryRow(`SELECT j.current_version_id, v.spec_json
FROM jobs j JOIN job_versions v ON v.id = j.current_version_id
WHERE j.name = ?`, in.Schedule.JobName).Scan(&versionID, &specJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Run{}, "", "", fmt.Errorf("materialise %s: no job version is current: %w", sourceName, ErrNotFound)
		}
		return Run{}, "", "", fmt.Errorf("find the current version of job %s: %w", in.Schedule.JobName, err)
	}
	job, err := spec.FromIR([]byte(specJSON))
	if err != nil {
		return Run{}, "", "", fmt.Errorf("read the frozen spec of job %s (version %s): %w",
			in.Schedule.JobName, versionID, err)
	}
	limit := job.MaxConcurrent
	if limit < 1 {
		limit = spec.DefaultMaxConcurrent
	}

	// The occupancy read and the decision are one step inside this
	// transaction. Nothing between the SELECT and the INSERT can commit.
	active, blocking, err := activeRunsForJobTx(tx, in.Schedule.JobName, at)
	if err != nil {
		return Run{}, "", "", err
	}

	if active >= limit && overlapOf(in.Schedule.Overlap) != "queue" {
		code := skipCodeFor(active, limit)
		text := fmt.Sprintf("%d of %d concurrency slots of job %s are held",
			active, limit, in.Schedule.JobName)
		if code == reason.TICKSkippedOverlap {
			text = fmt.Sprintf("the previous run %s is still going (%d of %d active)",
				blocking, active, limit)
		}
		if err := markTickStoodDownTx(tx, tickID, at, code, text, deferDataJSON(blocking, active, limit)); err != nil {
			return Run{}, "", "", err
		}
		return Run{}, OutcomeSkipped, code, nil
	}

	deferred := active >= limit
	concKey := resolvedConcurrencyKey(job, "{}", in.RunKey)
	run, err := s.materializeTickRun(tx, in, now, at, tickID, sourceName, fireAt, actor,
		versionID, job, deferred, blocking, active, limit, concKey)
	if err != nil {
		return Run{}, "", "", err
	}
	return run, OutcomeTriggered, "", nil
}

// markTickStoodDownTx converts the just claimed triggered tick row into the
// stand-down it became: skipped, with the code, the human line and the data
// that names the run which held the slot. The row keeps its fire-time, so a
// replay still hits the UNIQUE gate instead of writing a second opinion.
const markTickStoodDownSQL = `UPDATE ticks SET
outcome = 'skipped', reason_code = ?, reason_text = ?, reason_data = ?,
finished_at = ?, duration_ms = 0
WHERE id = ?`

func markTickStoodDownTx(tx *sql.Tx, tickID string, at int64,
	code reason.Code, text, data string,
) error {
	if _, err := tx.Exec(markTickStoodDownSQL,
		string(code), text, data, at, tickID); err != nil {
		return fmt.Errorf("mark tick %s stood down: %w", tickID, err)
	}
	return nil
}

// materializeTickRun writes the trigger, the dedup registration, the queued
// run frozen to the job's current version, its steps, and the queued or
// deferred event.
//
// SEAM (#60, M2-06): the run is born queued with no lease columns. Run leases
// attach when an executor CLAIMS the run, which is #60's claim statement, not
// this insert. Nothing here claims, fences or heartbeats.
func (s *Store) materializeTickRun(tx *sql.Tx, in TickInput, now time.Time, at int64,
	tickID, sourceName string, fireAt time.Time, actor string,
	versionID string, job *spec.Job, deferred bool, blocking string, active, limit int,
	concKey string,
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

	run.ConcurrencyKey = concKey

	availableAt := now
	eventKind := "run.queued"
	var eventCode reason.Code
	var eventData string
	if deferred {
		// The ceiling is held: the run is born queued but held into the
		// future, and the row says why in both words the schema knows,
		// defer_reason for the CHECK and the reason code for people.
		availableAt = now.Add(DefaultDeferBackoff)
		run.DeferReason = deferReasonModel()
		run.ReasonCode = string(deferReasonCode())
		run.ReasonData = deferDataJSON(blocking, active, limit)
		eventKind = "run.deferred"
		eventCode = deferReasonCode()
		eventData = run.ReasonData
	}

	// Insert-first against the partial unique index (#17). The conflict
	// target names ux_runs_conc_key by its exact predicate, so only a held
	// key is ignored and any other refusal still aborts. written==0 IS the
	// conflict signal; nothing was read ahead of it.
	result, err := tx.Exec(`INSERT INTO runs
(id, job_name, job_version_id, trigger_id, origin, run_key, state, available_at,
 defer_reason, reason_code, reason_data, scheduled_for, params_json, attempt,
 max_attempts, created_at, updated_at, concurrency_key, max_parallel)
VALUES (?, ?, ?, ?, 'schedule', ?, 'queued', ?, ?, ?, ?, ?, '{}', 0, 1, ?, ?, ?, ?)
ON CONFLICT (concurrency_key) WHERE concurrency_key IS NOT NULL
AND state IN ('queued', 'running') DO NOTHING`,
		run.ID, run.JobName, run.JobVersionID, nullIfEmpty(run.TriggerID), in.RunKey,
		availableAt.UnixMilli(), nullIfEmpty(run.DeferReason),
		nullIfEmpty(run.ReasonCode), nullIfEmpty(run.ReasonData),
		fireAt.UnixMilli(), at, at, nullIfEmpty(concKey), runMaxParallel(job))
	if err != nil {
		return Run{}, fmt.Errorf("create the run of schedule %s: %w", sourceName, err)
	}
	written, err := result.RowsAffected()
	if err != nil {
		return Run{}, fmt.Errorf("create the run of schedule %s: %w", sourceName, err)
	}
	if written == 0 && concKey != "" {
		// The key is held. The insert wrote nothing; the policy decides what
		// this fire becomes.
		blockingRun := blockingRunForKeyTx(tx, concKey)
		data := concDeferDataJSON(concKey, blockingRun)
		if job.OnConflict == spec.OnConflictSkip {
			// No run, no steps, no history beyond the record of the
			// refusal itself. The dedup registration this fire made for
			// its own id is withdrawn, so a later evaluation of the same
			// event can try again instead of folding into a run that
			// never existed.
			if err := rejectTriggerConcurrencyKeyTx(tx, triggerID, sourceName, 0, in.RunKey, runID, blockingRun, concKey); err != nil {
				return Run{}, err
			}
			text := fmt.Sprintf("the run %s holds concurrency key %s", blockingRun, concKey)
			if err := markTickStoodDownTx(tx, tickID, at, reason.TICKSkippedConcurrency, text, data); err != nil {
				return Run{}, err
			}
			return Run{}, nil
		}
		// Defer (the default): store the loser queued but KEYLESS, naming
		// both the wanted key and the blocker. Holding the key here would
		// collide with the very blocker that caused the deferral, so the
		// deferred run carries none at all (model comment:
		// concurrency_key.go).
		availableAt = now.Add(DefaultDeferBackoff)
		run.ConcurrencyKey = ""
		run.DeferReason = model.DeferReasonConcurrencyKey
		run.ReasonCode = string(reason.RUNDeferredConcurrencyKey)
		run.ReasonData = data
		eventKind = "run.deferred"
		eventCode = reason.RUNDeferredConcurrencyKey
		eventData = data
		if _, err := tx.Exec(`INSERT INTO runs
(id, job_name, job_version_id, trigger_id, origin, run_key, state, available_at,
 defer_reason, reason_code, reason_data, scheduled_for, params_json, attempt,
 max_attempts, created_at, updated_at, concurrency_key, max_parallel)
VALUES (?, ?, ?, ?, 'schedule', ?, 'queued', ?, ?, ?, ?, ?, '{}', 0, 1, ?, ?, ?, ?)`,
			run.ID, run.JobName, run.JobVersionID, nullIfEmpty(run.TriggerID), in.RunKey,
			availableAt.UnixMilli(), nullIfEmpty(run.DeferReason),
			nullIfEmpty(run.ReasonCode), nullIfEmpty(run.ReasonData),
			fireAt.UnixMilli(), at, at, nil, runMaxParallel(job)); err != nil {
			return Run{}, fmt.Errorf("create the deferred run of schedule %s: %w", sourceName, err)
		}
	}

	if err := insertSteps(tx, run.ID, job.Steps); err != nil {
		return Run{}, err
	}
	faults.Point("M2:tick:after_run")

	event := RunEvent{
		RunID:   run.ID,
		At:      now,
		Kind:    eventKind,
		ToState: "queued",
		Actor:   actor,
	}
	if run.DeferReason != "" {
		// Both deferral shapes land here: a job-overlap hold (#68) and a
		// key hold (#17) read as one deferred row with its reason beside
		// it, which is what the display and fsck I14 already expect.
		run.AvailableAt = availableAt
		event.ReasonCode = string(eventCode)
		event.DetailJSON = eventData
	}
	if err := appendRunEvent(tx, event); err != nil {
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
		&r.Overlap,
		&r.pausedRaw, r.nulls.lastTick, &r.nextTickRaw, &r.createdRaw, &r.updatedRaw,
		&r.shadowRaw,
	}
}

type scheduleNulls struct {
	lastTick *sql.NullInt64
}

// finalize moves the scanned primitives into their exported shapes.
func (r *ScheduleRow) finalize() {
	r.Paused = r.pausedRaw == 1
	r.Shadow = r.shadowRaw == 1
	if r.nulls.lastTick.Valid {
		t := time.UnixMilli(r.nulls.lastTick.Int64).UTC()
		r.LastTickAt = &t
	}
	r.NextTickAt = time.UnixMilli(r.nextTickRaw).UTC()
	r.CreatedAt = time.UnixMilli(r.createdRaw).UTC()
	r.UpdatedAt = time.UnixMilli(r.updatedRaw).UTC()
}

// listAllSchedulesSQL returns every schedule, active first, then paused.
// The column list matches scanTargets exactly, overlap included: a column
// missing here is a Scan error on the first row, not a zero value.
const listAllSchedulesSQL = `SELECT id, job_name, name, kind, expr, timezone,
       spring_forward, fall_back, catchup, catchup_limit, catchup_window_ms,
       overlap, paused, last_tick_at, next_tick_at, created_at, updated_at, shadow
  FROM schedules
 ORDER BY paused ASC, job_name ASC, name ASC`

// ListAllSchedules returns every schedule row: unpaused first, then paused,
// alphabetically within each group.
func (s *Store) ListAllSchedules(ctx context.Context) ([]ScheduleRow, error) {
	rows, err := s.r.QueryContext(ctx, listAllSchedulesSQL)
	if err != nil {
		return nil, fmt.Errorf("list all schedules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ScheduleRow
	for rows.Next() {
		var row ScheduleRow
		if err := rows.Scan(row.scanTargets()...); err != nil {
			return nil, fmt.Errorf("scan a schedule row: %w", err)
		}
		row.finalize()
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list all schedules: %w", err)
	}
	return out, nil
}

// getScheduleSQL reads one schedule by its natural key. The column list
// matches scanTargets exactly, overlap included.
const getScheduleSQL = `SELECT id, job_name, name, kind, expr, timezone,
       spring_forward, fall_back, catchup, catchup_limit, catchup_window_ms,
       overlap, paused, last_tick_at, next_tick_at, created_at, updated_at, shadow
  FROM schedules
 WHERE job_name = ? AND name = ?`

// GetSchedule reads one schedule by job name and schedule name.
func (s *Store) GetSchedule(ctx context.Context, jobName, name string) (ScheduleRow, error) {
	var row ScheduleRow
	err := s.r.QueryRowContext(ctx, getScheduleSQL, jobName, name).Scan(row.scanTargets()...)
	if errors.Is(err, sql.ErrNoRows) {
		return ScheduleRow{}, fmt.Errorf("find schedule %s/%s: %w", jobName, name, ErrNotFound)
	}
	if err != nil {
		return ScheduleRow{}, fmt.Errorf("find schedule %s/%s: %w", jobName, name, err)
	}
	row.finalize()
	return row, nil
}

// PauseSchedule sets paused=1 on the schedule and returns the new row.
// Idempotent: calling on an already-paused schedule succeeds without error
// and does not change updated_at.
func (s *Store) PauseSchedule(ctx context.Context, jobName, name string) (ScheduleRow, error) {
	now := s.clk.Now().UTC()
	var row ScheduleRow

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE schedules SET paused = 1, updated_at = ?
 WHERE job_name = ? AND name = ? AND paused = 0`, now.UnixMilli(), jobName, name)
		if err != nil {
			return fmt.Errorf("pause schedule %s/%s: %w", jobName, name, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("pause schedule %s/%s: %w", jobName, name, err)
		}
		if changed == 0 {
			// Already paused: read back without rewriting.
		}
		err = tx.QueryRow(getScheduleSQL, jobName, name).Scan(row.scanTargets()...)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("pause schedule %s/%s: %w", jobName, name, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("pause schedule %s/%s: %w", jobName, name, err)
		}
		row.finalize()
		return nil
	})
	if err != nil {
		return ScheduleRow{}, err
	}
	return row, nil
}

// ResumeSchedule sets paused=0, writes the caller's pre-computed next_tick_at,
// and returns the new row. The caller computes nextTickAt from the cursor and
// cron expression to avoid re-parsing inside the transaction.
func (s *Store) ResumeSchedule(ctx context.Context, jobName, name string, nextTickAt time.Time) (ScheduleRow, error) {
	now := s.clk.Now().UTC()
	var row ScheduleRow

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE schedules SET paused = 0, next_tick_at = ?, updated_at = ?
 WHERE job_name = ? AND name = ? AND paused = 1`,
			nextTickAt.UnixMilli(), now.UnixMilli(), jobName, name)
		if err != nil {
			return fmt.Errorf("resume schedule %s/%s: %w", jobName, name, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("resume schedule %s/%s: %w", jobName, name, err)
		}
		if changed == 0 {
			// Already active: read back without rewriting.
		}
		err = tx.QueryRow(getScheduleSQL, jobName, name).Scan(row.scanTargets()...)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("resume schedule %s/%s: %w", jobName, name, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("resume schedule %s/%s: %w", jobName, name, err)
		}
		row.finalize()
		return nil
	})
	if err != nil {
		return ScheduleRow{}, err
	}
	return row, nil
}
