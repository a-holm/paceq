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

// The sensor half of the tick/trigger/run pipeline. A schedule is materialised
// in one transaction because the scheduler decided everything before it wrote;
// a sensor cannot do that, because between deciding (running the sensor) and
// committing there is real time in which the world moves. What the two share
// is the same refusal: the cursor never moves without every run it produced
// being committed in the same transaction (plan 02 section 4.1, guarantee G4).
//
// CommitSensorTick is that transaction. It takes the sensor's intention row
// (written by BeginSensorTick), the run keys and cursor the evaluation
// produced, and commits tick + triggers + runs + run_keys + run_events +
// cursor in ONE BEGIN IMMEDIATE transaction. A crash anywhere in that window
// rolls the whole thing back: the DB sees either none of it or all of it, so
// restart re-evaluates from the old cursor and the run_keys gate folds the
// replay into the existing run. That is the exactly-once cursor advance over
// at-least-once run creation the design is built on (plan 02 section 4.3).

// sensorCommitHook is the fault point the crash test kills a child at, mid
// commit. Nothing sets it outside tests; see CommitSensorTick where it is
// called. It mirrors beforeCommit in migrate.go.
var sensorCommitHook func()

// SensorRow is one sensor as the commit path reads it: the cursor the
// evaluation started from, the version that guards it, and the dedup epoch
// that makes a cursor reset safe.
type SensorRow struct {
	Name          string
	JobName       string
	Kind          string
	IntervalMS    int64
	MinIntervalMS int64
	TimeoutMS     int64
	Paused        bool
	Cursor        string
	CursorVersion int64
	DedupEpoch    int64
	NextEvalAt    int64
}

// BeginSensorTickInput names the sensor evaluation that is about to start.
type BeginSensorTickInput struct {
	SensorName      string
	CursorBefore    string
	DaemonSessionID string
	Now             time.Time
}

// BeginSensorTickResult is everything the commit half needs to guard the
// evaluation it is about to record.
type BeginSensorTickResult struct {
	// TickID is the intention row CommitSensorTick will close.
	TickID string

	// CursorVersion is the CAS guard read at the start. CommitSensorTick
	// only advances the cursor when this value still holds, so a commit from
	// an evaluation that was already overtaken is refused instead of writing
	// an old result over a new one.
	CursorVersion int64
}

// BeginSensorTick writes the intention row for one sensor evaluation: a tick
// in 'running' carrying the cursor it started from. A crash after this point
// leaves a visible interrupted tick that reconciliation closes; there is no
// such thing as a sensor evaluation the database silently forgets started.
//
// The write is deliberately separate from CommitSensorTick. The sensor runs
// between them, outside any transaction (plan 00 section 3.11: no process
// execution or network I/O inside a write transaction).
func (s *Store) BeginSensorTick(ctx context.Context, in BeginSensorTickInput) (BeginSensorTickResult, error) {
	now := in.Now
	if now.IsZero() {
		now = s.clk.Now().UTC()
	}
	at := now.UnixMilli()

	tickID, err := id.New(now)
	if err != nil {
		return BeginSensorTickResult{}, fmt.Errorf("mint a sensor tick id: %w", err)
	}

	var out BeginSensorTickResult
	out.TickID = tickID

	err = s.withTx(ctx, func(tx *sql.Tx) error {
		var version int64
		err := tx.QueryRow(`SELECT cursor_version FROM sensors WHERE name = ?`,
			in.SensorName).Scan(&version)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("begin sensor tick for %s: %w", in.SensorName, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("read sensor %s for its tick: %w", in.SensorName, err)
		}

		if _, err := tx.Exec(`INSERT INTO ticks
(id, source_kind, source_name, started_at, last_started_at, outcome, cursor_before, daemon_session_id)
VALUES (?, 'sensor', ?, ?, ?, 'running', ?, ?)`,
			tickID, in.SensorName, at, at, nullIfEmpty(in.CursorBefore), nullIfEmpty(in.DaemonSessionID)); err != nil {
			return fmt.Errorf("record the sensor intention tick for %s: %w", in.SensorName, err)
		}

		out.CursorVersion = version
		return nil
	})
	if err != nil {
		return BeginSensorTickResult{}, err
	}
	return out, nil
}

// SensorTrigger is one trigger a sensor evaluation produced: the run key that
// deduplicates it and the parameters that vary per trigger. Each becomes a
// triggers row and, on a new run key, a runs row.
type SensorTrigger struct {
	RunKey     string
	ParamsJSON string
}

// SensorTickCommitInput is everything a completed sensor evaluation decided,
// ready to be written or refused as one unit.
type SensorTickCommitInput struct {
	// TickID is the intention row opened by BeginSensorTick.
	TickID string

	SensorName string
	JobName    string

	// CursorVersion is the guard BeginSensorTick read. It is compared inside
	// the transaction; a stale value refuses the commit.
	CursorVersion int64

	// CursorAfter moves the sensor cursor when Outcome is triggered. It is
	// the value the whole evaluation built towards.
	CursorAfter string

	// DedupEpoch is the sensor's dedup epoch, the same value BeginSensorTick
	// saw on the row. It prevents a cursor reset from replaying an old key
	// into an already-ringed table.
	DedupEpoch int64

	// Triggers are the run keys this evaluation decided to fire. Each is
	// registered against the run_keys gate first, so a replayed evaluation
	// folds into the run that already exists instead of creating a twin.
	Triggers []SensorTrigger

	// Outcome is OutcomeTriggered, OutcomeSkipped or OutcomeError. Only a
	// triggered evaluation moves the cursor and creates runs.
	Outcome string

	// ReasonCode and ReasonText belong on a skipped or error tick. A skipped
	// sensor's own reason travels verbatim (plan: the sensor says why it
	// skipped and paceq does not paraphrase).
	ReasonCode reason.Code
	ReasonText string

	// NextEvalAt is when the sensor becomes due again. Set on every commit.
	NextEvalAt int64

	// DurationMs is how long the evaluation took, for the tick row.
	DurationMs int64

	Now time.Time
}

// SensorTickCommitResult reports what one atomic commit did.
type SensorTickCommitResult struct {
	// Accepted is how many new runs were created.
	Accepted int

	// Deduped is how many triggers folded into an existing run.
	Deduped int

	// RunIDs are the ids of the accepted runs, in trigger order.
	RunIDs []string

	// Fenced is true when the commit was refused because the sensor already
	// advanced past the cursor_version this evaluation started from. Nothing
	// was written except the tick being marked error with TICK_MISSED_LEASE_LOST.
	Fenced bool
}

// CommitSensorTick records one finished sensor evaluation, all or nothing: the
// tick outcome, every trigger, every run and its event, the run_keys dedup
// gate, and the cursor advance share ONE BEGIN IMMEDIATE transaction. A crash
// at any point between BEGIN IMMEDIATE and COMMIT leaves the database as if
// the evaluation had never committed, so a restart re-evaluates from the old
// cursor and the dedup gate makes the replay a no-op. There is no window in
// which runs exist without their cursor, or a cursor exists without its runs.
//
// The cursor advance is guarded. CursorVersion was read at evaluation start;
// if the sensor has since been advanced by another evaluation (a takeover, a
// reset, a concurrent pass), the CAS matches zero rows, the commit is refused
// and the tick records TICK_MISSED_LEASE_LOST. An old result never overwrites
// a newer one.
func (s *Store) CommitSensorTick(ctx context.Context, in SensorTickCommitInput) (SensorTickCommitResult, error) {
	now := in.Now
	if now.IsZero() {
		now = s.clk.Now().UTC()
	}
	at := now.UnixMilli()

	// Ids are minted before the transaction opens: each costs a read of the
	// entropy source, and the write model forbids that while the write lock
	// is held.
	triggerIDs := make([]string, len(in.Triggers))
	runIDs := make([]string, len(in.Triggers))
	for i := range in.Triggers {
		tid, err := id.New(now)
		if err != nil {
			return SensorTickCommitResult{}, fmt.Errorf("mint a trigger id: %w", err)
		}
		rid, err := id.New(now)
		if err != nil {
			return SensorTickCommitResult{}, fmt.Errorf("mint a run id: %w", err)
		}
		triggerIDs[i] = tid
		runIDs[i] = rid
	}

	out := SensorTickCommitResult{}
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		// Step 0: the cursor CAS guard. The evaluation whose result we are
		// committing read this version at start; if the sensor moved since,
		// nothing here may write. Zero rows means the guard is stale.
		res, err := tx.Exec(`UPDATE sensors
SET cursor = COALESCE(?, cursor),
    cursor_updated_at = CASE WHEN ? IS NULL THEN cursor_updated_at ELSE ? END,
    cursor_version = cursor_version + 1,
    next_eval_at = ?,
    updated_at = ?
WHERE name = ? AND cursor_version = ?`,
			nullIfEmpty(in.CursorAfter), nullIfEmpty(in.CursorAfter), at, in.NextEvalAt, at, in.SensorName, in.CursorVersion)
		if err != nil {
			return fmt.Errorf("advance the cursor of sensor %s: %w", in.SensorName, err)
		}
		written, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("count the cursor advance of sensor %s: %w", in.SensorName, err)
		}
		if written == 0 {
			// A stale evaluation woke up and tried to commit. Nothing else
			// is written; the tick records the refusal so explain can say
			// why a started evaluation produced nothing.
			out.Fenced = true
			return closeSensorTickTx(tx, in.TickID, at, OutcomeError,
				reason.TICKMissedLeaseLost, in.DurationMs, "", 0, 0, in.ReasonText)
		}
		faults.Point("M3:commit:after_cursor")

		// Triggered evaluations produce runs; skipped and error evaluations
		// close their tick and leave the cursor as it was. In all three cases
		// the cursor_version has already been bumped, which is what fences a
		// concurrently committed evaluation's replay.
		if in.Outcome != OutcomeTriggered {
			return closeSensorTickTx(tx, in.TickID, at, in.Outcome,
				in.ReasonCode, in.DurationMs, "", 0, 0, in.ReasonText)
		}

		// The current version of the job is chosen inside the transaction, so
		// an apply racing the commit still freezes one whole version. Its spec
		// supplies the steps every run freezes.
		var versionID, specJSON string
		if err := tx.QueryRow(`SELECT j.current_version_id, v.spec_json
FROM jobs j JOIN job_versions v ON v.id = j.current_version_id
WHERE j.name = ?`, in.JobName).Scan(&versionID, &specJSON); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("materialise sensor run for %s: no job version is current: %w", in.JobName, ErrNotFound)
			}
			return fmt.Errorf("find the current version of job %s: %w", in.JobName, err)
		}
		job, err := spec.FromIR([]byte(specJSON))
		if err != nil {
			return fmt.Errorf("read the frozen spec of job %s (version %s): %w", in.JobName, versionID, err)
		}

		for i, tr := range in.Triggers {
			accepted, err := commitSensorTriggerTx(tx, commitTriggerParams{
				tickID:    in.TickID,
				triggerID: triggerIDs[i],
				runID:     runIDs[i],
				sensor:    in.SensorName,
				epoch:     in.DedupEpoch,
				jobName:   in.JobName,
				versionID: versionID,
				job:       job,
				runKey:    tr.RunKey,
				params:    tr.ParamsJSON,
				at:        at,
			})
			if err != nil {
				return err
			}
			if accepted {
				out.Accepted++
				out.RunIDs = append(out.RunIDs, runIDs[i])
			} else {
				out.Deduped++
			}
		}
		faults.Point("M3:commit:after_runs")

		// sensorCommitHook is the fault point the crash test kills a child at,
		// mid commit, after the runs landed but before the transaction COMMITs.
		// A kill here must leave no partial state, which is what the atomicity
		// guarantee is about. Nothing sets it outside tests. It mirrors
		// beforeCommit in migrate.go for the same reason: a subprocess kill is
		// the honest way to prove a transaction cannot be half applied.
		if sensorCommitHook != nil {
			sensorCommitHook()
		}

		return closeSensorTickTx(tx, in.TickID, at, OutcomeTriggered,
			in.ReasonCode, in.DurationMs, in.CursorAfter, out.Accepted, out.Deduped, in.ReasonText)
	})
	if err != nil {
		return SensorTickCommitResult{}, err
	}
	return out, nil
}

// commitTriggerParams carries what commitSensorTriggerTx needs to materialise
// one trigger and its run, all within the caller's open transaction.
type commitTriggerParams struct {
	tickID    string
	triggerID string
	runID     string
	sensor    string
	epoch     int64
	jobName   string
	versionID string
	job       *spec.Job
	runKey    string
	params    string
	at        int64
}

// commitSensorTriggerTx materialises one sensor trigger and its run, or folds
// it into an existing run when the run key was already seen. The dedup gate is
// run_keys, inserted first with ON CONFLICT DO NOTHING and read back through
// RowsAffected, never a SELECT-then-INSERT (plan 11 section 5.2). Each trigger
// writes a triggers row either way, so explain can show a folded trigger
// rather than a gap.
//
// It reports whether the trigger created a new run (true) or folded into an
// existing run (false).
func commitSensorTriggerTx(tx *sql.Tx, p commitTriggerParams) (accepted bool, _ error) {
	params := p.params
	if params == "" {
		params = "{}"
	}

	// Register the run key against this sensor and epoch. A row that inserts
	// is a new run; one that conflicts is a replay and folds into the run the
	// key already points at.
	res, err := tx.Exec(`INSERT INTO run_keys (source_id, epoch, run_key, first_seen_at, run_id)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(source_id, epoch, run_key) DO NOTHING`,
		p.sensor, p.epoch, p.runKey, p.at, p.runID)
	if err != nil {
		return false, fmt.Errorf("register the run key %s: %w", p.runKey, err)
	}
	newKey, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count the run key %s registration: %w", p.runKey, err)
	}

	if newKey == 0 {
		// Deduped: the key (or its replay) already produced a run. The
		// trigger row records the fold and points at the original run.
		var original string
		if err := tx.QueryRow(`SELECT run_id FROM run_keys
WHERE source_id = ? AND epoch = ? AND run_key = ?`,
			p.sensor, p.epoch, p.runKey).Scan(&original); err != nil {
			return false, fmt.Errorf("find the run behind key %s: %w", p.runKey, err)
		}
		_, err := tx.Exec(`INSERT INTO triggers
(id, tick_id, job_name, run_key, params_json, created_at, outcome, reason_code, run_id)
VALUES (?, ?, ?, ?, ?, ?, 'deduped', ?, ?)`,
			p.triggerID, p.tickID, p.jobName, p.runKey, params, p.at,
			string(reason.TRIGGERDedupedRunKey), original)
		if err != nil {
			return false, fmt.Errorf("record the deduped trigger for %s: %w", p.runKey, err)
		}
		return false, nil
	}

	// A new run: the trigger, the queued run with its steps, and its queued
	// event all land in the same transaction as the cursor advance. The
	// trigger is inserted with run_id NULL and the run points back at it,
	// exactly as the schedule path does, because triggers and runs are
	// mutually foreign: both circular references cannot be satisfied by one
	// insert. The run's trigger_id is the link; the trigger run_id column is
	// for a deduped trigger to name its original run.
	if _, err := tx.Exec(`INSERT INTO triggers
(id, tick_id, job_name, run_key, params_json, created_at, outcome, reason_code)
VALUES (?, ?, ?, ?, ?, ?, 'accepted', ?)`,
		p.triggerID, p.tickID, p.jobName, p.runKey, params, p.at,
		string(reason.TRIGGERAccepted)); err != nil {
		return false, fmt.Errorf("record the accepted trigger for %s: %w", p.runKey, err)
	}

	if _, err := tx.Exec(`INSERT INTO runs
(id, job_name, job_version_id, trigger_id, origin, run_key, state, available_at,
 params_json, attempt, max_attempts, created_at, updated_at)
VALUES (?, ?, ?, ?, 'sensor', ?, 'queued', ?, ?, 0, 1, ?, ?)`,
		p.runID, p.jobName, p.versionID, p.triggerID, p.runKey, p.at, params, p.at, p.at); err != nil {
		return false, fmt.Errorf("create the sensor run for %s: %w", p.runKey, err)
	}

	if err := insertSteps(tx, p.runID, p.job.Steps); err != nil {
		return false, err
	}

	if err := appendRunEvent(tx, RunEvent{
		RunID:   p.runID,
		At:      time.UnixMilli(p.at).UTC(),
		Kind:    "run.queued",
		ToState: "queued",
		Actor:   "sensor",
	}); err != nil {
		return false, err
	}
	return true, nil
}

// closeSensorTickTx writes a tick's outcome. It is the only place a sensor tick
// gets closed, used by every exit of CommitSensorTick so the 'running'
// intention row never lingers. A skipped sensor's own reason is stored verbatim
// (reasonText), because only the sensor knows what its skip meant.
func closeSensorTickTx(tx *sql.Tx, tickID string, at int64, outcome string,
	code reason.Code, durationMs int64, cursorAfter string, accepted, deduped int,
	reasonText string,
) error {
	_, err := tx.Exec(`UPDATE ticks
SET outcome = ?, finished_at = ?, duration_ms = ?, cursor_after = ?,
    trigger_count = ?, deduped_count = ?, reason_code = ?, reason_text = ?
WHERE id = ?`,
		outcome, at, durationMs, nullIfEmpty(cursorAfter), accepted, deduped,
		nullableCode(code), reasonText, tickID)
	if err != nil {
		return fmt.Errorf("close the sensor tick %s: %w", tickID, err)
	}
	return nil
}

// nullableCode keeps an empty reason code out of the database as NULL. An empty
// code is not a code, and the ticks CHECK demands a reason exactly when a
// triggered or running outcome claims none.
func nullableCode(code reason.Code) any {
	if string(code) == "" {
		return nil
	}
	return string(code)
}
