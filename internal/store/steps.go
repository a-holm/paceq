package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// The step lifecycle writes. The engine drives these: StartStep when a worker
// claims a pending step, FinishStep when the runner returns a verdict. Every
// terminal verdict carries its log metadata in the same transaction, because
// a verdict whose evidence went missing is exactly the drift the write model
// refuses to allow.
var (
	// ErrStepNotPending is returned when a step that is not pending was sent
	// to start. A step already running cannot start again without losing the
	// first attempt's started_at.
	ErrStepNotPending = errors.New("the step is not pending")

	// ErrStepNotRunning is returned for a finish of a step that is not
	// running. A verdict on a step nobody started has no transition to carry.
	ErrStepNotRunning = errors.New("the step is not running")

	// ErrReadOnly is returned when a store opened through OpenReadOnly is
	// asked to write.
	ErrReadOnly = errors.New("the store is open read only")
)

// StepLog is what the log sink reported at Finish. It lands in the steps row
// beside the verdict, in one transaction with it.
type StepLog struct {
	// RelPath is the log file's path relative to the log root, so the state
	// directory can move without rewriting rows.
	RelPath   string
	Bytes     int64
	Truncated bool
	ErrorTail string
}

// StepFinish is one terminal verdict on one attempt of one step, plus the
// metadata of the log that proves it happened.
type StepFinish struct {
	RunID       string
	Step        string
	ToState     string // succeeded, failed, skipped or cancelled
	ReasonCode  string
	ReasonText  string
	Error       string
	ExitCode    int
	HasExitCode bool
	Signal      string

	FinishedAt time.Time

	// Actor names who decided: engine, reaper, operator. Empty is system.
	Actor string

	Log StepLog
}

// terminalStates are the states a step may finish into. The database CHECK
// would refuse anything else anyway; naming them here turns a constraint
// violation into an error that says which word was wrong before any row is
// touched.
var terminalStates = map[string]bool{
	"succeeded": true,
	"failed":    true,
	"skipped":   true,
	"cancelled": true,
}

// StartStep claims a pending step for its next attempt: state running, the
// attempt counter up by one, started_at set. The event rides in the same
// transaction, so history and state cannot disagree.
func (s *Store) StartStep(ctx context.Context, runID, step string, at time.Time) error {
	if at.IsZero() {
		at = s.clk.Now().UTC()
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE steps SET state = 'running', attempt = attempt + 1,
	started_at = ?, finished_at = NULL
WHERE run_id = ? AND name = ? AND state = 'pending'`, at.UnixMilli(), runID, step)
		if err != nil {
			return fmt.Errorf("start step %s of run %s: %w", step, runID, err)
		}
		written, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("start step %s of run %s: %w", step, runID, err)
		}
		if written == 0 {
			return fmt.Errorf("start step %s of run %s: %w", step, runID, ErrStepNotPending)
		}
		return appendRunEvent(tx, RunEvent{
			RunID:     runID,
			StepName:  step,
			At:        at,
			Kind:      "step.started",
			FromState: "pending",
			ToState:   "running",
			Actor:     "engine",
		})
	})
}

// FinishStep records the verdict of one attempt and the metadata of its log:
// state, reason, exit code, timing, and beside them log_path, log_bytes,
// log_truncated and error_tail. All of it is one transaction. The caller does
// every piece of file I/O BEFORE calling this: sink.Finish runs first, and
// what happens inside the transaction here is pure SQL.
func (s *Store) FinishStep(ctx context.Context, f StepFinish) error {
	if !terminalStates[f.ToState] {
		return fmt.Errorf("finish step %s of run %s into %q: not a terminal state, want one of succeeded, failed, skipped, cancelled",
			f.Step, f.RunID, f.ToState)
	}
	finishedAt := f.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = s.clk.Now().UTC()
	}
	var exitCode any
	if f.HasExitCode {
		exitCode = f.ExitCode
	}
	var signal any
	if f.Signal != "" {
		signal = f.Signal
	}

	return s.withTx(ctx, func(tx *sql.Tx) error {
		// The verdict. The WHERE clause pins the transition: finishing a
		// step twice, or one that never ran, affects no row and fails.
		result, err := tx.Exec(`UPDATE steps SET state = ?, reason_code = ?, reason_text = ?,
	error = ?, exit_code = ?, signal = ?, finished_at = ?, duration_ms = ? - started_at
WHERE run_id = ? AND name = ? AND state = 'running'`,
			f.ToState, nullIfEmpty(f.ReasonCode), nullIfEmpty(f.ReasonText),
			nullIfEmpty(f.Error), exitCode, signal, finishedAt.UnixMilli(),
			finishedAt.UnixMilli(), f.RunID, f.Step)
		if err != nil {
			return fmt.Errorf("finish step %s of run %s: %w", f.Step, f.RunID, err)
		}
		written, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("finish step %s of run %s: %w", f.Step, f.RunID, err)
		}
		if written == 0 {
			return fmt.Errorf("finish step %s of run %s: %w", f.Step, f.RunID, ErrStepNotRunning)
		}

		// The evidence about the log, in the SAME transaction as the
		// verdict above. Splitting these into two transactions is the bug
		// TestFinishStepRollsBackWholesale exists to catch.
		logPath, logBytes, logTruncated, errorTail := f.Log.RelPath, f.Log.Bytes, 0, f.Log.ErrorTail
		if f.Log.Truncated {
			logTruncated = 1
		}
		result, err = tx.Exec(`UPDATE steps SET log_path = ?, log_bytes = ?, log_truncated = ?,
	error_tail = ?
WHERE run_id = ? AND name = ?`,
			nullIfEmpty(logPath), logBytes, logTruncated, nullIfEmpty(errorTail),
			f.RunID, f.Step)
		if err != nil {
			return fmt.Errorf("record the log of step %s of run %s: %w", f.Step, f.RunID, err)
		}
		written, err = result.RowsAffected()
		if err != nil || written == 0 {
			// Unreachable after the update above hit one row, but a
			// mistake here must not pass silently.
			return fmt.Errorf("record the log of step %s of run %s: %w", f.Step, f.RunID, ErrStepNotRunning)
		}

		return appendRunEvent(tx, RunEvent{
			RunID:      f.RunID,
			StepName:   f.Step,
			At:         finishedAt,
			Kind:       "step.finished",
			FromState:  "running",
			ToState:    f.ToState,
			ReasonCode: f.ReasonCode,
			Actor:      f.Actor,
		})
	})
}
