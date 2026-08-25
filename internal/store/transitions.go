package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// This file is the transition layer. Every state change a run or a step goes
// through is decided here by internal/model's machines and written here as
// exactly one UPDATE plus exactly one run_events row, in one transaction
// (G10). A state change without its event, or an event without its state
// change, cannot be produced by any caller, because no caller reaches these
// rows except through this file.
//
// The machines decide; this file only performs. When a machine refuses, the
// refusal returns before a single row is touched, so a refused transition
// leaves neither state nor history behind.

// ErrNotClaimable is returned when a run cannot be claimed: it is not queued,
// it is not available yet, or a cancellation is waiting to be observed. It is
// an ordinary outcome for a worker polling for work, not a fault.
var (
	ErrNotClaimable   = errors.New("the run cannot be claimed")
	ErrStepNotPending = errors.New("the step is not pending")
)

// LeaseInput says who claims a run and for how long.
type LeaseInput struct {
	// Owner is the executor's name. Every later write to this run has to
	// come from the same name, which is what makes the lease a fence and
	// not a decoration.
	Owner string

	// TTL is how long the claim lasts. Zero means DefaultRunLeaseTTL.
	TTL time.Duration
}

// CancelRequest is the durable cancellation record read back after it was
// written. The request is not a transition and carries no event: the event
// belongs to whoever observes the request and stops the run (02 section 5.8).
type CancelRequest struct {
	CancelRequestedAt time.Time
	CancelRequestedBy string
	CancelReason      string
}

// RequestCancel records that somebody wants the run stopped, durably and
// before anything is killed. The first request stands: a second one changes
// nothing, so two people asking at once cannot disagree about who asked or
// why. Observing the request is a different act, done by whoever holds the
// lease, and that is the transition with the event.
func (s *Store) RequestCancel(ctx context.Context, runID, by, why string) (CancelRequest, error) {
	now := s.clk.Now().UTC()
	var out CancelRequest

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		// nolint:fencing: a cancellation request belongs to whoever asks,
		// not to whoever holds the lease; the flag is monotonic and the
		// holder is the one who acts on it.
		result, err := tx.Exec(`UPDATE runs SET
			cancel_requested_at = COALESCE(cancel_requested_at, ?),
			cancel_requested_by = COALESCE(cancel_requested_by, ?),
			cancel_reason = COALESCE(cancel_reason, ?),
			updated_at = ?
		WHERE id = ?`, now.UnixMilli(), nullIfEmpty(by), why, now.UnixMilli(), runID)
		if err != nil {
			return fmt.Errorf("request the cancellation of run %s: %w", runID, err)
		}
		written, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("request the cancellation of run %s: %w", runID, err)
		}
		if written == 0 {
			return fmt.Errorf("request the cancellation of run %s: %w", runID, ErrRunNotFound)
		}
		var at sql.NullInt64
		var byName, reason sql.NullString
		if err := tx.QueryRow(`SELECT cancel_requested_at, cancel_requested_by, cancel_reason
FROM runs WHERE id = ?`, runID).Scan(&at, &byName, &reason); err != nil {
			return fmt.Errorf("read back the cancellation of run %s: %w", runID, err)
		}
		out = CancelRequest{
			CancelRequestedAt: timeOrZero(at),
			CancelRequestedBy: byName.String,
			CancelReason:      reason.String,
		}
		return nil
	})
	if err != nil {
		return CancelRequest{}, err
	}
	return out, nil
}

// CancelRequested reports whether a cancellation is waiting, and who asked.
// The executor reads this between steps and on its poll while a step runs.
func (s *Store) CancelRequested(ctx context.Context, runID string) (bool, string, error) {
	var (
		at      sql.NullInt64
		by      sql.NullString
		ignored sql.NullString
	)
	err := s.r.QueryRowContext(ctx, `SELECT cancel_requested_at, cancel_requested_by, cancel_reason
FROM runs WHERE id = ?`, runID).Scan(&at, &by, &ignored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", fmt.Errorf("read the cancellation of run %s: %w", runID, ErrRunNotFound)
	}
	if err != nil {
		return false, "", fmt.Errorf("read the cancellation of run %s: %w", runID, err)
	}
	return at.Valid, by.String, nil
}

// RecordAttemptProcess stamps which live process is carrying one running
// step: its pid and the kernel's start ticks for that pid, read at spawn
// time. The engine writes it through OnStart the instant a spawn succeeds, so
// the evidence is on file before any verdict could exist, and startup
// reconciliation can later prove a surviving process is (or is not) the child
// it was told about.
//
// The write is fenced like every other holder's write: only the executor
// that owns the run's lease may name processes for it, so a replaced
// executor cannot relabel someone else's process as one of ours.
func (s *Store) RecordAttemptProcess(ctx context.Context, runID, step string, ref LeaseRef, pid int, startTicks int64) error {
	if !ref.held() {
		return fmt.Errorf("baseline step %s of run %s: no holder was named", step, runID)
	}
	now := s.clk.Now().UTC()
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := checkLeaseTx(tx, runID, ref, now); err != nil {
			return err
		}
		result, err := tx.Exec(`UPDATE steps SET pid = ?, pid_start_ticks = ?
WHERE run_id = ? AND name = ?`, pid, startTicks, runID, step)
		if err != nil {
			return err
		}
		if changed, err := result.RowsAffected(); err != nil {
			return err
		} else if changed == 0 {
			return fmt.Errorf("baseline step %s of run %s: no such step", step, runID)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("record the process of step %s of run %s: %w", step, runID, err)
	}
	return nil
}

// StartStep opens the first attempt of a pending step: running, attempt up by
// one, started_at stamped, its event in the same transaction. The run has to
// be running and still held by the writer at the writer's fencing token: a
// step may not move while its run is queued, whatever the step machine alone
// would allow, and it may not move under a lease the writer has lost.
func (s *Store) StartStep(ctx context.Context, runID, name string, ref LeaseRef) error {
	if !ref.held() {
		return fmt.Errorf("start step %s of run %s: no holder was named", name, runID)
	}
	now := s.clk.Now().UTC()

	return s.withTx(ctx, func(tx *sql.Tx) error {
		if err := checkLeaseTx(tx, runID, ref, now); err != nil {
			return fmt.Errorf("start step %s of run %s: %w", name, runID, err)
		}
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		if run.State != string(model.RunRunning) {
			return fmt.Errorf("start step %s of run %s: the run is %s, not running",
				name, runID, run.State)
		}

		step, err := readStepTx(tx, runID, name)
		if err != nil {
			return err
		}
		if step.State != string(model.StepPending) {
			return fmt.Errorf("start step %s of run %s: %w", name, runID, ErrStepNotPending)
		}
		cur, err := model.ParseStepState(step.State)
		if err != nil {
			return err
		}

		state, effects, err := model.NextStepState(cur, model.EvStepStarted, model.Guards{})
		if err != nil {
			return fmt.Errorf("start step %s of run %s: %w", name, runID, err)
		}

		return finishTransition(tx, "start_step", func() error {
			_, err := tx.Exec(`UPDATE steps SET state = 'running', attempt = attempt + 1,
				started_at = ?, finished_at = NULL
			WHERE run_id = ? AND name = ? AND state = 'pending'`,
				now.UnixMilli(), runID, name)
			return err
		}, tx, RunEvent{
			RunID: runID, StepName: name, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
		})
	})
}

// LogMeta is what the log sink reported for one attempt: where the file is
// relative to the log root, how big it grew, whether the quota cut it, and
// the last lines of output. It lands beside the verdict, in the same
// transaction, because a verdict whose evidence went missing is exactly the
// drift the write model refuses.
type LogMeta struct {
	RelPath   string
	Bytes     int64
	Truncated bool
	ErrorTail string
}

// RetryPlan is the schedule a caller computed for a further attempt of a
// step whose machine transition went back to pending. The machine still
// decides whether the step goes back to pending; the plan only fills in the
// when and the words. Backoff arithmetic lives with the caller, which holds
// both the policy and the clock.
type RetryPlan struct {
	// NextAttemptAt is when the step becomes runnable again. Zero falls
	// back to now, the M1 behaviour before backoff existed.
	NextAttemptAt time.Time

	// ReasonCode names the scheduled retry on the row and its event.
	// Empty keeps the outcome's own code.
	ReasonCode reason.Code

	// DetailJSON replaces the event's detail object on the pending path,
	// where the facts that matter are the attempt number and the
	// backoff, not the exit verdict.
	DetailJSON string
}

// StepOutcome is one event the engine hands to a step: the runner came back,
// or an upstream ended in a way that closes this step, or a cancellation was
// observed. Event names the model event, and the machine decides whether the
// step may take it.
type StepOutcome struct {
	// Event is one of the step machine's input names: step_succeeded,
	// step_failed, cancel_observed or upstream_failed.
	Event string

	// ReasonCode explains a terminal outcome. The machine refuses a
	// terminal transition without one.
	ReasonCode reason.Code

	// ExitCode is what the process exited with. Nil means there is none:
	// a signalled step and a step that never ran have no exit code, and
	// zero is a perfectly ordinary success.
	ExitCode *int

	Signal     string
	FinishedAt time.Time

	// LogMeta carries the attempt's log facts. Empty means the attempt
	// produced no log, which is what a skip is.
	LogMeta LogMeta

	// DetailJSON is the canonical detail object on the event and on the
	// row's reason_data, empty for none.
	DetailJSON string

	// Retry schedules a further attempt when this event sends the step
	// back to pending. Nil means runnable again at once. It never changes
	// the transition itself; the machine decides from the row.
	Retry *RetryPlan

	// Artifacts are the references the step published through its output
	// file (#13). They are written inside this verdict's transaction and
	// only when the machine lands the step on succeeded: a failed step
	// publishes nothing.
	Artifacts []Artifact
}

// RecordStepOutcome applies one event to one step. The machine decides
// whether the step may take the transition and what it demands; this writes
// the verdict, its log facts and its event in one transaction. A step that
// fails with attempts left goes back to pending for its next attempt: parked
// at next_attempt_at when the caller attached a RetryPlan, runnable again at
// once when it did not. The claim gate, not this method, decides when the
// next attempt may start.
//
// The ref is the fence. A holder writes only while its token still matches;
// recovery passes the zero ref, which is refused against any live lease. A
// writer that lost the lease gets ErrLeaseLost and nothing on the row moves.
func (s *Store) RecordStepOutcome(ctx context.Context, runID, name string, out StepOutcome, ref LeaseRef) error {
	now := s.clk.Now().UTC()
	finishedAt := out.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = now
	}
	detail := out.DetailJSON
	if detail == "" {
		detail = "{}"
	}

	return s.withTx(ctx, func(tx *sql.Tx) error {
		if err := checkLeaseTx(tx, runID, ref, now); err != nil {
			return fmt.Errorf("record %s on step %s of run %s: %w", out.Event, name, runID, err)
		}
		step, err := readStepTx(tx, runID, name)
		if err != nil {
			return err
		}
		cur, err := model.ParseStepState(step.State)
		if err != nil {
			return err
		}

		guards := model.Guards{
			ReasonCode:   string(out.ReasonCode),
			AttemptsLeft: step.Attempt < step.MaxAttempts,
		}
		state, effects, err := model.NextStepState(cur, model.Event(out.Event), guards)
		if err != nil {
			return fmt.Errorf("record %s on step %s of run %s: %w", out.Event, name, runID, err)
		}

		var exitCode any
		if out.ExitCode != nil {
			exitCode = *out.ExitCode
		}
		var signal any
		if out.Signal != "" {
			signal = out.Signal
		}
		var nextAttempt any
		rowReason := out.ReasonCode
		rowDetail := detail
		if state == model.StepPending {
			// The retry transition: back to pending, runnable when the
			// plan says, or at once without one.
			if out.Retry != nil && !out.Retry.NextAttemptAt.IsZero() {
				nextAttempt = out.Retry.NextAttemptAt.UTC().UnixMilli()
			} else {
				nextAttempt = finishedAt.UnixMilli()
			}
			if out.Retry != nil {
				if out.Retry.ReasonCode != "" {
					rowReason = out.Retry.ReasonCode
				}
				if out.Retry.DetailJSON != "" {
					rowDetail = out.Retry.DetailJSON
				}
			}
		}
		truncated := 0
		if out.LogMeta.Truncated {
			truncated = 1
		}

		if err := finishTransition(tx, "record_outcome", func() error {
			_, err := tx.Exec(`UPDATE steps SET state = ?, reason_code = ?, reason_data = ?,
				exit_code = ?, signal = ?,
				finished_at = CASE WHEN ? THEN ? ELSE finished_at END,
				duration_ms = CASE WHEN started_at IS NULL OR NOT ? THEN NULL
					ELSE ? - started_at END,
				log_path = ?, log_bytes = ?, log_truncated = ?, error_tail = ?,
				next_attempt_at = ?
				WHERE run_id = ? AND name = ? AND state = ?`,
				string(state), nullIfEmpty(string(rowReason)), rowDetail,
				exitCode, signal,
				state != model.StepPending, finishedAt.UnixMilli(),
				state != model.StepPending,
				finishedAt.UnixMilli(),
				nullIfEmpty(out.LogMeta.RelPath), out.LogMeta.Bytes, truncated,
				nullIfEmpty(out.LogMeta.ErrorTail),
				nextAttempt,
				runID, name, string(cur))
			return err
		}, tx, RunEvent{
			RunID: runID, StepName: name, At: finishedAt, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			ReasonCode: string(rowReason),
			DetailJSON: rowDetail,
		}); err != nil {
			return err
		}
		// M4-03: a step that just failed with retry exhausted closes the
		// whole graph that depended on it. The skip is part of THIS
		// transaction, committed atomically with the failure, so no
		// observer ever sees the failed step with its dependants still
		// pending.
		if state == model.StepFailed {
			if err := propagateSkipTx(tx, runID, name, step.Attempt, finishedAt); err != nil {
				return fmt.Errorf("propagate the failure of %s of run %s: %w", name, runID, err)
			}
		}
		// Publication (#13): a succeeded step's references commit with
		// its verdict, in this same transaction. A failed, skipped or
		// cancelled verdict carries no rows, so no crash can strand an
		// artifact beside a step that never finished.
		if state == model.StepSucceeded && len(out.Artifacts) > 0 {
			if err := insertArtifactsTx(tx, runID, name, finishedAt, out.Artifacts); err != nil {
				return fmt.Errorf("publish the artifacts of %s in run %s: %w", name, runID, err)
			}
		}
		return nil
	})
}

// propagateSkipTx settles every pending step that transitively needs the
// failed step as skipped, inside the caller's transaction. It is what a failed
// step must never do: leave a dependant waiting forever, or let one run
// against output that never arrived.
//
// The closure is computed over the frozen step_deps edges with a recursive
// CTE. It uses UNION, not UNION ALL, so a diamond (a step reachable through
// two paths) appears exactly once and the recursion terminates. A diamond
// reached through two paths is written exactly once, never twice. A direct
// direct dependant of the failure reads STEP_SKIPPED_UPSTREAM_FAILED on its
// row and event; anything reached through another skip reads
// STEP_SKIPPED_UPSTREAM_SKIPPED, because the skip closed it, not the
// failure. Both carry the failed step in reason_data so explain can walk
// straight back to the root.
func propagateSkipTx(tx *sql.Tx, runID, failedStep string, attempt int, now time.Time) error {
	rows, err := tx.Query(`WITH RECURSIVE closure(step_name) AS (
			SELECT step_name FROM step_deps WHERE run_id = ? AND depends_on = ?
			UNION
			SELECT d.step_name FROM step_deps d
				JOIN closure c ON d.depends_on = c.step_name
				WHERE d.run_id = ?
		)
		SELECT s.name, s.idx,
			EXISTS (SELECT 1 FROM step_deps direct
				WHERE direct.run_id = s.run_id AND direct.step_name = s.name
					AND direct.depends_on = ?) AS is_direct
		FROM steps s
		WHERE s.run_id = ? AND s.name IN (SELECT step_name FROM closure) AND s.state = 'pending'
			ORDER BY s.idx`,
		runID, failedStep, runID, failedStep, runID)
	if err != nil {
		return fmt.Errorf("close the downstream of %s in run %s: %w", failedStep, runID, err)
	}
	var skipped []struct {
		name   string
		direct bool
	}
	for rows.Next() {
		var n string
		var idx, isDirect int
		if err := rows.Scan(&n, &idx, &isDirect); err != nil {
			_ = rows.Close()
			return fmt.Errorf("close the downstream of %s in run %s: %w", failedStep, runID, err)
		}
		skipped = append(skipped, struct {
			name   string
			direct bool
		}{n, isDirect != 0})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("close the downstream of %s in run %s: %w", failedStep, runID, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close the downstream of %s in run %s: %w", failedStep, runID, err)
	}

	detail := fmt.Sprintf(`{"upstream":"%s","attempt":%d}`, failedStep, attempt)
	for _, s := range skipped {
		code := reason.STEPSkippedUpstreamSkipped
		if s.direct {
			code = reason.STEPSkippedUpstreamFailed
		}
		to, effects, err := model.NextStepState(model.StepPending, model.EvUpstreamFailed,
			model.Guards{ReasonCode: string(code)})
		if err != nil {
			return fmt.Errorf("skip step %s in run %s: %w", s.name, runID, err)
		}
		if err := finishTransition(tx, "skip_propagation", func() error {
			// nolint:fencing: the caller's transaction already checked the
			// holder's token; steps carry no token of their own.
			_, err := tx.Exec(`UPDATE steps SET state = 'skipped', finished_at = ?,
				reason_code = ?, reason_data = ?
				WHERE run_id = ? AND name = ? AND state = 'pending'`,
				now.UnixMilli(), string(code), detail, runID, s.name)
			return err
		}, tx, RunEvent{
			RunID: runID, StepName: s.name, At: now, Kind: emitKind(effects),
			FromState: string(model.StepPending), ToState: string(to),
			ReasonCode: string(code),
			DetailJSON: detail,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileRunStates drives every run whose steps have all ended but whose own
// row still says non-terminal to the state those steps aggregate to. It is the
// crash backstop (M4-03): an executor that died between its final step verdict
// and the run verdict leaves a run stranded as running over terminal steps,
// and this closes it on restart.
//
// It is idempotent. A run that is already terminal, or whose aggregate is
// still running, is skipped; the second pass changes nothing. It also never
// reaches a run a live executor is still driving: a run whose lease has not
// expired still belongs to a process that may be about to finish it.
func (s *Store) ReconcileRunStates(ctx context.Context) error {
	now := s.clk.Now().UTC()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		type stranded struct {
			state string
			lease int64
			steps []string
		}
		byRun := map[string]*stranded{}
		rows, err := tx.Query(`SELECT r.id, r.state, COALESCE(r.lease_expires_at, 0),
			COALESCE(s.state, '')
		FROM runs r LEFT JOIN steps s ON s.run_id = r.id
		ORDER BY r.id, s.idx`)
		if err != nil {
			return fmt.Errorf("reconcile run states: %w", err)
		}
		for rows.Next() {
			var id, runState, stepState string
			var lease int64
			if err := rows.Scan(&id, &runState, &lease, &stepState); err != nil {
				_ = rows.Close()
				return fmt.Errorf("reconcile run states: %w", err)
			}
			p, ok := byRun[id]
			if !ok {
				p = &stranded{state: runState, lease: lease}
				byRun[id] = p
			}
			if stepState != "" {
				p.steps = append(p.steps, stepState)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("reconcile run states: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("reconcile run states: %w", err)
		}

		for id, p := range byRun {
			cur, err := model.ParseRunState(p.state)
			if err != nil {
				return fmt.Errorf("reconcile run %s: %w", id, err)
			}
			if cur.IsTerminal() {
				continue
			}
			// A live lease is a live decision; leave it to the executor.
			if p.lease > now.UnixMilli() {
				continue
			}
			var aggSteps []model.StepState
			for _, st := range p.steps {
				aggSteps = append(aggSteps, model.StepState(st))
			}
			agg := model.RunAggregate(aggSteps)
			if agg == model.RunRunning {
				// Still work open: nothing to converge.
				continue
			}
			if err := finalizeReconciledRunTx(tx, id, cur, agg, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// finalizeReconciledRunTx closes a stranded run to the aggregate its steps
// already point at. The reason matches the step facts: a failed run names the
// first failed step, a succeeded run says so, and a cancelled run says so. The
// run_events row notes the recovery so explain can tell a clean finish from a
// reconciliation.
func finalizeReconciledRunTx(tx *sql.Tx, runID string, cur model.RunState, agg model.RunState, now time.Time) error {
	var (
		code  reason.Code
		data  string
		event string
	)
	switch agg {
	case model.RunFailed:
		code = reason.RUNFailedStep
		data = `{"step":` + `"` + firstFailedStepName(tx, runID) + `"}`
		event = "run.failed"
	case model.RunCancelled:
		code = reason.RUNCancelledManual
		data = "{}"
		event = "run.cancelled"
	default:
		code = reason.RUNSucceeded
		data = "{}"
		event = "run.succeeded"
	}

	if err := finishTransition(tx, "reconcile_run", func() error {
		// nolint:fencing: reconcile owns the run wholesale; it only touches a
		// row whose lease is gone, and it takes the lease away as part of the
		// close, so there is no live holder whose token could be walked over.
		result, err := tx.Exec(`UPDATE runs SET state = ?, reason_code = ?, reason_data = ?,
			finished_at = ?, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE id = ? AND state NOT IN ('succeeded', 'failed', 'cancelled')`,
			string(agg), string(code), data, now.UnixMilli(), now.UnixMilli(), runID)
		if err != nil {
			return err
		}
		written, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if written == 0 {
			return fmt.Errorf("reconcile run %s: %w", runID, ErrLeaseLost)
		}
		return nil
	}, tx, RunEvent{
		RunID: runID, At: now, Kind: event,
		FromState: string(cur), ToState: string(agg),
		ReasonCode: string(code),
		DetailJSON: data,
	}); err != nil {
		return fmt.Errorf("reconcile run %s: %w", runID, err)
	}
	return nil
}

// firstFailedStepName names the first failed step of a run, in spec order.
func firstFailedStepName(tx *sql.Tx, runID string) string {
	var name string
	err := tx.QueryRow(`SELECT name FROM steps
		WHERE run_id = ? AND state = 'failed' ORDER BY idx LIMIT 1`, runID).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "unknown"
	}
	if err != nil {
		return "unknown"
	}
	return name
}

// FinishReason is how a run ends: the reason code the machine validated and
// the canonical detail object beside it, such as which step failed.
type FinishReason struct {
	Code reason.Code
	Data string
}

// FinishRun closes a run whose steps are all terminal. The aggregate comes
// from the machine's guards, which read the step rows inside the same
// transaction, so the verdict can never disagree with the steps it describes:
// any failed step fails the run, and a run with a step still open is refused
// outright. The lease is released here.
//
// The verdict is a CAS on the fence: the UPDATE carries owner and epoch, and
// zero rows affected means the lease was taken over while this writer worked.
// The refusal comes back as ErrLeaseLost and nothing at all is written: worst
// case is duplicate work, never duplicate state.
func (s *Store) FinishRun(ctx context.Context, runID string, ref LeaseRef, fr FinishReason) (string, error) {
	if !ref.held() {
		return "", fmt.Errorf("finish run %s: no holder was named", runID)
	}
	now := s.clk.Now().UTC()
	next := ""

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := checkLeaseTx(tx, runID, ref, now); err != nil {
			return fmt.Errorf("finish run %s: %w", runID, err)
		}
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}

		steps, err := readStepsTx(tx, runID)
		if err != nil {
			return err
		}
		allTerminal, anyFailed := true, false
		for _, step := range steps {
			st, err := model.ParseStepState(step.State)
			if err != nil {
				return err
			}
			if !st.IsTerminal() {
				allTerminal = false
			}
			if st == model.StepFailed || st == model.StepCancelled {
				anyFailed = true
			}
		}

		guards := model.Guards{
			LeaseValid: run.LeaseOwner == ref.Owner && run.LeaseEpoch == ref.Epoch &&
				(run.LeaseExpiresAt.IsZero() || run.LeaseExpiresAt.After(now)),
			AllStepsTerminal: allTerminal,
			AnyStepFailed:    anyFailed,
			ReasonCode:       string(fr.Code),
			Now:              now.UnixMilli(),
			AvailableAt:      run.AvailableAt.UnixMilli(),
		}
		state, effects, err := model.NextRunState(cur, model.EvAllStepsDone, guards)
		if err != nil {
			return fmt.Errorf("finish run %s: %w", runID, err)
		}

		data := fr.Data
		if data == "" {
			data = "{}"
		}
		if err := finishTransition(tx, "finish_run", func() error {
			result, err := tx.Exec(`UPDATE runs SET state = ?, reason_code = ?, reason_data = ?,
				finished_at = ?, duration_ms = CASE WHEN started_at IS NULL THEN NULL
					ELSE ? - started_at END,
				lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
				WHERE id = ? AND state = ? AND lease_owner = ? AND lease_epoch = ?`,
				string(state), string(fr.Code), data, now.UnixMilli(), now.UnixMilli(),
				now.UnixMilli(), runID, string(cur), ref.Owner, ref.Epoch)
			if err != nil {
				return err
			}
			written, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if written == 0 {
				return fmt.Errorf("finish run %s: %w", runID, ErrLeaseLost)
			}
			return nil
		}, tx, RunEvent{
			RunID: runID, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			ReasonCode: string(fr.Code),
			DetailJSON: mergeEpochDetail(data, ref.Epoch),
		}); err != nil {
			return err
		}
		next = string(state)
		return nil
	})
	if err != nil {
		return "", err
	}
	return next, nil
}

// ObserveRunCancel effectuates a cancellation somebody requested earlier: the
// caller has already killed the process group outside any transaction (the
// machine lists that effect first, and the engine performs it before calling
// here), the steps that were running are cancelled by their own events, and
// this closes whatever pending steps remain and then the run itself.
//
// The caller must still hold the lease at its token; the write is a CAS like
// every other result write, so a holder that lost the run while killing the
// process group cannot cancel over whatever the new owner is doing.
//
// It refuses when nobody asked: a run nobody asked to cancel is never
// cancelled, whatever the caller claims. The event names the person who made
// the request, not the executor that observed it.
func (s *Store) ObserveRunCancel(ctx context.Context, runID string, ref LeaseRef, actor string, code reason.Code) error {
	if !ref.held() {
		return fmt.Errorf("observe the cancellation of run %s: no holder was named", runID)
	}
	now := s.clk.Now().UTC()

	return s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		if run.CancelRequestedAt.IsZero() {
			return fmt.Errorf("observe the cancellation of run %s: nobody requested one", runID)
		}
		if err := checkLeaseTx(tx, runID, ref, now); err != nil {
			return fmt.Errorf("observe the cancellation of run %s: %w", runID, err)
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}

		guards := model.Guards{
			LeaseValid: true,
			ReasonCode: string(code),
		}
		state, effects, err := model.NextRunState(cur, model.EvCancelObserved, guards)
		if err != nil {
			return fmt.Errorf("observe the cancellation of run %s: %w", runID, err)
		}

		if err := skipPendingStepsTx(tx, runID, now, string(reason.STEPCancelled)); err != nil {
			return err
		}
		return finishTransition(tx, "observe_cancel", func() error {
			result, err := tx.Exec(`UPDATE runs SET state = 'cancelled',
				finished_at = ?, reason_code = ?, reason_data = ?,
				lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE id = ? AND state = ? AND lease_owner = ? AND lease_epoch = ?`,
				now.UnixMilli(), string(code), "{}", now.UnixMilli(),
				runID, string(cur), ref.Owner, ref.Epoch)
			if err != nil {
				return err
			}
			written, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if written == 0 {
				return fmt.Errorf("observe the cancellation of run %s: %w", runID, ErrLeaseLost)
			}
			return nil
		}, tx, RunEvent{
			RunID: runID, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			ReasonCode: string(code),
			Actor:      actor,
			DetailJSON: epochDetail(ref.Epoch),
		})
	})
}

// nextRunnableStepSQL is the claim predicate: the lowest index that is
// pending, past its retry gate, and whose every frozen upstream has
// succeeded. It is a constant so the query plan test asserts the plan of the
// exact statement that runs, not of a copy of it.
const nextRunnableStepSQL = `SELECT s.name FROM steps s
WHERE s.run_id = ? AND s.state = 'pending'
	AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= ?)
	AND NOT EXISTS (
		SELECT 1 FROM step_deps d JOIN steps up
			ON up.run_id = d.run_id AND up.name = d.depends_on
		WHERE d.run_id = s.run_id AND d.step_name = s.name AND up.state <> 'succeeded')
ORDER BY s.idx LIMIT 1`

// NextRunnableStep names the next step the engine may start: the lowest index
// that is pending, past its retry gate, and whose every frozen upstream has
// succeeded. A step waiting on upstream that has not succeeded is skipped
// over, which is the degenerated claim predicate M4-02 replaces with the
// whole graph.
func (s *Store) NextRunnableStep(ctx context.Context, runID string) (string, bool, error) {
	var name string
	err := s.r.QueryRowContext(ctx, nextRunnableStepSQL, runID,
		s.clk.Now().UTC().UnixMilli()).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("find the next runnable step of run %s: %w", runID, err)
	}
	return name, true, nil
}

// nextRetryWaitSQL names the soonest parked retry of a run: the earliest
// next_attempt_at among its pending steps. It is a constant like its claim
// gate sibling above, so a query plan test can pin the statement that runs.
const nextRetryWaitSQL = `SELECT MIN(next_attempt_at) FROM steps
WHERE run_id = ? AND state = 'pending' AND next_attempt_at IS NOT NULL`

// NextRetryWait reports how long the executor must wait before some pending
// step of the run passes its retry gate: the gap to the soonest
// next_attempt_at, clamped to zero once it is due. Waiting is false when no
// pending step carries a time at all, which is the executor's signal that
// nothing here will ever become runnable again. This read is what keeps
// retry free of state of its own: the engine sleeps on the answer instead of
// owning a scheduler.
func (s *Store) NextRetryWait(ctx context.Context, runID string) (time.Duration, bool, error) {
	var due sql.NullInt64
	err := s.r.QueryRowContext(ctx, nextRetryWaitSQL, runID).Scan(&due)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, false, fmt.Errorf("read the next retry wait of run %s: %w", runID, err)
	}
	if !due.Valid {
		return 0, false, nil
	}
	wait := time.Duration(due.Int64-s.clk.Now().UTC().UnixMilli()) * time.Millisecond
	if wait < 0 {
		wait = 0
	}
	return wait, true, nil
}

// PendingSteps lists a run's steps that have not started, in index order.
func (s *Store) PendingSteps(ctx context.Context, runID string) ([]Step, error) {
	var out []Step
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT name, idx, state, attempt, max_attempts, exit_code,
signal, started_at, finished_at, duration_ms, reason_code, reason_text, reason_data, error,
log_path, log_bytes, log_truncated, error_tail, next_attempt_at
FROM steps WHERE run_id = ? AND state = 'pending' ORDER BY idx`, runID)
		if err != nil {
			return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
		}
		out, err = scanSteps(rows)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// skipPendingStepsTx moves every pending step of a run to skipped, each
// through the machine with its own event. It is what keeps a terminal run
// from sitting over open steps: a run ends, or its steps all do.
func skipPendingStepsTx(tx *sql.Tx, runID string, now time.Time, code string) error {
	rows, err := tx.Query(`SELECT name FROM steps WHERE run_id = ? AND state = 'pending' ORDER BY idx`, runID)
	if err != nil {
		return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("read the pending steps of run %s: %w", runID, err)
	}

	for _, name := range names {
		state, effects, err := model.NextStepState(model.StepPending, model.EvUpstreamFailed,
			model.Guards{ReasonCode: code})
		if err != nil {
			return fmt.Errorf("skip step %s of run %s: %w", name, runID, err)
		}
		if err := finishTransition(tx, "skip_pending", func() error {
			// nolint:fencing: the caller's transaction already checked the
			// holder's token, or the run is queued and has no lease to check;
			// steps carry no token of their own.
			_, err := tx.Exec(`UPDATE steps SET state = 'skipped', finished_at = ?,
				reason_code = ?, reason_data = '{}'
			WHERE run_id = ? AND name = ? AND state = 'pending'`,
				now.UnixMilli(), code, runID, name)
			return err
		}, tx, RunEvent{
			RunID: runID, StepName: name, At: now, Kind: emitKind(effects),
			FromState: string(model.StepPending), ToState: string(state),
			ReasonCode: code,
		}); err != nil {
			return err
		}
	}
	return nil
}

// finishTransition pairs the state write with the event write and refuses to
// commit one without the other. fn performs the UPDATE; if it touched no row,
// the transition never happened and the event is not written either.
//
// where names the transition for the crash harness (#75). The fault point
// between the two writes is the exact window G10 closes: a process killed
// there loses both writes, because neither was committed. In a build without
// the pulseq_faults tag the call folds away to nothing.
func finishTransition(tx *sql.Tx, where string, fn func() error, txForEvent *sql.Tx, e RunEvent) error {
	if err := fn(); err != nil {
		return fmt.Errorf("record %s on run %s: %w", e.Kind, e.RunID, err)
	}
	faults.Point("M1:transition:after_update:" + where)
	return appendRunEvent(txForEvent, e)
}

// emitKind reads the event name the machine put in its emit effect. A
// transition without one is a machine bug, not a caller's.
func emitKind(effects []model.Effect) string {
	for _, fx := range effects {
		if fx.Kind == model.EffectEmit {
			return fx.Arg
		}
	}
	return ""
}

// readRunTx reads one run by its whole id inside a transaction. Prefixes are
// a read-side convenience; a writer names the row it changes in full.
func readRunTx(tx *sql.Tx, runID string) (Run, error) {
	var run Run
	var (
		trigger, runKey, concurrency, params sql.NullString
		deferReason, reasonCode, reasonText  sql.NullString
		reasonData, failure                  sql.NullString
		leaseOwner, cancelBy, cancelWhy      sql.NullString
		scheduledFor, startedAt, finishedAt  sql.NullInt64
		leaseExpiresAt, cancelRequestedAt    sql.NullInt64
		heartbeatAt                          sql.NullInt64
		availableAt, createdAt, updatedAt    int64
		leaseEpoch, crashCount               int64
		replayOf                             sql.NullString
	)
	err := tx.QueryRow(`SELECT `+runColumns+` FROM runs WHERE id = ?`, runID).Scan(
		&run.ID, &run.JobName, &run.JobVersionID, &trigger, &run.Origin,
		&runKey, &run.State, &concurrency, &availableAt, &deferReason, &scheduledFor, &params,
		&run.Attempt, &run.MaxAttempts, &leaseOwner, &leaseEpoch, &leaseExpiresAt,
		&heartbeatAt, &cancelRequestedAt, &cancelBy, &cancelWhy, &reasonCode, &reasonText,
		&reasonData, &failure, &crashCount, &createdAt, &startedAt, &finishedAt, &updatedAt,
		&replayOf)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("look up run %q: %w", runID, ErrRunNotFound)
	}
	if err != nil {
		return Run{}, fmt.Errorf("look up run %q: %w", runID, err)
	}
	run.TriggerID = trigger.String
	run.RunKey = runKey.String
	run.ConcurrencyKey = concurrency.String
	run.DeferReason = deferReason.String
	run.ParamsJSON = params.String
	run.ReasonCode = reasonCode.String
	run.ReasonText = reasonText.String
	run.ReasonData = reasonData.String
	run.Error = failure.String
	run.ReplayOf = replayOf.String
	run.LeaseOwner = leaseOwner.String
	run.LeaseEpoch = leaseEpoch
	run.LeaseExpiresAt = timeOrZero(leaseExpiresAt)
	run.HeartbeatAt = timeOrZero(heartbeatAt)
	run.CrashCount = int(crashCount)
	if cancelRequestedAt.Valid {
		run.CancelRequestedAt = time.UnixMilli(cancelRequestedAt.Int64).UTC()
	}
	run.CancelRequestedBy = cancelBy.String
	run.CancelReason = cancelWhy.String
	run.AvailableAt = time.UnixMilli(availableAt).UTC()
	run.ScheduledFor = timeOrZero(scheduledFor)
	run.CreatedAt = time.UnixMilli(createdAt).UTC()
	run.StartedAt = timeOrZero(startedAt)
	run.FinishedAt = timeOrZero(finishedAt)
	run.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	return run, nil
}

// stepRow is the columns readStepTx needs, in the order the steps projection
// names them.
func readStepTx(tx *sql.Tx, runID, name string) (Step, error) {
	var step Step
	var (
		exitCode      sql.NullInt64
		signal        sql.NullString
		reasonCode    sql.NullString
		reasonText    sql.NullString
		reasonData    sql.NullString
		failure       sql.NullString
		logs          sql.NullString
		errorTail     sql.NullString
		startedAt     sql.NullInt64
		finishedAt    sql.NullInt64
		durationMS    sql.NullInt64
		nextAttemptAt sql.NullInt64
	)
	err := tx.QueryRow(`SELECT name, idx, state, attempt, max_attempts, exit_code, signal,
started_at, finished_at, duration_ms, reason_code, reason_text, reason_data, error,
log_path, log_bytes, log_truncated, error_tail, next_attempt_at
FROM steps WHERE run_id = ? AND name = ?`, runID, name).Scan(
		&step.Name, &step.Index, &step.State, &step.Attempt, &step.MaxAttempts,
		&exitCode, &signal, &startedAt, &finishedAt, &durationMS, &reasonCode, &reasonText,
		&reasonData, &failure, &logs, &step.LogBytes, &step.LogTruncated, &errorTail,
		&nextAttemptAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Step{}, fmt.Errorf("look up step %s of run %s: %w", name, runID, ErrRunNotFound)
	}
	if err != nil {
		return Step{}, fmt.Errorf("look up step %s of run %s: %w", name, runID, err)
	}
	step.ExitCode = int(exitCode.Int64)
	step.HasExitCode = exitCode.Valid
	step.Signal = signal.String
	step.StartedAt = timeOrZero(startedAt)
	step.FinishedAt = timeOrZero(finishedAt)
	step.DurationMS = durationMS.Int64
	step.ReasonCode = reasonCode.String
	step.ReasonText = reasonText.String
	step.ReasonData = reasonData.String
	step.Error = failure.String
	step.LogPath = logs.String
	step.ErrorTail = errorTail.String
	step.NextAttemptAt = timeOrZero(nextAttemptAt)
	return step, nil
}

// readStepsTx is readSteps for inside a transaction, so FinishRun judges the
// steps from the same snapshot it writes the verdict into.
func readStepsTx(tx *sql.Tx, runID string) ([]Step, error) {
	rows, err := tx.Query(`SELECT name, idx, state, attempt, max_attempts, exit_code, signal,
started_at, finished_at, duration_ms, reason_code, reason_text, reason_data, error,
log_path, log_bytes, log_truncated, error_tail, next_attempt_at
FROM steps WHERE run_id = ? ORDER BY idx`, runID)
	if err != nil {
		return nil, fmt.Errorf("read the steps of run %s: %w", runID, err)
	}
	return scanSteps(rows)
}

// RequeueCrashedRun puts a run whose executor died back in the queue. It is
// the store half of the restart story the crash harness (#75) proves: a run
// left running by a SIGKILL is requeued through the machine's own
// lease_expired transition, never reset behind its back.
//
// The refusal is the safety catch. A lease that has not expired yet belongs
// to a process that may still be alive, and requeuing under it would let two
// executors drive one run. Only an expired lease counts as evidence that the
// previous owner is gone; the state directory lock adds its own guarantee on
// this platform, but the machine's guard does not rely on it.
//
// The transition writes what the machine demands: the epoch goes up so any
// writer from the dead attempt is fenced out, crash_count counts the loss of
// the executor against the run, and defer_reason records why the requeued run
// sits waiting (I14). One event row tells the story, in the same transaction.
func (s *Store) RequeueCrashedRun(ctx context.Context, runID string) error {
	now := s.clk.Now().UTC()

	return s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}

		expired := !run.LeaseExpiresAt.IsZero() && !run.LeaseExpiresAt.After(now)
		if cur != model.RunRunning || !expired {
			return fmt.Errorf("requeue run %s: the lease is not expired"+
				" (state %s, expires at %s)", runID, run.State, run.LeaseExpiresAt)
		}

		state, effects, err := model.NextRunState(cur, model.EvLeaseExpired, model.Guards{
			LeaseValid:      false,
			CrashBudgetLeft: true,
		})
		if err != nil {
			return fmt.Errorf("requeue run %s: %w", runID, err)
		}
		if state != model.RunQueued {
			// M1 carries no poison budget here; startup recovery requeues
			// whatever it converges on, and the running reaper owns the
			// quarantine decision. A refusal to believe that is a bug,
			// not a state.
			return fmt.Errorf("requeue run %s: the machine sent a running run to %s", runID, state)
		}

		return finishTransition(tx, "requeue_crashed", func() error {
			result, err := tx.Exec(`UPDATE runs SET state = 'queued',
				lease_owner = NULL, lease_expires_at = NULL,
				lease_epoch = lease_epoch + 1, crash_count = crash_count + 1,
				defer_reason = ?, available_at = ?, updated_at = ?
				WHERE id = ? AND state = 'running' AND lease_epoch = ?`,
				model.DeferReasonAfterCrash, now.UnixMilli(), now.UnixMilli(),
				runID, run.LeaseEpoch)
			if err != nil {
				return err
			}
			written, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if written == 0 {
				return fmt.Errorf("requeue run %s: %w", runID, ErrLeaseLost)
			}
			return nil
		}, tx, RunEvent{
			RunID: runID, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			DetailJSON: mergeEpochDetail(`{"defer_reason":"`+model.DeferReasonAfterCrash+`"}`, run.LeaseEpoch+1),
		})
	})
}

// DrainRun hands a claimed run back to the queue at a clean stop. It is the
// whole handback in one transaction: the running steps go back to pending
// with their attempts restored (05 section 3.2, point 4), because the attempt
// was cut short by the daemon's own stop and produced no verdict, and then
// the run itself is requeued without counting a crash, because the executor
// left on purpose.
//
// The epoch still rises, so any writer from the drained attempt stays fenced
// out, and available_at moves to now so the next executor can claim at once.
// The CAS decides whether anything is owed: a caller whose lease has moved on
// gets handed=false and writes nothing.
func (s *Store) DrainRun(ctx context.Context, runID string, ref LeaseRef, code reason.Code) (bool, error) {
	if !ref.held() {
		return false, fmt.Errorf("drain run %s: no holder was named", runID)
	}
	now := s.clk.Now().UTC()

	handed := false
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		// Nothing of ours is out there any more: another holder claimed, or
		// the reaper took the run. Quietly reporting nothing owed is correct,
		// not a failure; the row belongs to someone else now.
		if run.State != string(model.RunRunning) ||
			run.LeaseOwner != ref.Owner || run.LeaseEpoch != ref.Epoch {
			return nil
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}

		steps, err := readStepsTx(tx, runID)
		if err != nil {
			return err
		}
		for _, step := range steps {
			if model.StepState(step.State) != model.StepRunning {
				continue
			}
			curStep, err := model.ParseStepState(step.State)
			if err != nil {
				return err
			}
			state, effects, err := model.NextStepState(curStep,
				model.EvShutdownDrain, model.Guards{ReasonCode: string(code)})
			if err != nil {
				return fmt.Errorf("interrupt step %s of run %s: %w", step.Name, runID, err)
			}
			if err := finishTransition(tx, "interrupt_step", func() error {
				// nolint:fencing: this transaction read the row and matched the
				// caller's owner and token before writing; the run update below
				// carries the same CAS, so a stale handback writes nothing.
				_, err := tx.Exec(`UPDATE steps SET state = 'pending',
					attempt = attempt - 1,
					reason_code = ?, next_attempt_at = ?, finished_at = NULL,
					duration_ms = NULL
					WHERE run_id = ? AND name = ? AND state = 'running'`,
					string(code), now.UnixMilli(), runID, step.Name)
				return err
			}, tx, RunEvent{
				RunID: runID, StepName: step.Name, At: now, Kind: emitKind(effects),
				FromState: string(curStep), ToState: string(state),
				ReasonCode: string(code),
				DetailJSON: `{"why":"daemon_stop"}`,
			}); err != nil {
				return err
			}
		}

		held := run.LeaseExpiresAt.IsZero() || run.LeaseExpiresAt.After(now)
		state, effects, err := model.NextRunState(cur, model.EvShutdownDrain, model.Guards{
			LeaseValid: held,
		})
		if err != nil {
			return fmt.Errorf("drain run %s: %w", runID, err)
		}
		if state != model.RunQueued {
			return fmt.Errorf("drain run %s: the machine sent a running run to %s", runID, state)
		}

		if err := finishTransition(tx, "requeue_drained", func() error {
			result, err := tx.Exec(`UPDATE runs SET state = 'queued',
				lease_owner = NULL, lease_expires_at = NULL,
				lease_epoch = lease_epoch + 1,
				defer_reason = ?, available_at = ?, updated_at = ?
				WHERE id = ? AND state = 'running' AND lease_owner = ? AND lease_epoch = ?`,
				model.DeferReasonAfterShutdown, now.UnixMilli(), now.UnixMilli(),
				runID, ref.Owner, ref.Epoch)
			if err != nil {
				return err
			}
			written, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if written == 0 {
				return fmt.Errorf("drain run %s: %w", runID, ErrLeaseLost)
			}
			return nil
		}, tx, RunEvent{
			RunID: runID, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			DetailJSON: mergeEpochDetail(`{"defer_reason":"`+model.DeferReasonAfterShutdown+`"}`, run.LeaseEpoch+1),
		}); err != nil {
			return err
		}
		handed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return handed, nil
}

// The fault injection writes. Fsck's negative proofs (#75) plant broken rows
// behind the machines' backs, and these five statements are the only doors
// for that: each is named for the exact corruption it writes, and nothing in
// the engine or the CLI may call them. They sit here, beside the real state
// writes, so the architecture's one rule stays true: a run or step row only
// ever changes in this file, even when the change is a deliberate lie.

// plantStepPendingTx flips one step of a terminal run back to pending: the
// row I2 exists for.
//
// nolint:fencing: the fault injector's whole job is a deliberate unfenced lie.
func plantStepPendingTx(tx *sql.Tx, runID, step string) error {
	_, err := tx.Exec(`UPDATE steps SET state = 'pending', reason_code = NULL
WHERE run_id = ? AND name = ?`, runID, step)
	return err
}

// plantFirstSucceededStepFailedTx marks a run's first succeeded step failed
// while the run still says succeeded: the disagreement I10 exists for.
//
// nolint:fencing: the fault injector's whole job is a deliberate unfenced lie.
func plantFirstSucceededStepFailedTx(tx *sql.Tx, runID string) error {
	_, err := tx.Exec(`UPDATE steps SET state = 'failed'
WHERE run_id = ? AND state = 'succeeded'`, runID)
	return err
}

// plantStepFinishedBeforeStartedTx moves every finished step's end before
// its beginning: the shape I13 refuses.
//
// nolint:fencing: the fault injector's whole job is a deliberate unfenced lie.
func plantStepFinishedBeforeStartedTx(tx *sql.Tx) error {
	_, err := tx.Exec(`UPDATE steps SET finished_at = started_at - 1
WHERE started_at IS NOT NULL AND finished_at IS NOT NULL`)
	return err
}

// plantUnexplainedDeferralTx pushes a queued run's availability into the
// future and clears its defer_reason: a run held back that says no more why.
// The CHECK constraint refuses this shape, so the caller lifts the checks
// around the call.
//
// nolint:fencing: the fault injector's whole job is a deliberate unfenced lie.
func plantUnexplainedDeferralTx(tx *sql.Tx, runID string) error {
	_, err := tx.Exec(`UPDATE runs SET available_at = created_at + 3600000,
		defer_reason = NULL WHERE id = ?`, runID)
	return err
}

// plantUnexplainedTerminalRunTx clears a terminal run's reason code: the
// catalogue rule swept along with the invariants.
//
// nolint:fencing: the fault injector's whole job is a deliberate unfenced lie.
func plantUnexplainedTerminalRunTx(tx *sql.Tx, runID string) error {
	_, err := tx.Exec(`UPDATE runs SET reason_code = '' WHERE id = ?`, runID)
	return err
}
