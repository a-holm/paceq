package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// Run level retry, M4-04. One command reopens one terminal run: the run row
// goes back to queued under a raised fencing token (T14, the only way out of
// a terminal state), and its failed and skipped steps go back to pending so
// the claim predicate offers them again.
//
// The whole reopen is ONE transaction. A crash between "run is queued" and
// "steps are pending" would strand a queued run whose steps are all terminal:
// the next claim would aggregate it straight back to failed and the retry
// would be a silent no-op. The run flip and the step resets commit together
// or not at all, which is why this file has one exported entry point instead
// of two methods an ordinary caller could sequence.

// ErrRunNotRetryable is returned when the run has nothing a retry could redo.
// A succeeded run is redone by replay, which makes a new row and leaves this
// one as history.
var (
	ErrRunNotRetryable  = errors.New("this run has nothing to retry")
	ErrNothingToReopen  = errors.New("no step of this run is failed or skipped")
	ErrRunNotTerminal   = errors.New("the run has not finished")
	ErrStepNotInThisRun = errors.New("no such step in this run")
)

// ReopenOpts says how much of the run an operator reopen reopens.
type ReopenOpts struct {
	// OnlyStep restricts the reopen to this step plus its transitive
	// downstream closure in the frozen graph of THIS run. Nil reopens every
	// failed and skipped step. A succeeded step named here is refused: its
	// work is what the retry builds on.
	OnlyStep *string

	// RetryBudget is how many attempts every reopened step gains. Zero or
	// less means one. The attempt counter itself is never touched: history
	// stays tight from 1 across generations (02 section 3.3).
	RetryBudget int

	// Forced records that the caller passed --force past the unknown
	// outcome warning. It changes nothing here; it rides on the event so
	// the history shows the reopen was a deliberate double effect risk.
	Forced bool
}

// ReopenResult says what one reopen did: the fencing token the run now sits
// behind, and which steps are pending again, in spec order.
type ReopenResult struct {
	NewEpoch int64
	Reopened []string
}

// ReopenTerminalRunByOperator reopens a terminal run on an operator's behalf.
// It is deliberately named for who may call it: no scheduler, reaper or other
// automation reaches T14, because nothing automatic can spell this method
// (enforced by the architecture test that walks every caller).
//
// The machine decides the move; this file performs what it demands. The event
// lands as kind='operator_reopen' (02 section 3.1), carrying the actor, the
// reason code and the new token, because the fencing history (I11) has to end
// exactly where the row does.
func (s *Store) ReopenTerminalRunByOperator(ctx context.Context, runID string, actor string, opt ReopenOpts) (ReopenResult, error) {
	if actor == "" {
		actor = "system"
	}
	budget := opt.RetryBudget
	if budget <= 0 {
		budget = 1
	}

	now := s.clk.Now().UTC()
	var out ReopenResult

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, runID)
		if err != nil {
			return err
		}
		cur, err := model.ParseRunState(run.State)
		if err != nil {
			return err
		}
		switch {
		case cur == model.RunSucceeded:
			return fmt.Errorf("reopen run %s: %w (it succeeded; replay makes a new run instead)",
				runID, ErrRunNotRetryable)
		case !cur.IsTerminal():
			return fmt.Errorf("reopen run %s: %w (it is %s)", runID, ErrRunNotTerminal, run.State)
		}

		if opt.OnlyStep != nil {
			if _, err := readStepTx(tx, runID, *opt.OnlyStep); err != nil {
				return fmt.Errorf("reopen run %s --step %s: %w (%s)",
					runID, *opt.OnlyStep, ErrStepNotInThisRun, *opt.OnlyStep)
			}
		}

		reopened, err := reopenTargetsTx(tx, runID, opt.OnlyStep)
		if err != nil {
			return err
		}
		if len(reopened) == 0 && opt.OnlyStep != nil {
			return fmt.Errorf("reopen run %s --step %s: %w", runID, *opt.OnlyStep,
				ErrNothingToReopen)
		}
		if len(reopened) == 0 {
			return fmt.Errorf("reopen run %s: %w", runID, ErrNothingToReopen)
		}

		state, _, err := model.NextRunState(cur, model.EvOperatorRetry, model.Guards{})
		if err != nil {
			return fmt.Errorf("reopen run %s: %w", runID, err)
		}

		// T14. The UPDATE carries lease_epoch = lease_epoch + 1, which is
		// both the fence and its own proof: a writer holding last
		// generation's token fails the CAS below and writes nothing.
		// available_at goes back to the row's own creation stamp, not to
		// now: the schema holds every queued run without a defer reason to
		// have been visible since it was created, and a reopened run is
		// visible immediately either way, because its creation is in the
		// past by definition. The moment of the reopen lives on the
		// operator_reopen event, where history reads it.
		if err := finishTransition(tx, "operator_reopen", func() error {
			result, err := tx.Exec(`UPDATE runs SET
				state = 'queued',
				lease_epoch = lease_epoch + 1,
				lease_owner = NULL,
				lease_expires_at = NULL,
				finished_at = NULL,
				duration_ms = NULL,
				error = NULL,
				reason_code = ?,
				reason_data = '{}',
				defer_reason = NULL,
				cancel_requested_at = NULL,
				cancel_requested_by = NULL,
				cancel_reason = NULL,
				available_at = ?,
				updated_at = ?
				WHERE id = ? AND state = ? AND lease_epoch = ?`,
				string(reason.RUNReopenedOperator), run.CreatedAt.UnixMilli(), now.UnixMilli(),
				runID, run.State, run.LeaseEpoch)
			if err != nil {
				return err
			}
			written, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if written == 0 {
				return fmt.Errorf("reopen run %s: %w", runID, ErrLeaseLost)
			}
			return nil
		}, tx, RunEvent{
			RunID: runID, At: now,
			// The catalogue name for T14 is operator_reopen, not the
			// machine's emit label: this kind is the public event the
			// issue spells out, and explain reads it back verbatim.
			Kind:       "operator_reopen",
			FromState:  string(cur),
			ToState:    string(state),
			ReasonCode: string(reason.RUNReopenedOperator),
			Actor:      actor,
			DetailJSON: mergeEpochDetail(reopenDetail(opt), run.LeaseEpoch+1),
		}); err != nil {
			return err
		}
		out.NewEpoch = run.LeaseEpoch + 1
		faults.Point("M4:reopen:after_run_update")

		for _, target := range reopened {
			stepCur, err := model.ParseStepState(target.state)
			if err != nil {
				return err
			}
			stepNext, fx, err := model.NextStepState(stepCur, model.EvOperatorRetry, model.Guards{})
			if err != nil {
				return fmt.Errorf("reopen step %s of run %s: %w", target.name, runID, err)
			}
			if err := finishTransition(tx, "step_reopen", func() error {
				// nolint:fencing: the run just went back to queued under a
				// new epoch and carries no live lease; these rows belong to
				// nobody until the next claim, which is the write this
				// transaction exists to schedule.
				_, err := tx.Exec(`UPDATE steps SET
					state = 'pending',
					max_attempts = max_attempts + ?,
					next_attempt_at = NULL,
					started_at = NULL,
					finished_at = NULL,
					duration_ms = NULL,
					exit_code = NULL,
					signal = NULL,
					error = NULL,
					reason_code = NULL,
					reason_data = NULL,
					pid = NULL,
					pid_start_ticks = NULL
					WHERE run_id = ? AND name = ? AND state = ?`,
					budget, runID, target.name, target.state)
				return err
			}, tx, RunEvent{
				RunID: runID, StepName: target.name, At: now,
				Kind:      emitKind(fx),
				FromState: target.state,
				ToState:   string(stepNext),
				Actor:     actor,
			}); err != nil {
				return err
			}
			out.Reopened = append(out.Reopened, target.name)
		}
		faults.Point("M4:reopen:after_steps")
		return nil
	})
	if err != nil {
		return ReopenResult{}, err
	}
	return out, nil
}

// reopenTarget is one step waiting to be reopened: its name and the state it
// sits in right now, so every later write CASes against what it read.
type reopenTarget struct {
	name  string
	state string
	idx   int
}

// reopenTargetsTx names the steps a reopen puts back to pending, in spec
// order: every failed or skipped step of the run, or the ones behind OnlyStep.
//
// The closure walks the FROZEN step_deps of this run with a recursive CTE in
// the downstream direction, UNION so a diamond appears once. It is M4-03's
// skip propagation mirrored: skips flowed down these edges when the failure
// happened, and the undo flows down the same edges now. Never the current
// spec: the edges frozen at materialisation are the graph this run means.
func reopenTargetsTx(tx *sql.Tx, runID string, onlyStep *string) ([]reopenTarget, error) {
	const everything = `SELECT name, idx, state FROM steps
		WHERE run_id = ? AND state IN ('failed', 'skipped')
		ORDER BY idx`

	const belowOne = `WITH RECURSIVE downstream(step_name) AS (
			SELECT ?
			UNION
			SELECT d.step_name FROM step_deps d
				JOIN downstream c ON d.depends_on = c.step_name
			WHERE d.run_id = ?
		)
		SELECT s.name, s.idx, s.state FROM steps s
		JOIN downstream t ON t.step_name = s.name
		WHERE s.run_id = ? AND s.state IN ('failed', 'skipped')
		ORDER BY s.idx`

	var query string
	var args []any
	if onlyStep != nil {
		query = belowOne
		args = []any{*onlyStep, runID, runID}
	} else {
		query = everything
		args = []any{runID}
	}

	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("read the reopen targets of run %s: %w", runID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []reopenTarget
	for rows.Next() {
		var t reopenTarget
		if err := rows.Scan(&t.name, &t.idx, &t.state); err != nil {
			return nil, fmt.Errorf("scan a reopen target of run %s: %w", runID, err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan a reopen target of run %s: %w", runID, err)
	}
	return out, rows.Close()
}

// reopenDetail renders the facts the operator's decision rode on, without the
// token; mergeEpochDetail folds that in.
func reopenDetail(opt ReopenOpts) string {
	detail := map[string]any{}
	if opt.OnlyStep != nil {
		detail["only_step"] = *opt.OnlyStep
	}
	if opt.Forced {
		detail["forced"] = true
	}
	if len(detail) == 0 {
		return "{}"
	}
	b, err := json.Marshal(detail)
	if err != nil {
		return "{}"
	}
	return string(b)
}
