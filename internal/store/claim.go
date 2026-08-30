package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/a-holm/paceq/internal/faults"
)

// claim.go is the M4-02 claim predicate. A step of a run is claimable if and
// only if no frozen step_deps edge runs from it to an upstream step that is
// not succeeded. The gate lives here, in one atomic UPDATE on the single
// writer connection, and nowhere else: the engine holds no graph in memory,
// and the order of a DAG is whatever the predicate admits next.
//
// ClaimNextStep is the whole decision and the whole reservation in one
// statement. It selects the candidate, applies the predicate, respects the
// run's own gates (running, not asked to stop, lease still this writer's at
// the fencing epoch) and its parallel budget, and flips the one row to
// running in the same transaction that writes the step.started event. A
// caller that sees a step claimed knows no one else got there first: the
// single writer keeps the decision and the flip in one write lock, so there
// is no read-then-write window at all (05 section 6.4, 07 section 4.1).

// ErrRunNotHeld is returned when a write named a run that is not held at the
// caller's fencing epoch.
var ErrRunNotHeld = errors.New("the run lease is not held at the fencing epoch")

// ClaimedStep is one step this process just took for its run: everything the
// executor needs to run and then record it, plus the run's parallel cap read
// off the same row for sizing a local pool.
type ClaimedStep struct {
	RunID       string
	Name        string
	Attempt     int
	MaxParallel int
}

// claimStepSQL is the admission gate. The OUTER statement flips exactly the
// chosen row to running inside the one BEGIN IMMEDIATE transaction; there is
// no read-then-write, so there is no window for the DAG state to move between
// the decision and the reservation (no TOCTOU on these rows).
//
// The predicate admits a pending step when every one of these holds:
//   - the run that owns it is running, was not asked to stop, and is still
//     held by the caller at the fencing epoch (the M2-06 run lease),
//   - the step is past its retry gate,
//   - every frozen upstream edge has succeeded (the DAG claim),
//   - the run is under its max_parallel, so admitting this step cannot push
//     it over the cap.
//
// The ORDER BY is the deterministic tie break every golden and crash test
// relies on: the spec position, then the name. LIMIT 1 keeps the statement
// single row and therefore cheap for the query plan.
const claimStepSQL = `
UPDATE steps
   SET state      = 'running',
       attempt    = attempt + 1,
       started_at = COALESCE(started_at, ?1),
       reason_code = NULL
 WHERE (run_id, name) = (
   SELECT s.run_id, s.name
     FROM steps s
     JOIN runs r ON r.id = s.run_id
    WHERE s.run_id = ?2
      AND s.state = 'pending'
      AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= ?1)
      AND r.state = 'running'
      AND r.cancel_requested_at IS NULL
      AND r.lease_owner = ?3
      AND r.lease_epoch = ?4
      AND (SELECT COUNT(*) FROM steps x
            WHERE x.run_id = s.run_id AND x.state = 'running') < r.max_parallel
      AND NOT EXISTS (
        SELECT 1
          FROM step_deps d
          JOIN steps u ON u.run_id = d.run_id AND u.name = d.depends_on
         WHERE d.run_id    = s.run_id
           AND d.step_name = s.name
           AND u.state <> 'succeeded')
   ORDER BY s.idx, s.name
   LIMIT 1)
RETURNING run_id, name, attempt`

// ClaimNextStep claims, if the predicate allows, the next step of one run:
// the pending step with every frozen upstream succeeded, past its retry gate,
// inside the run's parallel cap, and still owned by the caller's fencing
// token. It reports whether a step was claimed, never an error for the
// ordinary nothing-now outcome.
//
// The claim is the reservation: it flips the step to running and writes the
// step.started event in the same BEGIN IMMEDIATE transaction, so a crash
// mid claim leaves the run as if the step had never been surveyed.
func (s *Store) ClaimNextStep(ctx context.Context, runID string, ref LeaseRef) (*ClaimedStep, error) {
	if !ref.held() {
		return nil, fmt.Errorf("claim a step of run %s: no holder was named", runID)
	}
	now := s.clk.Now().UTC()
	nowMS := now.UnixMilli()

	var out *ClaimedStep
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(claimStepSQL, nowMS, runID, ref.Owner, ref.Epoch)
		if err != nil {
			return fmt.Errorf("claim a step of run %s: %w", runID, err)
		}
		var step *ClaimedStep
		for rows.Next() {
			var rid, name string
			var attempt int
			if err := rows.Scan(&rid, &name, &attempt); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan the claimed step of run %s: %w", runID, err)
			}
			if rid == runID {
				step = &ClaimedStep{RunID: runID, Name: name, Attempt: attempt}
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read the claimed step of run %s: %w", runID, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close the claimed step of run %s: %w", runID, err)
		}
		if step == nil {
			// The predicate admitted nothing. The run may be lost, finished,
			// cancelled, asked to stop, carrying a future retry, or simply
			// done; the caller distinguishes those outside the claim.
			out = nil
			return nil
		}

		var cap int
		if err := tx.QueryRow(`SELECT max_parallel FROM runs WHERE id = ?`, runID).Scan(&cap); err != nil {
			return fmt.Errorf("read the parallel cap of run %s: %w", runID, err)
		}
		step.MaxParallel = cap

		// The event that proves the transition happened, in the same
		// transaction as the flip. Without it the transition is an event
		// the transaction cannot tell, and the write model would have a lie.
		faults.Point("M4:claim:after_update")
		if err := appendRunEvent(tx, RunEvent{
			RunID:      runID,
			StepName:   step.Name,
			At:         now,
			Kind:       "step.started",
			FromState:  "pending",
			ToState:    "running",
			Actor:      ref.Owner,
			DetailJSON: fmt.Sprintf(`{"lease_epoch":%d}`, ref.Epoch),
		}); err != nil {
			return err
		}
		out = step
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
