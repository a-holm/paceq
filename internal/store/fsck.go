package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/a-holm/paceq/internal/model"
)

// fsck: the invariant sweep. Each check here is one SQL statement or one
// fold over rows, and each returns the rows that break the rule it names.
// The checks are what the crash harness of M1-12 plants violations against,
// so a check must catch exactly what its name says, or it is lying about
// coverage.
//
//   - I1  A run in running holds a live lease: an owner, and an expiry still
//         ahead of the wall clock minus the skew allowance.
//   - I2  A terminal run has no step still pending or running.
//   - I10 The run's stored state is what its steps aggregate to.
//   - I11 The fencing token never falls: the lease_epoch values recorded in
//         a run's event history are non-decreasing and end at the row's own
//         epoch.
//   - I13 Timestamps are monotone: created <= started <= finished, and no
//         stamp sits at zero where one exists at all.
//   - I14 A queued run held for the future carries its defer reason.
//   - I15 The event chain is continuous: every event's from_state is the
//         to_state of the event before it, per run and per step.
//
// The M1-05 reason code rule rides along, because the same sweep reads every
// terminal row anyway.

// Violation is one broken invariant: which check caught it and on what row.
type Violation struct {
	// Check names the invariant: "I2", "I10", "I13", "I14", "I15" or
	// "reason" for the catalogue rule.
	Check string

	// Subject names the row: "run <id>" or "run <id> step <name>".
	Subject string

	// Detail says what was expected and what was found.
	Detail string
}

// Fsck sweeps the whole database and returns every violation it finds, empty
// when the state is sound. It reads only: a checker that writes would be
// part of whatever it is checking.
func (s *Store) Fsck(ctx context.Context) ([]Violation, error) {
	var out []Violation

	// I1: a running run holds a live lease. The skew allowance is the same
	// one the reaper waits out, so a lease that is merely inside the takeover
	// window still counts as held.
	skewCut := s.clk.Now().UTC().Add(-DefaultClockSkewAllowance).UnixMilli()
	rows, err := s.r.QueryContext(ctx, `SELECT id FROM runs
WHERE state = 'running'
	AND (lease_owner IS NULL OR lease_owner = ''
		OR lease_expires_at IS NULL
		OR lease_expires_at <= ?)`, skewCut)
	if err != nil {
		return nil, fmt.Errorf("fsck I1: %w", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("fsck I1: %w", err)
		}
		out = append(out, Violation{
			Check:   "I1",
			Subject: "run " + id,
			Detail:  "the run is running without a live lease",
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("fsck I1: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fsck I1: %w", err)
	}

	// I2: nothing open under a finished run.
	rows, err = s.r.QueryContext(ctx, `SELECT r.id, s.name FROM runs r
JOIN steps s ON s.run_id = r.id
WHERE r.state IN ('succeeded', 'failed', 'cancelled')
	AND s.state IN ('pending', 'running')`)
	if err != nil {
		return nil, fmt.Errorf("fsck I2: %w", err)
	}
	for rows.Next() {
		var runID, step string
		if err := rows.Scan(&runID, &step); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("fsck I2: %w", err)
		}
		out = append(out, Violation{
			Check:   "I2",
			Subject: "run " + runID + " step " + step,
			Detail:  "the run is terminal but the step is not",
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("fsck I2: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fsck I2: %w", err)
	}

	// I10: the stored state is the aggregate of the steps, decided by the
	// same model function the engine finishes runs with. One truth, two
	// users; if they disagree, a row was written behind both.
	type runSteps struct {
		state string
		steps []model.StepState
	}
	byRun := map[string]*runSteps{}
	rows, err = s.r.QueryContext(ctx, `SELECT r.id, r.state, COALESCE(s.state, '')
FROM runs r LEFT JOIN steps s ON s.run_id = r.id ORDER BY r.id`)
	if err != nil {
		return nil, fmt.Errorf("fsck I10: %w", err)
	}
	for rows.Next() {
		var id, runState, stepState string
		if err := rows.Scan(&id, &runState, &stepState); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("fsck I10: %w", err)
		}
		r, ok := byRun[id]
		if !ok {
			r = &runSteps{state: runState}
			byRun[id] = r
		}
		if stepState != "" {
			r.steps = append(r.steps, model.StepState(stepState))
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("fsck I10: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fsck I10: %w", err)
	}
	for id, r := range byRun {
		want := model.RunAggregate(r.steps)
		if string(want) != r.state && !unclaimedWork(want, r.state) {
			out = append(out, Violation{
				Check:   "I10",
				Subject: "run " + id,
				Detail: fmt.Sprintf("state is %s, the steps aggregate to %s",
					r.state, want),
			})
		}
	}

	// I13: time only moves forward, and a present stamp is a real one.
	rows, err = s.r.QueryContext(ctx, `SELECT 'run', id FROM runs
WHERE created_at <= 0
	OR (started_at IS NOT NULL AND started_at < created_at)
	OR (finished_at IS NOT NULL AND started_at IS NOT NULL AND finished_at < started_at)
UNION ALL
SELECT 'step', run_id || '/' || name FROM steps
WHERE (started_at IS NOT NULL AND started_at <= 0)
	OR (finished_at IS NOT NULL AND started_at IS NOT NULL AND finished_at < started_at)`)
	if err != nil {
		return nil, fmt.Errorf("fsck I13: %w", err)
	}
	for rows.Next() {
		var kind, key string
		if err := rows.Scan(&kind, &key); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("fsck I13: %w", err)
		}
		out = append(out, Violation{
			Check:   "I13",
			Subject: kind + " " + key,
			Detail:  "timestamps are not monotone or carry zero",
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("fsck I13: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fsck I13: %w", err)
	}

	// I14: a held back run always says why. Deferred is not a state, so
	// the check is on the queued rows whose available_at lies ahead.
	rows, err = s.r.QueryContext(ctx, `SELECT id FROM runs
WHERE state = 'queued' AND available_at > created_at
	AND (defer_reason IS NULL OR defer_reason = '')`)
	if err != nil {
		return nil, fmt.Errorf("fsck I14: %w", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("fsck I14: %w", err)
		}
		out = append(out, Violation{
			Check:   "I14",
			Subject: "run " + id,
			Detail:  "the run is held for the future with no defer_reason",
		},
		)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("fsck I14: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fsck I14: %w", err)
	}

	// I15: the chain of events tells one story per run and per step. The
	// window walks each partition in id order; any from_state that does
	// not pick up the previous to_state is a break. The first row of a
	// partition starts wherever the row was when the run was
	// materialised, so it is judged by nothing but its neighbours.
	rows, err = s.r.QueryContext(ctx, `SELECT run_id, who FROM (
	SELECT run_id,
		COALESCE(step_name, '') || ':' || kind || ':' ||
			COALESCE(from_state, '<start>') || '->' || to_state AS who,
		from_state,
		LAG(to_state) OVER w AS prev_to
	FROM run_events
	WINDOW w AS (PARTITION BY run_id, COALESCE(step_name, '') ORDER BY id))
WHERE prev_to IS NOT NULL AND COALESCE(from_state, '<start>') <> prev_to`)
	if err != nil {
		return nil, fmt.Errorf("fsck I15: %w", err)
	}
	for rows.Next() {
		var runID, who string
		if err := rows.Scan(&runID, &who); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("fsck I15: %w", err)
		}
		out = append(out, Violation{
			Check:   "I15",
			Subject: "run " + runID,
			Detail:  "event chain breaks at " + who,
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("fsck I15: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fsck I15: %w", err)
	}

	// I11: the fencing token never falls. Every run level transition records
	// the token it moved under in its event detail, so the history can be
	// replayed as a sequence of tokens. The sequence has to be non-decreasing,
	// it has to end exactly at the row's own epoch, and a row whose epoch rose
	// without any history saying so is a violation on its own: that is the
	// shape of an unfenced write.
	rows, err = s.r.QueryContext(ctx, `SELECT run_id, detail_json FROM run_events
WHERE step_name IS NULL AND kind <> 'run.result_discarded'
ORDER BY run_id, id`)
	if err != nil {
		return nil, fmt.Errorf("fsck I11: %w", err)
	}
	epochsByRun := map[string][]int64{}
	for rows.Next() {
		var (
			runID  string
			detail string
		)
		if err := rows.Scan(&runID, &detail); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("fsck I11: %w", err)
		}
		var decoded struct {
			LeaseEpoch *int64 `json:"lease_epoch"`
		}
		if json.Unmarshal([]byte(detail), &decoded) == nil && decoded.LeaseEpoch != nil {
			epochsByRun[runID] = append(epochsByRun[runID], *decoded.LeaseEpoch)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("fsck I11: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fsck I11: %w", err)
	}

	rowEpochs := map[string]int64{}
	rows, err = s.r.QueryContext(ctx, `SELECT id, lease_epoch FROM runs`)
	if err != nil {
		return nil, fmt.Errorf("fsck I11: %w", err)
	}
	for rows.Next() {
		var id string
		var epoch int64
		if err := rows.Scan(&id, &epoch); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("fsck I11: %w", err)
		}
		rowEpochs[id] = epoch
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("fsck I11: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("fsck I11: %w", err)
	}

	for id, epoch := range rowEpochs {
		history := epochsByRun[id]
		falling := false
		for i := 1; i < len(history); i++ {
			if history[i] < history[i-1] {
				falling = true
			}
		}
		switch {
		case len(history) > 0 && falling:
			out = append(out, Violation{
				Check:   "I11",
				Subject: "run " + id,
				Detail: fmt.Sprintf("the recorded tokens %v are not non-decreasing"+
					" against the row's epoch %d", history, epoch),
			})
		case len(history) > 0 && history[len(history)-1] != epoch:
			out = append(out, Violation{
				Check:   "I11",
				Subject: "run " + id,
				Detail: fmt.Sprintf("the history ends at token %d but the row says %d",
					history[len(history)-1], epoch),
			})
		case len(history) == 0 && epoch > 0:
			out = append(out, Violation{
				Check:   "I11",
				Subject: "run " + id,
				Detail:  fmt.Sprintf("the row is at epoch %d with no token in its history", epoch),
			})
		}
	}

	// The reason code rule, swept along with everything else.
	unexplained, err := s.UnexplainedReasons(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range unexplained {
		out = append(out, Violation{
			Check:   "reason",
			Subject: r.Kind + " " + r.Key,
			Detail:  "ended without a usable reason code",
		})
	}

	return out, nil
}

// unclaimedWork is the one shape the aggregate cannot name: steps that are
// still pending describe both a run nobody has claimed yet (queued) and a
// claimed run whose first step has not started (running). Both are correct,
// so the aggregate saying running is allowed to sit beside either stored
// state. Everything else must match exactly: a failed or cancelled aggregate
// under a succeeded row is the drift this check exists to catch. The crash
// harness (#75) holds the queued half of this: a run materialised and not yet
// claimed must sweep clean.
func unclaimedWork(want model.RunState, have string) bool {
	return want == model.RunRunning && have == "queued"
}
