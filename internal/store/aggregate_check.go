package store

import (
	"context"
	"fmt"

	"github.com/a-holm/paceq/internal/model"
)

// AggregateMismatch names one run whose stored state disagrees with what its
// own steps aggregate to: the exact row issue #20's fifth acceptance item
// exists for, and the fact behind fsck's I10 finding.
type AggregateMismatch struct {
	RunID     string
	State     string
	Aggregate model.RunState
}

// RunAggregateMismatches sweeps every run and returns the ones whose state is
// not the aggregate of their steps. It is the explicit form of the crash
// suite's step-and-run consistency rule: at-least-once execution may repeat
// an attempt, but the run row and its steps must always tell one story.
//
// Fsck reports the same fact as I10 by calling this method, so the harness
// battery and the invariant engine cannot drift apart. A run whose steps
// aggregate to work no executor has claimed yet is not a mismatch; that
// exception belongs to the recovery windows and lives beside the aggregate
// function itself.
func (s *Store) RunAggregateMismatches(ctx context.Context) ([]AggregateMismatch, error) {
	rows, err := s.r.QueryContext(ctx, fsckI10SQL)
	if err != nil {
		return nil, fmt.Errorf("sweep for aggregate mismatches: %w", err)
	}
	type runSteps struct {
		state string
		steps []model.StepState
	}
	byRun := map[string]*runSteps{}
	for rows.Next() {
		var id, runState, stepState string
		if err := rows.Scan(&id, &runState, &stepState); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("sweep for aggregate mismatches: %w", err)
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
		return nil, fmt.Errorf("sweep for aggregate mismatches: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("sweep for aggregate mismatches: %w", err)
	}

	var out []AggregateMismatch
	for id, r := range byRun {
		want := model.RunAggregate(r.steps)
		if string(want) != r.state && !unclaimedWork(want, r.state) {
			out = append(out, AggregateMismatch{
				RunID:     id,
				State:     r.state,
				Aggregate: want,
			})
		}
	}
	return out, nil
}
