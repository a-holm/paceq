package store

import (
	"context"
	"fmt"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
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
// battery and the invariant engine cannot drift apart. What counts as
// agreement is model.RunStateAgrees, beside the fold itself, and which reason
// codes mean the run failed for something no step can express is
// reason.IsRunLevelFailure, beside the codes themselves, so this package holds
// no part of the rule and no caller can find one half without the other.
func (s *Store) RunAggregateMismatches(ctx context.Context) ([]AggregateMismatch, error) {
	rows, err := s.r.QueryContext(ctx, fsckI10SQL)
	if err != nil {
		return nil, fmt.Errorf("sweep for aggregate mismatches: %w", err)
	}
	type runSteps struct {
		state  string
		reason string
		steps  []model.StepState
	}
	byRun := map[string]*runSteps{}
	for rows.Next() {
		var id, runState, runReason, stepState string
		if err := rows.Scan(&id, &runState, &runReason, &stepState); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("sweep for aggregate mismatches: %w", err)
		}
		r, ok := byRun[id]
		if !ok {
			r = &runSteps{state: runState, reason: runReason}
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
		have, err := model.ParseRunState(r.state)
		if err != nil {
			return nil, fmt.Errorf("sweep for aggregate mismatches: run %s: %w", id, err)
		}
		failed := reason.IsRunLevelFailure(reason.Code(r.reason))
		if model.RunStateAgrees(have, r.steps, failed) {
			continue
		}
		out = append(out, AggregateMismatch{
			RunID:     id,
			State:     r.state,
			Aggregate: model.RunAggregate(r.steps, failed),
		})
	}
	return out, nil
}
