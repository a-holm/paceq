package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// This file holds the clean stop. A daemon that shuts down on purpose gives
// its in flight work back through the machines, exactly as every other
// transition does, so a restart never meets a verdict nobody wrote or an
// attempt somebody else spent. The crash paths live in transitions.go and
// recover.go; nothing here counts a crash, because nothing crashed.

// claimableLimit caps one read of the queue. The dispatcher re-reads on every
// wake, so a backlog longer than this is processed over ticks instead of
// materialised in one slice.
const claimableLimit = 1000

// InterruptStepForShutdown puts one running step back to pending with its
// attempt restored (05 section 3.2, point 4): the attempt was cut short by the
// daemon's own stop, produced no verdict, and must not spend the retry budget.
// The step becomes runnable again at once, so the next executor claims it
// without ceremony.
//
// The machine refuses anything that is not a running step, and the event row
// names code, which for a daemon drain is RUN_INTERRUPTED_SHUTDOWN.
func (s *Store) InterruptStepForShutdown(ctx context.Context, runID, name string, code reason.Code) error {
	now := s.clk.Now().UTC()

	return s.withTx(ctx, func(tx *sql.Tx) error {
		step, err := readStepTx(tx, runID, name)
		if err != nil {
			return err
		}
		cur, err := model.ParseStepState(step.State)
		if err != nil {
			return err
		}

		state, effects, err := model.NextStepState(cur, model.EvShutdownDrain, model.Guards{
			ReasonCode: string(code),
		})
		if err != nil {
			return fmt.Errorf("interrupt step %s of run %s: %w", name, runID, err)
		}

		return finishTransition(tx, "interrupt_step", func() error {
			_, err := tx.Exec(`UPDATE steps SET state = 'pending',
				attempt = attempt - 1,
				reason_code = ?, next_attempt_at = ?, finished_at = NULL,
				duration_ms = NULL
			WHERE run_id = ? AND name = ? AND state = 'running'`,
				string(code), now.UnixMilli(), runID, name)
			return err
		}, tx, RunEvent{
			RunID: runID, StepName: name, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			ReasonCode: string(code),
			DetailJSON: `{"why":"daemon_stop"}`,
		})
	})
}

// RequeueRunAfterDrain hands a claimed run back to the queue at a clean stop.
// It is the mirror of RequeueCrashedRun: here the caller still holds the lease
// it is giving up, so the guard is inverted, and no crash is counted, because
// the executor left on purpose.
//
// The epoch still rises, so any writer from the drained attempt stays fenced
// out, and available_at moves to now so the next executor can claim at once.
func (s *Store) RequeueRunAfterDrain(ctx context.Context, runID string) error {
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

		held := !run.LeaseExpiresAt.IsZero() && run.LeaseExpiresAt.After(now)
		state, effects, err := model.NextRunState(cur, model.EvShutdownDrain, model.Guards{
			LeaseValid: held,
		})
		if err != nil {
			return fmt.Errorf("drain run %s: %w", runID, err)
		}
		if state != model.RunQueued {
			return fmt.Errorf("drain run %s: the machine sent a running run to %s", runID, state)
		}

		return finishTransition(tx, "requeue_drained", func() error {
			_, err := tx.Exec(`UPDATE runs SET state = 'queued',
				lease_owner = NULL, lease_expires_at = NULL,
				lease_epoch = lease_epoch + 1,
				defer_reason = ?, available_at = ?, updated_at = ?
			WHERE id = ? AND state = 'running'`,
				model.DeferReasonAfterShutdown, now.UnixMilli(), now.UnixMilli(), runID)
			return err
		}, tx, RunEvent{
			RunID: runID, At: now, Kind: emitKind(effects),
			FromState: string(cur), ToState: string(state),
			DetailJSON: `{"defer_reason":"` + model.DeferReasonAfterShutdown + `"}`,
		})
	})
}

// ClaimableRunIDs names the queued runs that are due now, in claim order: by
// available_at first, then id, which is the order the claim index keeps. A
// parked run whose time has not arrived is invisible here, exactly as the
// claim gate would have it.
func (s *Store) ClaimableRunIDs(ctx context.Context) ([]string, error) {
	now := s.clk.Now().UTC()

	var out []string
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT id FROM runs
WHERE state = 'queued' AND available_at <= ?
ORDER BY available_at, id
LIMIT ?`, now.UnixMilli(), claimableLimit)
		if err != nil {
			return fmt.Errorf("list claimable runs: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("scan a claimable run: %w", err)
			}
			out = append(out, id)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CheckpointTruncate runs PRAGMA wal_checkpoint(TRUNCATE) through the writer
// pool: every committed frame moves into the database and the wal file shrinks
// to zero bytes. The daemon calls it after its last write, before Close, so
// the next start opens a small file instead of replaying a long wal.
//
// An error here is reported, not fatal: a checkpoint that could not finish
// costs startup speed, never correctness.
func (s *Store) CheckpointTruncate(ctx context.Context) error {
	_, err := s.w.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	if err != nil {
		return fmt.Errorf("checkpoint the wal: %w", err)
	}
	return nil
}
