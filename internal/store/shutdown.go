package store

import (
	"context"
	"fmt"
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
