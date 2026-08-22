package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// readDeadline caps every read. A long lived read transaction starves WAL
// checkpointing, which is the most likely way this database grows without
// bound in production.
const readDeadline = 5 * time.Second

const (
	// maxWriteAttempts bounds the retry loop. Three is enough for a snapshot
	// conflict, which resolves on the next transaction, and small enough that a
	// persistent conflict surfaces instead of being absorbed.
	maxWriteAttempts = 3

	// retryBackoff is multiplied by the attempt number, so the waits are 5 ms
	// and 10 ms. The write lock is held by someone else for microseconds, not
	// seconds; a long backoff would only add latency.
	retryBackoff = 5 * time.Millisecond

	// busySnapshotCode is SQLITE_BUSY_SNAPSHOT: SQLITE_BUSY | (2<<8).
	busySnapshotCode = 517
)

// reader is the read side of the database. It is deliberately narrower than
// *sql.DB: there is no BeginTx, so a read path cannot open an explicit
// transaction even by accident.
type reader interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ErrReadOnly is returned when a store opened through OpenReadOnly is asked
// to write. It lives beside the transaction machinery rather than beside any
// one writer, because refusing is the transaction machinery's own act.
var ErrReadOnly = errors.New("the store is open read only")

// withTx runs fn inside one BEGIN IMMEDIATE transaction on the single write
// connection. It commits when fn returns nil and rolls back otherwise.
//
// Rules, from the locked write model. Breaking one is a review stopper:
//
//   - No process execution, file I/O or network I/O inside fn. The write lock
//     is the scarcest resource in the system, and fn holds it.
//   - Consume every RETURNING row before fn returns. The writer pool has one
//     connection, so an unread *sql.Rows deadlocks the process against itself.
//   - Keep fn short. Lock hold time is what this design optimises, not
//     transactions per second.
//
// fn receives only *sql.Tx. It gets no context, because a context inside a
// write transaction is an invitation to call out of the process while holding
// the write lock.
func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if s.w == nil {
		return fmt.Errorf("write refused: %w", ErrReadOnly)
	}
	for attempt := 1; ; attempt++ {
		err := s.withTxOnce(ctx, fn)
		if err == nil || attempt == maxWriteAttempts || !isBusySnapshot(err) {
			return err
		}
		timer := s.clk.NewTimer(time.Duration(attempt) * retryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return joinContextErr(ctx, err)
		case <-timer.C:
		}
	}
}

// isBusySnapshot reports whether err is SQLITE_BUSY_SNAPSHOT, the one error
// worth retrying. It is the outcome of a lock upgrade against a stale snapshot:
// the snapshot cannot become fresh, so busy_timeout never retries it and the
// transaction has to run again from the start.
//
// Plain SQLITE_BUSY is deliberately not retried here. busy_timeout already
// covers it, and a retry loop that swallows anything else hides a logic error
// behind a delay, which is worse than the error.
//
// The check goes through the Code method rather than the driver's concrete
// error type, so the escape hatch to another SQLite driver does not have to
// rewrite the retry policy.
func isBusySnapshot(err error) bool {
	var coded interface{ Code() int }
	return errors.As(err, &coded) && coded.Code() == busySnapshotCode
}

func (s *Store) withTxOnce(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return joinContextErr(ctx, fmt.Errorf("begin write transaction: %w", err))
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return joinContextErr(ctx, err)
	}
	if err := tx.Commit(); err != nil {
		return joinContextErr(ctx, fmt.Errorf("commit write transaction: %w", err))
	}
	return nil
}

// joinContextErr puts the context error in front of a failure that happened
// while the context was ending. err must not be nil.
//
// database/sql rolls a transaction back from its own goroutine as soon as the
// context ends, so the statement or the commit that loses that race reports
// sql.ErrTxDone and the cancellation disappears behind it. A caller deciding
// whether to retry has to be able to tell the two apart.
//
// Both errors stay in the chain, so errors.Is and errors.As still find the
// driver error. A failure with a live context is returned untouched: a
// transaction that really is finished must not start looking like a
// cancellation.
func joinContextErr(ctx context.Context, err error) error {
	ctxErr := ctx.Err()
	if ctxErr == nil || errors.Is(err, ctxErr) {
		return err
	}
	return fmt.Errorf("%w: %w", ctxErr, err)
}

// withRead runs fn against the read-only pool, never an explicit transaction.
// The context handed to fn carries a deadline so no read can hold a read
// snapshot open indefinitely.
func (s *Store) withRead(ctx context.Context, fn func(context.Context, reader) error) error {
	ctx, cancel := context.WithTimeout(ctx, readDeadline)
	defer cancel()
	return fn(ctx, s.r)
}
