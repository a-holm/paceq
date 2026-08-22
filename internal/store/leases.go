package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// LeaseGrant is one live lease row as the database computed it.
//
// A lease is the right for one holder to act as the singleton of one role:
// "scheduler" decides when jobs fire, "reaper" reaps what died, and so on.
// Epoch is the fencing token: it grows by exactly one every time the lease
// changes hands through an expiry, and never moves on a renewal, so a stale
// holder can always be told apart from the current one by comparing numbers.
type LeaseGrant struct {
	Name       string
	Holder     string
	Epoch      int64
	AcquiredAt time.Time
	ExpiresAt  time.Time
}

// LeaseEvent is one recorded moment in a lease's life: taken, lost or taken
// over from a dead holder. It is the stored explanation beside the structured
// log line, written through AppendLeaseEvent.
type LeaseEvent struct {
	At     time.Time
	Lease  string
	Holder string
	Epoch  int64
	Code   reason.Code
}

// acquireOrRenewSQL is the whole lease admission decision in one statement,
// per 11 section 4.2: insert first, never check first. A check then act split,
// even inside one transaction, invites a later edit that splits it into two
// transactions, and that split is a TOCTOU hole.
//
// The three paths through one statement:
//
//   - No row yet: the VALUES branch inserts epoch 1.
//   - Same holder again (a renewal): the CASE arms keep epoch and acquired_at
//     and move only expires_at. A renewal must not bump the fencing token, or
//     every fencing predicate turns into noise.
//   - Another holder's row whose time is up: the WHERE admits the takeover,
//     and epoch grows by exactly one while acquired_at resets to now.
//
// An empty result means another holder owns a lease that is still alive. That
// answer is the follower state, not an error.
//
// Time comes from the process that owns the statement: the caller passes a
// duration only, never a now value, so no clock comparison between processes
// ever happens (11 section 4.5).
const acquireOrRenewSQL = `INSERT INTO leases (name, holder, epoch, acquired_at, expires_at)
VALUES (?, ?, 1, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    holder      = excluded.holder,
    epoch       = CASE WHEN leases.holder = excluded.holder
                       THEN leases.epoch ELSE leases.epoch + 1 END,
    acquired_at = CASE WHEN leases.holder = excluded.holder
                       THEN leases.acquired_at ELSE excluded.acquired_at END,
    expires_at  = excluded.expires_at
WHERE leases.holder = excluded.holder OR leases.expires_at <= excluded.acquired_at
RETURNING holder, epoch, acquired_at, expires_at`

// AcquireOrRenew takes the named lease for holder, or renews the holding one
// holder already has. It is idempotent: calling it again with the same inputs
// moves nothing but expires_at.
//
// ok is false when another holder owns a lease that is still alive; the grant
// returned then is empty and the caller is the follower. ttl counts from the
// moment the database applies the statement, which this store reads from its
// own injected clock. The method deliberately takes no now parameter: lease
// time is always computed here, never compared across processes.
func (s *Store) AcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (LeaseGrant, bool, error) {
	now := s.clk.Now()
	at := now.UnixMilli()
	expiresAt := now.Add(ttl).UnixMilli()

	var g LeaseGrant
	var acquired, expires int64
	err := s.w.QueryRowContext(ctx, acquireOrRenewSQL, name, holder, at, expiresAt).
		Scan(&g.Holder, &g.Epoch, &acquired, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return LeaseGrant{}, false, nil
	}
	if err != nil {
		return LeaseGrant{}, false, fmt.Errorf("acquire or renew lease %s: %w", name, err)
	}
	g.Name = name
	g.AcquiredAt = time.UnixMilli(acquired).UTC()
	g.ExpiresAt = time.UnixMilli(expires).UTC()
	return g, true, nil
}

// ReleaseLease deletes the named lease row when holder still owns it, which is
// the clean shutdown path: the next process can take over immediately instead
// of waiting out the ttl.
//
// released is false when there was nothing of holder's to delete: either the
// lease was never held, or it expired and another holder took it over. Neither
// case is an error, and neither may touch the new holder's row.
func (s *Store) ReleaseLease(ctx context.Context, name, holder string) (bool, error) {
	result, err := s.w.ExecContext(ctx,
		`DELETE FROM leases WHERE name = ? AND holder = ?`, name, holder)
	if err != nil {
		return false, fmt.Errorf("release lease %s: %w", name, err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("release lease %s: count deleted rows: %w", name, err)
	}
	return deleted > 0, nil
}

// LeaseHolder returns the current row for the named lease, whether anyone
// holds it or not. It is the read side for status views and for whoever has to
// decide how long ago a holder was last seen. It reads only: nothing in this
// method writes, so it is safe next to a running daemon.
func (s *Store) LeaseHolder(ctx context.Context, name string) (LeaseGrant, bool, error) {
	var g LeaseGrant
	var acquired, expires int64
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		err := r.QueryRowContext(ctx, `SELECT holder, epoch, acquired_at, expires_at
  FROM leases
 WHERE name = ?`, name).Scan(&g.Holder, &g.Epoch, &acquired, &expires)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read lease %s: %w", name, err)
		}
		g.Name = name
		g.AcquiredAt = time.UnixMilli(acquired).UTC()
		g.ExpiresAt = time.UnixMilli(expires).UTC()
		return nil
	})
	if err != nil {
		return LeaseGrant{}, false, err
	}
	if g.Holder == "" && g.Epoch == 0 {
		return LeaseGrant{}, false, nil
	}
	return g, true, nil
}

// appendLeaseEventSQL stores the explanation in the same shape as every other
// event row: what happened, to whom, at which fencing token.
const appendLeaseEventSQL = `INSERT INTO lease_events (at, lease, holder, epoch, reason_code, detail_json)
VALUES (?, ?, ?, ?, ?, '{}')`

// AppendLeaseEvent records one moment of a lease's life: taken, lost or taken
// over. The code comes from the closed reason catalogue, so the row explains
// itself the way run_events rows do.
func (s *Store) AppendLeaseEvent(ctx context.Context, e LeaseEvent) error {
	_, err := s.w.ExecContext(ctx, appendLeaseEventSQL,
		e.At.UnixMilli(), e.Lease, e.Holder, e.Epoch, string(e.Code))
	if err != nil {
		return fmt.Errorf("append lease event for %s: %w", e.Lease, err)
	}
	return nil
}

// LeaseEvents returns the recorded moments of one lease, newest first. limit
// caps the read; pass a small number unless something really needs the history.
func (s *Store) LeaseEvents(ctx context.Context, name string, limit int) ([]LeaseEvent, error) {
	if limit <= 0 {
		limit = 50
	}
	var events []LeaseEvent
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT at, lease, holder, epoch, reason_code
  FROM lease_events
 WHERE lease = ?
 ORDER BY id DESC
 LIMIT ?`, name, limit)
		if err != nil {
			return fmt.Errorf("read lease events for %s: %w", name, err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				e    LeaseEvent
				at   int64
				code string
			)
			if err := rows.Scan(&at, &e.Lease, &e.Holder, &e.Epoch, &code); err != nil {
				return fmt.Errorf("scan lease event for %s: %w", name, err)
			}
			e.At = time.UnixMilli(at).UTC()
			e.Code = reason.Code(code)
			events = append(events, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}
