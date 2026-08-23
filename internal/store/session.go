package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/id"
)

// bootKey is the meta row that carries the boot id of the machine the last
// session ran on.
const bootKey = "current_boot_id"

// Session is one run of paceq against this database. Its id is the instance
// identity: a ULID minted at startup, not a process id, because process ids are
// recycled and a recycled one would make a dead run look like a live one both
// in the history and in a lease.
type Session struct {
	ID         string
	Version    string
	BootID     string
	PID        int
	StartedAt  time.Time
	LastSeenAt time.Time
	StoppedAt  time.Time
	StopReason string
}

// Stopped reports whether the run this session describes has ended.
func (s Session) Stopped() bool { return !s.StoppedAt.IsZero() }

// StartSession opens this process's session row and closes whatever the last
// run left behind.
//
// Three things happen in one transaction, because a reader that saw them apart
// would draw the wrong conclusion. Sessions still open belong to a run that
// never got to say goodbye, so they are marked crashed. The new row records who
// is running, on which boot, from when. The boot id is written to meta, so the
// next start can tell a restart of paceq from a restart of the machine.
//
// A changed boot id is the strongest evidence this system has: the machine
// restarted, so no process paceq started can still be alive. BootChanged
// reports it after this call returns.
func (s *Store) StartSession(ctx context.Context, version string) (Session, error) {
	// The boot id is read before the transaction opens. Reading a file while
	// holding the write lock is the one thing the write model forbids outright.
	boot := s.bootIdentity()

	now := s.clk.Now().UTC()
	sessionID, err := id.New(now)
	if err != nil {
		return Session{}, fmt.Errorf("mint a session id: %w", err)
	}
	at := now.UnixMilli()
	pid := os.Getpid()

	changed := false
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		var previous string
		err := tx.QueryRow("SELECT value FROM meta WHERE key = ?", bootKey).Scan(&previous)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read the recorded boot id: %w", err)
		}
		// An unknown boot id on either side is not evidence of anything. The
		// first start of a database has no previous value, and a platform
		// without the kernel file has no current one.
		changed = boot != "" && previous != "" && boot != previous

		if _, err := tx.Exec(`UPDATE daemon_sessions
SET stopped_at = ?, stop_reason = 'crash'
WHERE stopped_at IS NULL`, at); err != nil {
			return fmt.Errorf("close the sessions left open by the last run: %w", err)
		}

		if _, err := tx.Exec(`INSERT INTO daemon_sessions
	(id, version, boot_id, pid, started_at, last_seen_at)
VALUES (?, ?, ?, ?, ?, ?)`, sessionID, version, nullIfEmpty(boot), pid, at, at); err != nil {
			return fmt.Errorf("record this session: %w", err)
		}

		if boot == "" {
			return nil
		}
		if _, err := tx.Exec(`INSERT INTO meta (key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, bootKey, boot); err != nil {
			return fmt.Errorf("record the boot id: %w", err)
		}
		return nil
	})
	if err != nil {
		return Session{}, err
	}
	s.bootChanged.Store(changed)

	return Session{
		ID:         sessionID,
		Version:    version,
		BootID:     boot,
		PID:        pid,
		StartedAt:  now,
		LastSeenAt: now,
	}, nil
}

// BootChanged reports whether the machine has restarted since the last session
// ran. It answers for the session StartSession opened, and is false until then.
//
// The evidence lasts for one start. StartSession reads the recorded boot id and
// overwrites it in the same transaction, so the first start after a reboot is
// the only one that sees the change: every later start on that boot reports
// false. Whoever reconciles has to act on it then, or the fact is gone.
//
// False is not the same as "no restart": a platform without a boot id reports
// false, and then a surviving process can only be ruled out by waiting for its
// lease to expire.
func (s *Store) BootChanged() bool { return s.bootChanged.Load() }

// TouchSession moves the session's heartbeat forward. Nothing here decides how
// often that happens; the daemon owns the interval.
//
// The heartbeat is what turns a crashed session into a bounded outage: the gap
// runs from the last heartbeat to the restart, and without it the whole run
// would be unaccounted for.
func (s *Store) TouchSession(ctx context.Context, sessionID string) error {
	at := s.clk.Now().UnixMilli()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE daemon_sessions
SET last_seen_at = ?
WHERE id = ? AND stopped_at IS NULL`, at, sessionID)
		if err != nil {
			return fmt.Errorf("touch session %s: %w", sessionID, err)
		}
		return requireOneRow(result, sessionID, "touch")
	})
}

// StopSession records a clean shutdown. A session that ends any other way keeps
// its open row, which is what the next start reads as a crash.
func (s *Store) StopSession(ctx context.Context, sessionID string) error {
	at := s.clk.Now().UnixMilli()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.Exec(`UPDATE daemon_sessions
SET stopped_at = ?, last_seen_at = ?, stop_reason = 'clean'
WHERE id = ? AND stopped_at IS NULL`, at, at, sessionID)
		if err != nil {
			return fmt.Errorf("stop session %s: %w", sessionID, err)
		}
		return requireOneRow(result, sessionID, "stop")
	})
}

// OpenSession is the session that has not ended, if there is one. It is what
// names the process holding the state lock, and what tells a restart that the
// last run never finished.
func (s *Store) OpenSession(ctx context.Context) (Session, bool, error) {
	var found Session
	ok := false
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		var (
			boot     sql.NullString
			started  int64
			lastSeen int64
		)
		err := r.QueryRowContext(ctx, `SELECT id, version, boot_id, pid, started_at, last_seen_at
  FROM daemon_sessions
 WHERE stopped_at IS NULL
 ORDER BY started_at DESC
 LIMIT 1`).Scan(&found.ID, &found.Version, &boot, &found.PID, &started, &lastSeen)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read the open session: %w", err)
		}
		found.BootID = boot.String
		found.StartedAt = time.UnixMilli(started).UTC()
		found.LastSeenAt = time.UnixMilli(lastSeen).UTC()
		ok = true
		return nil
	})
	if err != nil {
		return Session{}, false, err
	}
	return found, ok, nil
}

// LatestSession is the most recent session row, open or closed. It is how an
// operator or the health surface asks "who ran here last, and how did it
// end": a closed row carries its stop_reason, an open one means the process
// died without saying goodbye.
func (s *Store) LatestSession(ctx context.Context) (Session, bool, error) {
	var found Session
	ok := false
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		var (
			boot     sql.NullString
			started  int64
			lastSeen int64
			stopped  sql.NullInt64
			reason   sql.NullString
		)
		err := r.QueryRowContext(ctx, `SELECT id, version, boot_id, pid, started_at,
last_seen_at, stopped_at, stop_reason
  FROM daemon_sessions
 ORDER BY started_at DESC
 LIMIT 1`).Scan(&found.ID, &found.Version, &boot, &found.PID, &started,
			&lastSeen, &stopped, &reason)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read the latest session: %w", err)
		}
		found.BootID = boot.String
		found.StartedAt = time.UnixMilli(started).UTC()
		found.LastSeenAt = time.UnixMilli(lastSeen).UTC()
		found.StoppedAt = timeOrZero(stopped)
		found.StopReason = reason.String
		ok = true
		return nil
	})
	if err != nil {
		return Session{}, false, err
	}
	return found, ok, nil
}

// bootIdentity is the machine's boot id, or the empty string when the platform
// does not offer one. Losing it is a degradation, not a failure: paceq then
// falls back to lease expiry as its only evidence that a process is gone, which
// is slower and still correct.
//
// It is deliberately read on every call rather than cached. The value cannot
// change while the process lives, but a cache would have to be invalidated in
// tests that reproduce a restart, and a /proc read costs nothing next to the
// transaction it precedes.
func (s *Store) bootIdentity() string {
	value, err := s.bootID()
	if err != nil {
		s.bootWarn.Do(func() {
			slog.Warn("boot id unavailable: a machine restart cannot be detected, "+
				"so a surviving process can only be ruled out by waiting for its lease to expire",
				"error", err)
		})
		return ""
	}
	return strings.TrimSpace(value)
}

// requireOneRow turns "no such session" into an error. Silently updating
// nothing would let a caller heartbeat an id that does not exist and believe
// the run is being recorded.
func requireOneRow(result sql.Result, sessionID, what string) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s session %s: %w", what, sessionID, err)
	}
	if changed == 0 {
		return fmt.Errorf("%s session %s: no open session has that id", what, sessionID)
	}
	return nil
}

// nullIfEmpty keeps an unknown boot id out of the database as NULL. An empty
// string would be a value, and a query could not tell it from a real one.
func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
