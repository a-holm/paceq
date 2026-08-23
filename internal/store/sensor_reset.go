package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// The dedup-epoch half of M3-04 (issue #6). The run_keys gate and the
// insert-first discipline live in sensor_tick.go (landed with M3-03 #4); this
// file is the other half, the epoch as something an operator can move. The
// product sentence is "50 files, 50 runs; rerun, 0; reset, 50 new". The gate
// gives the middle clause; these methods give the last one.
//
// The two notions the issue separates are cursor and run_key (10 section 5,
// F4c). They are two concepts with two reset flags and are never implicitly
// coupled. A reset bumps the epoch, which makes every old run key a new
// fingerprint in the new epoch; a cursor move alone changes nothing about the
// dedup table, so old keys keep dedupping. The reset row keeps its old epoch
// rows rather than deleting them (bumping is O(1) and reversible; deleting is
// O(n) and irreversible), unless the operator asks to forget run keys, which
// is the rare someone-really-wants-it-clean escape hatch.

// ResetSensorInput names the sensor being reset and the scope of the reset.
// A nil Cursor sets the cursor to NULL (the full "start over" form); a value
// replays from a chosen point. ForgetRunKeys erases the sensor's run_key
// rows; it is the only path that deletes them, and is never implicit.
type ResetSensorInput struct {
	Name          string
	SetCursor     *string
	ForgetRunKeys bool
}

// sensorResetHook is the fault point the reset crash test kills a child at,
// after the epoch bump but before the optional forget. Nothing sets it outside
// tests; see ResetSensor where it is called.
var sensorResetHook func()

// ResetResult reports what one reset did: the epoch it started from and where
// the bump took it, so the caller (the CLI, later) can tell the operator.
type ResetResult struct {
	Sensor     string
	OldEpoch   int64
	NewEpoch   int64
	Cursor     *string
	ForgotKeys bool
}

// ResetSensor atomically raises the sensor's dedup_epoch by one, so every run
// key this sensor ever registered becomes a new fingerprint in the fresh
// epoch. It is the store-level backing for `sensors reset`: the CLI surface
// lands in M3-06, and this is the method it will call.
//
// No read happens before the write: the bump is one UPDATE with a RETURNING
// readback, so two resets can never collide on the same new value (insert-
// first discipline, 11 section 5.2).
func (s *Store) ResetSensor(ctx context.Context, in ResetSensorInput) (ResetResult, error) {
	if in.Name == "" {
		return ResetResult{}, fmt.Errorf("reset sensor: %w", errors.New("empty sensor name"))
	}
	at := s.clk.Now().UTC().UnixMilli()
	out := ResetResult{
		Sensor:     in.Name,
		ForgotKeys: in.ForgetRunKeys,
	}

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var newEpoch int64
		var cursor sql.NullString
		err := tx.QueryRow(`UPDATE sensors
SET dedup_epoch       = dedup_epoch + 1,
    cursor            = ?,
    cursor_updated_at = ?,
    cursor_version    = cursor_version + 1,
    next_eval_at      = ?,
    updated_at        = ?
WHERE name = ?
RETURNING dedup_epoch, cursor`,
			nullablePtr(in.SetCursor), at, at, at, in.Name).Scan(&newEpoch, &cursor)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("reset sensor %s: %w", in.Name, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("reset sensor %s: %w", in.Name, err)
		}
		out.OldEpoch = newEpoch - 1
		out.NewEpoch = newEpoch
		if cursor.Valid {
			v := cursor.String
			out.Cursor = &v
		}

		// sensorResetHook is the fault point the crash test kills a child at,
		// after the epoch bump but before the optional forget. A kill here must
		// leave the database entirely old or entirely new, never with the epoch
		// raised and the keys gone, which is what atomicity means for a reset.
		// Nothing sets it outside tests; it mirrors sensorCommitHook.
		if sensorResetHook != nil {
			sensorResetHook()
		}

		if in.ForgetRunKeys {
			if _, err := tx.Exec(`DELETE FROM run_keys WHERE source_id = ?`, in.Name); err != nil {
				return fmt.Errorf("forget run keys of sensor %s: %w", in.Name, err)
			}
		}
		return nil
	})
	if err != nil {
		return ResetResult{}, err
	}
	return out, nil
}

// CursorInput names a sensor and the value to move its cursor to. Moving the
// cursor is deliberately distinct from a reset: it touches cursor and the
// cursor guard, and nothing about the dedup table. This is the store-level
// half of `cursor set` (the M3-06 CLI arrives later).
type CursorInput struct {
	Name   string
	Cursor string
}

// SetSensorCursor moves a sensor's cursor without touching its dedup epoch.
// The old run keys keep dedupping, because the epoch they are tagged with is
// still the current one. This is the "cursor set" row of the reset table in
// the issue: spool the cursor without replay, the dedup gate still stops old
// keys (10 section 5 F4c).
func (s *Store) SetSensorCursor(ctx context.Context, in CursorInput) error {
	if in.Name == "" {
		return fmt.Errorf("set sensor cursor: %w", errors.New("empty sensor name"))
	}
	at := s.clk.Now().UTC().UnixMilli()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE sensors
SET cursor            = ?,
    cursor_updated_at = ?,
    cursor_version    = cursor_version + 1,
    updated_at        = ?
WHERE name = ?`,
			in.Cursor, at, at, in.Name)
		if err != nil {
			return fmt.Errorf("set cursor of sensor %s: %w", in.Name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("count the cursor move of sensor %s: %w", in.Name, err)
		}
		if n == 0 {
			return fmt.Errorf("set cursor of sensor %s: %w", in.Name, ErrNotFound)
		}
		return nil
	})
}

// nullablePtr turns a nil pointer into an SQL NULL cursor value.
func nullablePtr(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}
