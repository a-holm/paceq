package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Maintenance plumbing: vacuum, checkpoint, backup and the meta keys that
// record what the last maintenance cycle did. Every statement here lives in
// internal/store because SQL may live nowhere else; callers see narrow
// methods, never handles.

// Meta keys this package writes. doctor reads the backup ones through
// BackupStatus, and the metrics surface reads the same rows later.
const (
	MetaKeyBackupLastAt        = "backup_last_at"
	MetaKeyBackupLastStatus    = "backup_last_status"
	MetaKeyBackupLastPath      = "backup_last_path"
	MetaKeyBackupLastError     = "backup_last_error"
	MetaKeyBackupLastDeepCheck = "backup_last_deep_check_at"

	MetaKeyGCCycleLastAt     = "gc_last_cycle_at"
	MetaKeyGCCycleLastStatus = "gc_last_cycle_status"
	MetaKeyGCCycleLastError  = "gc_last_cycle_error"
)

// Backup statuses as they land in meta. A copy that failed verification is a
// failed backup, not a backup with an asterisk: the file is removed and the
// status says so.
const (
	BackupStatusVerified = "verified"
	BackupStatusFailed   = "failed"
)

// SetMeta writes key/value pairs into meta inside one transaction.
func (s *Store) SetMeta(ctx context.Context, kv map[string]string) error {
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		for k, v := range kv {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO meta (key, value) VALUES (?, ?)
				 ON CONFLICT (key) DO UPDATE SET value = excluded.value`, k, v); err != nil {
				return fmt.Errorf("write meta key %q: %w", k, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("store meta: %w", err)
	}
	return nil
}

// MetaValue reads one meta key. The second return is false when the key has
// never been written, which callers must treat as "never ran" rather than an
// error.
func (s *Store) MetaValue(ctx context.Context, key string) (string, bool, error) {
	var (
		value string
		found bool
	)
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		err := r.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
		switch {
		case err == nil:
			found = true
			return nil
		case errors.Is(err, sql.ErrNoRows):
			return nil
		default:
			return fmt.Errorf("read meta key %q: %w", key, err)
		}
	})
	return value, found, err
}

// IncrementalVacuum releases up to maxPages free pages back to the
// filesystem. The page cap is what keeps the lock window short and
// predictable: 2000 pages of ordinary size is roughly 8 MB per night
// (07 section 6.3). Requires auto_vacuum=INCREMENTAL, which Open arms on new
// databases; on a database without free pages it is a no-op.
func (s *Store) IncrementalVacuum(ctx context.Context, maxPages int) error {
	if _, err := s.w.ExecContext(ctx,
		fmt.Sprintf("PRAGMA incremental_vacuum(%d)", maxPages)); err != nil {
		return fmt.Errorf("incremental_vacuum(%d): %w", maxPages, err)
	}
	return nil
}

// PageCount reports PRAGMA page_count.
func (s *Store) PageCount(ctx context.Context) (int64, error) {
	var pages int64
	if err := s.w.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); err != nil {
		return 0, fmt.Errorf("read page_count: %w", err)
	}
	return pages, nil
}

// FreelistCount reports PRAGMA freelist_count.
func (s *Store) FreelistCount(ctx context.Context) (int64, error) {
	var n int64
	if err := s.w.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&n); err != nil {
		return 0, fmt.Errorf("read freelist_count: %w", err)
	}
	return n, nil
}

// PageSize reports PRAGMA page_size.
func (s *Store) PageSize(ctx context.Context) (int64, error) {
	var n int64
	if err := s.w.QueryRowContext(ctx, "PRAGMA page_size").Scan(&n); err != nil {
		return 0, fmt.Errorf("read page_size: %w", err)
	}
	return n, nil
}

// FullVacuum rewrites the database file. It takes an exclusive lock and needs
// twice the disk, which is why nothing in the daemon ever calls it: it exists
// for `paceq db compact --i-know-this-blocks` and nothing else.
func (s *Store) FullVacuum(ctx context.Context) error {
	if _, err := s.w.ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("full vacuum: %w", err)
	}
	return nil
}

// VacuumInto writes a consistent, defragmented snapshot of the whole database
// to dst in one statement. Readers and writers keep working while it runs;
// the snapshot is as of one transaction. It never replaces verification:
// a copy nobody has checked is a hypothesis, so callers must follow with
// VerifyDatabaseFile.
func (s *Store) VacuumInto(ctx context.Context, dst string) error {
	if strings.TrimSpace(dst) == "" {
		return fmt.Errorf("vacuum into: destination path is empty")
	}
	if _, err := s.w.ExecContext(ctx, "VACUUM INTO ?", dst); err != nil {
		return fmt.Errorf("vacuum into %s: %w", dst, err)
	}
	return nil
}

// VerifyDatabaseFile opens path read-only in a fresh handle and runs
// quick_check, or integrity_check when deep is set. "ok" passes; anything
// else, including a file that cannot be opened at all, fails. This is the
// only way a backup counts as taken.
func VerifyDatabaseFile(ctx context.Context, path string, deep bool) error {
	uri, err := dsn(path, "mode=ro", nil)
	if err != nil {
		return fmt.Errorf("verify %s: %w", path, err)
	}
	db, err := sql.Open(driverName, uri)
	if err != nil {
		return fmt.Errorf("open backup copy for verification: %w", err)
	}
	defer db.Close()
	pragma := "quick_check"
	if deep {
		pragma = "integrity_check"
	}
	var res string
	if err := db.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&res); err != nil {
		return fmt.Errorf("%s on %s: %w", pragma, path, err)
	}
	if res != "ok" {
		return fmt.Errorf("%s = %q on %s", pragma, res, path)
	}
	return nil
}

// ActiveRunsExist reports whether any run is queued or running right now.
// The WAL checkpoint stays away while the answer is yes: truncating the WAL
// mid-flight would fight the writers it belongs to (07 section 6.4).
func (s *Store) ActiveRunsExist(ctx context.Context) (bool, error) {
	var active bool
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		return r.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM runs WHERE state IN ('queued', 'running'))`).Scan(&active)
	})
	if err != nil {
		return false, fmt.Errorf("look for active runs: %w", err)
	}
	return active, nil
}

// WalCheckpointTruncate checkpoints the WAL and truncates it to zero length.
// Callers gate this behind ActiveRunsExist; the pragma itself does not check.
func (s *Store) WalCheckpointTruncate(ctx context.Context) error {
	var (
		blocked  int64
		logPages int64
		moved    int64
	)
	if err := s.w.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").
		Scan(&blocked, &logPages, &moved); err != nil {
		return fmt.Errorf("wal_checkpoint(TRUNCATE): %w", err)
	}
	if blocked != 0 {
		return fmt.Errorf("wal_checkpoint(TRUNCATE): blocked by %d readers", blocked)
	}
	return nil
}

// RecordBackup writes the four backup meta keys in one transaction, plus the
// deep-check stamp when deepCheckAt is non-zero. at/status/path/err describe
// the most recent attempt, whatever its outcome.
func (s *Store) RecordBackup(ctx context.Context, at time.Time, status, path, errMsg string, deepCheckAt time.Time) error {
	kv := map[string]string{
		MetaKeyBackupLastAt:     at.UTC().Format(time.RFC3339),
		MetaKeyBackupLastStatus: status,
		MetaKeyBackupLastPath:   path,
		MetaKeyBackupLastError:  errMsg,
	}
	if !deepCheckAt.IsZero() {
		kv[MetaKeyBackupLastDeepCheck] = deepCheckAt.UTC().Format(time.RFC3339)
	}
	return s.SetMeta(ctx, kv)
}

// BackupInfo is what doctor shows about backups: when the last attempt ran,
// whether the copy verified, where it lives, and why it failed if it did.
type BackupInfo struct {
	LastAt        time.Time
	Status        string
	Path          string
	Error         string
	LastDeepCheck time.Time
	// HasBackup is false when this database has never attempted a backup.
	HasBackup bool
}

// Age returns how old the last backup attempt is relative to now.
func (b BackupInfo) Age(now time.Time) time.Duration { return now.Sub(b.LastAt) }

// Verified reports whether the last attempt produced a verified copy.
func (b BackupInfo) Verified() bool { return b.Status == BackupStatusVerified }

// BackupStatus reads the backup meta keys. A database that has never backed
// up returns the zero Info with HasBackup false - that absence is itself a
// finding doctor must be able to report.
func (s *Store) BackupStatus(ctx context.Context) (BackupInfo, error) {
	get := func(key string) (string, bool, error) { return s.MetaValue(ctx, key) }
	parseTime := func(raw string, found bool, err error) (time.Time, error) {
		if err != nil || !found || raw == "" {
			return time.Time{}, err
		}
		t, parseErr := time.Parse(time.RFC3339, raw)
		if parseErr != nil {
			return time.Time{}, fmt.Errorf("meta %s: %w", MetaKeyBackupLastAt, parseErr)
		}
		return t, nil
	}

	var info BackupInfo
	raw, found, err := get(MetaKeyBackupLastAt)
	if err != nil {
		return info, err
	}
	info.LastAt, err = parseTime(raw, found, err)
	if err != nil {
		return info, err
	}
	info.HasBackup = found && raw != ""
	if info.Status, _, err = get(MetaKeyBackupLastStatus); err != nil {
		return info, err
	}
	if info.Path, _, err = get(MetaKeyBackupLastPath); err != nil {
		return info, err
	}
	if info.Error, _, err = get(MetaKeyBackupLastError); err != nil {
		return info, err
	}
	rawDeep, foundDeep, err := get(MetaKeyBackupLastDeepCheck)
	if err != nil {
		return info, err
	}
	info.LastDeepCheck, err = parseTime(rawDeep, foundDeep, err)
	return info, err
}
