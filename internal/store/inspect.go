package store

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/a-holm/paceq/internal/spec"
)

// SchemaVersion is the schema version recorded in this database file. It is the
// number doctor reports, and the one that decides whether a binary may write to
// the file at all.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	return s.readUserVersion(ctx)
}

// JournalMode is the journal mode the database file is in. Open refuses
// anything but WAL, so a report built on this states what the file holds rather
// than what the code assumes.
func (s *Store) JournalMode(ctx context.Context) (string, error) {
	var mode string
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		if err := r.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
			return fmt.Errorf("read PRAGMA journal_mode: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return mode, nil
}

// SynchronousMode is the database's synchronous setting, read back as the
// numeric PRAGMA value ("2" is FULL, "1" NORMAL). The DSN pins it per
// connection, so the read states what a scrape-time connection runs with
// rather than what a hand-edited config intended.
func (s *Store) SynchronousMode(ctx context.Context) (string, error) {
	var mode string
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		if err := r.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&mode); err != nil {
			return fmt.Errorf("read PRAGMA synchronous: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return mode, nil
}

// ForeignKeys reports whether the database enforces its foreign keys. The DSN
// turns the rule on for every connection; a read-only open cannot set it and
// only verifies, so a database whose rows a foreign tool edited is still
// checked on read.
func (s *Store) ForeignKeys(ctx context.Context) (bool, error) {
	var on int
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		if err := r.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&on); err != nil {
			return fmt.Errorf("read PRAGMA foreign_keys: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return on == 1, nil
}

// JobsWithoutFreshnessSLA names the jobs whose current version declares no
// expected_within, the jobs monitoring cannot alarm on (06 SLO 6). The
// extraction reads one top-level key, the same cheap read the SLA metric
// makes; a version whose bytes cannot be read back is skipped, because apply
// refused it and a scrape must not hang on history.
func (s *Store) JobsWithoutFreshnessSLA(ctx context.Context) ([]string, error) {
	var out []string
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT j.name, v.spec_json
  FROM jobs j
  JOIN job_versions v ON v.id = j.current_version_id
 ORDER BY j.name`)
		if err != nil {
			return fmt.Errorf("list current job versions for the freshness check: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name, specJSON string
			if err := rows.Scan(&name, &specJSON); err != nil {
				return fmt.Errorf("scan a job version for the freshness check: %w", err)
			}
			_, ok, err := spec.ExpectedWithinFromIR([]byte(specJSON))
			if err != nil {
				continue
			}
			if !ok {
				out = append(out, name)
			}
		}
		return rows.Err()
	})
	return out, err
}

// KnownSchemaVersion is the highest schema version this build carries. It
// answers without a database, because version has to answer on a machine that
// has none.
func KnownSchemaVersion() (int, error) {
	sub, err := fs.Sub(migrationFS, migrationDir)
	if err != nil {
		return 0, fmt.Errorf("open embedded migrations: %w", err)
	}
	all, err := loadMigrations(sub)
	if err != nil {
		return 0, err
	}
	return maxVersion(all), nil
}
