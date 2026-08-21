package store

import (
	"context"
	"fmt"
	"io/fs"
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
