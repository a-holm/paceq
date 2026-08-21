package store

import (
	"context"
	"fmt"
)

// AutoVacuumMode is the file level setting that decides whether SQLite ever
// returns freed pages to the filesystem. It is stored in the database header,
// so it survives restarts and belongs to the file rather than the connection.
type AutoVacuumMode int

const (
	// AutoVacuumNone never shrinks the file. Deleted pages are reused, and the
	// file only ever grows.
	AutoVacuumNone AutoVacuumMode = 0
	// AutoVacuumFull moves pages on every commit, which puts the cost on the
	// hot path.
	AutoVacuumFull AutoVacuumMode = 1
	// AutoVacuumIncremental keeps a free page list that PRAGMA
	// incremental_vacuum releases in bounded batches, off the hot path.
	AutoVacuumIncremental AutoVacuumMode = 2
)

func (m AutoVacuumMode) String() string {
	switch m {
	case AutoVacuumNone:
		return "NONE"
	case AutoVacuumFull:
		return "FULL"
	case AutoVacuumIncremental:
		return "INCREMENTAL"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// AutoVacuum is the mode the database file was created with. doctor reports it,
// because a database left at NONE never gives disk back after retention and the
// way out costs a full VACUUM.
func (s *Store) AutoVacuum(ctx context.Context) (AutoVacuumMode, error) {
	var mode int
	if err := s.w.QueryRowContext(ctx, "PRAGMA auto_vacuum").Scan(&mode); err != nil {
		return AutoVacuumNone, fmt.Errorf("read PRAGMA auto_vacuum: %w", err)
	}
	return AutoVacuumMode(mode), nil
}

// ensureAutoVacuum sets INCREMENTAL on a database that has no schema yet, which
// is the only moment it is free. SQLite writes the setting into the header when
// the first page is written, and after that the only way to change it is a full
// VACUUM holding an exclusive lock and needing twice the disk.
//
// The setting therefore cannot live in migration 0001: by the time a migration
// runs, the engine has already created its ledger, and the header is written.
// Open owns it instead, because Open is the one place that materialises the
// file, whether the caller is the daemon, the CLI or a test.
//
// A database that already has a schema is left exactly as it is. Rewriting a
// populated database behind the operator's back is the opposite of what this
// project promises, so that case is a doctor finding with a manual way out.
func (s *Store) ensureAutoVacuum(ctx context.Context) error {
	empty, err := s.schemaIsEmpty(ctx)
	if err != nil {
		return err
	}
	if !empty {
		return nil
	}

	mode, err := s.AutoVacuum(ctx)
	if err != nil {
		return err
	}
	if mode == AutoVacuumIncremental {
		return nil
	}

	if _, err := s.w.ExecContext(ctx, "PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
		return fmt.Errorf("set auto_vacuum: %w", err)
	}
	// The pragma only arms the setting. VACUUM is what rewrites the header, and
	// on a database with no schema it copies nothing.
	if _, err := s.w.ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("vacuum a new database to apply auto_vacuum: %w", err)
	}

	switch mode, err = s.AutoVacuum(ctx); {
	case err != nil:
		return err
	case mode != AutoVacuumIncremental:
		return fmt.Errorf("auto_vacuum is %s after creating %s, want %s: the database would never "+
			"give disk back after retention, refusing to start", mode, s.path, AutoVacuumIncremental)
	}
	return nil
}

// schemaIsEmpty reports whether the database holds no objects at all, which is
// true of a file paceq has just created and false from the first migration on.
func (s *Store) schemaIsEmpty(ctx context.Context) (bool, error) {
	var count int
	if err := s.w.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema").Scan(&count); err != nil {
		return false, fmt.Errorf("read sqlite_schema: %w", err)
	}
	return count == 0, nil
}
