package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// rawDB is a connection with none of this project's settings applied. The
// auto_vacuum ordering is a property of SQLite itself, and the test says so by
// not going through Open.
func rawDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open(driverName, testDSN(t, path, "mode=rwc"))
	if err != nil {
		t.Fatalf("open %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close %q: %v", path, err)
		}
	})
	db.SetMaxOpenConns(1)
	return db
}

func rawAutoVacuum(t *testing.T, db *sql.DB) int {
	t.Helper()

	var mode int
	if err := db.QueryRowContext(context.Background(), "PRAGMA auto_vacuum").Scan(&mode); err != nil {
		t.Fatalf("read auto_vacuum: %v", err)
	}
	return mode
}

// TestAutoVacuumOnlyTakesEffectBeforeTheFirstTable is the trap this issue
// exists to avoid, written down as a test so nobody later tidies the ordering
// away. Setting the pragma after a table exists is a silent no-op, and undoing
// that costs a full VACUUM with an exclusive lock and twice the disk.
func TestAutoVacuumOnlyTakesEffectBeforeTheFirstTable(t *testing.T) {
	ctx := context.Background()

	t.Run("pragma before the first table", func(t *testing.T) {
		db := rawDB(t, filepath.Join(t.TempDir(), "state.db"))
		if _, err := db.ExecContext(ctx, "PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
			t.Fatalf("set auto_vacuum: %v", err)
		}
		if _, err := db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if got := rawAutoVacuum(t, db); got != 2 {
			t.Fatalf("auto_vacuum is %d, want 2 (INCREMENTAL)", got)
		}
	})

	t.Run("pragma after the first table", func(t *testing.T) {
		db := rawDB(t, filepath.Join(t.TempDir(), "state.db"))
		if _, err := db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if _, err := db.ExecContext(ctx, "PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
			t.Fatalf("set auto_vacuum: %v", err)
		}
		if got := rawAutoVacuum(t, db); got != 0 {
			t.Fatalf("auto_vacuum is %d, want 0: the pragma is supposed to be a no-op here", got)
		}
	})
}

// TestNewDatabaseIsIncremental is the setting this issue cannot get wrong
// twice: every database paceq creates has to be able to give disk back after
// retention, and the decision is made once, at creation.
func TestNewDatabaseIsIncremental(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	got, err := s.AutoVacuum(ctx)
	if err != nil {
		t.Fatalf("read auto_vacuum: %v", err)
	}
	if got != AutoVacuumIncremental {
		t.Fatalf("a new database has auto_vacuum %s, want %s", got, AutoVacuumIncremental)
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if got, err = s.AutoVacuum(ctx); err != nil || got != AutoVacuumIncremental {
		t.Fatalf("after migrating auto_vacuum is %s (err %v), want %s", got, err, AutoVacuumIncremental)
	}
}

// TestExistingDatabaseKeepsItsAutoVacuum is the other half. Turning the setting
// on for a database that already holds data means a full VACUUM, which takes an
// exclusive lock and twice the disk, so opening a database never does it
// silently. doctor reports it instead.
func TestExistingDatabaseKeepsItsAutoVacuum(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")

	db := rawDB(t, path)
	if _, err := db.ExecContext(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("open existing database: %v", err)
	}
	defer func() { _ = s.Close() }()

	got, err := s.AutoVacuum(ctx)
	if err != nil {
		t.Fatalf("read auto_vacuum: %v", err)
	}
	if got != AutoVacuumNone {
		t.Fatalf("auto_vacuum is %s, want %s: opening a populated database must not rewrite it",
			got, AutoVacuumNone)
	}
}
