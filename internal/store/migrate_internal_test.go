package store

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

// migrationFile builds one entry for a fixture migration set.
func migrationFile(body string) *fstest.MapFile {
	return &fstest.MapFile{Data: []byte(body)}
}

// testStore opens a store on a fresh database file. Migration behaviour is only
// observable against a real file: WAL, the single writer connection and
// transactional DDL do not exist in an in-memory database.
func testStore(t *testing.T) *Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store at %q: %v", path, err)
		}
	})
	return s
}

func userVersion(t *testing.T, s *Store) int {
	t.Helper()

	var v int
	if err := s.w.QueryRowContext(context.Background(), "PRAGMA user_version").Scan(&v); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	return v
}

func tableExists(t *testing.T, s *Store, name string) bool {
	t.Helper()

	var count int
	err := s.w.QueryRowContext(context.Background(),
		"SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?", name).Scan(&count)
	if err != nil {
		t.Fatalf("look up table %q: %v", name, err)
	}
	return count > 0
}

var twoMigrations = fstest.MapFS{
	"0001_init.sql":  migrationFile("CREATE TABLE widgets (id INTEGER PRIMARY KEY) STRICT;"),
	"0002_extra.sql": migrationFile("CREATE TABLE gadgets (id INTEGER PRIMARY KEY) STRICT;"),
}

// TestMigrateAppliesEveryMigrationToAnEmptyDatabase is the tracer: a fresh
// database ends up with every table, a schema_migrations row per file and
// user_version at the highest applied version.
func TestMigrateAppliesEveryMigrationToAnEmptyDatabase(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.migrateFS(ctx, fs.FS(twoMigrations)); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	for _, name := range []string{"widgets", "gadgets", "schema_migrations"} {
		if !tableExists(t, s, name) {
			t.Errorf("table %q does not exist after migrating", name)
		}
	}
	if got := userVersion(t, s); got != 2 {
		t.Errorf("user_version is %d, want 2", got)
	}

	applied, err := s.appliedMigrations(ctx)
	if err != nil {
		t.Fatalf("read applied migrations: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("schema_migrations holds %d rows, want 2", len(applied))
	}
	if applied[0].Version != 1 || applied[0].Name != "init" {
		t.Errorf("first applied row is %d/%q, want 1/\"init\"", applied[0].Version, applied[0].Name)
	}
	if applied[0].Checksum == "" {
		t.Error("first applied row has an empty checksum")
	}
	if applied[0].AppliedAt <= 0 {
		t.Errorf("first applied row has applied_at %d, want a positive millisecond timestamp", applied[0].AppliedAt)
	}
}

// TestLoadMigrationsRefusesAMalformedSet covers the rules that keep the applied
// order the same on every machine: contiguous versions from 1, one file per
// version, one name shape.
func TestLoadMigrationsRefusesAMalformedSet(t *testing.T) {
	cases := []struct {
		name    string
		files   fstest.MapFS
		wantErr string
	}{
		{
			name: "gap in the version numbers",
			files: fstest.MapFS{
				"0001_init.sql":  migrationFile("CREATE TABLE a (id INTEGER PRIMARY KEY) STRICT;"),
				"0003_third.sql": migrationFile("CREATE TABLE c (id INTEGER PRIMARY KEY) STRICT;"),
			},
			wantErr: "0002",
		},
		{
			name: "two files claim the same version",
			files: fstest.MapFS{
				"0001_init.sql":  migrationFile("CREATE TABLE a (id INTEGER PRIMARY KEY) STRICT;"),
				"0001_again.sql": migrationFile("CREATE TABLE b (id INTEGER PRIMARY KEY) STRICT;"),
			},
			wantErr: "0001_again.sql",
		},
		{
			name:    "file name without a version prefix",
			files:   fstest.MapFS{"init.sql": migrationFile("CREATE TABLE a (id INTEGER PRIMARY KEY) STRICT;")},
			wantErr: "init.sql",
		},
		{
			name:    "set does not start at version 1",
			files:   fstest.MapFS{"0002_second.sql": migrationFile("CREATE TABLE b (id INTEGER PRIMARY KEY) STRICT;")},
			wantErr: "0001",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadMigrations(tc.files)
			if err == nil {
				t.Fatal("loadMigrations accepted the set, want an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// TestLoadMigrationsOrdersByVersion pins the order and the per file checksum.
func TestLoadMigrationsOrdersByVersion(t *testing.T) {
	all, err := loadMigrations(fstest.MapFS{
		"0002_second.sql": migrationFile("CREATE TABLE b (id INTEGER PRIMARY KEY) STRICT;"),
		"0001_init.sql":   migrationFile("CREATE TABLE a (id INTEGER PRIMARY KEY) STRICT;"),
		"0003_third.sql":  migrationFile("CREATE TABLE c (id INTEGER PRIMARY KEY) STRICT;"),
	})
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}

	var got []string
	for _, m := range all {
		got = append(got, fmt.Sprintf("%04d_%s", m.Version, m.Name))
	}
	want := []string{"0001_init", "0002_second", "0003_third"}
	if !slices.Equal(got, want) {
		t.Errorf("order is %v, want %v", got, want)
	}
	if all[0].Checksum == all[1].Checksum {
		t.Error("two different migrations share a checksum")
	}
}

// TestLoadMigrationsAcceptsAnEmptySet is the state the repository is in until
// the initial schema lands: no SQL file, no error, nothing to apply.
func TestLoadMigrationsAcceptsAnEmptySet(t *testing.T) {
	all, err := loadMigrations(fstest.MapFS{"README.md": migrationFile("not a migration")})
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("loaded %d migrations from an empty set", len(all))
	}
}

// TestMigrateIsIdempotent is the property that lets every startup call Migrate:
// a second run applies nothing and rewrites nothing.
func TestMigrateIsIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.migrateFS(ctx, twoMigrations); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	first, err := s.appliedMigrations(ctx)
	if err != nil {
		t.Fatalf("read applied migrations: %v", err)
	}

	if err := s.migrateFS(ctx, twoMigrations); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	second, err := s.appliedMigrations(ctx)
	if err != nil {
		t.Fatalf("read applied migrations: %v", err)
	}

	if !slices.Equal(first, second) {
		t.Errorf("second migrate changed the ledger: %v became %v", first, second)
	}
}

// TestMigrateRefusesAnEditedMigration is the checksum fence. An edited file
// means two machines that both report the same schema version have different
// schemas, which is why this is a refusal and not a warning.
func TestMigrateRefusesAnEditedMigration(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.migrateFS(ctx, twoMigrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	edited := fstest.MapFS{
		"0001_init.sql":  migrationFile("CREATE TABLE widgets (id INTEGER PRIMARY KEY, extra TEXT) STRICT;"),
		"0002_extra.sql": twoMigrations["0002_extra.sql"],
	}
	err := s.migrateFS(ctx, edited)
	if err == nil {
		t.Fatal("migrate accepted an edited migration, want a refusal")
	}
	for _, want := range []string{"0001_init.sql", "immutable"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestMigrateRefusesAnAppliedMigrationTheBinaryDoesNotCarry is the rolled back
// binary case: the database has run a migration this build has never seen.
func TestMigrateRefusesAnAppliedMigrationTheBinaryDoesNotCarry(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.migrateFS(ctx, twoMigrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// A ledger row this binary has no file for, with user_version left where it
	// is, so the version fence cannot be what answers.
	_, err := s.w.ExecContext(ctx,
		"INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (999, 'future', 'x', 1)")
	if err != nil {
		t.Fatalf("insert ledger row: %v", err)
	}

	err = s.migrateFS(ctx, twoMigrations)
	if err == nil {
		t.Fatal("migrate accepted a database newer than the binary, want a refusal")
	}
	for _, want := range []string{"0999", "future"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestMigrateRefusesAUserVersionAboveTheBinaryMaximum is the fence against
// running an old binary on a new database file. It has to name both numbers, or
// the operator cannot tell which build to reach for.
func TestMigrateRefusesAUserVersionAboveTheBinaryMaximum(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if _, err := s.w.ExecContext(ctx, "PRAGMA user_version = 999"); err != nil {
		t.Fatalf("set user_version: %v", err)
	}

	err := s.migrateFS(ctx, twoMigrations)
	if err == nil {
		t.Fatal("migrate ran against a database newer than the binary, want a refusal")
	}
	for _, want := range []string{"999", "2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if got := userVersion(t, s); got != 999 {
		t.Errorf("user_version is %d after the refusal, want it untouched at 999", got)
	}
}

// TestMigrateRefusesAMigrationInsertedBeforeAnAppliedOne catches the merge that
// numbers a new migration below one the database has already run. Applying it
// would give this database a different history from every other one.
func TestMigrateRefusesAMigrationInsertedBeforeAnAppliedOne(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.migrateFS(ctx, twoMigrations); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version = 1"); err != nil {
		t.Fatalf("forget migration 1: %v", err)
	}

	err := s.migrateFS(ctx, twoMigrations)
	if err == nil {
		t.Fatal("migrate applied a migration below the applied version, want a refusal")
	}
	if !strings.Contains(err.Error(), "0001") {
		t.Errorf("error %q does not name the out of order migration", err)
	}
}

// TestMigrateRollsBackAFailingMigration proves the transaction boundary from
// the other side: a migration whose second statement fails leaves neither the
// first statement's table nor a ledger row.
func TestMigrateRollsBackAFailingMigration(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	broken := fstest.MapFS{
		"0001_init.sql": migrationFile(
			"CREATE TABLE widgets (id INTEGER PRIMARY KEY) STRICT;\n" +
				"CREATE TABLE gadgets (id INTEGER PRIMARY KEY) STRICT;\n" +
				"CREATE TABLE widgets (id INTEGER PRIMARY KEY) STRICT;\n"),
	}
	if err := s.migrateFS(ctx, broken); err == nil {
		t.Fatal("migrate reported success for a failing migration")
	}

	for _, name := range []string{"widgets", "gadgets"} {
		if tableExists(t, s, name) {
			t.Errorf("table %q survived the failed migration", name)
		}
	}
	applied, err := s.appliedMigrations(ctx)
	if err != nil {
		t.Fatalf("read applied migrations: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("ledger holds %d rows after a failed migration, want 0", len(applied))
	}
	if got := userVersion(t, s); got != 0 {
		t.Errorf("user_version is %d after a failed migration, want 0", got)
	}
}

// openStore opens a second connection pair to an existing database file, which
// is what a second paceq process amounts to.
func openStore(t *testing.T, path string) *Store {
	t.Helper()

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store at %q: %v", path, err)
		}
	})
	return s
}

// TestMigrateRunsOnceWhenTwoProcessesStartTogether is the overlapping restart:
// two paceq processes come up against the same file at the same moment. One
// migrates, the other finds the work done. Neither reports an error, and no
// migration runs twice.
func TestMigrateRunsOnceWhenTwoProcessesStartTogether(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first := openStore(t, path)
	second := openStore(t, path)
	ctx := context.Background()

	errs := make(chan error, 2)
	start := make(chan struct{})
	for _, s := range []*Store{first, second} {
		go func() {
			<-start
			errs <- s.migrateFS(ctx, twoMigrations)
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent migrate: %v", err)
		}
	}

	applied, err := first.appliedMigrations(ctx)
	if err != nil {
		t.Fatalf("read applied migrations: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("ledger holds %d rows, want 2: a migration ran twice or not at all", len(applied))
	}
	if got := userVersion(t, first); got != 2 {
		t.Errorf("user_version is %d, want 2", got)
	}
}

// TestMigrateWaitsThenReportsAHeldLock covers the process that never lets go:
// the second start waits for its budget and then says who is holding the
// database, instead of migrating alongside it.
func TestMigrateWaitsThenReportsAHeldLock(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	defer func(previous time.Duration) { migrationLockWait = previous }(migrationLockWait)
	migrationLockWait = 20 * time.Millisecond

	if err := s.ensureMigrationTables(ctx); err != nil {
		t.Fatalf("create ledger tables: %v", err)
	}
	held := s.clk.Now().Add(time.Hour).UnixMilli()
	_, err := s.w.ExecContext(ctx,
		"INSERT INTO schema_migration_lock (id, holder, acquired_at, expires_at) VALUES (1, 'other-host/42', 0, ?)",
		held)
	if err != nil {
		t.Fatalf("hold the lock: %v", err)
	}

	err = s.migrateFS(ctx, twoMigrations)
	if err == nil {
		t.Fatal("migrate ran while another process held the lock")
	}
	if !strings.Contains(err.Error(), "other-host/42") {
		t.Errorf("error %q does not name the holder", err)
	}
	if tableExists(t, s, "widgets") {
		t.Error("migrate applied a migration while the lock was held")
	}
}

// TestMigrateTakesOverAnExpiredLock keeps a killed process from blocking every
// later start forever.
func TestMigrateTakesOverAnExpiredLock(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if err := s.ensureMigrationTables(ctx); err != nil {
		t.Fatalf("create ledger tables: %v", err)
	}
	expired := s.clk.Now().Add(-time.Hour).UnixMilli()
	_, err := s.w.ExecContext(ctx,
		"INSERT INTO schema_migration_lock (id, holder, acquired_at, expires_at) VALUES (1, 'dead-host/7', 0, ?)",
		expired)
	if err != nil {
		t.Fatalf("leave an expired lock: %v", err)
	}

	if err := s.migrateFS(ctx, twoMigrations); err != nil {
		t.Fatalf("migrate over an expired lock: %v", err)
	}
	if got := userVersion(t, s); got != 2 {
		t.Errorf("user_version is %d, want 2", got)
	}
}
