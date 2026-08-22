package store

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSchemaVersionFollowsMigrate pins what doctor reports. The version is 0
// before anything is applied and the build's highest migration afterwards, so a
// report that names a version says which schema the file actually holds.
func TestSchemaVersionFollowsMigrate(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	before, err := s.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("read the schema version of a fresh database: %v", err)
	}
	if before != 0 {
		t.Errorf("schema version of a fresh database is %d, want 0", before)
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	after, err := s.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("read the schema version after migrating: %v", err)
	}
	known, err := KnownSchemaVersion()
	if err != nil {
		t.Fatalf("read the known schema version: %v", err)
	}
	if after != known {
		t.Errorf("schema version after migrating is %d, want the build's %d", after, known)
	}
}

// TestKnownSchemaVersionMatchesTheEmbeddedFiles keeps the number version
// reports honest against the files that carry the schema.
func TestKnownSchemaVersionMatchesTheEmbeddedFiles(t *testing.T) {
	known, err := KnownSchemaVersion()
	if err != nil {
		t.Fatalf("read the known schema version: %v", err)
	}

	sub, err := fs.Sub(migrationFS, migrationDir)
	if err != nil {
		t.Fatalf("open the embedded migrations: %v", err)
	}
	all, err := loadMigrations(sub)
	if err != nil {
		t.Fatalf("load the embedded migrations: %v", err)
	}
	if want := maxVersion(all); known != want {
		t.Errorf("KnownSchemaVersion() = %d, want %d", known, want)
	}
	if known < 1 {
		t.Errorf("KnownSchemaVersion() = %d, want at least the base schema", known)
	}
}

// TestJournalModeReportsWAL is what lets doctor state the mode rather than
// assume it. Open refuses anything but WAL, so the reported value is the one
// the file is in.
func TestJournalModeReportsWAL(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)

	mode, err := s.JournalMode(ctx)
	if err != nil {
		t.Fatalf("read the journal mode: %v", err)
	}
	if !strings.EqualFold(mode, "wal") {
		t.Errorf("journal mode is %q, want wal", mode)
	}
}

// TestCheckModeAcceptsPrivatePaths covers the modes paceq creates itself.
func TestCheckModeAcceptsPrivatePaths(t *testing.T) {
	dir := stateDir(t)
	file := filepath.Join(dir, "state.db")
	if err := os.WriteFile(file, []byte("x"), DatabaseMode); err != nil {
		t.Fatalf("write %s: %v", file, err)
	}

	for _, c := range []struct {
		path string
		want fs.FileMode
	}{
		{path: dir, want: DirMode},
		{path: file, want: DatabaseMode},
	} {
		if err := CheckMode(c.path, c.want); err != nil {
			t.Errorf("CheckMode(%s, %#o) = %v, want nil", c.path, c.want, err)
		}
	}
}

// TestCheckModeRefusesWiderPaths is the fail closed rule, as a type a caller
// can act on. The message has to name the path, the mode found and the mode
// required, and offer the command that fixes it.
func TestCheckModeRefusesWiderPaths(t *testing.T) {
	cases := []struct {
		name string
		mode fs.FileMode
		want fs.FileMode
	}{
		{name: "group readable directory", mode: 0o750, want: DirMode},
		{name: "world readable directory", mode: 0o755, want: DirMode},
		{name: "group readable file", mode: 0o640, want: DatabaseMode},
		{name: "world readable file", mode: 0o644, want: DatabaseMode},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := stateDir(t)
			if c.want == DatabaseMode {
				path = filepath.Join(path, "state.db")
				if err := os.WriteFile(path, []byte("x"), DatabaseMode); err != nil {
					t.Fatalf("write %s: %v", path, err)
				}
			}
			if err := os.Chmod(path, c.mode); err != nil {
				t.Fatalf("chmod %s: %v", path, err)
			}

			err := CheckMode(path, c.want)
			var perm *PermissionError
			if !errors.As(err, &perm) {
				t.Fatalf("CheckMode(%s, %#o) = %v, want a *PermissionError", path, c.want, err)
			}
			if perm.Path != path || perm.Got != c.mode || perm.Want != c.want {
				t.Errorf("PermissionError = %+v, want path %s, got %#o, want %#o",
					perm, path, c.mode, c.want)
			}
			for _, want := range []string{path, "PQ5001", "chmod"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not mention %q", err, want)
				}
			}
			if !strings.Contains(err.Error(), "\n  ") {
				t.Errorf("refusal %q has no indented next step", err)
			}
		})
	}
}

// TestOpenStateRefusalCarriesThePermissionType is what lets the CLI map a wide
// database to its own exit code instead of reporting an internal failure.
func TestOpenStateRefusalCarriesThePermissionType(t *testing.T) {
	dir := stateDir(t)
	ctx := context.Background()

	s, err := OpenState(ctx, dir, Options{})
	if err != nil {
		t.Fatalf("open the state directory: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	dbPath := filepath.Join(dir, DatabaseFileName)
	if err := os.Chmod(dbPath, 0o644); err != nil {
		t.Fatalf("chmod %s: %v", dbPath, err)
	}

	again, err := OpenState(ctx, dir, Options{})
	if err == nil {
		_ = again.Close()
		t.Fatal("paceq started on a world readable database")
	}
	var perm *PermissionError
	if !errors.As(err, &perm) {
		t.Fatalf("OpenState refused with %v, want a *PermissionError", err)
	}
	if perm.Path != dbPath {
		t.Errorf("PermissionError names %s, want %s", perm.Path, dbPath)
	}
}

// openTempStore is a store on a real file in a private state directory,
// closed when the test ends.
func openTempStore(t *testing.T) *Store {
	t.Helper()

	path := filepath.Join(stateDir(t), DatabaseFileName)
	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close %s: %v", path, err)
		}
	})
	return s
}
