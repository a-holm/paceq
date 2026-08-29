package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// tempPath is a database path inside t.TempDir(). Every store test uses a real
// file: WAL behaviour and file locking cannot be observed against ":memory:".
func tempPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(t.TempDir(), "state.db")
}

// testDSN renders a connection string for the tests that open a pool of their
// own instead of going through Open. It goes through dsn, so a tempdir whose
// name carries a # or a ? still opens the file the test named. params is
// everything that would follow the ? in a hand-written DSN.
func testDSN(t *testing.T, path, params string) string {
	t.Helper()

	got, err := dsn(path, params, nil)
	if err != nil {
		t.Fatalf("dsn(%q): %v", path, err)
	}
	return got
}

func TestOpenCreatesTheDatabaseFile(t *testing.T) {
	path := tempPath(t)

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat database file: %v", err)
	}
	if !info.Mode().IsRegular() {
		t.Errorf("database path %q is not a regular file, mode %s", path, info.Mode())
	}
}
