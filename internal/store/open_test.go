package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// tempPath is a database path inside t.TempDir(). Every store test uses a real
// file: WAL behaviour and file locking cannot be observed against ":memory:".
func tempPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(t.TempDir(), "state.db")
}

func TestOpenCreatesTheDatabaseFile(t *testing.T) {
	path := tempPath(t)

	s, err := store.Open(context.Background(), path, store.Options{})
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
