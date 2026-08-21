package testutil

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// TempStore opens a store on a real database file in t.TempDir() and closes it
// when the test ends.
//
// The file is real on purpose. WAL, file locking and the single writer
// connection are the behaviour under test throughout this project, and none of
// them exists in an in-memory database.
func TempStore(t *testing.T) *store.Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(context.Background(), path, store.Options{})
	if err != nil {
		t.Fatalf("open test store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close test store at %q: %v", path, err)
		}
	})
	return s
}
