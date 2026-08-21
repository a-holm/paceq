package testutil_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/testutil"
)

// TestTempStoreIsARealFile pins the rule that store tests never run against
// ":memory:". WAL behaviour and real file locking are the whole point of the
// write model, and neither exists in an in-memory database.
func TestTempStoreIsARealFile(t *testing.T) {
	path := testutil.TempStore(t).Path()

	// t.TempDir() hands out a fresh subdirectory per call, so the shared parent
	// is what identifies the test's own temporary tree.
	if root := filepath.Dir(t.TempDir()); !strings.HasPrefix(path, root+string(filepath.Separator)) {
		t.Errorf("TempStore path %q is not under this test's temporary directory %q", path, root)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	if !info.Mode().IsRegular() {
		t.Errorf("TempStore path %q is not a regular file, mode %s", path, info.Mode())
	}

	// A WAL database keeps a sidecar next to the file while a connection is
	// open. An in-memory database has none.
	if _, err := os.Stat(path + "-wal"); err != nil {
		t.Errorf("no WAL sidecar next to %q: %v", path, err)
	}
}

func TestTempStoreGivesEachCallItsOwnFile(t *testing.T) {
	first := testutil.TempStore(t).Path()
	second := testutil.TempStore(t).Path()

	if first == second {
		t.Errorf("two TempStore calls share the file %q", first)
	}
}
