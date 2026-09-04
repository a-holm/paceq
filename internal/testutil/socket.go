package testutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/a-holm/paceq/internal/sockpath"
)

// SocketPath answers with a path for a test's unix socket, in a short
// directory of its own that goes away with the test.
//
// t.TempDir() is the wrong place for one. Its name carries the test's, so the
// test name and TMPDIR together decide whether the socket fits in
// sockpath.MaxLen bytes, and a test that binds under it passes or fails on how
// it was named and on where the machine keeps its temporary files. This path is
// short and varies with neither.
func SocketPath(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "pq")
	if err != nil {
		t.Fatalf("create a socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	path := filepath.Join(dir, "s.sock")
	if err := sockpath.Validate(path); err != nil {
		t.Fatalf("no unix socket fits under TMPDIR: %v", err)
	}
	return path
}
