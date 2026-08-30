package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// MaxUnixSocketPath is the longest path a named unix socket may carry.
// sockaddr_un.sun_path holds 108 bytes and a named address needs its
// terminator among them, so 107 is the last length that binds. Go refuses a
// longer name with EINVAL before the bind syscall ever runs, so the failure
// reads "invalid argument" and says nothing about length.
const MaxUnixSocketPath = 107

// SocketPath answers with a path for a test's unix socket, in a short
// directory of its own that goes away with the test.
//
// t.TempDir() is the wrong place for one. Its name carries the test's, so the
// test name and TMPDIR together decide whether the socket fits in the 107
// bytes, and a test that binds under it passes or fails on how it was named
// and on where the machine keeps its temporary files. This path is short and
// varies with neither.
func SocketPath(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "pq")
	if err != nil {
		t.Fatalf("create a socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	path := filepath.Join(dir, "s.sock")
	if len(path) > MaxUnixSocketPath {
		t.Fatalf("no unix socket fits under TMPDIR: %s is %d bytes and the kernel takes %d",
			path, len(path), MaxUnixSocketPath)
	}
	return path
}
