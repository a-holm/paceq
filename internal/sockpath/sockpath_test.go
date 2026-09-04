//go:build unix

package sockpath_test

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/sockpath"
)

// TestBoundaryIsMeasuredAgainstTheKernel binds the last length that fits and
// the first that does not, so MaxLen is a measurement rather than a belief. No
// Go error names a length, so nothing else in the tree can catch the constant
// drifting away from the platform.
func TestBoundaryIsMeasuredAgainstTheKernel(t *testing.T) {
	dir := shortDir(t)

	fits := padTo(t, dir, sockpath.MaxLen)
	l, err := net.Listen("unix", fits)
	if err != nil {
		t.Fatalf("a %d byte path did not bind, so the maximum is below %d: %v",
			len(fits), sockpath.MaxLen, err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close the listener: %v", err)
	}
	if err := sockpath.Validate(fits); err != nil {
		t.Errorf("Validate refused a %d byte path the kernel took: %v", len(fits), err)
	}

	over := padTo(t, dir, sockpath.MaxLen+1)
	l, err = net.Listen("unix", over)
	if err == nil {
		_ = l.Close()
		t.Fatalf("a %d byte path bound, so the maximum is above %d", len(over), sockpath.MaxLen)
	}
	if err := sockpath.Validate(over); err == nil {
		t.Fatalf("Validate accepted a %d byte path the kernel refused with %v", len(over), err)
	}
}

// TestValidateNamesTheLengthAndTheLimit holds the message to the one thing the
// kernel's EINVAL never says.
func TestValidateNamesTheLengthAndTheLimit(t *testing.T) {
	path := "/tmp/" + strings.Repeat("s", 200)
	err := sockpath.Validate(path)
	if err == nil {
		t.Fatalf("a %d byte path was accepted", len(path))
	}
	for _, want := range []string{strconv.Itoa(len(path)), strconv.Itoa(sockpath.MaxLen), path} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q: %v", want, err)
		}
	}
}

// shortDir is a temporary directory with room for a socket name inside it. A
// long TMPDIR makes the boundary unmeasurable here, which is a fact about the
// machine and not a failure of the code.
func shortDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "pq")
	if err != nil {
		t.Fatalf("create a socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	if len(dir)+2 > sockpath.MaxLen {
		t.Skipf("TMPDIR gives %d bytes, so no socket of %d bytes fits under it",
			len(dir), sockpath.MaxLen)
	}
	return dir
}

// padTo names a socket inside dir whose whole path is exactly n bytes.
func padTo(t *testing.T, dir string, n int) string {
	t.Helper()

	name := strings.Repeat("s", n-len(dir)-1)
	path := filepath.Join(dir, name)
	if len(path) != n {
		t.Fatalf("the padded path is %d bytes, want %d: %s", len(path), n, path)
	}
	return path
}
