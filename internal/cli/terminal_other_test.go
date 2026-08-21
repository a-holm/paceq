//go:build !linux

package cli

import (
	"os"
	"runtime"
	"testing"
)

// terminalFile has no implementation outside linux: opening a pseudo terminal
// is per platform work, and linux is what paceq is built and tested on. The
// cases that need one say so rather than running against a pipe and proving the
// opposite of what they are about.
func terminalFile(t *testing.T) (*os.File, func() string) {
	t.Helper()

	t.Skipf("no pseudo terminal helper on %s: the terminal cases run on linux", runtime.GOOS)
	return nil, nil
}
