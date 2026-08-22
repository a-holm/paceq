//go:build unix

package runner

import (
	"os"
	"syscall"
)

// exitStatus reads the wait status out of a finished process. The platform
// split exists because syscall.WaitStatus differs per operating system; the
// classification logic above it does not.
func exitStatus(st *os.ProcessState) (code int, sig syscall.Signal, signaled bool) {
	ws, ok := st.Sys().(syscall.WaitStatus)
	if !ok {
		return st.ExitCode(), 0, false
	}
	if ws.Signaled() {
		return -1, ws.Signal(), true
	}
	return st.ExitCode(), 0, false
}
