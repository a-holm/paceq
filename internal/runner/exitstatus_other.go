//go:build !unix

package runner

import (
	"os"
	"syscall"
)

// exitStatus on platforms without a unix wait status: only exit codes exist
// there, so no death ever classifies as Signalled.
func exitStatus(st *os.ProcessState) (code int, sig syscall.Signal, signaled bool) {
	return st.ExitCode(), 0, false
}
