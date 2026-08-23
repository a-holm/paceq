//go:build linux

package reconcile

import (
	"syscall"
)

// unixSignals is the real thing: group addressed signals, TERM to ask and
// KILL to stop asking.
type unixSignals struct{}

// defaultSignals returns the platform's signaller.
func defaultSignals() Signaller { return unixSignals{} }

// Term asks a process group to exit. ESRCH just means it is already gone,
// which is success as far as the sweep is concerned, so the error is
// discarded by design at the call site.
func (unixSignals) Term(pgid int) error {
	return syscall.Kill(-pgid, syscall.SIGTERM)
}

// Kill stops asking.
func (unixSignals) Kill(pgid int) {
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
}
