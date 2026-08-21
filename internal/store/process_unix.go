//go:build unix

package store

import (
	"errors"
	"syscall"
)

// processAlive reports whether a process with this id exists. Signal 0 performs
// the permission and existence checks without delivering anything, so EPERM
// means the process is there and owned by somebody else.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
