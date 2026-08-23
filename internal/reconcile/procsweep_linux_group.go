//go:build linux

package reconcile

import (
	"os"
	"syscall"
)

// ownGroup is the process group this sweep belongs to. It is excluded from
// every consideration outright: the one group this code must never signal is
// its own.
func ownGroup() int { return syscall.Getpgrp() }

// getpgid reads the process group of a found process. Kept beside the scan
// because it is the only syscall the walk needs beyond file reads.
func getpgid(p *os.Process) (int, error) {
	return syscall.Getpgid(p.Pid)
}
