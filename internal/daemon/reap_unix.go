//go:build unix

package daemon

import "syscall"

// ownProcessGroup is the group this daemon runs in. It is excluded from the
// drain's escalation outright: the one group this code may never signal is
// its own.
func ownProcessGroup() int { return syscall.Getpgrp() }
