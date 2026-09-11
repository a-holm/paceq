//go:build unix

package daemon

import "syscall"

// killProcessGroup delivers one signal to a whole process group.
func killProcessGroup(pgid int, sig syscall.Signal) error {
	return syscall.Kill(-pgid, sig)
}
