//go:build unix

package cli

import "syscall"

// isTTY asks the kernel whether a descriptor is a terminal, by making the
// terminal ioctl with no buffer to write the answer into. A terminal answers
// EFAULT, because the address is missing; everything else answers ENOTTY,
// because the ioctl means nothing to it.
//
// The cheaper test, a character device bit in the file mode, calls /dev/null a
// terminal, and `paceq version > /dev/null` would then print text where every
// other redirection prints JSON.
func isTTY(fd uintptr) bool {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, terminalRequest, 0)
	return errno == syscall.EFAULT
}
