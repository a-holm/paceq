//go:build unix

package store

import (
	"os"
	"syscall"
)

// lockExclusive takes an exclusive, non blocking flock. Blocking would turn a
// second start into a process that hangs with no output, which reads as a bug
// in paceq rather than as the operator's second daemon.
func lockExclusive(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// unlockFile drops the lock while keeping the file open. Closing the descriptor
// would release it too, but an explicit unlock keeps Release honest about what
// it does.
func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
