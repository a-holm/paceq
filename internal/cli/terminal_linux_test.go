//go:build linux

package cli

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"
	"unsafe"
)

// terminalFile opens a real pseudo terminal and hands back the slave side, to
// be used as a command's stdout, together with everything written to it, read
// from the master.
//
// A fake that answers "yes, a terminal" would prove nothing: the check asks the
// kernel about a descriptor, so only a descriptor the kernel calls a terminal
// can exercise it.
func terminalFile(t *testing.T) (*os.File, func() string) {
	t.Helper()

	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open /dev/ptmx: %v", err)
	}
	t.Cleanup(func() { _ = master.Close() })

	// The slave is locked until unlockpt, and its number is only readable
	// through the master. This is what the C library's grantpt and unlockpt do.
	var unlocked int32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(),
		syscall.TIOCSPTLCK, uintptr(unsafe.Pointer(&unlocked))); errno != 0 {
		t.Fatalf("unlock the pseudo terminal: %v", errno)
	}
	var number uint32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(),
		syscall.TIOCGPTN, uintptr(unsafe.Pointer(&number))); errno != 0 {
		t.Fatalf("read the pseudo terminal number: %v", errno)
	}

	path := fmt.Sprintf("/dev/pts/%d", number)
	slave, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = slave.Close() })

	// The master is drained in the background: a terminal buffers a few
	// kilobytes, and a command that writes more would otherwise block.
	done := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(master)
		done <- string(data)
	}()

	return slave, func() string { return <-done }
}
