//go:build unix

package store

import (
	"os"
	"os/exec"
	"testing"
)

// TestProcessAlive pins the three answers the migration lock's takeover rests
// on. A process owned by somebody else is the interesting one: signal 0 reports
// EPERM there, which means the process exists, and reading that as "gone" would
// let a start take a lock another user's paceq is holding.
func TestProcessAlive(t *testing.T) {
	if !processAlive(os.Getpid()) {
		t.Error("this test's own process is reported as gone")
	}

	if os.Geteuid() == 0 {
		t.Log("running as root, so no process id answers EPERM")
	} else if !processAlive(1) {
		t.Error("process 1 is reported as gone: EPERM means the process exists")
	}

	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run a short lived process: %v", err)
	}
	if processAlive(cmd.Process.Pid) {
		t.Errorf("a process that has exited (%d) is reported as alive", cmd.Process.Pid)
	}
}
