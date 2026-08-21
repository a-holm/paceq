//go:build unix

package store

import (
	"os"
	"os/exec"
	"strconv"
	"testing"
)

// TestHolderOnThisBootFollowsTheProcess is the other half of the boot check: on
// the boot the holder was written, the process check decides, and a holder that
// has exited must not keep the lock.
func TestHolderOnThisBootFollowsTheProcess(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Skipf("no hostname on this machine: %v", err)
	}

	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run a short lived process: %v", err)
	}
	holder := host + "/boot-one/" + strconv.Itoa(cmd.Process.Pid)

	if holderAlive(holder, "boot-one") {
		t.Errorf("holder %q has exited but reads as alive", holder)
	}
}
