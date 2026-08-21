//go:build unix

package store

import (
	"bufio"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	// stateLockEnv carries the state directory to the child process. Its
	// presence is what turns the helper test into the process that holds the
	// lock and waits to be killed.
	stateLockEnv = "PACEQ_STATE_LOCK_DIR"

	// lockHeldLine is what the child prints once the lock is really taken. The
	// parent waits for it instead of guessing how long a test binary needs to
	// start.
	lockHeldLine = "state-lock-held"
)

// retakeBudget is how long the second process may take to get the lock after
// the first one is killed. The kernel drops an flock when the process dies, so
// anything slower means paceq has grown pid file semantics: a stale lock that
// needs cleaning up.
const retakeBudget = 100 * time.Millisecond

// TestStateLockAcrossProcesses is the acceptance test for the guarantee: a
// second real process cannot take the lock, and the moment the first one is
// killed the lock is free again with nothing to clean up.
func TestStateLockAcrossProcesses(t *testing.T) {
	if os.Getenv(stateLockEnv) != "" {
		t.Skip("child process, driven by the parent test")
	}

	dir := stateDir(t)
	cmd := exec.Command(os.Args[0], "-test.run=TestStateLockChild", "-test.count=1")
	cmd.Env = append(os.Environ(), stateLockEnv+"="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("pipe the child's output: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the child: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	waitForLine(t, out, lockHeldLine)

	lock, err := AcquireStateLock(dir)
	if err == nil {
		_ = lock.Release()
		t.Fatal("this process took the state lock while another process held it")
	}
	var locked *LockedError
	if !errors.As(err, &locked) {
		t.Fatalf("error %v is not a *LockedError", err)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill the child: %v", err)
	}
	_ = cmd.Wait()

	start := time.Now()
	lock, err = AcquireStateLock(dir)
	if err != nil {
		t.Fatalf("take the state lock after the holder was killed: %v", err)
	}
	defer func() { _ = lock.Release() }()
	if elapsed := time.Since(start); elapsed > retakeBudget {
		t.Errorf("taking the lock after a kill took %s, want under %s", elapsed, retakeBudget)
	}
}

// TestStateLockChild is the process the test above kills. It takes the lock,
// says so and then waits to be killed, which is the only way to prove that the
// kernel and not paceq is what releases the lock.
func TestStateLockChild(t *testing.T) {
	dir := os.Getenv(stateLockEnv)
	if dir == "" {
		t.Skip("driven by TestStateLockAcrossProcesses")
	}

	if _, err := AcquireStateLock(dir); err != nil {
		t.Fatalf("child could not take the state lock: %v", err)
	}
	if _, err := os.Stdout.WriteString(lockHeldLine + "\n"); err != nil {
		t.Fatalf("announce the lock: %v", err)
	}
	select {}
}

// waitForLine reads the child's output until the expected line appears.
func waitForLine(t *testing.T, r io.Reader, want string) {
	t.Helper()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == want {
			return
		}
	}
	t.Fatalf("the child never printed %q: %v", want, scanner.Err())
}
