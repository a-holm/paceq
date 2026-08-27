//go:build unix

package notify

import (
	"context"
	"os/exec"
	"sync"
	"syscall"

	"github.com/a-holm/paceq/internal/clock"
)

// graceWindow is one polite second between SIGTERM and SIGKILL. It is a
// constant because it bounds a signal escalation, not user behaviour.
const graceWindow = 1e9 // nanoseconds, handed to the clock abstraction

// setOwnProcessGroup puts the child at the head of its own process group.
// This is the setting that makes every kill below a group kill, and it is not
// optional: without it, grandchildren survive their timeout and hold files
// and ports (runner's same rule, 05 section 6.5).
func setOwnProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// watchForTimeout escalates the context deadline to the child's whole process
// group. exec's own cancel only signals the leader; a script that ignores or
// resets SIGTERM would otherwise hold the dispatcher hostage for ever. The
// returned stop stops watching after a clean Wait.
func watchForTimeout(cmd *exec.Cmd, ctx context.Context) (stop func()) {
	pgid := -cmd.Process.Pid // The leader led its own group since Setpgid.
	done := make(chan struct{})
	var once sync.Once
	go func() {
		select {
		case <-ctx.Done():
			_ = syscall.Kill(pgid, syscall.SIGTERM)
			// One polite second through the shared clock abstraction,
			// then the hammer. The deadline already fired; nothing here
			// may block long.
			timer := clock.System().NewTimer(graceWindow)
			defer timer.Stop()
			select {
			case <-timer.C:
				_ = syscall.Kill(pgid, syscall.SIGKILL)
			case <-done:
			}
		case <-done:
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}
