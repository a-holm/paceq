//go:build !unix

package notify

import (
	"context"
	"os/exec"
)

// setOwnProcessGroup has no equivalent on this platform; the child runs in
// the caller's group and the escalation below degrades to killing the
// process itself.
func setOwnProcessGroup(cmd *exec.Cmd) {}

func watchForTimeout(cmd *exec.Cmd, ctx context.Context) (stop func()) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		case <-done:
		}
	}()
	return func() { close(done) }
}
