package runner

import (
	"fmt"
	"syscall"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/procfs"
)

// pollTick is how often the verified kill re-reads the group while its grace
// runs. A group that took the TERM and died ends the wait early; a group
// that ignores TERM gets the whole grace and then the KILL. The tick is a
// polling bound, not a timing assertion, so no outcome depends on it.
const pollTick = 20 * time.Millisecond

// VerifiedGroupKill is the pid-reuse guard every signal this package sends
// through a shim must pass (issue #39, design choice 4). A pid that has been
// recycled carries a different /proc start time than the one recorded when
// the process was spawned; signalling it would kill a stranger's work. A pid
// that is gone has no start time at all. Both refuse.
//
// The ticks value comes from the same reader the baseline recorder used, so
// the comparison is always against the kernel's own arithmetic.
func VerifiedGroupKill(pgid int, wantTicks int64, grace time.Duration, clk clock.Clock) error {
	if pgid <= 0 {
		return fmt.Errorf("verified kill: no process group named")
	}
	if clk == nil {
		clk = clock.System()
	}
	if grace <= 0 {
		grace = DefaultKillGrace
	}
	got, ok := procfs.ProcStartTicks(pgid)
	if !ok || got != wantTicks {
		return fmt.Errorf("verified kill: group %d is not the process recorded (ticks %d): signal refused",
			pgid, got)
	}
	_ = killProcessGroup(-pgid, syscall.SIGTERM)

	// Hold the grace, but only as long as the group is actually there: a
	// job that took the TERM ends this wait in one poll tick, which is
	// what keeps a shim's own exit prompt after its child's death.
	deadline := clk.Now().Add(grace)
	for {
		if ticks, ok := procfs.ProcStartTicks(pgid); !ok || ticks != wantTicks {
			return nil // the group is gone; the KILL would only hit a stranger
		}
		if !clk.Now().Before(deadline) {
			break
		}
		timer := clk.NewTimer(pollTick)
		<-timer.C
		timer.Stop()
	}
	if again, ok := procfs.ProcStartTicks(pgid); ok && again == wantTicks {
		_ = killProcessGroup(-pgid, syscall.SIGKILL)
	}
	return nil
}
