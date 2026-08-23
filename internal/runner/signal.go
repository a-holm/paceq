package runner

import (
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// groupKiller is what an escalation uses to deliver signals.
type groupKiller = func(pgid int, sig syscall.Signal) error

// killProcessGroup is the real delivery. The tests replace it through
// captureGroupKill, and escalations can carry their own fake through
// newEscalation.
var killProcessGroup groupKiller = func(pgid int, sig syscall.Signal) error {
	return syscall.Kill(pgid, sig)
}

// terminateGroup signals a whole process group. The negative pid is not a
// stylistic choice: signalling the bare pid would leave grandchildren running
// as orphans holding files and ports, which is exactly the leak Setpgid
// exists to prevent.
func terminateGroup(pid int, sig syscall.Signal) error {
	return killProcessGroup(-pid, sig)
}

// captureGroupKill replaces the killer seam for one test and restores it.
func captureGroupKill(fake func(pgid int, sig syscall.Signal) error) (restore func()) {
	old := killProcessGroup
	killProcessGroup = fake
	return func() { killProcessGroup = old }
}

// escalation owns the SIGTERM then SIGKILL sequence against one process
// group. fire is idempotent: whichever trigger arrives first, a deadline from
// the clock timer or a cancelled parent context through exec's Cancel hook,
// starts the sequence; nothing can start it twice.
type escalation struct {
	grace time.Duration
	clk   clock.Clock
	kill  groupKiller

	once     sync.Once
	group    atomic.Int64
	fired    chan struct{}
	stopOnce sync.Once
	done     chan struct{}
	exited   chan struct{}
}

// newEscalation prepares the sequence. A nil killer means the real
// killProcessGroup; setGroup must be called after Start, because the group id
// is the child's pid and only exists then.
func newEscalation(kill groupKiller, grace time.Duration, clk clock.Clock) *escalation {
	if kill == nil {
		kill = killProcessGroup
	}
	return &escalation{
		grace:  grace,
		clk:    clk,
		kill:   kill,
		fired:  make(chan struct{}),
		done:   make(chan struct{}),
		exited: make(chan struct{}),
	}
}

// Escalator is the public face of the SIGTERM then SIGKILL sequence against
// one process group. The sensor evaluator builds on it directly instead of
// maintaining a second implementation of the grace escalation; the daemon's
// hard stop reaches its groups because Start registers them through
// RegisterProcessGroup.
type Escalator struct{ e *escalation }

// NewEscalator prepares the sequence against one future process group. grace
// is the tolerated silence between SIGTERM and SIGKILL.
func NewEscalator(grace time.Duration, clk clock.Clock) *Escalator {
	return &Escalator{e: newEscalation(nil, grace, clk)}
}

// Fire sends SIGTERM to the whole group now and arms SIGKILL for it once the
// grace elapses. It is idempotent and returns the error exec's Cancel hook
// expects.
func (x *Escalator) Fire() error { return x.e.fire() }

// SetGroup records which process group the sequence targets. Call it after
// Start, once the child pid exists.
func (x *Escalator) SetGroup(pgid int) { x.e.setGroup(pgid) }

// Stop disarms a pending SIGKILL and waits until the escalation goroutine has
// returned, so a leak probe after a finished attempt finds nothing left
// behind.
func (x *Escalator) Stop() { x.e.stop() }

// setGroup records which process group the sequence targets.
func (e *escalation) setGroup(pgid int) { e.group.Store(int64(pgid)) }

// fire sends SIGTERM to the whole group now and arms SIGKILL for the whole
// group once the grace elapses. The error return exists to satisfy exec's
// Cancel signature; a failed delivery is not an error state here, ESRCH just
// means the group is already gone. It never blocks longer than a signal call.
func (e *escalation) fire() error {
	e.once.Do(func() {
		close(e.fired)
		pgid := int(e.group.Load())
		if pgid == 0 {
			close(e.exited)
			return
		}
		// A delivery failure is deliberately ignored here: ESRCH means the
		// group is already gone, which is the outcome we want anyway.
		_ = e.kill(-pgid, syscall.SIGTERM)
		go e.escalate(pgid)
	})
	return nil
}

// escalate waits out the grace period, then kills the group. The done channel
// ends it early when Wait has already reaped the leader and the runner is
// finished with this attempt.
func (e *escalation) escalate(pgid int) {
	defer close(e.exited)
	timer := e.clk.NewTimer(e.grace)
	defer timer.Stop()
	select {
	case <-timer.C:
		_ = e.kill(-pgid, syscall.SIGKILL)
	case <-e.done:
	}
}

// stop disarms the escalation and waits until any goroutine it started has
// returned. After stop, Run owns nothing: no timer goroutine, no pending
// kill, nothing for a leak counter to find.
func (e *escalation) stop() {
	e.stopOnce.Do(func() { close(e.done) })
	select {
	case <-e.fired:
		<-e.exited
	default:
	}
}

// sigName gives a signal its canonical POSIX spelling. syscall's own String
// says "killed" where an operator reading a log expects "SIGKILL".
func sigName(sig syscall.Signal) string {
	if name, ok := signalNames[sig]; ok {
		return name
	}
	return fmt.Sprintf("SIG%d", int(sig))
}

var signalNames = map[syscall.Signal]string{
	syscall.SIGHUP:  "SIGHUP",
	syscall.SIGINT:  "SIGINT",
	syscall.SIGQUIT: "SIGQUIT",
	syscall.SIGABRT: "SIGABRT",
	syscall.SIGKILL: "SIGKILL",
	syscall.SIGSEGV: "SIGSEGV",
	syscall.SIGTERM: "SIGTERM",
	syscall.SIGUSR1: "SIGUSR1",
	syscall.SIGUSR2: "SIGUSR2",
}
