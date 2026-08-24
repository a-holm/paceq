//go:build unix

package runner

import (
	"sync"
	"syscall"
)

// sysProcAttr puts the child in its own process group. This is the setting
// that makes every kill in this package a group kill, and it is not optional:
// without it the grandchildren survive and hold files and ports.
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// SysProcAttr is the public form of sysProcAttr. The sensor evaluator uses it
// so its subprocess gets the same process group guarantee as a step: the child
// leads its own group, and every escalation on it reaches the whole group.
func SysProcAttr() *syscall.SysProcAttr { return sysProcAttr() }

// coreLimitMu serialises the brief window in which this process runs with
// RLIMIT_CORE set to zero, because rlimits are per process, not per child,
// and two concurrent Starts would otherwise restore different values.
var coreLimitMu sync.Mutex

// activeGroups holds the group id of every child this process started and has
// not reaped yet. It is what a hard stop needs: the second signal cannot walk
// runs and engines, it has to reach every group at once through one call.
var activeGroups sync.Map // pgid int -> struct{}

// registerGroup records a live process group and returns the function that
// takes it back off. Run registers as soon as the child exists and releases
// after Wait, so the registry never names a group whose leader is reaped.
func registerGroup(pgid int) (release func()) {
	activeGroups.Store(pgid, struct{}{})
	var once sync.Once
	return func() {
		once.Do(func() { activeGroups.Delete(pgid) })
	}
}

// RegisterProcessGroup is the public form of registerGroup. The sensor
// evaluator calls it so a daemon hard stop (the second signal, which sweeps
// every group through KillAllProcessGroups) reaches a sensor subprocess with
// the same certainty it reaches a step.
func RegisterProcessGroup(pgid int) func() { return registerGroup(pgid) }

// KillAllProcessGroups signals every registered group. Delivery failures are
// ignored on purpose: ESRCH only means the group is already gone, which is
// the outcome a hard stop wants anyway. It is called from the second-signal
// path immediately before exit, so nothing here may block.
func KillAllProcessGroups(sig syscall.Signal) {
	activeGroups.Range(func(key, _ any) bool {
		pgid, ok := key.(int)
		if ok {
			_ = killProcessGroup(-pgid, sig)
		}
		return true
	})
}

// zeroCoreLimit drops RLIMIT_CORE to zero around the fork and exec, so a
// crashing job cannot write a core file full of daemon secrets. Rlimits are
// inherited across exec, which is what makes the window work: the child
// snapshots its limits at fork, so restoring after Start cannot affect it.
//
// The restore is best effort. If the platform refuses the change the job
// still runs; the guarantee returns with the hardening milestone that adds a
// proper shim. The mutex bounds the exposure for concurrent runs.
func zeroCoreLimit() (release func()) {
	coreLimitMu.Lock()
	var old syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_CORE, &old); err != nil {
		coreLimitMu.Unlock()
		return func() {}
	}
	zero := old
	zero.Cur = 0
	if err := syscall.Setrlimit(syscall.RLIMIT_CORE, &zero); err != nil {
		coreLimitMu.Unlock()
		return func() {}
	}
	return func() {
		_ = syscall.Setrlimit(syscall.RLIMIT_CORE, &old)
		coreLimitMu.Unlock()
	}
}
