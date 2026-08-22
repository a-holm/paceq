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

// coreLimitMu serialises the brief window in which this process runs with
// RLIMIT_CORE set to zero, because rlimits are per process, not per child,
// and two concurrent Starts would otherwise restore different values.
var coreLimitMu sync.Mutex

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
