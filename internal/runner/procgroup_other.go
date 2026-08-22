//go:build !unix

package runner

import "syscall"

// sysProcAttr on platforms without process groups: the runner still works,
// but kills can only address the direct child. The supported targets, linux
// and darwin, all take the unix path above.
func sysProcAttr() *syscall.SysProcAttr { return &syscall.SysProcAttr{} }

// zeroCoreLimit has no rlimit hook on this platform.
func zeroCoreLimit() (release func()) { return func() {} }

// registerGroup on platforms without process groups: there is no group to
// track, so the release is a no-op.
func registerGroup(pgid int) (release func()) { return func() {} }

// KillAllProcessGroups on platforms without process groups: nothing to reach.
func KillAllProcessGroups(sig syscall.Signal) {}
