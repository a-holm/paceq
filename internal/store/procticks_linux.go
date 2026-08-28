//go:build linux

package store

import "github.com/a-holm/paceq/internal/procfs"

// ReadProcessStartTicks returns field 22 of /proc/<pid>/stat: the kernel's
// record of when the process started, in clock ticks since boot. Together
// with the pid it forms the process identity the orphan sweep verifies before
// any signal (issue #62): a pid alone is recyclable, pid plus start ticks is
// not.
//
// The parse itself lives in internal/procfs, the one reader of the kernel's
// process facts, shared with the exec shim's kill verification (#39) so the
// two can never disagree about what a /proc line says.
//
// ok is false when the process does not exist any more, has already been
// reaped, or cannot be read by this user. Every caller fails closed on
// false: an unreadable process is nobody's to kill.
func ReadProcessStartTicks(pid int) (int64, bool) {
	return procfs.ProcStartTicks(pid)
}

// ProcessStartTicksReadable reports whether this platform can read start
// ticks at all. It is how the sweep decides between verifying and failing
// closed without pretending.
func ProcessStartTicksReadable() bool {
	return true
}
