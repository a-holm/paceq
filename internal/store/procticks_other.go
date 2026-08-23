//go:build !linux

package store

// ReadProcessStartTicks has no answer off Linux: there is no /proc and no
// kernel start time to read. ok is false for every pid, which makes every
// caller fail closed, exactly as an unreadable process does on Linux.
//
// The degradation is the documented one (issue #62, test plan item 10): the
// orphan sweep skips what it cannot verify, and lease expiry stays the only
// evidence of death.
func ReadProcessStartTicks(int) (int64, bool) { return 0, false }

// ProcessStartTicksReadable reports that this platform cannot verify process
// identities by start ticks.
func ProcessStartTicksReadable() bool { return false }
