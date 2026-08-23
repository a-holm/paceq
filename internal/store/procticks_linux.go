//go:build linux

package store

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ReadProcessStartTicks returns field 22 of /proc/<pid>/stat: the kernel's
// record of when the process started, in clock ticks since boot. Together
// with the pid it forms the process identity the orphan sweep verifies before
// any signal (issue #62): a pid alone is recyclable, pid plus start ticks is
// not.
//
// ok is false when the process does not exist any more, has already been
// reaped, or cannot be read by this user. Every caller fails closed on
// false: an unreadable process is nobody's to kill.
func ReadProcessStartTicks(pid int) (int64, bool) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	// The second field, comm, sits in parentheses and may contain spaces and
	// further parentheses of its own, so the parse resumes after its last
	// closing bracket rather than splitting naively.
	close := bytes.LastIndexByte(raw, ')')
	if close < 0 {
		return 0, false
	}
	fields := strings.Fields(string(raw[close+1:]))
	// What follows starts at field 3 (state), so field 22 is index 19 here.
	if len(fields) < 20 {
		return 0, false
	}
	ticks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0, false
	}
	return ticks, true
}

// ProcessStartTicksReadable reports whether this platform can read start
// ticks at all. It is how the sweep decides between verifying and failing
// closed without pretending.
func ProcessStartTicksReadable() bool {
	return true
}
