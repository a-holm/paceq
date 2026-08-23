//go:build linux

package reconcile

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/a-holm/paceq/internal/store"
)

// runIDEnv is the variable the runner exports into every job process
// (internal/runner/env.go). Its presence is the first, weakest hint that a
// process is ours; the baseline and start ticks do the real proving.
const runIDEnv = "PACEQ_RUN_ID="

// defaultScan walks /proc and returns every live process whose environment
// carries PACEQ_RUN_ID. Anything it cannot read - a process that died mid
// walk, one owned by another user whose environ is closed to us - is skipped
// without ceremony: the sweep fails closed on unreadable processes rather
// than guessing about them.
var defaultScan = scanProcfs

func scanProcfs() ([]Process, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}

	var out []Process
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // not a process entry
		}
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
		if err != nil {
			continue // died already, or not ours to read: skip, never guess
		}
		runID, ok := runIDFromEnviron(raw)
		if !ok {
			continue // no marker: outside this sweep's business entirely
		}
		p := Process{PID: pid, RunID: runID}
		p.StartTicks, p.TicksOK = store.ReadProcessStartTicks(pid)
		if pgid, err := os.FindProcess(pid); err == nil {
			// FindProcess only succeeds; the group read can still fail for
			// a process that just exited.
			if g, err := getpgid(pgid); err == nil {
				p.PGID = g
			}
		}
		out = append(out, p)
	}
	return out, nil
}

// runIDFromEnviron picks PACEQ_RUN_ID out of a NUL separated /proc environ
// block.
func runIDFromEnviron(raw []byte) (string, bool) {
	for _, kv := range strings.Split(string(raw), "\x00") {
		if after, ok := strings.CutPrefix(kv, runIDEnv); ok && after != "" {
			return after, true
		}
	}
	return "", false
}
