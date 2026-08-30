package serve

import (
	"bytes"
	"os"
	"sort"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// drainGrace is the term-to-kill gap the loop below runs the daemon with. It
// is short on purpose: the escalation it must exercise is the same at any
// length, and the row makes its claim a hundred times over. A step that
// ignores SIGTERM still costs the shim its own grace and the daemon the
// grace it owes the child group afterwards, so the floor holds whatever the
// number is; only the wall clock changes.
const drainGrace = time.Second

// drainBudget is what the daemon is told running steps may have. Two graces
// is the shipped ratio an operator gets from `--drain-timeout 20s` at the
// default ten second grace, and it is the ratio that matters: an ignore-term
// step costs the shim one grace and the daemon another, so the escalation the
// daemon owes the group does not fit inside the budget for the work. What the
// budget bounds is how long a step may finish in. Ending the group afterwards
// is not optional and has no budget to run out of.
const drainBudget = 2 * drainGrace

// drainStops is how many consecutive stops the loop makes. What it catches
// happens on some stops and not others, so a single stop proves nothing and
// the count is the instrument. The default keeps the row inside a normal test
// run; PACEQ_DRAIN_STOPS raises it to the hundred the acceptance criterion
// asks for.
func drainStops(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("PACEQ_DRAIN_STOPS")
	if raw == "" {
		return 10
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("PACEQ_DRAIN_STOPS=%q is not a positive count", raw)
	}
	return n
}

// TestEveryGracefulStopWaitsForTheProcessGroup states what a graceful stop of
// a step that ignores SIGTERM owes the machine, in two parts.
//
// The floor: the daemon cannot be finished before it has spent a term-to-kill
// grace, because the group cannot be gone before then. A stop that returns
// faster than the grace stopped waiting for something it still owned.
//
// The survivor: after the daemon has exited, nothing carrying the run id may
// still be running. This is the half that bites. The stop kills its own shim
// once the grace is up, which orphans the job process the shim was about to
// kill, and the escalation the daemon then owes that group is the second
// grace - the one that does not fit inside a drain budget of two.
//
// Both claims are only worth making repeatedly: which of the two racing kills
// lands first varies per stop, so a single stop says nothing and the
// distribution is what carries the evidence. It is logged either way.
func TestEveryGracefulStopWaitsForTheProcessGroup(t *testing.T) {
	stops := drainStops(t)
	took := make([]time.Duration, 0, stops)
	fast, orphaned := 0, 0

	for i := 0; i < stops; i++ {
		d, orphan := oneGracefulStop(t, i)
		took = append(took, d)
		if d < drainGrace {
			fast++
			t.Errorf("stop %d finished %s after the drain began, faster than the %s grace "+
				"the group is owed: the daemon stopped waiting for a process it still owned",
				i, d, drainGrace)
		}
		if orphan {
			orphaned++
			t.Errorf("stop %d left a process carrying the run id alive after the daemon exited", i)
		}
	}

	sorted := append([]time.Duration(nil), took...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
	t.Logf("%d graceful stops at a %s grace: min %s, median %s, max %s; %d under the grace, %d orphaned",
		len(sorted), drainGrace, sorted[0], sorted[len(sorted)/2], sorted[len(sorted)-1], fast, orphaned)
}

// oneGracefulStop runs one daemon over one ignore-term step, stops it once,
// and reports how long the stop took measured from the daemon's own
// drain-start line, plus whether anything carrying the run id outlived the
// daemon. It leaves nothing running either way: a survivor is reported and
// then killed, so a failing row cannot litter the machine.
func oneGracefulStop(t *testing.T, i int) (time.Duration, bool) {
	t.Helper()
	ws := newWorkspace(t)
	runID := seedQueuedRun(t, ws, stepCommand(t), "ignore-term", "120s")
	defer killRunProcesses(runID)

	p := startServe(t, ws, "--kill-grace", drainGrace.String(), "--drain-timeout", drainBudget.String())
	p.waitReady(t)
	waitForChildRunning(t, ws, p, runID)

	p.signal(t, "only", syscall.SIGTERM)
	// The measurement starts where the issue's measurement started: the
	// daemon's own announcement that phase two has begun. Observing it
	// costs a moment, so the reading is never longer than the drain
	// really was.
	p.waitForDrainStart(t)
	started := time.Now()
	code := p.waitExit(t, 30*time.Second)
	took := time.Since(started)
	if code != 0 {
		t.Fatalf("stop %d exited %d, want 0\nstderr:\n%s", i, code, p.stderrSnapshot())
	}
	return took, waitForNoRunProcess(runID, 2*time.Second)
}

// waitForNoRunProcess reports whether anything carrying the run id is still
// alive after a short settling wait. /proc can lag a reaped process by a
// moment; it may not lag it forever.
func waitForNoRunProcess(runID string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for procCarriesRunID(runID) {
		if time.Now().After(deadline) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// killRunProcesses ends anything still carrying the run id, by pid. It is the
// loop's own cleanup, never part of what it measures: the product has already
// been judged by the time it runs.
func killRunProcesses(runID string) {
	marker := []byte("PACEQ_RUN_ID=" + runID)
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		raw, err := os.ReadFile("/proc/" + entry.Name() + "/environ")
		if err != nil || !bytes.Contains(raw, marker) {
			continue
		}
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}
