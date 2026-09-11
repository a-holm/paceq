package serve

import (
	"bytes"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The property issue #232 exists to protect: a stop does not return while a
// job process it started is still running. The rows below read that off the
// machine, after the daemon's exit status has been collected, because that is
// the only moment at which the question has one answer.
//
// How long the stop took is deliberately not asserted anywhere here. A step
// that answers SIGTERM dies in milliseconds and the daemon is right to follow
// it out; a step that ignores SIGTERM costs whichever escalation reaches it
// first, and which one that is varies per stop. Worse, a clock cannot tell
// the two daemons apart: one that slept out a grace and then abandoned the
// group passes a duration floor, and the group it abandoned is exactly the
// defect. So the duration is logged as an observation and never as a claim.

// drainGrace is the term-to-kill gap the rows run the daemon with. It is
// short on purpose: the escalation is the same at any length and only the
// wall clock changes.
const drainGrace = time.Second

// drainBudget is what the graceful row tells the daemon running steps may
// have. Two graces is the shipped ratio an operator gets from
// `--drain-timeout 20s` at the default ten second grace.
const drainBudget = 2 * drainGrace

// spentBudget is a drain timeout a step that ignores SIGTERM cannot finish
// inside. Nothing but a SIGKILL ends that step, the nearest one is half a
// grace away, and this is a fifth of that: the drain reaches its deadline
// with the group still on the machine on every run of the row.
const spentBudget = drainGrace / 5

// drainStops is how many consecutive stops the graceful row makes. The race
// it samples lands differently on different stops, so the count is the
// instrument. PACEQ_DRAIN_STOPS raises it to the hundred the acceptance
// criterion asks for.
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

// TestAStopEndsTheStepGroupItsBudgetRanOutOn is the row that separates the two
// daemons on every run rather than on some of them.
//
// The drain timeout is a fifth of the soonest kill that can reach this step,
// so the budget always expires with the step's process group still on the
// machine. A daemon that treats the budget as the end of its obligation exits
// there, reports the stop clean, and leaves the group running. A daemon that
// treats the budget as a bound on how long the work may have goes on to end
// the group, and only then exits.
//
// That is the whole of #232 in one stop: the budget bounds the waiting, never
// the ending.
func TestAStopEndsTheStepGroupItsBudgetRanOutOn(t *testing.T) {
	ws := newWorkspace(t)
	runID := seedQueuedRun(t, ws, stepCommand(t), "ignore-term", "120s")
	defer killRunProcesses(runID)

	p := startServe(t, ws,
		"--kill-grace", drainGrace.String(),
		"--drain-timeout", spentBudget.String())
	p.waitReady(t)
	waitForChildRunning(t, ws, p, runID)

	p.signal(t, "only", syscall.SIGTERM)
	code := p.waitExit(t, 60*time.Second)

	if left := survivingRunProcesses(runID); len(left) > 0 {
		t.Errorf("the daemon exited %d with %d job processes of its own still running: %s\n"+
			"the drain timeout bounds how long a step may finish in, not whether its "+
			"process group is ended\ndaemon stderr:\n%s",
			code, len(left), strings.Join(left, ", "), p.stderrSnapshot())
	}
	if code != 0 {
		t.Errorf("the stop exited %d, want 0: it ended every group it answered for\nstderr:\n%s",
			code, p.stderrSnapshot())
	}
}

// TestNoGracefulStopLeavesAJobProcessRunning makes the same claim about the
// ordinary stop, the one with a drain budget an operator would actually
// configure, and makes it repeatedly.
//
// Here the step's fate is a race. The daemon's escalation kills the shim at
// the grace; the shim's own escalation kills the job's group at the same
// moment, and whichever lands first decides whether the job is ended or
// orphaned by the death of the process that was about to end it. A single
// stop therefore says nothing, and the row is the distribution.
func TestNoGracefulStopLeavesAJobProcessRunning(t *testing.T) {
	stops := drainStops(t)
	took := make([]time.Duration, 0, stops)
	left := 0

	for i := 0; i < stops; i++ {
		d, survivors := oneGracefulStop(t, i)
		took = append(took, d)
		if len(survivors) > 0 {
			left++
			t.Errorf("stop %d returned with %d job processes still running: %s",
				i, len(survivors), strings.Join(survivors, ", "))
		}
	}

	sorted := append([]time.Duration(nil), took...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
	// Logged, never asserted. The spread is what a reader needs to see that
	// both sides of the race were sampled; neither end of it is a fault.
	t.Logf("%d graceful stops at a %s grace: min %s, median %s, max %s; %d left a job process running",
		len(sorted), drainGrace, sorted[0], sorted[len(sorted)/2], sorted[len(sorted)-1], left)
}

// oneGracefulStop runs one daemon over one ignore-term step, stops it once,
// and reports how long the stop took measured from the daemon's own
// drain-start line, plus every process still carrying the run id once the
// daemon's exit status has been collected. It leaves nothing running either
// way: a survivor is reported and then killed, so a failing row cannot litter
// the machine.
func oneGracefulStop(t *testing.T, i int) (time.Duration, []string) {
	t.Helper()
	ws := newWorkspace(t)
	runID := seedQueuedRun(t, ws, stepCommand(t), "ignore-term", "120s")
	defer killRunProcesses(runID)

	p := startServe(t, ws,
		"--kill-grace", drainGrace.String(),
		"--drain-timeout", drainBudget.String())
	p.waitReady(t)
	waitForChildRunning(t, ws, p, runID)

	p.signal(t, "only", syscall.SIGTERM)
	// The reading starts at the daemon's own announcement that phase two
	// has begun. Observing it costs a moment, so the number is never
	// longer than the drain really was.
	p.waitForDrainStart(t)
	started := time.Now()
	code := p.waitExit(t, 30*time.Second)
	survivors := survivingRunProcesses(runID)
	if code != 0 && len(survivors) == 0 {
		t.Fatalf("stop %d exited %d having ended every group, want 0\nstderr:\n%s",
			i, code, p.stderrSnapshot())
	}
	return time.Since(started), survivors
}

// survivingRunProcesses names every live process carrying the run id, by pid
// and by the binary it is running, so a failing row says what it found rather
// than only that it found something.
//
// It is read once, with no settling wait. A daemon that ended its groups saw
// them leave /proc before it exited, so there is nothing left to wait for
// here; a wait would only give a daemon that did not end them time to look as
// if it had.
func survivingRunProcesses(runID string) []string {
	var out []string
	forEachRunProcess(runID, func(pid int, dir string) {
		argv0 := "unknown"
		if raw, err := os.ReadFile(dir + "/cmdline"); err == nil {
			if first, _, _ := bytes.Cut(raw, []byte{0}); len(first) > 0 {
				argv0 = string(first)
			}
		}
		out = append(out, "pid "+strconv.Itoa(pid)+" ("+argv0+")")
	})
	sort.Strings(out)
	return out
}

// killRunProcesses ends anything still carrying the run id, by pid. It is the
// row's own cleanup, never part of what it measures: the product has already
// been judged by the time it runs.
func killRunProcesses(runID string) {
	forEachRunProcess(runID, func(pid int, _ string) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	})
}

// forEachRunProcess walks /proc and calls visit for every live process whose
// environment names the run id. A process that leaves between the listing and
// the read is skipped: it is not running, which is the only thing any caller
// here asks about.
func forEachRunProcess(runID string, visit func(pid int, dir string)) {
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
		dir := "/proc/" + entry.Name()
		raw, err := os.ReadFile(dir + "/environ")
		if err != nil || !bytes.Contains(raw, marker) {
			continue
		}
		visit(pid, dir)
	}
}
