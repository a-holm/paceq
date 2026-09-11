package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reconcile"
)

// The drain's evidence (issue #232). An executor goroutine that has returned
// says one thing only: that goroutine is gone. The step's process group is a
// different object, in a different process, with a lifetime this process can
// read but can never infer - the shim the executor waits on leads its own
// group, and the job's command leads another. Every wait the drain used to
// make was on the near side of that boundary.
//
// So the last thing phase two does is look. The look is the one
// internal/reconcile's orphan sweep already makes and the serve harness's
// requireNoOrphan repeats: walk /proc and pick out the processes whose
// environment carries a run id. Here the predicate is narrower than the
// sweep's and stronger for it. These are the runs this process was executing
// when the drain began, so a live process carrying one of them is this
// daemon's work by construction, and needs no baseline to prove it.

// ErrStepProcessSurvived is what Serve reports when the drain ended with a
// job process still running. It is deliberately not a clean stop: an operator
// told the stop succeeded is an operator who does not go looking.
var ErrStepProcessSurvived = errors.New("a step's process group survived the stop")

const (
	// reapPoll is how often the drain re-reads /proc while it waits for a
	// group to go. Each read walks every process on the machine, so the
	// interval is generous rather than tight: it bounds a poll, never an
	// outcome, and it is only reached when something actually survived.
	reapPoll = 100 * time.Millisecond

	// reapSettle is how long the drain waits after SIGKILL for the kernel
	// and the process's parent to finish with it. A killed process leaves
	// /proc as soon as it is reaped; this is the bound on "as soon as".
	reapSettle = 2 * time.Second
)

// stepProcess is one live process carrying a run the drain answers for.
type stepProcess struct {
	PID   int
	PGID  int
	RunID string
}

func (p stepProcess) String() string {
	return fmt.Sprintf("pid %d (pgid %d, run %s)", p.PID, p.PGID, p.RunID)
}

// scanRunProcesses is the seam over the /proc walk. Production reads the
// machine; a unit test hands over a census of its own.
var scanRunProcesses = reconcile.ScanProcs

// signalGroup is the seam over delivery. The negative pid is the whole point:
// a step's grandchildren die with their leader instead of being re-parented
// into new mischief.
var signalGroup = killProcessGroup

// reapRunGroups ends every process group still carrying one of runs and
// reports what it could not end. It is the drain's own escalation, not a
// second copy of the runner's: by the time it runs, the executor that owned
// the group has already returned, so there is nobody left to ask politely on
// the daemon's behalf.
//
// The sequence is the one every kill in this codebase makes. SIGTERM to each
// group, the grace to answer it, SIGKILL to whatever is left, and a settling
// wait so the answer is "/proc is quiet" rather than "the signal was sent".
func reapRunGroups(ctx context.Context, clk clock.Clock, log *slog.Logger, runs []string, grace time.Duration) []stepProcess {
	wanted := make(map[string]bool, len(runs))
	for _, id := range runs {
		if id != "" {
			wanted[id] = true
		}
	}
	if len(wanted) == 0 {
		return nil
	}

	live := liveRunProcesses(wanted)
	if len(live) == 0 {
		return nil
	}
	for _, p := range live {
		log.Warn("a step's process group outlived its executor; the drain is ending it",
			"pid", p.PID, "pgid", p.PGID, "run", p.RunID)
		signalStepGroup(p, syscall.SIGTERM)
	}

	if live = waitGroupsGone(ctx, clk, wanted, grace); len(live) == 0 {
		log.Info("the drain ended the process groups its executors left behind")
		return nil
	}
	for _, p := range live {
		log.Warn("a step's process group ignored the drain's SIGTERM; killing it",
			"pid", p.PID, "pgid", p.PGID, "run", p.RunID, "grace", grace.String())
		signalStepGroup(p, syscall.SIGKILL)
	}
	if left := waitGroupsGone(ctx, clk, wanted, reapSettle); len(left) > 0 {
		return left
	}
	log.Info("the drain ended the process groups its executors left behind")
	return nil
}

// waitGroupsGone re-reads /proc until nothing carries one of the wanted run
// ids, or until the budget runs out. It answers with what is still there.
func waitGroupsGone(ctx context.Context, clk clock.Clock, wanted map[string]bool, within time.Duration) []stepProcess {
	deadline := clk.Now().Add(within)
	for {
		live := liveRunProcesses(wanted)
		if len(live) == 0 {
			return nil
		}
		if !clk.Now().Before(deadline) {
			return live
		}
		timer := clk.NewTimer(reapPoll)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return live
		}
		timer.Stop()
	}
}

// liveRunProcesses is the census: every live process whose environment names
// one of the runs this drain answers for. This process and its own group are
// excluded outright, the one group no code here may ever signal.
func liveRunProcesses(wanted map[string]bool) []stepProcess {
	if scanRunProcesses == nil {
		return nil
	}
	procs, err := scanRunProcesses()
	if err != nil {
		return nil
	}
	self, group := os.Getpid(), ownProcessGroup()
	var out []stepProcess
	for _, p := range procs {
		if !wanted[p.RunID] || p.PID == self {
			continue
		}
		if p.PGID != 0 && p.PGID == group {
			continue
		}
		out = append(out, stepProcess{PID: p.PID, PGID: p.PGID, RunID: p.RunID})
	}
	return out
}

// signalStepGroup delivers to the group, and only to a group worth naming. A
// process whose group could not be read (pgid 0) or whose group is init's is
// left alone: an unaddressable target is a reason to report, never a reason
// to guess.
func signalStepGroup(p stepProcess, sig syscall.Signal) {
	if p.PGID <= 1 {
		return
	}
	_ = signalGroup(p.PGID, sig)
}

// describeStepProcesses renders the survivors for one log line.
func describeStepProcesses(procs []stepProcess) string {
	parts := make([]string, 0, len(procs))
	for _, p := range procs {
		parts = append(parts, p.String())
	}
	return strings.Join(parts, ", ")
}
