package reconcile

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// The orphan process sweep. A crashed daemon's children survive it: each one
// led its own process group (the runner sets Setpgid), so they outlive their
// parent holding files, ports and the illusion that their run is still going.
//
// The sweep's whole discipline is about never killing an innocent process:
//
//   - It only ever considers a process whose environment carries
//     PACEQ_RUN_ID naming a run this database actually knows.
//   - That pid must sit in the persisted baseline of one of our attempts
//     (migration 0007). A pid we never started is nobody's to signal.
//   - /proc's start ticks must match the baseline exactly. This is AC6's
//     refusal: manipulated or stale baselines mean no signal, because the
//     only thing worse than an orphan is a recycled pid carrying someone
//     else's work.
//   - The start ticks are read twice, once when found and once immediately
//     before signalling, so a pid reused mid sweep cannot be shot through
//     the gap.
//   - Signals address the process GROUP (-pgid), never a bare child pid, so
//     grandchildren die with their leader instead of being re-parented into
//     new mischief. The sweep's own group is excluded outright.

// Process is one candidate the scanner reports: a live process whose environ
// carried PACEQ_RUN_ID. Everything else about it was read from /proc at scan
// time.
// ScanProcs is the exported probe doctor's orphan check uses (M6-06): every
// live process this user can read whose environment carries PACEQ_RUN_ID. It
// is the same scan the startup sweep walks, so doctor and reconciliation can
// never disagree about what a live process is. Off Linux it answers "nothing
// scannable" by being nil-backed, exactly like the startup sweep.
func ScanProcs() ([]Process, error) {
	if defaultScan == nil {
		return nil, nil
	}
	return defaultScan()
}

type Process struct {
	PID        int
	PGID       int
	RunID      string // the value of PACEQ_RUN_ID in its environment
	StartTicks int64  // field 22 of /proc/<pid>/stat, when TicksOK
	TicksOK    bool
}

// Signaller sends what the sweep has to send, addressed to groups.
type Signaller interface {
	// Term asks the group to exit. An error means the group is already gone
	// or was never ours to touch; both are fine to ignore.
	Term(pgid int) error

	// Kill does not ask. It is the escalation after the grace passes.
	Kill(pgid int)
}

// sweepProcesses runs the decision core over whatever the platform scanner
// found. Errors from the store are real failures; errors from individual
// processes were already resolved by skipping them inside the scanner.
func sweepProcesses(ctx context.Context, st *store.Store, opts *Options) error {
	scan := opts.ScanProcs
	if scan == nil {
		scan = defaultScan
	}
	if scan == nil {
		// Platform without a /proc equivalent: nothing to find, which is the
		// documented non-Linux degradation (test plan item 10).
		return nil
	}
	procs, err := scan()
	if err != nil {
		return fmt.Errorf("scan for orphaned processes: %w", err)
	}
	if len(procs) == 0 {
		return nil
	}

	knownRows, err := st.KnownAttempts(ctx)
	if err != nil {
		return fmt.Errorf("read the attempt baselines: %w", err)
	}
	activeRows, err := st.ActiveAttempts(ctx)
	if err != nil {
		return fmt.Errorf("read the active attempts: %w", err)
	}

	// runID -> pid -> the start ticks recorded when we spawned it.
	known := map[string]map[int]int64{}
	for _, k := range knownRows {
		if known[k.RunID] == nil {
			known[k.RunID] = map[int]int64{}
		}
		known[k.RunID][k.PID] = k.StartTicks
	}
	active := map[string]int64{}
	for _, a := range activeRows {
		active[attemptKey(a.RunID, a.PID)] = a.StartTicks
	}

	signals := opts.Signals
	if signals == nil {
		signals = defaultSignals()
	}
	reread := opts.tickReader()

	for _, p := range procs {
		if p.PID == opts.pid() || p.PGID == opts.pgid() {
			continue // never our own blood
		}
		baseline, ok := known[p.RunID][p.PID]
		if !ok {
			continue // not a pid any attempt of ours ever held
		}
		if !p.TicksOK || p.StartTicks != baseline {
			// AC6: the identity on file does not match the machine. Either
			// the baseline was tampered with or the pid was recycled; both
			// refuse the kill.
			opts.logger().Warn("refusing to signal a process whose start ticks do not match the baseline",
				"pid", p.PID, "run", p.RunID,
				"baseline_ticks", baseline, "seen_ticks", p.StartTicks)
			continue
		}
		if act, ok := active[attemptKey(p.RunID, p.PID)]; ok && act == p.StartTicks {
			continue // a legitimate worker of a running attempt
		}

		// Re-read right before shooting: if this pid died and was recycled
		// between the scan and now, the fresh read will disagree.
		again, ok := reread(p.PID)
		if !ok || again != p.StartTicks {
			opts.logger().Warn("a candidate orphan changed identity between reads; leaving it alone",
				"pid", p.PID, "run", p.RunID)
			continue
		}

		opts.logger().Warn("killing an orphaned process group",
			"pid", p.PID, "pgid", p.PGID, "run", p.RunID)
		_ = signals.Term(p.PGID)
		escalate(ctx, opts.clock(), signals, p.PGID, opts.grace())

		if err := st.RecordOrphanKill(ctx, p.RunID, p.PID); err != nil {
			// The signal went out regardless; say why the audit row did not
			// land rather than pretending the kill is fully explained.
			opts.logger().Warn("could not record the orphan kill event",
				"run", p.RunID, "pid", p.PID, "error", err)
		}
	}
	return nil
}

func attemptKey(runID string, pid int) string {
	return fmt.Sprintf("%s/%d", runID, pid)
}

// escalate turns SIGTERM into SIGKILL after the grace. The timer belongs to
// the caller's clock, so tests decide when (and whether) escalation fires;
// a cancelled context stands down without sending anything.
func escalate(ctx context.Context, clk clock.Clock, s Signaller, pgid int, grace time.Duration) {
	timer := clk.NewTimer(grace)
	go func() {
		defer timer.Stop()
		select {
		case <-timer.C:
			s.Kill(pgid)
		case <-ctx.Done():
		}
	}()
}

// ownPID and ownGroup name the sweeping process itself. ownGroup is
// per-platform because process groups are a unix idea.
func ownPID() int { return os.Getpid() }
