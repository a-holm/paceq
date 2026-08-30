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
type Process struct {
	PID        int
	PGID       int
	RunID      string // the value of PACEQ_RUN_ID in its environment
	StartTicks int64  // field 22 of /proc/<pid>/stat, when TicksOK
	TicksOK    bool
}

// ScanProcs is the exported probe doctor's orphan check uses (M6-06): every
// live process this user can read whose environment carries PACEQ_RUN_ID. The
// walk is machine wide, because a job process records nothing about which
// installation started it, so other installations' processes come back too.
// Ownership is decided after the scan, by Ownership over this database's
// attempt baselines. Doctor and the sweep walk separately, at their own
// moments, but they put what they find through the one predicate. Off Linux it answers "nothing scannable" by being nil-backed,
// exactly like the startup sweep.
func ScanProcs() ([]Process, error) {
	if defaultScan == nil {
		return nil, nil
	}
	return defaultScan()
}

// Claim is what this database's attempt baselines say about a scanned
// process: the sweep's refusal rules as an answer rather than as control
// flow, so a report can name what the sweep would consider. The sweep adds
// refusals of its own before it signals, so a claim bounds it rather than
// predicting it.
type Claim int

const (
	// ClaimForeign is a pid no attempt of ours ever held under this run.
	// Another installation on this machine started it.
	ClaimForeign Claim = iota
	// ClaimMismatch is a pid a baseline of ours holds whose /proc start
	// ticks disagree with it: the identity on file is not the identity on
	// the machine, so the process is not ours either.
	ClaimMismatch
	// ClaimRunning is a pid an active attempt names, start ticks and all.
	ClaimRunning
	// ClaimOrphan is ours, and no active attempt names it.
	ClaimOrphan
)

// Ownership answers whether this installation started a scanned process and
// whether anything of ours still owns it. The startup sweep and doctor's
// orphan check both classify through it, which is what keeps a report from
// advising a kill the sweep would refuse to make.
type Ownership struct {
	// runID -> pid -> the start ticks recorded when we spawned it.
	known  map[string]map[int]int64
	active map[string]int64
}

// NewOwnership builds the predicate from a store's KnownAttempts, every
// baseline ever recorded, and its ActiveAttempts, the baselines whose run and
// step are still running.
func NewOwnership(known, active []store.AttemptProcess) *Ownership {
	o := &Ownership{known: map[string]map[int]int64{}, active: map[string]int64{}}
	for _, k := range known {
		if o.known[k.RunID] == nil {
			o.known[k.RunID] = map[int]int64{}
		}
		o.known[k.RunID][k.PID] = k.StartTicks
	}
	for _, a := range active {
		o.active[attemptKey(a.RunID, a.PID)] = a.StartTicks
	}
	return o
}

// Classify grades one scanned process. The baseline ticks come back with the
// claim so a caller can say what the machine was compared against; they are
// zero for a process no baseline holds.
func (o *Ownership) Classify(p Process) (Claim, int64) {
	baseline, ok := o.known[p.RunID][p.PID]
	if !ok {
		return ClaimForeign, 0
	}
	if !p.TicksOK || p.StartTicks != baseline {
		return ClaimMismatch, baseline
	}
	if act, ok := o.active[attemptKey(p.RunID, p.PID)]; ok && act == p.StartTicks {
		return ClaimRunning, baseline
	}
	return ClaimOrphan, baseline
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

	own := NewOwnership(knownRows, activeRows)

	signals := opts.Signals
	if signals == nil {
		signals = defaultSignals()
	}
	reread := opts.tickReader()

	for _, p := range procs {
		if p.PID == opts.pid() || p.PGID == opts.pgid() {
			continue // never our own blood
		}
		claim, baseline := own.Classify(p)
		switch claim {
		case ClaimForeign:
			continue // not a pid any attempt of ours ever held
		case ClaimRunning:
			continue // a legitimate worker of a running attempt
		case ClaimMismatch:
			// AC6: the identity on file does not match the machine. Either
			// the baseline was tampered with or the pid was recycled; both
			// refuse the kill.
			opts.logger().Warn("refusing to signal a process whose start ticks do not match the baseline",
				"pid", p.PID, "run", p.RunID,
				"baseline_ticks", baseline, "seen_ticks", p.StartTicks)
			continue
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
