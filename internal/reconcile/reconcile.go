package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Startup reconciliation (issue #62). After a crash four kinds of rot exist
// at once: runs still marked running with nobody driving them, orphaned
// process groups still alive, ticks started but never finished, and a hole in
// history where nothing was evaluated. OnStartup is the deterministic,
// idempotent sweep that cleans all four, and it runs once per boot, after the
// state lock and migrations, before any loop starts.
//
// The sequence is deliberately not a state machine. Every step can be
// interrupted and repeated without harm, which is what makes reconciliation
// itself safe to crash through (window W15): the next start simply repeats
// whatever the dead one left half done.

// GapThreshold is the noise floor for outage records. Downtime shorter than
// this writes no outages row at all: an operator reading history wants
// explanations for real holes, not one row per quick restart.
const GapThreshold = time.Minute

// CriticalViolationSummary renders the refusal line from a list of store
// violations: which findings the catalogue grades Critical and on what, first
// detail included so the operator knows where to look. It returns an empty
// string when nothing in the list refuses a start.
//
// The grade comes from store.CriticalViolations, the one place that answers
// "does this refuse a boot", so a regrade in the catalogue moves this gate
// and the health report together.
func CriticalViolationSummary(all []store.Violation) string {
	var named []string
	detail := ""
	for _, v := range store.CriticalViolations(all) {
		named = append(named, v.Check+" ("+v.Subject+")")
		if detail == "" {
			detail = v.Detail
		}
	}
	if len(named) == 0 {
		return ""
	}
	return strings.Join(named, ", ") + ": " + detail
}

// DefaultOrphanGrace is how long an orphaned process group gets to exit on
// SIGTERM before the sweep escalates to SIGKILL.
const DefaultOrphanGrace = 10 * time.Second

// Options carries what the sweep needs from its caller.
type Options struct {
	// Clock drives every timing decision. Required.
	Clock clock.Clock

	// Log receives the sweep's story. Nil means slog.Default.
	Log *slog.Logger

	// SessionID and SessionStartedAt describe the session the caller just
	// opened. SessionID stamps synthetic ticks; SessionStartedAt is the
	// cutoff that keeps hanging-tick failure away from this process's own
	// work in flight.
	SessionID        string
	SessionStartedAt time.Time

	// GapFrom is the last heartbeat of the session that died, captured
	// before StartSession closed it. Zero means there was no stale open
	// session, which is the clean-stop or first-start case: no outage row
	// will be written. PrevSessionID names that session on the row.
	GapFrom       time.Time
	PrevSessionID string

	// Wake releases queued work whose available_at has passed. Releasing is
	// the claim predicate's job already; the call only tells the dispatcher
	// to look now instead of at its next tick. Nil is fine.
	Wake func()

	// OrphanGrace overrides DefaultOrphanGrace. Zero means default.
	OrphanGrace time.Duration

	// SelfPID and SelfPGID let tests stand in for this process. Zero means
	// the real values.
	SelfPID  int
	SelfPGID int

	// SpoolDir is the attempts directory of the result spool (#39). Empty
	// skips the spool pass entirely: an installation whose executor never
	// shims has nothing here, and the lease-based handback below stays the
	// whole recovery story.
	SpoolDir string

	// Test seams. ScanProcs replaces the platform /proc scan; Signals
	// replaces real signalling. Both nil in production.
	ScanProcs func() ([]Process, error)
	Signals   Signaller

	rereadTicks func(pid int) (int64, bool)
}

func (o *Options) logger() *slog.Logger {
	if o.Log != nil {
		return o.Log
	}
	return slog.Default()
}

func (o *Options) grace() time.Duration {
	if o.OrphanGrace > 0 {
		return o.OrphanGrace
	}
	return DefaultOrphanGrace
}

func (o *Options) clock() clock.Clock {
	if o.Clock != nil {
		return o.Clock
	}
	return clock.System()
}

func (o *Options) pid() int {
	if o.SelfPID != 0 {
		return o.SelfPID
	}
	return ownPID()
}

func (o *Options) pgid() int {
	if o.SelfPGID != 0 {
		return o.SelfPGID
	}
	return ownGroup()
}

func (o *Options) tickReader() func(pid int) (int64, bool) {
	if o.rereadTicks != nil {
		return o.rereadTicks
	}
	return store.ReadProcessStartTicks
}

// OnStartup sweeps everything one crash can leave behind. It assumes R0 and
// R1 already happened: StartSession has closed the stale session as 'crash',
// opened this one, written current_boot_id, and BootChanged reports whether
// the machine restarted. Those facts are spent here, inside this start.
//
// The order matters twice over. The process sweep runs BEFORE any run is
// handed back, so a surviving child is killed while its run still says
// running and the event chain never sees a handback followed by a kill of a
// run that is no longer running. And every write lands before the caller
// starts a single loop, so the first dispatcher pass meets a world with no
// crash residue in it.
func OnStartup(ctx context.Context, st *store.Store, opts Options) error {
	if opts.Clock == nil && st == nil {
		return errors.New("reconcile: OnStartup needs a store and a clock")
	}
	t0 := opts.clock().Now().UTC()
	boot := st.BootChanged()
	log := opts.logger()
	log.Info("startup reconciliation begins", "boot_changed", boot)

	// R0/R1 happened in StartSession. This fault point sits right where the
	// boot fact is about to be spent: killing here leaves every rot intact
	// and the boot change unread, exactly the shape the next start must
	// converge from.
	faults.Point("M2:reconcile:after_bootid")

	// R3, before R2: kill what survived while ownership still says running.
	// A machine restart needs none of this - no child can have survived the
	// machine - so the whole scan is skipped on boot change.
	if !boot {
		if err := sweepProcesses(ctx, st, &opts); err != nil {
			// The sweep reads other processes' business; any number of
			// things outside paceq's control can go wrong mid walk. It is
			// evidence gathering, never a reason to refuse serving.
			log.Warn("the orphan process sweep did not finish", "error", err)
		}
	} else {
		log.Info("process sweep skipped: the machine restarted, so no child process can have survived")
	}

	// R2, first half: consume what the dead attempts' shims wrote. The
	// result spool turns a crash between a child's exit and its verdict's
	// commit from "assume the worst and rerun" into "commit what really
	// happened" (issue #39, window W8). This pass runs BEFORE the run
	// handback, while the lease epoch on the row is still the one the
	// results were written under — after the reaper bumps it, a valid
	// result would read as stale.
	consumeSpool(ctx, st, &opts)

	// R2, second half: hand back every run whose executor is provably gone.
	// An expired lease past the skew is proof enough on an unchanged boot; a
	// changed boot proves every holder dead at once, so the expiry stops
	// mattering.
	reaped, err := st.ReapExpiredRuns(ctx, store.ReapOptions{IgnoreLease: boot})
	if err != nil {
		return fmt.Errorf("startup reconciliation could not hand back the runs left running: %w", err)
	}
	for _, r := range reaped {
		log.Info("handed back a run its executor lost",
			"run", r.ID, "state", r.State, "epoch", r.LeaseEpoch)
	}
	faults.Point("M2:reconcile:after_reap")

	// R2b: converge runs whose steps have all ended but whose row still says
	// otherwise. An executor killed between its final step verdict and the run
	// verdict leaves exactly that shape, and this closes it (M4-03). The pass
	// is idempotent and skips any run a live lease still belongs to.
	if err := st.ReconcileRunStates(ctx); err != nil {
		return fmt.Errorf("startup reconciliation could not converge the run states: %w", err)
	}

	// R4: close evaluations the dying daemon left open. The cutoff is this
	// session's start, so anything opened since belongs to us and stays.
	closed, err := st.FailHangingTicks(ctx, opts.SessionStartedAt)
	if err != nil {
		return fmt.Errorf("startup reconciliation could not close the hanging ticks: %w", err)
	}
	for _, c := range closed {
		log.Info("closed a tick its daemon died inside",
			"tick", c, "reason", reason.TICKErrorDaemonCrashed)
	}

	// Finish what an interrupted predecessor wrote half of: outage rows whose
	// synthetic ticks never landed. The rows are their own completion marker.
	if err := backfillOutages(ctx, st, opts); err != nil {
		return fmt.Errorf("startup reconciliation could not finish an earlier outage record: %w", err)
	}

	// R7, R8's vocabulary and R9: the downtime gap becomes history. Nothing
	// here for a clean stop or a first start.
	if !opts.GapFrom.IsZero() {
		if err := recordGap(ctx, st, opts, t0, boot); err != nil {
			return fmt.Errorf("startup reconciliation could not record the outage gap: %w", err)
		}
	}

	// R6: releasing due runs is the claim predicate's ordinary job. Saying so
	// out loud to the dispatcher only saves it a tick of latency.
	if opts.Wake != nil {
		opts.Wake()
	}

	log.Info("startup reconciliation finished")
	return nil
}

// Periodic is the safety net: the same sequence minus the boot-scoped steps,
// run again and again while the daemon serves. Every piece tolerates "already
// clean" by writing nothing when there is nothing to do, which is what makes
// it safe to repeat forever.
//
// It shares the reaper's role lease in production, so it can never race the
// expired-lease sweep it resembles; both actors would otherwise widen each
// other's windows.
func Periodic(ctx context.Context, st *store.Store, opts Options) error {
	log := opts.logger()

	reaped, err := st.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		return fmt.Errorf("periodic reconciliation could not reap expired leases: %w", err)
	}
	for _, r := range reaped {
		log.Info("reaped an expired run", "run", r.ID, "state", r.State, "epoch", r.LeaseEpoch)
	}

	// The continuous half of the spool consumer (#39): results whose
	// executor died after startup, or whose lease outlived this process's
	// first pass, get committed here. Results still owned by a live lease
	// are skipped inside the store, so this can never race the executor
	// that is about to read its own file.
	consumeSpool(ctx, st, &opts)

	if err := st.ReconcileRunStates(ctx); err != nil {
		return fmt.Errorf("periodic reconciliation could not converge the run states: %w", err)
	}

	if err := sweepProcesses(ctx, st, &opts); err != nil {
		log.Warn("the periodic process sweep did not finish", "error", err)
	}

	if _, err := st.FailHangingTicks(ctx, opts.SessionStartedAt); err != nil {
		return fmt.Errorf("periodic reconciliation could not close hanging ticks: %w", err)
	}
	return nil
}
