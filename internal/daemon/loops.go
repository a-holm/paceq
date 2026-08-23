package daemon

import (
	"context"
	"log/slog"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/store"
)

// The loop bodies. Each one is the same shape on purpose (05 section 3.2):
// select on the context, the ticker and the bus wake, then do one unit of
// work. The ticker is the safety net; the bus is the fast path. Losing a wake
// costs one tick of latency, never correctness, which is what lets the bus be
// switched off entirely.
//
// Every loop takes its world through small interfaces so a synctest bubble
// can run them without SQLite. Serve wires the real store in.

// ScheduleSource is the seam M2-05 fills: whatever turns due schedules into
// triggers. Nil means idle, which is correct for this milestone: nothing is
// materialised until that lands, and the loop still ticks so the topology,
// the heartbeat and the health surface are all live before then. It stays an
// interface here so the daemon never learns cronx's shape early.
type ScheduleSource interface {
	Tick(ctx context.Context) error
}

// sessionToucher moves the session heartbeat forward.
type sessionToucher interface {
	TouchSession(ctx context.Context, sessionID string) error
}

// runQueue names the runs that may be claimed right now.
type runQueue interface {
	ClaimableRunIDs(ctx context.Context) ([]string, error)
}

// loops carries what every loop shares.
type loops struct {
	clk    clock.Clock
	bus    *notify.Bus
	status *statuses
	log    *slog.Logger
	heart  *Heart
}

// schedulerLoop owns the schedule decisions. In this milestone it ticks,
// marks itself alive in the health surface and calls the source seam when one
// is wired; M2-05 turns the tick into trigger materialisation and catch-up.
func schedulerLoop(ctx context.Context, d loops, every time.Duration, src ScheduleSource) (err error) {
	return loop(ctx, d, "scheduler", every, notify.TopicScheduleChanged, func(ctx context.Context) error {
		if src == nil {
			return nil
		}
		err := src.Tick(ctx)
		if d.heart != nil {
			d.heart.Beat()
		}
		return err
	})
}

// sensorRuntime is the M3 placeholder: it exists, it ticks, it decides
// nothing yet. Its slot in the topology is real from day one so adding the
// evaluator later changes no wiring.
func sensorLoop(ctx context.Context, d loops, every time.Duration) error {
	return loop(ctx, d, "sensor", every, notify.TopicScheduleChanged, func(context.Context) error {
		return nil
	})
}

// reapSweeper is what the reaper loop needs of the engine. The interface
// keeps the loop testable without SQLite.
type reapSweeper interface {
	ReapExpiredRuns(ctx context.Context) ([]store.ReapedRun, error)
}

// reaperLoop sweeps for expired run leases while this instance holds the
// reaper role lease, and carries the periodic reconciliation safety net
// (issue #62) inside the same leadership. The lease decides who sweeps; the
// ticker decides when; the reconciler runs on its own cadence so a 30 second
// safety net is not dragged to the reaper's faster clock. A nil reconciler
// keeps the old shape for tests.
func reaperLoop(ctx context.Context, d loops, every time.Duration, sweeper reapSweeper,
	reconcileEvery time.Duration, rec func(context.Context) error,
) error {
	var lastReconciled clock.Mono
	haveReconciled := false

	return loop(ctx, d, "reaper", every, notify.TopicCancelRequested, func(ctx context.Context) error {
		reaped, err := sweeper.ReapExpiredRuns(ctx)
		if err != nil {
			return err
		}
		for _, r := range reaped {
			d.log.Info("reaped an expired run",
				"run", r.ID, "state", r.State, "epoch", r.LeaseEpoch)
		}
		if len(reaped) > 0 {
			// Every reap frees a slot or requeues a run; both are facts
			// the dispatcher should hear about now rather than at its
			// next tick (#68).
			d.bus.Notify(notify.TopicRunQueued)
		}

		// The periodic reconciliation safety net (#62) rides the same
		// reaper leadership on its own cadence. A nil reconciler keeps
		// the loop to pure reaping.
		if rec == nil || reconcileEvery <= 0 {
			return nil
		}
		if haveReconciled && d.clk.Since(lastReconciled) < reconcileEvery {
			return nil
		}
		if err := rec(ctx); err != nil {
			return err
		}
		lastReconciled = d.clk.Mark()
		haveReconciled = true
		return nil
	})
}

// janitorLoop is the M5-05 placeholder for retention and checkpointing. The
// shutdown checkpoint does not need it, and retention policies come later.
func janitorLoop(ctx context.Context, d loops, every time.Duration) error {
	return loop(ctx, d, "janitor", every, notify.TopicScheduleChanged, func(context.Context) error {
		return nil
	})
}

// heartbeatLoop keeps last_seen_at moving. The gap between two heartbeats is
// what bounds how much of a crash counts as unaccounted time, which is why
// the interval is small and the write cheap.
func heartbeatLoop(ctx context.Context, d loops, every time.Duration, toucher sessionToucher, sessionID string) error {
	return loop(ctx, d, "heartbeat", every, "", func(ctx context.Context) error {
		if err := toucher.TouchSession(ctx, sessionID); err != nil {
			// One failed touch is not a dead daemon: the database
			// may be busy for a moment. The next tick tries again,
			// and the log line keeps the failure visible.
			d.log.Warn("session heartbeat did not land", "error", err)
		}
		return nil
	})
}

// dispatcherLoop hands claimable runs to the executor pool, one decision at a
// time. It is deliberately single threaded: the whole decision layer, from
// seeing a queued run to handing it over, runs in one goroutine, so no two
// dispatches can race (00 section 4.1).
func dispatcherLoop(ctx context.Context, d loops, every time.Duration, queue runQueue, submit func(string) bool) error {
	return loop(ctx, d, "dispatcher", every, notify.TopicRunQueued, func(ctx context.Context) error {
		ids, err := queue.ClaimableRunIDs(ctx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			// A full pool leaves the run queued; the next tick or
			// wake looks again. Nothing is lost by waiting.
			if !submit(id) {
				break
			}
		}
		return nil
	})
}

// loop is the shared skeleton: one start line, then select on context, ticker
// and wake until the context ends. Returning ctx.Err() is what makes an
// errgroup wait finish cleanly on a stop.
func loop(ctx context.Context, d loops, name string, every time.Duration, topic notify.Topic, work func(context.Context) error) error {
	d.log.Info("loop started", "loop", name)

	tick := d.clk.NewTicker(every)
	defer tick.Stop()

	var wake <-chan struct{}
	if topic != "" && !d.bus.Disabled() {
		wake = d.bus.Subscribe(topic)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		case <-wake:
		}
		d.status.mark(name)
		if err := work(ctx); err != nil {
			d.log.Error("loop failed", "loop", name, "error", err)
			return err
		}
	}
}
