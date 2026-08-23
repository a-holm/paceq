package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/leases"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/reconcile"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
)

// Serve runs the daemon until the context is cancelled or a loop fails, then
// stops cleanly. The startup order is the one the reliability plan fixes (02
// section 5.9): lock, verify pragmas and migrate, open the session row,
// converge what a crash left behind, start the loops, report ready.
func Serve(ctx context.Context, cfg Config, clk clock.Clock) error {
	if cfg.StateDir == "" {
		return errors.New("serve: no state directory was named")
	}
	log := cfg.logger()

	st, err := store.OpenState(ctx, cfg.StateDir, store.Options{Clock: clk})
	if err != nil {
		return err
	}
	faults.Point("M2:serve:after_flock")

	if err := st.Migrate(ctx); err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}
	faults.Point("M2:serve:after_migration")

	// The health gate, before anything writes: a database already in a state
	// the code cannot reason about must not be mutated further. Only the
	// critical subset refuses; ordinary drift is full fsck's business later.
	// It runs before StartSession so a refusal preserves BootChanged() for
	// the next attempt after the operator fixes the state.
	violations, err := st.QuickFsck(ctx)
	if err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: the startup health check failed: %w", err)
	}
	if summary := reconcile.CriticalViolationSummary(violations); summary != "" {
		_ = st.Close()
		return fmt.Errorf("serve: startup refused: %s; run \"paceq fsck --repair\" "+
			"and confirm manually before starting", summary)
	}

	// The gap is captured BEFORE StartSession closes the stale row. After
	// StartSession the only open session is our own fresh one, whose
	// LastSeenAt is just now, making every gap read zero. Reading the
	// prior session's last heartbeat here means the outage row explains
	// real downtime, not runtime.
	var gapFrom time.Time
	prevSession := ""
	if prevSess, found, err := st.OpenSession(ctx); err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	} else if found {
		gapFrom = prevSess.LastSeenAt
		prevSession = prevSess.ID
	}

	sess, err := st.StartSession(ctx, cfg.Version)
	if err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}
	log.Info("session opened", "session", sess.ID, "pid", sess.PID,
		"boot_changed", st.BootChanged())

	// The bus exists before reconciliation so the sweep's release notice has
	// somewhere to go; the loops that subscribe come later.
	bus := notify.New()
	if cfg.DisableNotifyBus {
		bus = notify.Disabled()
	}

	// Startup reconciliation (M2-07) replaces the old per-run recovery call
	// that sat here: one whole-state sweep over everything the crash left -
	// orphaned process groups, runs still marked running, ticks never
	// finished - and the outages row that explains the downtime gap. Nothing
	// else in this process is running yet, so there is nobody to race.
	if err := reconcile.OnStartup(ctx, st, reconcile.Options{
		Clock:            clk,
		Log:              log,
		SessionID:        sess.ID,
		SessionStartedAt: sess.StartedAt,
		GapFrom:          gapFrom,
		PrevSessionID:    prevSession,
		Wake:             func() { bus.Notify(notify.TopicRunQueued) },
	}); err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}

	statuses := newStatuses(func() time.Time { return clk.Now() })
	d := loops{clk: clk, bus: bus, status: statuses, log: log}

	// The scheduler owns the schedule decisions under its fenced role lease;
	// the loop below only wakes it and marks the health surface.
	schedSrc, err := scheduler.New(scheduler.Config{Store: st, Clock: clk, Holder: sess.ID, Log: log})
	if err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}

	// The executors answer to their own context, not to the group's: a stop
	// must be able to let the intake die first while process groups are
	// still draining, which is the whole two phase idea.
	execCtx, stopExec := context.WithCancel(context.WithoutCancel(ctx))
	eng := newEngine(cfg, st, clk)
	pool := newExecutorPool(eng, eng, log, cfg.workerCount())
	// A run leaving the pool frees its concurrency slot; the dispatcher
	// hears about it at once, so a deferred run starts on the release
	// rather than on the next tick (#68). A disabled bus drops the wake
	// and the ticker carries it alone, like every other topic.
	pool.afterRun = func() { bus.Notify(notify.TopicRunQueued) }

	grp := newGroup(ctx)
	intakeCtx, stopIntake := context.WithCancel(grp.ctx)

	var intakeAcks []<-chan struct{}
	// launch starts one loop. A non-nil ack channel belongs to the intake:
	// closing it is the loop's acknowledgement that it takes no more work.
	launch := func(runCtx context.Context, ack chan struct{}, fn func(context.Context) error) {
		if ack != nil {
			intakeAcks = append(intakeAcks, ack)
		}
		grp.goLoop(func(c context.Context) error {
			if ack != nil {
				defer close(ack)
			}
			return fn(c)
		})
	}
	intakeAck := func() chan struct{} { return make(chan struct{}) }

	tickEvery := cfg.tickInterval()

	launch(intakeCtx, intakeAck(), func(c context.Context) error {
		return schedulerLoop(c, d, tickEvery, schedSrc)
	})
	launch(grp.ctx, nil, func(c context.Context) error {
		return sensorLoop(c, d, tickEvery)
	})
	launch(intakeCtx, intakeAck(), func(c context.Context) error {
		return dispatcherLoop(c, d, tickEvery, st, func(runID string) bool {
			return pool.submit(execCtx, runID)
		})
	})
	// The renewal goroutine lives with the group, not with the intake: the
	// executors need their leases renewed during the drain, right up until
	// the last process group comes down.
	launch(grp.ctx, nil, func(c context.Context) error {
		return eng.RunLeaseRenewals(c)
	})
	// The reaper owns expired-lease sweeps under its fenced role lease; the
	// 30 second reconciliation safety net rides along inside the same
	// leadership, so the two sweeps that touch the same rows can never run
	// as concurrent actors. Losing leadership cancels both underneath it.
	launch(grp.ctx, nil, func(c context.Context) error {
		return leases.RunAsLeader(c, st, leases.Options{
			Name:   "reaper",
			Holder: cfg.owner(),
			Clock:  clk,
			Log:    log,
		}, func(body context.Context, _ int64) error {
			return reaperLoop(body, d, cfg.reapEvery(), eng,
				cfg.reconcileEvery(), func(rc context.Context) error {
					return reconcile.Periodic(rc, st, reconcile.Options{
						Clock:            clk,
						Log:              log,
						SessionID:        sess.ID,
						SessionStartedAt: sess.StartedAt,
					})
				})
		})
	})
	launch(grp.ctx, nil, func(c context.Context) error {
		return janitorLoop(c, d, tickEvery)
	})
	stopHealth := startHealthEndpoint(cfg, statuses, log)
	launch(grp.ctx, nil, func(c context.Context) error {
		return heartbeatLoop(c, d, cfg.heartbeatEvery(), st, sess.ID)
	})

	faults.Point("M2:serve:after_loops_started")
	log.Info("daemon ready",
		"workers", cfg.workerCount(),
		"tick", tickEvery.String(),
		"drain_timeout", cfg.drainTimeout().String(),
		"notify_bus", !cfg.DisableNotifyBus,
		"socket", cfg.SocketPath != "",
	)

	if cfg.Signals != nil {
		go watchHardStop(cfg.Signals, log, cfg.OnHardStop)
	}

	// The group's wait moves to a channel so the shutdown can bound how
	// long it waits for the remaining loops, and so its verdict travels as
	// a value: reading a variable another goroutine writes would be a race,
	// receiving from a channel is not.
	loopsDone := make(chan struct{})
	waiter := make(chan error, 1) // buffered: nobody may be left blocked on it
	go func() {
		err := grp.wait()
		waiter <- err
		close(loopsDone)
	}()

	// This select is the daemon's working life. Between startup and a stop
	// request there is nothing to decide: hold until the caller asks for a
	// stop, or until a loop fails and cancels the rest. A daemon that
	// returned on its own would release the state lock moments after
	// taking it, and every other promise on this process rests on that
	// lock.
	var loopErr error
	select {
	case <-ctx.Done():
	case loopErr = <-waiter:
	}

	sd := &shutdown{
		cfg:          cfg,
		clk:          clk,
		log:          log,
		statuses:     statuses,
		stopIntake:   stopIntake,
		intakeAcks:   intakeAcks,
		stopExec:     stopExec,
		execDrained:  pool.drained(),
		apiStopped:   stopHealth,
		loopsDrained: loopsDone,
		closeSession: func(c context.Context) error { return st.StopSession(c, sess.ID) },
		checkpoint:   st.CheckpointTruncate,
		closeStore:   st.Close,
	}

	err = sd.run(stopCause(loopErr, grp.ctx))
	<-loopsDone // reap the waiter goroutine before returning
	return err
}

func newEngine(cfg Config, st *store.Store, clk clock.Clock) *engine.Engine {
	return &engine.Engine{
		Store:    st,
		LogRoot:  logsink.NewRoot(cfg.StateDir),
		Clock:    clk,
		Owner:    cfg.owner(),
		LeaseTTL: cfg.leaseTTL(),
	}
}

// stopCause picks what the stop reports. A loop's own failure is a failure;
// anything else is the caller's cancellation, which the shutdown reports as
// clean by returning nil.
func stopCause(loopErr error, gctx context.Context) error {
	if loopErr != nil && !errors.Is(loopErr, context.Canceled) {
		return loopErr
	}
	return gctx.Err()
}
