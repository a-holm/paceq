package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/a-holm/paceq/internal/buildinfo"
	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/janitor"
	"github.com/a-holm/paceq/internal/leases"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/obs"
	"github.com/a-holm/paceq/internal/obs/sdnotify"
	"github.com/a-holm/paceq/internal/reconcile"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/sensor"
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
	_ = sdnotify.Status("opening database")

	// The counters exist before the store opens so every tick that commits
	// from the first second on is counted (#40). They are the in-memory
	// half of /metrics; a restart resetting them is expected and handled
	// by Prometheus's own rate arithmetic.
	counters := obs.NewCounters()
	st, err := store.OpenState(ctx, cfg.StateDir, store.Options{Clock: clk, Metrics: counters})
	if err != nil {
		return err
	}
	faults.Point("M2:serve:after_flock")

	_ = sdnotify.Status("running migrations")
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
		return startupRefusal(summary)
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
	_ = sdnotify.Status("reconciling")
	if err := reconcile.OnStartup(ctx, st, reconcile.Options{
		Clock:            clk,
		Log:              log,
		SessionID:        sess.ID,
		SessionStartedAt: sess.StartedAt,
		SpoolDir:         reconcile.SpoolDirUnder(cfg.StateDir),
		GapFrom:          gapFrom,
		PrevSessionID:    prevSession,
		Wake:             func() { bus.Notify(notify.TopicRunQueued) },
	}); err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}

	// R11, the last step of the reconciliation sequence (M6-06): the
	// whole-state invariant sweep, after the reconciler has settled what the
	// crash left, so the reconciler's own work is not reported as drift.
	// Every finding lands in the integrity event log; a critical one refuses
	// the start with the same anatomy the boot gate uses, because a state
	// the code cannot reason about must not be served, however it got there.
	_ = sdnotify.Status("checking invariants")
	// The sweep runs cancel-proof: a stop arriving mid-sweep is a stop, not
	// a failed boot, and the sweep is bounded to seconds by its own
	// per-statement deadline either way.
	sweepCtx := context.WithoutCancel(ctx)
	sweepViolations, err := st.Fsck(sweepCtx)
	if err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: the invariant sweep failed: %w", err)
	}
	if critical := firstCriticalSummary(sweepViolations); critical != "" {
		_ = st.Close()
		return startupRefusal(critical)
	}
	if err := recordStartupFindings(sweepCtx, st, clk, log, sweepViolations); err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}

	heart := NewHeart(clk)
	statuses := newStatuses(func() time.Time { return clk.Now() })
	d := loops{clk: clk, bus: bus, status: statuses, log: log, heart: heart}

	// The scheduler owns the schedule decisions under its fenced role lease;
	// the loop below only wakes it and marks the health surface.
	schedSrc, err := scheduler.New(scheduler.Config{Store: st, Clock: clk, Holder: sess.ID, Log: log, Shadow: cfg.Shadow})
	if err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}

	// Shadow mode (#32) persists its marker and its comparison source so
	// every other process - status, report, explain - can state honestly
	// whether the schedules it looks at executed anything. A normal start
	// clears the marker; shadow capture then sweeps the log source. The
	// writes ride a cancel-proof context: a graceful stop may have cancelled
	// ctx by the time the daemon shuts down, and clearing the marker then
	// must not turn a clean stop into a reported error.
	metaCtx := context.WithoutCancel(ctx)
	var obsSrc scheduler.ObservationSource
	if cfg.Shadow {
		spec, err := scheduler.ParseObserveSpec(cfg.Observe)
		if err != nil {
			_ = st.Close()
			return fmt.Errorf("serve: %w", err)
		}
		if err := st.SetShadowRuntime(metaCtx, true, spec.StoreName()); err != nil {
			_ = st.Close()
			return fmt.Errorf("serve: %w", err)
		}
		log.Info("SHADOW MODE: scheduling fully evaluated, nothing executes",
			"observe", spec.StoreName())
		switch spec.Kind {
		case "journald":
			obsSrc = scheduler.JournaldSource{}
		case "file":
			obsSrc = scheduler.FileSource{Path: spec.Path}
		}
	} else if err := st.SetShadowRuntime(metaCtx, false, ""); err != nil {
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}
	if cfg.Shadow && obsSrc != nil {
		wm, _ := st.LatestShadowObservationStamp(ctx, obsSrc.Name())
		stamp := wm
		go func() {
			// The ticker comes from the daemon's clock, like every other
			// loop: time must never be taken directly inside the daemon.
			tick := clk.NewTicker(scheduler.ObserveInterval)
			defer tick.Stop()
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				n, err := scheduler.Sweep(ctx, st, obsSrc, stamp, clk.Now().UTC())
				if err != nil {
					log.Warn("shadow observation sweep failed", "err", err.Error())
					continue
				}
				if n > 0 {
					log.Info("recorded observed cron starts", "count", n, "source", obsSrc.Name())
				}
				if fresh, err := st.LatestShadowObservationStamp(ctx, obsSrc.Name()); err == nil {
					stamp = fresh
				}
			}
		}()
	}

	// The executors answer to their own context, not to the group's: a stop
	// must be able to let the intake die first while process groups are
	// still draining, which is the whole two phase idea.
	execCtx, stopExec := context.WithCancel(context.WithoutCancel(ctx))
	// Notification configuration is read once per boot (#29): a config.yaml
	// with bad values refuses the boot outright, because an invalid alerting
	// configuration is exactly how "nobody got told" happens silently.
	notifyCfg, nerr := LoadNotificationConfig(cfg.StateDir, cfg.ConfigDir)
	if nerr != nil {
		stopExec()
		_ = st.Close()
		return fmt.Errorf("serve: %w", nerr)
	}
	notifs := NewNotifications(st, clk, log, notifyCfg, os.Stderr)
	ops := newOpsNotifier(st, notifyCfg, clk, log)
	// The disk-guard (#44) watches the filesystem that holds the state and
	// the log directory's byte cap; the WAL watch rides the same cadence.
	// Its hold is installed before any loop starts, so no tick that arrives
	// after the first measurement can admit a run the disk cannot afford.
	guard := obs.NewGuard(obs.GuardConfig{
		StateDir: cfg.StateDir,
		LogDir:   filepath.Join(cfg.StateDir, logsink.LogDirName),
		Limits:   cfg.Limits,
		Clock:    clk,
		Log:      log,
		Pruner:   st,
		Emit:     ops.emitDisk,
	})
	walWatch := obs.NewWALWatch(obs.WALWatchConfig{
		DBPath: filepath.Join(cfg.StateDir, store.DatabaseFileName),
		Limits: cfg.Limits,
		Clock:  clk,
		Log:    log,
		Emit:   ops.emitWAL,
	})
	st.SetRunHold(runHoldGate(guard))
	eng := newEngine(cfg, st, clk)
	if notifyCfg != nil {
		eng.Notify = notify.NewPlanner(notifyCfg.Defaults, func() time.Time { return clk.Now() })
		eng.Host = notifs.Host
	}
	pool := newExecutorPool(eng, eng, log, cfg.workerCount())
	// A run leaving the pool frees its concurrency slot; the dispatcher
	// hears about it at once, so a deferred run starts on the release
	// rather than on the next tick (#68). A disabled bus drops the wake
	// and the ticker carries it alone, like every other topic.
	pool.afterRun = func() { bus.Notify(notify.TopicRunQueued) }

	// The guard's first cycle runs before the executors do: loggopprydding
	// gets its chance to save the disk before the first run is admitted
	// (06 §15 risiko 3). It is cheap - one statfs and a directory listing.
	if err := guard.Step(ctx); err != nil {
		log.Warn("the disk-guard's first cycle failed", "err", err.Error())
	}

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

	// The sensor runtime is the long-lived evaluator context (M3-02): the
	// store says which sensors are due, the runtime evaluates each one off the
	// loop, and the sink commits every result as one transaction. Each tick
	// the sink writes carries this session id, so a tick names the daemon that
	// produced it. It does not name an evaluation in flight: the seam hands a
	// result over only once the sensor has answered, so a daemon that dies
	// mid-evaluation leaves no tick behind at all.
	sensorEv := sensor.NewEvaluator(sensor.Config{
		KillGrace: cfg.killGrace(),
	}, clk)
	sensorRt := sensor.NewRuntime(sensorEv, sensor.RuntimeConfig{
		Source:       sensorSource{st: st, clk: clk, log: log},
		Sink:         sensorSink{st: st, clk: clk, session: sess.ID},
		MaxParallel:  cfg.sensorMaxParallel(),
		DrainTimeout: cfg.drainTimeout(),
		Clock:        clk,
		Log:          log,
	})

	launch(intakeCtx, intakeAck(), func(c context.Context) error {
		return schedulerLoop(c, d, tickEvery, schedSrc)
	})
	launch(grp.ctx, nil, func(c context.Context) error {
		return sensorLoop(c, d, tickEvery, sensorRt)
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
	// Notification delivery (#29): one dispatcher under the flock-writer,
	// plus the freshness checker. Both drain on the shared tick; neither
	// holds a role lease, because the flock already guarantees one writer.
	launch(grp.ctx, nil, func(c context.Context) error {
		return notificationDispatchLoop(c, d, tickEvery, notifs)
	})
	launch(grp.ctx, nil, func(c context.Context) error {
		return slaCheckLoop(c, d, defaultSLACheckEvery, notifs)
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
	// Maintenance (#36) runs under its own fenced role lease: two daemons
	// against one database must never retain at the same time, and a slow
	// backup or a long drain of batches must never block leadership loss.
	// The loop wakes on the tick but only cycles when the nightly slot is
	// owed and nobody else holds the lease.
	maint := janitor.New(janitor.Config{
		Store:     st,
		Clock:     clk,
		LogRoot:   filepath.Join(cfg.StateDir, "logs"),
		BackupDir: filepath.Join(cfg.StateDir, "backups"),
		Policies:  cfg.Policies,
		Log:       log,
	})
	launch(grp.ctx, nil, func(c context.Context) error {
		return maintenanceLoop(c, d, tickEvery, st, maint, cfg.nightlyHour(), cfg.owner(), st)
	})
	// The disk-guard and the WAL watch (#44) share one loop on the thirty
	// second cadence the plan fixes; both mark the same health line.
	launch(grp.ctx, nil, func(c context.Context) error {
		return loop(c, d, "diskguard", defaultGuardEvery, "", func(c context.Context) error {
			if err := guard.Step(c); err != nil {
				return err
			}
			walWatch.Step(c)
			return nil
		})
	})
	// The collector is built from the store's read pool and the same
	// counters the store writes into: two sources, one document (#40).
	// The disk-guard's readings join it (#44): a watch that has never
	// measured leaves its families honestly absent.
	collector := obs.NewCollector(st, counters, clk,
		obs.Identity{
			Version:   buildinfo.Get().Version,
			Commit:    buildinfo.Get().Commit,
			GoVersion: buildinfo.Get().GoVersion,
		},
		filepath.Join(cfg.StateDir, logsink.LogDirName))
	collector.DiskWatch = guard
	stopHealth := startHealthEndpoint(cfg, statuses, log, st, collector, guard.Degraded)
	if err := startMetricsTCP(cfg.MetricsListen, collector); err != nil {
		if stopHealth != nil {
			stopHealth(context.Background())
		}
		// The loops are already launched; they live on the group's
		// context, so cancelling it is what brings them down before the
		// error propagates. Without this, a refused metrics bind would
		// leave a daemon-shaped process behind the failure.
		grp.cancel()
		stopIntake()
		stopExec()
		_ = st.Close()
		return fmt.Errorf("serve: %w", err)
	}
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

	// The systemd readiness signal goes out after migration AND reconciliation
	// are both done, so systemctl start returns only when the daemon is ready.
	_ = sdnotify.Ready("")

	// The watchdog goroutine pings systemd at half the WatchdogSec interval.
	// It reads the Heart, which is beaten by the scheduler loop on every real
	// tick, so a slow backup never causes a watchdog timeout restart loop.
	dogTimeout := watchdogUSecFromEnv()
	if dogTimeout > 0 {
		sdnotifier := sdnotifyDog{}
		go watchdogLoop(context.WithoutCancel(ctx), clk, heart, watchdogPingInterval(dogTimeout), sdnotifier)
	}

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
		StateDir: cfg.StateDir,
		LogRoot:  logsink.NewRoot(cfg.StateDir),
		Clock:    clk,
		Owner:    cfg.owner(),
		LeaseTTL: cfg.leaseTTL(),
		// The exec shim (issue #39): steps run through this binary's own
		// `paceq exec`, with results spooled for the restart story.
		Executable: shimExecutableFor(),
		SpoolDir:   reconcile.SpoolDirUnder(cfg.StateDir),
	}
}

// shimExecutableFor locates the image the daemon's executors launch as the
// shim. An unlocatable binary degrades to the direct spawn rather than
// refusing to serve; the fallback lives in the engine's spawn seam.
func shimExecutableFor() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return exe
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
