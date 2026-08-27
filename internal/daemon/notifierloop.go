package daemon

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/store"
)

// DeliveryBatch is how many notifications one claim hands out. Twenty
// matches the issue sketch: a batch is small enough that one slow target
// cannot monopolise a wake, large enough that bursts do not tick by.
const DeliveryBatch = 20

// MaxDrainBatches bounds how much work ONE wake may start. The claim's
// visibility window returns anything unfinished, so stopping here is safe:
// the next wake picks the rest up exactly where the bookkeeping says.
const MaxDrainBatches = 8

// defaultSLACheckEvery is how often expected_within verdicts are taken. The
// check reads two indexed summaries; once a minute keeps breach-opening
// latency trivial without touching the database every second.
const defaultSLACheckEvery = time.Minute

// Notifications carries everything the dispatch + SLA loops need. It fails
// closed: an unknown target becomes permanently failed history (visible in
// `notifications list`, counted by pulseq_notifications_failed_total), never
// silence (#29 AC five).
type Notifications struct {
	Store *store.Store
	Clock clock.Clock
	Log   *slog.Logger

	Config *NotificationConfig

	Host string

	StderrOut io.Writer

	lookup     func(target string) (notify.Notifier, bool)
	slaPlanner *notify.Planner
}

// NewNotifications builds the service from loaded configuration. A nil cfg
// means nothing is configured: the loops still run, find no rows the CLI
// paths could write anyway, and cost one indexed probe per tick.
func NewNotifications(st *store.Store, clk clock.Clock, log *slog.Logger,
	cfg *NotificationConfig, errOut io.Writer,
) *Notifications {
	n := &Notifications{
		Store:     st,
		Clock:     clk,
		Log:       log,
		Config:    cfg,
		StderrOut: errOut,
	}
	if n.Log == nil {
		n.Log = slog.Default()
	}
	if cfg == nil {
		n.lookup = func(string) (notify.Notifier, bool) { return nil, false }
		return n
	}
	stderrTargets := map[string]bool{}
	for _, name := range cfg.Stderr {
		stderrTargets[name] = true
	}
	n.lookup = func(target string) (notify.Notifier, bool) {
		if execN, ok := cfg.Notifiers[target]; ok {
			return execN, true
		}
		if stderrTargets[target] && n.StderrOut != nil {
			return &notify.StderrNotifier{Out: n.StderrOut}, true
		}
		return nil, false
	}
	n.slaPlanner = &notify.Planner{
		Defaults: cfg.Defaults,
		Now:      func() time.Time { return clk.Now() },
	}
	return n
}

// notificationDispatchLoop claims due notifications and delivers each to its
// target. Correctness rides on row state plus this ticker alone; losing a
// wake only waits for the next tick, which is what makes the loop honest at
// every speed (11 section 3.4).
func notificationDispatchLoop(ctx context.Context, d loops, every time.Duration, n *Notifications) error {
	if n == nil || n.Store == nil {
		return nil
	}
	return loop(ctx, d, "notify-dispatch", every, notify.TopicRunQueued, func(c context.Context) error {
		drainOutboxOnce(c, n)
		return nil
	})
}

func drainOutboxOnce(ctx context.Context, n *Notifications) {
	now := n.Clock.Now()
	for i := 0; i < MaxDrainBatches; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgs, err := n.Store.ClaimOutbox(ctx, DeliveryBatch, now, deliveryVisibility())
		if err != nil {
			n.Log.Warn("claiming notifications", "err", err)
			return
		}
		if len(msgs) == 0 { // Nothing due: the cheapest possible wake.
			return
		}
		for _, msg := range msgs {
			deliverOne(ctx, n, msg)
		}
	}
}

// deliveryVisibility is how long a claimed row stays invisible before a
// crash during delivery hands it back to rotation. It must exceed any single
// send attempt, or two dispatchers could hold the same event; the sixty
// second floor clears the ten minute ceiling too, since one daemon holds
// the flock and runs exactly one dispatcher.
func deliveryVisibility() time.Duration { return 15 * time.Minute }

func deliverOne(ctx context.Context, n *Notifications, msg store.OutboxMsg) {
	maxAttempts := n.maxAttempts()
	started := n.Clock.Now()

	var err error
	target, found := n.lookup(msg.Target)
	if !found {
		err = fmt.Errorf("unknown notifier %q: fix config.yaml or the job hooks", msg.Target)
	} else {
		cctx, cancel := context.WithTimeout(ctx, n.timeoutFor(msg.Target))
		err = target.Send(cctx, toNotifyMsg(msg))
		cancel()
	}
	n.Store.ObserveDelivery(n.Clock.Now().Sub(started))

	switch {
	case ctx.Err() != nil:
		// Shutting down: the claimed row returns after visibility, keeping
		// the documented at-least-once story intact across stops as well as
		// crashes.
	case err == nil:
		if derr := n.Store.MarkOutboxDelivered(ctx, msg.ID, n.Clock.Now()); derr != nil {
			n.Log.Warn("marking delivered", "id", msg.ID, "err", derr)
		}
	case msg.Attempts >= maxAttempts:
		gaveUp := fmt.Sprintf("gave up after %d attempts: %v", msg.Attempts, err)
		if ferr := n.Store.MarkOutboxFailed(ctx, msg.ID, n.Clock.Now(), gaveUp); ferr != nil {
			n.Log.Warn("marking failed", "id", msg.ID, "err", ferr)
		} else {
			n.Log.Warn("notification gave up permanently",
				"id", msg.ID, "topic", msg.Topic, "subject", msg.Subject,
				"target", msg.Target, "attempts", msg.Attempts,
				"last_error", err.Error())
		}
	default:
		backoff := notify.Backoff(msg.Attempts)
		rescheduleAt := n.Clock.Now().Add(backoff)
		if rerr := n.Store.RescheduleOutbox(ctx, msg.ID, rescheduleAt, err.Error()); rerr != nil {
			n.Log.Warn("rescheduling notification", "id", msg.ID, "err", rerr)
		}
		n.Log.Info("notification delivery failed, will retry",
			"id", msg.ID, "target", msg.Target, "attempt", msg.Attempts,
			"backoff", backoff.String(), "err", err.Error())
	}
}

// toNotifyMsg copies the claimed row into the leaf package's value type;
// both are flat data, so the mapping stays a dumb constructor that can
// neither drop nor invent fields.
func toNotifyMsg(m store.OutboxMsg) notify.OutboxMsg {
	return notify.OutboxMsg{
		ID:             m.ID,
		Topic:          m.Topic,
		Subject:        m.Subject,
		Target:         m.Target,
		Payload:        m.Payload,
		Attempts:       m.Attempts,
		Suppressed:     m.Suppressed,
		WindowOpenedAt: m.WindowOpenedAt,
	}
}

func (n *Notifications) maxAttempts() int {
	if n.Config != nil && n.Config.Defaults.MaxAttempts > 0 {
		return n.Config.Defaults.MaxAttempts
	}
	return notify.DefaultMaxAttempts
}

func (n *Notifications) timeoutFor(target string) time.Duration {
	if n.Config != nil {
		if t, ok := n.Config.Timeouts[target]; ok && t > 0 {
			return t
		}
	}
	return notify.DefaultDeliveryTimeout
}

// slaCheckLoop evaluates every job's expected_within against its newest
// success. The episode guard lives in the store, so a restart mid-breach
// re-emits nothing, recovery resets, and the next breach emits again
// (test plan seven).
func slaCheckLoop(ctx context.Context, d loops, every time.Duration, n *Notifications) error {
	if n == nil || n.Store == nil {
		return nil
	}
	if every <= 0 {
		every = defaultSLACheckEvery
	}
	return loop(ctx, d, "sla-check", every, notify.TopicScheduleChanged, func(c context.Context) error {
		checkSlaEpisodes(c, n)
		return nil
	})
}

func checkSlaEpisodes(ctx context.Context, n *Notifications) {
	slas, err := n.Store.MetricsJobSLAs(ctx)
	if err != nil {
		n.Log.Warn("reading job freshness expectations", "err", err)
		return
	}
	lastSuccess := map[string]time.Time{}
	stamps, err := n.Store.MetricsLastSuccesses(ctx)
	if err != nil {
		n.Log.Warn("reading last successes", "err", err)
		return
	}
	for _, s := range stamps {
		lastSuccess[s.Job] = s.At
	}
	now := n.Clock.Now()
	var changes []store.SLAEpisodeChange
	for _, sla := range slas {
		successAt, hadSuccess := lastSuccess[sla.Job]
		if !hadSuccess {
			// A job that has never succeeded has no freshness story yet;
			// the freshness metric carries the same rule, so alerting must
			// not turn applied-but-never-run into an alarm storm.
			continue
		}
		breaching := successAt.Add(sla.Within).Before(now)
		change := store.SLAEpisodeChange{Job: sla.Job, Breaching: breaching}
		if breaching {
			change.Notes = n.slaPlan(sla.Job, now, successAt, sla.Within)
		}
		changes = append(changes, change)
	}
	if len(changes) == 0 {
		return
	}
	if err := n.Store.ApplySLAEpisodes(ctx, changes, now); err != nil {
		n.Log.Warn("applying sla episodes", "err", err)
	}
}

func (n *Notifications) slaPlan(job string, breachedAt, lastSuccess time.Time, within time.Duration) []model.Notification {
	p := n.slaPlanner
	if p == nil {
		return nil
	}
	return p.SLAPlan(job, breachedAt, lastSuccess, within, hostOrDefault())
}

func hostOrDefault() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}
