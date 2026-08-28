package daemon

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/obs"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// defaultGuardEvery is the disk-guard's cycle cadence (06 §7.1: statfs is
// periodic, thirty seconds). It is deliberately not a configuration key:
// 10 §7's budget allows four new keys and they are all spent on thresholds,
// none on how often they are checked.
const defaultGuardEvery = 30 * time.Second

// opsNotifier turns the disk-guard's and the WAL watch's events into
// throttled outbox rows (#44). The targets are the notify_defaults
// on_failure list - the "something is wrong" list - because that is what the
// events are; an installation that named no notifiers hears nothing through
// the outbox, and the metrics, the log and doctor carry the same facts.
type opsNotifier struct {
	st       *store.Store
	targets  []string
	throttle time.Duration
	host     string
	clk      clock.Clock
	log      *slog.Logger
}

func newOpsNotifier(st *store.Store, cfg *NotificationConfig, clk clock.Clock, log *slog.Logger) *opsNotifier {
	if st == nil {
		return nil
	}
	n := &opsNotifier{st: st, host: HostName(), clk: clk, log: log}
	if cfg != nil {
		n.targets = cfg.Defaults.OnFailure
		n.throttle = cfg.Defaults.Throttle
	}
	return n
}

// emitDisk records one disk.low episode. Only the degraded state notifies:
// a warning changes no behaviour, and an outbox row per warning would train
// the operator to ignore exactly the alert that matters.
func (n *opsNotifier) emitDisk(ctx context.Context, e obs.DiskEvent) {
	if n == nil || e.State != obs.DiskDegraded {
		return
	}
	n.deliver(ctx, model.TopicDiskLow, "disk", map[string]any{
		"event":          model.TopicDiskLow,
		"at":             n.clk.Now().UTC().UnixMilli(),
		"state":          "degraded",
		"free_bytes":     e.FreeBytes,
		"total_bytes":    e.TotalBytes,
		"min_free_bytes": e.FloorBytes,
		"log_bytes":      e.LogBytes,
		"since":          e.Since.UnixMilli(),
		"host":           n.host,
		"doctor_cmd":     "paceq doctor",
		"prune_cmd":      "paceq prune",
	}, e.Since)
}

// emitWAL records one wal.growth episode at warn or error level.
func (n *opsNotifier) emitWAL(ctx context.Context, e obs.WALEvent) {
	if n == nil {
		return
	}
	n.deliver(ctx, model.TopicWALGrowth, "wal", map[string]any{
		"event":       model.TopicWALGrowth,
		"at":          n.clk.Now().UTC().UnixMilli(),
		"state":       "wal_growth",
		"level":       e.Level.String(),
		"wal_bytes":   e.WalBytes,
		"warn_bytes":  e.WarnBytes,
		"error_bytes": e.ErrorBytes,
		"since":       e.Since.UnixMilli(),
		"host":        n.host,
		"note":        "a long-lived read transaction is probably blocking checkpointing",
		"doctor_cmd":  "paceq doctor",
	}, e.Since)
}

// deliver fans the episode out over the configured targets and writes the
// rows. DedupKey rides the episode stamp, so every confirming check of one
// episode dedups to the same row, and the throttle window collapses episodes
// that arrive too soon after each other. A failure is loud but not fatal:
// the loop's next cycle re-emits the same episode and tries again.
func (n *opsNotifier) deliver(ctx context.Context, topic, subject string, fields map[string]any, since time.Time) {
	if len(n.targets) == 0 {
		return
	}
	b, err := json.Marshal(fields)
	if err != nil {
		b = []byte("{}") // encoding/json cannot fail on this shape; stay valid
	}
	now := n.clk.Now().UTC()
	notes := make([]model.Notification, 0, len(n.targets))
	for _, target := range n.targets {
		notes = append(notes, model.Notification{
			Topic:   topic,
			Subject: subject,
			Target:  target,
			Payload: string(b),
			DedupKey: strings.Join([]string{
				topic, subject, target,
				strconv.FormatInt(since.UnixMilli(), 10),
			}, "|"),
			Throttle:    n.throttle,
			CreatedAt:   now,
			AvailableAt: now,
		})
	}
	if err := n.st.RecordOpsNotifications(ctx, notes); err != nil {
		n.log.Warn("could not record the ops notification", "topic", topic, "err", err.Error())
	}
}

// runHoldGate is what the store's admission consults inside the tick
// transaction: one atomic read of the guard's state, and the verdict's code
// and words live here where the daemon can keep them beside the catalogue.
func runHoldGate(guard *obs.Guard) store.RunHoldFunc {
	return func() *store.RunHold {
		if guard == nil || !guard.Degraded() {
			return nil
		}
		return &store.RunHold{
			Code: reason.RUNRejectedDiskLow,
			Text: "the filesystem holding the state is under its free-space floor; " +
				"new runs are refused until space is freed",
			Data: guard.FactsForHold(),
		}
	}
}
