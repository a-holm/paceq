package obs

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The WAL watchdog (#44). 07 §6.4 calls unlimited WAL growth "the most
// likely production failure of this design", because a checkpoint can only
// shrink the WAL when no reader stands on an old snapshot: one long-lived
// read transaction is all it takes. The watchdog is the canary for exactly
// that regression, and its message says what is probably happening, not
// only how big the file is.
//
// The alarm is a diagnosis, not just a number: the thresholds are warn at
// limits.wal_warn_bytes (64 MiB) and error at four times that (256 MiB).
// The metric pulseq_wal_size_bytes is the scrape-time view; this watch is
// the event-time one.

// WALWatchConfig wires a watch to its world.
type WALWatchConfig struct {
	// DBPath is the database file's path; the WAL lives beside it as
	// DBPath + "-wal".
	DBPath string

	Limits DiskLimits

	Clock clock.Clock
	Log   *slog.Logger

	// Emit receives the alarm events for the outbox. Nil means the log
	// lines are all the alarm there is. The context is the guard loop's.
	Emit func(context.Context, WALEvent)

	// SizeOf is the file-size seam. Nil means os.Stat.
	SizeOf func(path string) (int64, bool)
}

// WALEvent is one WAL alarm fact. Since scopes the outbox episode: one
// crossing is one event no matter how many checks confirm it.
type WALEvent struct {
	Level      slog.Level
	WalBytes   int64
	WarnBytes  int64
	ErrorBytes int64
	Since      time.Time
}

// WALWatch checks the WAL's size one cycle at a time. It is not a loop:
// the disk-guard's loop drives it, so the two reads of the same filesystem
// stay one cadence.
type WALWatch struct {
	cfg WALWatchConfig
	log *slog.Logger

	mu      sync.Mutex
	level   int // 0 none, 1 warn, 2 error
	since   time.Time
	started bool
}

// NewWALWatch builds the watch.
func NewWALWatch(cfg WALWatchConfig) *WALWatch {
	cfg.Limits = cfg.Limits.WithDefaults()
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	return &WALWatch{cfg: cfg, log: log}
}

// Step reads the WAL size and raises or clears the alarm. Crossing a
// threshold upwards announces itself; dropping below the warn line logs
// the recovery. Within a level, every confirming check re-emits the same
// episode - the outbox dedup and throttle collapse those (#44).
func (w *WALWatch) Step(ctx context.Context) {
	size := w.walSize()
	next := w.levelOf(size)

	w.mu.Lock()
	prev := w.level
	if next > prev || (next > 0 && next == prev) {
		if !w.started {
			w.since = w.cfg.Clock.Now().UTC()
			w.started = true
		}
	}
	if next == 0 {
		w.started = false
	}
	w.level = next
	since := w.since
	w.mu.Unlock()

	switch {
	case next > prev && next == 2:
		w.log.Error("WAL is past the error level; a long-lived read transaction is probably blocking checkpointing",
			"wal_bytes", size, "error_bytes", w.cfg.Limits.WalWarnBytes*4)
	case next > prev && next == 1:
		w.log.Warn("WAL is past the warning level; check whether a reader holds an open transaction",
			"wal_bytes", size, "warn_bytes", w.cfg.Limits.WalWarnBytes)
	case next == 0 && prev > 0:
		w.log.Info("WAL is back under the warning level; checkpointing will shrink it",
			"wal_bytes", size, "warn_bytes", w.cfg.Limits.WalWarnBytes)
	}

	if next > prev || (next > 0 && next == prev) {
		if w.cfg.Emit != nil {
			w.cfg.Emit(ctx, WALEvent{
				Level:      levelOfWAL(next),
				WalBytes:   size,
				WarnBytes:  w.cfg.Limits.WalWarnBytes,
				ErrorBytes: w.cfg.Limits.WalWarnBytes * 4,
				Since:      since,
			})
		}
	}
}

func (w *WALWatch) walSize() int64 {
	if w.cfg.SizeOf != nil {
		size, ok := w.cfg.SizeOf(w.cfg.DBPath + "-wal")
		if !ok {
			return 0 // no WAL yet is the healthy state
		}
		return size
	}
	info, err := os.Stat(w.cfg.DBPath + "-wal")
	if err != nil {
		return 0
	}
	return info.Size()
}

func (w *WALWatch) levelOf(size int64) int {
	switch {
	case size > w.cfg.Limits.WalWarnBytes*4:
		return 2
	case size > w.cfg.Limits.WalWarnBytes:
		return 1
	default:
		return 0
	}
}

func levelOfWAL(level int) slog.Level {
	if level >= 2 {
		return slog.LevelError
	}
	return slog.LevelWarn
}
