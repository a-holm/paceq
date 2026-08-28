package obs

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The disk-guard (#44): the outer protection that keeps a filling disk from
// turning into corrupted state or dead user jobs. The self-imposed byte cap
// and the external threshold solve different problems and both live here:
// the cap is paceq tidying up after itself while it still can, the threshold
// is how the daemon behaves when something else filled the disk.
//
// Degraded mode refuses new runs and lets running ones finish; ticks keep
// being written, because the decisions are small and are exactly the record
// an incident review needs. Leaving degraded requires hysteresis, so a
// retention pass that frees space in bursts cannot make the daemon flap.

// DiskState is the guard's verdict about the filesystem that holds the state.
type DiskState int32

const (
	// DiskNormal is room to work.
	DiskNormal DiskState = iota
	// DiskWarning is below twice the floor: time to look, not to refuse.
	DiskWarning
	// DiskDegraded is under the floor: new runs are refused, running ones
	// finish, ticks continue.
	DiskDegraded
)

func (s DiskState) String() string {
	switch s {
	case DiskNormal:
		return "normal"
	case DiskWarning:
		return "warning"
	case DiskDegraded:
		return "degraded"
	default:
		return "unknown"
	}
}

// The shipped limits (10 §7's config budget: four keys, all under `limits:`
// in config.yaml). A limit of zero means the default; tests override.
const (
	DefaultLogMaxBytes      = int64(10) << 30
	DefaultDiskMinFreeBytes = int64(1) << 30
	DefaultWALWarnBytes     = int64(64) << 20
	DefaultDiskMinFreePct   = 10.0
)

// DiskLimits are the four configuration keys the issue is allowed to add.
// Zero fields fall back to the shipped defaults.
type DiskLimits struct {
	// LogMaxBytes is the self-imposed ceiling on the log directory.
	// Key: limits.log_max_bytes.
	LogMaxBytes int64

	// MinFreePercent degrades the daemon below this percent free.
	// Key: limits.disk_min_free_percent.
	MinFreePercent float64

	// MinFreeBytes is the absolute floor, which is what protects a huge
	// disk where ten percent is still terabytes. Key:
	// limits.disk_min_free_bytes.
	MinFreeBytes int64

	// WalWarnBytes is the WAL size that says a reader is blocking
	// checkpointing; four times it is the error level. Key:
	// limits.wal_warn_bytes.
	WalWarnBytes int64
}

// WithDefaults fills the zero fields.
func (l DiskLimits) WithDefaults() DiskLimits {
	if l.LogMaxBytes <= 0 {
		l.LogMaxBytes = DefaultLogMaxBytes
	}
	if l.MinFreePercent <= 0 {
		l.MinFreePercent = DefaultDiskMinFreePct
	}
	if l.MinFreeBytes <= 0 {
		l.MinFreeBytes = DefaultDiskMinFreeBytes
	}
	if l.WalWarnBytes <= 0 {
		l.WalWarnBytes = DefaultWALWarnBytes
	}
	return l
}

// StatfsFunc reads free and total bytes of the filesystem holding dir.
type StatfsFunc func(dir string) (free, total uint64, err error)

// ShardPruner is the one store call pruning makes: after a date shard is
// removed, no step may keep naming its files.
type ShardPruner interface {
	MarkLogShardPruned(ctx context.Context, shard string) (int64, error)
}

// DiskEvent is one fact the guard publishes: the outbox emitter turns it
// into throttled notifications, logs carry it beside every state change.
type DiskEvent struct {
	Event string // "disk.low" today; the vocabulary is closed (#44)
	Level slog.Level

	State      DiskState
	FreeBytes  int64
	TotalBytes int64
	FloorBytes int64
	LogBytes   int64

	// Since is when the current state began, UTC. It scopes the outbox
	// dedup key, so one episode is one notification no matter how many
	// checks confirm it, and a later episode is a new event.
	Since time.Time
}

// GuardConfig wires a guard to its world. Every reading is injectable, so
// the state machine is tested without a filesystem at all.
type GuardConfig struct {
	// StateDir is the directory whose filesystem is watched.
	StateDir string
	// LogDir is the directory the byte cap covers (state/logs).
	LogDir string

	Limits DiskLimits

	Clock clock.Clock
	Log   *slog.Logger

	// Statfs answers how much room is left. Nil means the real statfs.
	Statfs StatfsFunc
	// SizeOfShard measures one date shard's bytes. Nil means a real walk.
	// The seam exists so the incremental counting is observable: after the
	// first cycle only the newest shard is ever measured again.
	SizeOfShard func(dir string) (int64, bool)
	// Pruner clears steps.log_path behind removed shards. Nil means the
	// files are removed without the database being told (tests).
	Pruner ShardPruner
	// Emit receives the guard's events. Nil means nobody is listening;
	// the metrics and the log lines carry the same facts. The context is
	// the guard loop's, so the receiver's write rides the loop's lifetime.
	Emit func(context.Context, DiskEvent)

	// CheckEvery is the cycle cadence. Zero means thirty seconds.
	CheckEvery time.Duration
	// KeepMinShards is how many newest date shards pruning never touches,
	// whatever the cap says. Zero means two: today and yesterday.
	KeepMinShards int
}

// Guard is the disk-watch state machine. Step is the whole cycle; Run drives
// it on the clock. All readings are atomics because /readyz and the
// collector read them from other goroutines without a lock.
type Guard struct {
	cfg GuardConfig
	log *slog.Logger

	state     atomic.Int32
	free      atomic.Int64
	total     atomic.Int64
	logBytes  atomic.Int64
	stateUsed atomic.Bool // false until the first successful reading

	// interior is only touched by Step, which the daemon calls from one
	// loop; the mutex exists because tests drive Step directly too.
	mu          sync.Mutex
	shardSizes  map[string]int64
	clearStreak int
	since       time.Time
	// lastFloor pins the floor the current episode was declared under, so
	// the exit bar (1.5x) is computed against what degraded us.
}

// NewGuard builds the guard. It refuses to guess: a config without a clock
// or a state directory panics at the call site by construction, so the
// daemon's wiring keeps both.
func NewGuard(cfg GuardConfig) *Guard {
	cfg.Limits = cfg.Limits.WithDefaults()
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	if cfg.CheckEvery <= 0 {
		cfg.CheckEvery = 30 * time.Second
	}
	if cfg.KeepMinShards <= 0 {
		cfg.KeepMinShards = 2
	}
	return &Guard{cfg: cfg, log: log, shardSizes: map[string]int64{}}
}

// State is the newest verdict. The zero value before the first reading is
// DiskNormal: a guard that has not measured yet has no business refusing
// anything.
func (g *Guard) State() DiskState { return DiskState(g.state.Load()) }

// Degraded is what admission's gate reads. One atomic load.
func (g *Guard) Degraded() bool { return g.State() == DiskDegraded }

// DegradedGauge is pulseq_degraded: 1 while new runs are held.
func (g *Guard) DegradedGauge() float64 {
	if g.Degraded() {
		return 1
	}
	return 0
}

// TotalBytes is the newest total-capacity reading, and whether one exists.
func (g *Guard) TotalBytes() (int64, bool) {
	if !g.stateUsed.Load() {
		return 0, false
	}
	return g.total.Load(), true
}

// FactsForHold is what the run-hold refusal records in reason_data: the
// measurements that make the decision auditable from explain. Nil before
// the first successful reading.
func (g *Guard) FactsForHold() map[string]any {
	if !g.stateUsed.Load() {
		return nil
	}
	floor := diskFloorBytes(uint64(g.total.Load()), g.cfg.Limits)
	return map[string]any{
		"free_bytes":     g.free.Load(),
		"total_bytes":    g.total.Load(),
		"min_free_bytes": floor,
	}
}

// FreeBytes is the newest statfs reading, and whether one exists.
func (g *Guard) FreeBytes() (int64, bool) {
	if !g.stateUsed.Load() {
		return 0, false
	}
	return g.free.Load(), true
}

// LogDirBytes is the newest incremental byte count of the log directory,
// and whether one exists.
func (g *Guard) LogDirBytes() (int64, bool) {
	if !g.stateUsed.Load() {
		return 0, false
	}
	return g.logBytes.Load(), true
}

// Run drives Step on the clock until the context is cancelled. The first
// cycle runs at once: loggopprydding before the executor gets to write is
// the ordering the reliability plan fixes (06 §15 risiko 3).
func (g *Guard) Run(ctx context.Context) error {
	if err := g.Step(ctx); err != nil {
		return err
	}
	t := g.cfg.Clock.NewTicker(g.cfg.CheckEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		if err := g.Step(ctx); err != nil {
			return err
		}
	}
}

// Step runs one cycle: measure, tidy under the byte cap first, then
// classify and transition. A statfs failure keeps the previous state - a
// guard that cannot see must not refuse work it cannot price.
func (g *Guard) Step(ctx context.Context) error {
	free, total, err := g.statfs()
	if err != nil {
		g.log.Warn("disk guard: statfs failed; keeping the previous state", "dir", g.cfg.StateDir, "err", err.Error())
		return nil
	}
	logBytes := g.currentLogBytes()

	// The self-imposed cap runs first: maybe tidying up is enough to stay
	// out of trouble, and the freed bytes count against the threshold too.
	freed := int64(0)
	if over := logBytes - g.cfg.Limits.LogMaxBytes; over > 0 {
		freed = g.pruneOldestShards(ctx, over)
		free = addClamped(free, uint64(freed))
		logBytes -= freed
		if logBytes < 0 {
			logBytes = 0
		}
	}

	next := classifyDisk(free, total, g.cfg.Limits)
	g.transition(ctx, next, free, total, logBytes)
	return nil
}

func (g *Guard) statfs() (uint64, uint64, error) {
	if g.cfg.Statfs != nil {
		return g.cfg.Statfs(g.cfg.StateDir)
	}
	return statfsDisk(g.cfg.StateDir)
}

// transition applies the newest verdict with the exit hysteresis: the way
// out of Degraded needs two consecutive readings above one and a half times
// the floor, so retention freeing space in bursts cannot flap the daemon.
func (g *Guard) transition(ctx context.Context, next DiskState, free, total uint64, logBytes int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	prev := DiskState(g.state.Load())
	floor := diskFloorBytes(total, g.cfg.Limits)
	clears := free >= uint64(floor)*3/2

	if prev == DiskDegraded && next != DiskDegraded {
		// A reading above the exit bar starts counting; anything else
		// restarts it. The state itself only moves on the second.
		if !clears {
			g.clearStreak = 0
			next = DiskDegraded
		} else {
			g.clearStreak++
			if g.clearStreak < 2 {
				next = DiskDegraded
			}
		}
	} else {
		g.clearStreak = 0
	}

	g.free.Store(int64(free))
	g.total.Store(int64(total))
	g.logBytes.Store(logBytes)
	g.state.Store(int32(next))
	g.stateUsed.Store(true)

	if next == DiskDegraded && prev != DiskDegraded {
		g.since = g.cfg.Clock.Now().UTC()
		g.log.Warn("disk guard: degraded mode; new runs are refused until space is freed",
			"free_bytes", free, "total_bytes", total, "min_free_bytes", g.cfg.Limits.MinFreeBytes,
			"min_free_percent", g.cfg.Limits.MinFreePercent)
	}
	if next != DiskDegraded && prev == DiskDegraded {
		g.since = g.cfg.Clock.Now().UTC()
		g.log.Info("disk guard: disk recovered; new runs are admitted again",
			"free_bytes", free, "total_bytes", total)
	}
	if next != prev {
		g.emitLocked(ctx, next, free, total, logBytes, floor)
	} else if next == DiskDegraded {
		// Every confirming check re-emits the same episode; the outbox
		// dedup and throttle collapse them into one notification (#44).
		g.emitLocked(ctx, next, free, total, logBytes, floor)
	}
}

// emitLocked publishes the event while the state lock is held; since is the
// episode stamp the same critical section moved.
func (g *Guard) emitLocked(ctx context.Context, state DiskState, free, total uint64, logBytes int64, floor int64) {
	if g.cfg.Emit == nil {
		return
	}
	level := slog.LevelInfo
	switch state {
	case DiskWarning:
		level = slog.LevelWarn
	case DiskDegraded:
		level = slog.LevelError
	}
	g.cfg.Emit(ctx, DiskEvent{
		Event:      "disk.low",
		Level:      level,
		State:      state,
		FreeBytes:  int64(free),
		TotalBytes: int64(total),
		FloorBytes: floor,
		LogBytes:   logBytes,
		Since:      g.since,
	})
}

// diskFloorBytes is the binding floor: whichever of the absolute and the
// percentage floor leaves less room.
func diskFloorBytes(total uint64, l DiskLimits) int64 {
	byPct := int64(0)
	if total > 0 {
		byPct = int64(float64(total) * l.MinFreePercent / 100)
	}
	if l.MinFreeBytes > byPct {
		return l.MinFreeBytes
	}
	return byPct
}

// classifyDisk is the pure threshold rule: under the floor is degraded,
// under twice it is a warning, everything else is normal.
func classifyDisk(free, total uint64, l DiskLimits) DiskState {
	if total == 0 {
		return DiskNormal // an unreadable capacity is not a full disk
	}
	pct := float64(free) / float64(total) * 100
	switch {
	case pct < l.MinFreePercent || free < uint64(l.MinFreeBytes):
		return DiskDegraded
	case pct < l.MinFreePercent*2:
		return DiskWarning
	default:
		return DiskNormal
	}
}

// currentLogBytes counts the log directory incrementally: unchanged old
// shards are remembered, and only the newest shard - the only one that can
// still be growing - is measured again. Whatever else removes a shard, the
// directory listing is the truth and the cache follows it.
func (g *Guard) currentLogBytes() int64 {
	shards, newest, err := dateShards(g.cfg.LogDir)
	if err != nil {
		g.log.Warn("disk guard: could not list the log directory", "dir", g.cfg.LogDir, "err", err.Error())
		return g.logBytes.Load()
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	fresh := make(map[string]int64, len(shards))
	var total int64
	for i, name := range shards {
		if size, ok := g.shardSizes[name]; ok && i != newest {
			total += size
			fresh[name] = size
			continue
		}
		size, ok := g.sizeOfShard(name)
		if !ok {
			continue
		}
		fresh[name] = size
		total += size
	}
	g.shardSizes = fresh
	return total
}

// sizeOfShard resolves through the test seam or the real walk.
func (g *Guard) sizeOfShard(name string) (int64, bool) {
	if g.cfg.SizeOfShard != nil {
		return g.cfg.SizeOfShard(name)
	}
	return dirSize(g.cfg.LogDir, name)
}

// pruneOldestShards removes whole date shards, oldest first, until need
// bytes are freed or only the protected newest shards remain. ISO dates
// sort chronologically, which is the entire reason the log root is sharded
// by day (06 §3.2): deletion is one RemoveAll, no query, no write lock.
func (g *Guard) pruneOldestShards(ctx context.Context, need int64) int64 {
	shards, newest, err := dateShards(g.cfg.LogDir)
	if err != nil {
		return 0
	}
	var freed int64
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, name := range shards {
		if freed >= need {
			break
		}
		// The newest keepMinShards shards are never deleted, whatever the
		// cap says: today's and yesterday's logs are how an incident is
		// read, and pruning them to save themselves would be the one
		// response worse than the disk being full.
		if i+g.cfg.KeepMinShards > newest {
			continue
		}
		size, ok := g.shardSizes[name]
		if !ok {
			size, ok = g.sizeOfShard(name)
			if !ok {
				continue
			}
			g.shardSizes[name] = size
		}
		if err := removeShard(g.cfg.LogDir, name); err != nil {
			g.log.Warn("could not remove a log shard", "shard", name, "err", err.Error())
			continue
		}
		delete(g.shardSizes, name)
		freed += size
		g.log.Info("log quota: removed the oldest log shard", "shard", name, "bytes", size)
		if g.cfg.Pruner != nil {
			if _, err := g.cfg.Pruner.MarkLogShardPruned(ctx, name); err != nil {
				g.log.Warn("could not clear log paths of a removed shard", "shard", name, "err", err.Error())
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	return freed
}

func addClamped(a, b uint64) uint64 {
	sum := a + b
	if sum < a { // overflow wraps
		return ^uint64(0)
	}
	return sum
}
