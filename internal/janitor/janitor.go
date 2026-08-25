// Package janitor runs retention, vacuum hygiene and verified backup.
//
// The whole package serves one number: the write lock must never be held for
// longer than a few tens of milliseconds. Every deletion therefore goes
// through bounded batches of store.PruneBatchLimit rows, one transaction per
// batch, with a pause between batches, and every batch's transaction duration
// lands in a histogram so the promise is measured rather than assumed
// (07 section 6.1).
//
// A cycle is the nightly unit of work: retention table by table, whole log
// date shards, incremental_vacuum, a verified VACUUM INTO backup, and a WAL
// truncate checkpoint - but only while nothing is running. The daemon calls
// Cycle under its fenced maintenance lease; pulseq prune reuses the same
// machinery for manual runs.
package janitor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// BatchPause is how long the janitor sleeps between two deletion batches, so
// every other writer gets the lock back in between. Fifty milliseconds is the
// value the plan fixes; it is deliberately not configurable, because it is
// the mechanism that bounds the lock window, not a tuning knob.
const BatchPause = 50 * time.Millisecond

// DeepCheckEvery is how often a backup copy earns the full integrity_check
// instead of quick_check. Seven days makes the deep pass weekly by
// construction, tracked in meta rather than by day-of-week arithmetic.
const DeepCheckEvery = 7 * 24 * time.Hour

// dateShardLayout matches internal/logsink: one directory per UTC day under
// the log root, which turns log retention into removing whole directories.
const dateShardLayout = "2006-01-02"

// Store is everything the janitor needs from the database. It is the narrow
// consumer-side seam: no database handle crosses here, only the store's own
// maintenance vocabulary.
type Store interface {
	PruneRunsBatch(ctx context.Context, cutoff time.Time, keepMin int) (int64, error)
	PruneSkippedTicksBatch(ctx context.Context, cutoff time.Time) (int64, error)
	PruneTicksBatch(ctx context.Context, cutoff time.Time, keepMin int) (int64, error)
	PruneRunKeysBatch(ctx context.Context, cutoff time.Time) (int64, error)
	PruneSessionsBatch(ctx context.Context, cutoff time.Time, keepMin int) (int64, error)

	EstimateRetention(ctx context.Context, p store.Policies, now time.Time) (store.RetentionPlan, error)

	IncrementalVacuum(ctx context.Context, maxPages int) error
	WalCheckpointTruncate(ctx context.Context) error
	ActiveRunsExist(ctx context.Context) (bool, error)
	FreelistCount(ctx context.Context) (int64, error)

	VacuumInto(ctx context.Context, dst string) error
	RecordBackup(ctx context.Context, at time.Time, status, path, errMsg string, deepCheckAt time.Time) error
	BackupStatus(ctx context.Context) (store.BackupInfo, error)

	MetaValue(ctx context.Context, key string) (string, bool, error)
	SetMeta(ctx context.Context, kv map[string]string) error
}

// Config wires a Janitor to its world.
type Config struct {
	// Store holds the database.
	Store Store

	// Clock decides when tonight is. Tests hand over a fake.
	Clock clock.Clock

	// LogRoot is the state directory's logs directory, whose children are
	// date shards.
	LogRoot string

	// BackupDir receives the nightly VACUUM INTO copies.
	BackupDir string

	// Policies carries the retention configuration keys; zero fields fall
	// back to the shipped defaults.
	Policies store.Policies

	// Log receives structured progress. Nil means slog's default.
	Log *slog.Logger
}

// Janitor owns one instance's maintenance work. It is stateless between
// cycles apart from the lock-hold histogram.
type Janitor struct {
	st   Store
	clk  clock.Clock
	log  *slog.Logger
	pol  store.Policies
	root string
	bdir string
	hist lockHold
}

// New builds a janitor on cfg.
func New(cfg Config) *Janitor {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	return &Janitor{
		st:   cfg.Store,
		clk:  cfg.Clock,
		log:  log,
		pol:  cfg.Policies.WithDefaults(),
		root: cfg.LogRoot,
		bdir: cfg.BackupDir,
	}
}

// Policies reports the effective policy set after defaults filled the gaps.
func (j *Janitor) Policies() store.Policies { return j.pol }

// Totals counts what one pass deleted, per rule.
type Totals struct {
	Runs         int64 `json:"runs"`
	SkippedTicks int64 `json:"skipped_ticks"`
	Ticks        int64 `json:"ticks"`
	RunKeys      int64 `json:"run_keys"`
	Sessions     int64 `json:"daemon_sessions"`
}

// Total sums every deleted row across the database.
func (t Totals) Total() int64 {
	return t.Runs + t.SkippedTicks + t.Ticks + t.RunKeys + t.Sessions
}

// BackupOutcome says what the nightly backup did.
type BackupOutcome struct {
	Status  string // store.BackupStatusVerified or store.BackupStatusFailed
	Path    string
	Err     string
	Skipped bool // true when no backup was attempted (no destination configured)
}

// Result is one cycle's answer, shaped so status surfaces, logs and tests all
// read the same facts.
type Result struct {
	Deleted             Totals        `json:"deleted"`
	Batches             int           `json:"batches"`
	LogShards           []string      `json:"log_shards_removed"`
	VacuumPagesReleased int64         `json:"vacuum_pages_released"`
	Backup              BackupOutcome `json:"backup"`
	Checkpoint          string        `json:"checkpoint"` // ran | skipped_active_runs | failed
	Failures            []string      `json:"failures,omitempty"`

	// Phases lists the retention steps in the order they completed. run_keys
	// last is part of the contract, and this is how a caller (or a test)
	// sees the order rather than trusting it.
	Phases []string `json:"phases,omitempty"`
}

// failed records a phase failure: loud in the result, in the log, and later
// in meta, but never fatal to the daemon - tomorrow night retries.
func (r *Result) failed(phase string, err error) {
	r.Failures = append(r.Failures, fmt.Sprintf("%s: %v", phase, err))
}

// Plan is what prune --dry-run shows: per-rule row estimates plus the log
// shards age would remove.
type Plan struct {
	store.RetentionPlan
	LogShards []string `json:"log_shards"`
}

// PrunePlans computes the retention estimate without touching anything. The
// numbers are exact against today's predicates: the same keep-minimum
// protection the deletes carry is applied in the counting queries too.
func (j *Janitor) PrunePlans(ctx context.Context) (Plan, error) {
	now := j.clk.Now()
	plan, err := j.st.EstimateRetention(ctx, j.pol, now)
	if err != nil {
		return Plan{}, fmt.Errorf("estimate retention: %w", err)
	}
	p := Plan{RetentionPlan: plan}
	if j.root != "" {
		shards, _, estErr := staleLogShards(j.root, now, j.pol.LogShardDays)
		if estErr != nil {
			return Plan{}, estErr
		}
		if err != nil {
			return Plan{}, err
		}
		p.LogShards = shards
	}
	return p, nil
}

// Prune executes one manual retention pass: database tables oldest first,
// run_keys last, then whole log shards, then incremental_vacuum. It never
// backs up - backup belongs to the nightly cycle and to the operator's timer.
func (j *Janitor) Prune(ctx context.Context) (Result, error) {
	res, totals, err := j.pruneDatabase(ctx)
	res.Deleted = totals
	if err != nil {
		return res, err
	}
	if err := j.vacuum(ctx, &res); err != nil {
		res.failed("incremental_vacuum", err)
	}
	j.recordGCCycle(ctx, res)
	return res, nil
}

// pruneDatabase drains every table in order. The order is part of the
// contract: run_keys go last, because they are the longest-lived evidence and
// everything else may legitimately reference them.
func (j *Janitor) pruneDatabase(ctx context.Context) (Result, Totals, error) {
	var (
		res    Result
		totals Totals
		err    error
	)
	now := j.clk.Now()

	totals.Runs, res.Batches, err = j.drain(ctx, func(ctx context.Context) (int64, error) {
		return j.st.PruneRunsBatch(ctx, now.AddDate(0, 0, -j.pol.RunsDays), j.pol.RunsKeepMin)
	})
	if err != nil {
		return res, totals, fmt.Errorf("runs: %w", err)
	}
	res.Phases = append(res.Phases, "runs")

	var b int
	totals.SkippedTicks, b, err = j.drain(ctx, func(ctx context.Context) (int64, error) {
		return j.st.PruneSkippedTicksBatch(ctx, now.AddDate(0, 0, -j.pol.TicksSkippedDays))
	})
	res.Batches += b
	if err != nil {
		return res, totals, fmt.Errorf("skipped ticks: %w", err)
	}
	res.Phases = append(res.Phases, "skipped_ticks")

	totals.Ticks, b, err = j.drain(ctx, func(ctx context.Context) (int64, error) {
		return j.st.PruneTicksBatch(ctx, now.AddDate(0, 0, -j.pol.TicksDays), j.pol.TicksKeepMin)
	})
	res.Batches += b
	if err != nil {
		return res, totals, fmt.Errorf("ticks: %w", err)
	}
	res.Phases = append(res.Phases, "ticks")

	totals.Sessions, b, err = j.drain(ctx, func(ctx context.Context) (int64, error) {
		return j.st.PruneSessionsBatch(ctx, now.AddDate(0, 0, -j.pol.SessionsDays), j.pol.SessionsKeepMin)
	})
	res.Batches += b
	if err != nil {
		return res, totals, fmt.Errorf("daemon_sessions: %w", err)
	}
	res.Phases = append(res.Phases, "daemon_sessions")

	// Last, deliberately: deleting a dedup key means an old trigger can fire
	// again, so this horizon is the longest and the step comes after
	// everything that could still need the keys this pass removes.
	totals.RunKeys, b, err = j.drain(ctx, func(ctx context.Context) (int64, error) {
		return j.st.PruneRunKeysBatch(ctx, now.AddDate(0, 0, -j.pol.RunKeysDays))
	})
	res.Batches += b
	if err != nil {
		return res, totals, fmt.Errorf("run_keys: %w", err)
	}
	res.Phases = append(res.Phases, "run_keys")

	if j.root != "" {
		shards, shardErr := j.pruneLogShards(ctx, now)
		res.LogShards = shards
		if shardErr != nil {
			res.failed("log shards", shardErr)
			return res, totals, nil
		}
	}
	return res, totals, nil
}

// drain loops one batched delete until it comes back empty. Each batch is one
// BEGIN IMMEDIATE transaction; its wall duration lands in the histogram;
// between batches the janitor sleeps BatchPause so another writer can get in.
// The result is only mutated through the return values - callers own the
// accounting, or every batch lands in the tally twice.
func (j *Janitor) drain(ctx context.Context, batch func(context.Context) (int64, error)) (total int64, batches int, err error) {
	for {
		start := j.clk.Mark()
		n, err := batch(ctx)
		held := j.clk.Since(start)
		j.hist.record(held)
		batches++
		if err != nil {
			return total, batches, err
		}
		total += n
		if n == 0 {
			return total, batches, nil
		}
		timer := j.clk.NewTimer(BatchPause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return total, batches, ctx.Err()
		case <-timer.C:
		}
	}
}

// pruneLogShards removes whole date directories older than the log horizon.
// Entries that do not parse as dates are left alone: an unknown name in the
// log root is someone's file until proven otherwise.
func (j *Janitor) pruneLogShards(ctx context.Context, now time.Time) ([]string, error) {
	stale, _, err := staleLogShards(j.root, now, j.pol.LogShardDays)
	if err != nil {
		return nil, err
	}
	for _, name := range stale {
		if ctx.Err() != nil {
			return stale[:0], ctx.Err()
		}
		if err := os.RemoveAll(filepath.Join(j.root, name)); err != nil {
			return stale, fmt.Errorf("remove log shard %s: %w", name, err)
		}
		j.log.Info("removed an expired log shard", "shard", name)
	}
	return stale, nil
}

// staleLogShards lists the date-named children of root older than keepDays.
// The second return carries their sizes, which prune --dry-run prints.
func staleLogShards(root string, now time.Time, keepDays int) (names []string, _ map[string]int64, err error) {
	cutoff := now.AddDate(0, 0, -keepDays)
	ents, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read log root %s: %w", root, err)
	}
	sizes := map[string]int64{}
	for _, e := range ents {
		d, parseErr := time.Parse(dateShardLayout, e.Name())
		if parseErr != nil || !d.Before(cutoff) {
			continue
		}
		names = append(names, e.Name())
		if e.IsDir() {
			size := int64(0)
			_ = filepath.WalkDir(filepath.Join(root, e.Name()), func(_ string, entry fs.DirEntry, walkErr error) error {
				if walkErr == nil && entry.Type().IsRegular() {
					if info, statErr := entry.Info(); statErr == nil {
						size += info.Size()
					}
				}
				return nil
			})
			sizes[e.Name()] = size
		}
	}
	sort.Strings(names)
	return names, sizes, nil
}

// vacuum frees up to 2000 free pages back to the filesystem and reports the
// delta through the result. Frequent small releases beat one giant shrink:
// the page cap keeps the window short and predictable (07 section 6.3).
func (j *Janitor) vacuum(ctx context.Context, res *Result) error {
	before, err := j.st.FreelistCount(ctx)
	if err != nil {
		return fmt.Errorf("freelist before: %w", err)
	}
	if before == 0 {
		return nil
	}
	if err := j.st.IncrementalVacuum(ctx, 2000); err != nil {
		return err
	}
	after, err := j.st.FreelistCount(ctx)
	if err != nil {
		return fmt.Errorf("freelist after: %w", err)
	}
	res.VacuumPagesReleased = before - after
	return nil
}

// Cycle runs the whole nightly sequence: retention, log shards, incremental
// vacuum, verified backup, and a WAL truncate whenever no run is active.
// Phase failures are recorded and returned inside the result instead of being
// fatal: one bad night must not kill the daemon, but it must also never be
// silent.
func (j *Janitor) Cycle(ctx context.Context) (Result, error) {
	res, totals, dbErr := j.pruneDatabase(ctx)
	res.Deleted = totals
	if dbErr != nil {
		res.failed("retention", dbErr)
	}

	if err := j.vacuum(ctx, &res); err != nil {
		res.failed("incremental_vacuum", err)
	}

	if out := j.backup(ctx); out.Status != "" || out.Skipped {
		res.Backup = out
	}

	active, err := j.st.ActiveRunsExist(ctx)
	switch {
	case err != nil:
		res.failed("look for active runs", err)
		res.Checkpoint = "skipped_error"
	case active:
		// Truncating the WAL mid-flight fights the writers it belongs to.
		// Skipping is correct behaviour, not a failure (07 section 6.4).
		res.Checkpoint = "skipped_active_runs"
	default:
		if err := j.st.WalCheckpointTruncate(ctx); err != nil {
			res.failed("wal_checkpoint", err)
			res.Checkpoint = "failed"
		} else {
			res.Checkpoint = "ran"
		}
	}

	j.recordGCCycle(ctx, res)
	return res, nil
}

// backup takes one generation into BackupDir with VACUUM INTO, verifies the
// copy (quick_check, or integrity_check once the week has rolled), removes
// the copy when verification fails, rotates old generations, and records the
// outcome in meta either way. An unverified backup is worse than none.
func (j *Janitor) backup(ctx context.Context) BackupOutcome {
	out := BackupOutcome{Skipped: true}
	if j.bdir == "" {
		return out
	}
	out.Skipped = false

	info, err := j.st.BackupStatus(ctx)
	if err != nil {
		out.Status = store.BackupStatusFailed
		out.Err = fmt.Sprintf("read previous backup status: %v", err)
		return out
	}
	dst := j.backupPath()

	deep := !info.LastDeepCheck.IsZero() && j.clk.Now().Sub(info.LastDeepCheck) >= DeepCheckEvery
	pragma := "quick_check"
	if deep {
		pragma = "integrity_check"
	}

	if err := j.st.VacuumInto(ctx, dst); err != nil {
		out.Status = store.BackupStatusFailed
		out.Err = err.Error()
		_ = j.record(ctx, out, time.Time{})
		return out
	}

	if err := verifyCopy(ctx, dst, deep); err != nil {
		_ = os.Remove(dst) // an unverified copy is worse than none
		out.Status = store.BackupStatusFailed
		out.Err = pragma + ": " + err.Error()
		deepAt := time.Time{}
		if deep {
			// A deep check that ran and failed still counts as performed:
			// waiting another week to notice again helps nobody.
			deepAt = j.clk.Now()
		}
		_ = j.record(ctx, out, deepAt)
		return out
	}

	out.Status = store.BackupStatusVerified
	out.Path = dst
	deepAt := time.Time{}
	if deep {
		deepAt = j.clk.Now()
	}
	if err := j.record(ctx, out, deepAt); err != nil {
		out.Err = fmt.Sprintf("verified, but recording failed: %v", err)
	}
	if err := rotateGenerations(j.bdir, "state-*.db", j.pol.BackupRetain); err != nil {
		j.log.Error("backup rotation failed", "error", err)
	}
	return out
}

func (j *Janitor) backupPath() string {
	name := fmt.Sprintf("state-%s.db", j.clk.Now().UTC().Format("20060102T150405Z"))
	return filepath.Join(j.bdir, name)
}

func (j *Janitor) record(ctx context.Context, out BackupOutcome, deepCheckAt time.Time) error {
	return j.st.RecordBackup(ctx, j.clk.Now(), out.Status, out.Path, out.Err, deepCheckAt)
}

// verifyCopy checks a finished copy in a fresh read-only handle.
func verifyCopy(ctx context.Context, path string, deep bool) error {
	return store.VerifyDatabaseFile(ctx, path, deep)
}

// rotateGenerations keeps the newest retain files matching pattern and
// removes the rest. Names sort chronologically because they carry UTC
// timestamps.
func rotateGenerations(dir, pattern string, retain int) error {
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return fmt.Errorf("list backups in %s: %w", dir, err)
	}
	if len(matches) <= retain {
		return nil
	}
	sort.Strings(matches)
	for _, victim := range matches[:len(matches)-retain] {
		if err := os.Remove(victim); err != nil {
			return fmt.Errorf("rotate out %s: %w", victim, err)
		}
	}
	return nil
}

// recordGCCycle writes the gc_* meta keys, which is how maintenance becomes
// visible outside this process: doctor and the metrics surface read exactly
// these rows, and a failure recorded here is a failure nobody has to guess
// about.
func (j *Janitor) recordGCCycle(ctx context.Context, res Result) {
	status := "ok"
	if len(res.Failures) > 0 {
		status = "error"
	} else if res.Backup.Status == store.BackupStatusFailed {
		status = "error"
	}
	kv := map[string]string{
		store.MetaKeyGCCycleLastAt:     j.clk.Now().UTC().Format(time.RFC3339),
		store.MetaKeyGCCycleLastStatus: status,
	}
	if len(res.Failures) > 0 {
		kv[store.MetaKeyGCCycleLastError] = strings.Join(res.Failures, "; ")
	}
	if err := j.st.SetMeta(ctx, kv); err != nil {
		j.log.Error("recording the gc cycle failed", "error", err)
	}
}

// Due decides whether a nightly cycle is owed. A cycle is owed when the most
// recent local-hour slot at or before now is newer than the last recorded
// cycle. With no recorded cycle, the first completed slot since daemon start
// triggers - a fresh installation does not wait a full day for its first
// backup.
func Due(now, last time.Time, hour int) bool {
	slot := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, time.Local)
	if slot.After(now) {
		slot = slot.AddDate(0, 0, -1)
	}
	if last.IsZero() {
		return true
	}
	return slot.After(last)
}

// ShouldRun answers Due against the recorded gc_last_cycle_at.
func (j *Janitor) ShouldRun(ctx context.Context, hour int) bool {
	raw, found, err := j.st.MetaValue(ctx, store.MetaKeyGCCycleLastAt)
	if err != nil {
		j.log.Warn("reading the last gc cycle failed; treating maintenance as due", "error", err)
		return true
	}
	var last time.Time
	if found && raw != "" {
		if parsed, parseErr := time.Parse(time.RFC3339, raw); parseErr == nil {
			last = parsed
		}
	}
	return Due(j.clk.Now(), last, hour)
}

// NightlyHourDefault is the local hour the nightly maintenance aims for
// (07 section 6.5 names 03:00).
const NightlyHourDefault = 3
