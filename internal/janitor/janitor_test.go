package janitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// shaOfFile hashes a file so byte-identity can be asserted across a dry-run.
func shaOfFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// migratedTempStore opens a real, migrated database in a temp directory and
// closes it when the test ends.
func migratedTempStore(t *testing.T) *store.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(context.Background(), path, store.Options{})
	if err != nil {
		t.Fatalf("open test store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close test store: %v", err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate test store: %v", err)
	}
	return s
}

// newTestJanitor builds a janitor over a real store with the shipped defaults.
func newTestJanitor(t *testing.T, s *store.Store, logRoot, backupDir string) *Janitor {
	t.Helper()
	return New(Config{
		Store:     s,
		Clock:     clock.System(),
		LogRoot:   logRoot,
		BackupDir: backupDir,
	})
}

// TestRetentionNeverHoldsTheWriteLockPastFiftyMilliseconds is the acceptance
// measurement of #36, not an assumption: tens of thousands of terminal runs
// with children are drained batch by batch while another store handle - a
// second connection to the same file, the way a rival process would look -
// fights for the write lock every ten milliseconds. Two gates must both hold
// when the dust settles: the janitor's own per-batch histogram never passes
// fifty milliseconds, and the competing writer's waits neither breach the
// same budget nor fail once.
//
// Scale note: sixty thousand runs means one hundred twenty real batches, so
// the histogram carries a genuine distribution rather than a handful of
// lucky samples. The cost curve itself (flat in table size, linear in
// deleted rows) is what makes that scaling legitimate; the naive shape this
// replaced held the lock 2.2 seconds per batch on this very fixture.
func TestRetentionNeverHoldsTheWriteLockPastFiftyMilliseconds(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds and drains sixty thousand runs; too slow for -short")
	}

	// Same convention as the store's admission-budget gate: the race
	// detector multiplies every transaction's cost, and the gate measures
	// the same work either way.
	budget := lockHoldBudget
	if raceEnabled {
		budget = 250 * time.Millisecond
	}

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(ctx, dbPath, store.Options{})
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate test store: %v", err)
	}

	const (
		runCount = 60_000
		job      = "load"
	)
	// Everything lands past the 90 day horizon so retention has real work;
	// the keep-minimum still shields the newest fifty.
	now := time.Now().UTC().AddDate(0, 0, -(store.DefaultPolicies().RunsDays + 1))
	version := seedOneJobVersion(t, ctx, s, job)
	if err := s.SeedOldFinishedRuns(ctx, job, version, runCount, 1, now); err != nil {
		t.Fatalf("seed %d old runs: %v", runCount, err)
	}
	before, err := s.CountRows(ctx, "runs")
	if err != nil {
		t.Fatalf("count runs before: %v", err)
	}
	if before != runCount {
		t.Fatalf("seeded %d runs, table says %d", runCount, before)
	}

	j := newTestJanitor(t, s, t.TempDir(), "")

	// The rival opens its own handle to the SAME file, exactly as a second
	// process would. Its smallest write is a meta upsert every ten
	// milliseconds; its duration is lock wait plus a statement, which is
	// what a blocked writer actually feels.
	rival, err := store.Open(ctx, dbPath, store.Options{})
	if err != nil {
		t.Fatalf("open rival handle: %v", err)
	}
	defer func() { _ = rival.Close() }()

	var (
		waits    []time.Duration
		waitsMu  sync.Mutex
		fail     atomic.Int64
		firstErr atomic.Value // string
		stop     = make(chan struct{})
		done     = make(chan struct{})
	)
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				start := time.Now()
				err := rival.SetMeta(ctx, map[string]string{"retention_probe": "x"})
				waited := time.Since(start)
				if err != nil {
					fail.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
					continue
				}
				waitsMu.Lock()
				waits = append(waits, waited)
				waitsMu.Unlock()
			}
		}
	}()

	res, err := j.Prune(ctx)
	close(stop)
	<-done
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.Deleted.Total() == 0 {
		t.Fatal("the pass deleted nothing; the measurement would prove nothing")
	}

	if n := fail.Load(); n != 0 {
		msg, _ := firstErr.Load().(string)
		t.Fatalf("the rival writer failed %d times under retention (first error: %s)", n, msg)
	}
	waitsMu.Lock()
	samples := append([]time.Duration(nil), waits...)
	waitsMu.Unlock()
	if len(samples) < 50 {
		t.Fatalf("only %d writer samples; the rival did not really contend", len(samples))
	}
	sortDurations(samples)
	writerP99 := samples[int(0.99*float64(len(samples)-1))]

	hold := j.Hold()
	if !hold.Under(budget) {
		t.Fatalf("janitor held the write lock past the budget: %d batches, "+
			"max %v, p99 %v, p50 %v (budget %v)",
			hold.Samples, hold.Max, hold.P99, hold.P50, budget)
	}
	if writerP99 >= budget {
		t.Fatalf("rival writer p99 wait %v breaches the %v budget (%d samples)",
			writerP99, budget, len(samples))
	}

	after, err := s.CountRows(ctx, "runs")
	if err != nil {
		t.Fatalf("count runs after: %v", err)
	}
	if after != int64(store.DefaultPolicies().RunsKeepMin) {
		t.Fatalf("keep-minimum left %d runs, want %d", after, store.DefaultPolicies().RunsKeepMin)
	}
}

// lockHoldBudget is the number the acceptance criterion names.
const lockHoldBudget = 50 * time.Millisecond

func seedOneJobVersion(t *testing.T, ctx context.Context, s *store.Store, name string) string {
	t.Helper()
	jv, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:       name,
		SpecHash:      "sha256:" + name,
		SpecJSON:      `{"schema":"paceq.job.v1","name":"` + name + `","steps":[{"name":"build","run":["/bin/true"]}]}`,
		MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatalf("upsert job version for %s: %v", name, err)
	}
	return jv.ID
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for k := i; k > 0 && d[k] < d[k-1]; k-- {
			d[k], d[k-1] = d[k-1], d[k]
		}
	}
}

// TestPrunePlansEstimatesWithoutTouchingAnything pins the dry-run promise:
// the plan reports exactly what a real pass would delete, and the database
// file on disk is bit-identical afterwards.
func TestPrunePlansEstimatesWithoutTouchingAnything(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	s, err := store.Open(ctx, dbPath, store.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	version := seedOneJobVersion(t, ctx, s, "quarterly")
	if err := s.SeedOldFinishedRuns(ctx, "quarterly", version, 300, 2,
		time.Now().UTC().AddDate(0, 0, -(store.DefaultPolicies().RunsDays+1))); err != nil {
		t.Fatalf("seed: %v", err)
	}

	j := newTestJanitor(t, s, filepath.Join(filepath.Dir(dbPath), "logs"), "")

	plan, err := j.PrunePlans(ctx)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	want := int64(300 - store.DefaultPolicies().RunsKeepMin)
	if plan.Runs != want {
		t.Fatalf("plan estimates %d deletable runs, want %d", plan.Runs, want)
	}
	if plan.Total() != plan.Runs {
		t.Fatalf("plan claims other rules deleted rows in an empty database: %+v", plan.RetentionPlan)
	}

	before := shaOfFile(t, dbPath)
	if _, err := j.PrunePlans(ctx); err != nil {
		t.Fatalf("second plan: %v", err)
	}
	if after := shaOfFile(t, dbPath); before != after {
		t.Fatal("a dry-run changed the database file")
	}

	res, err := j.Prune(ctx)
	if err != nil {
		t.Fatalf("real prune: %v", err)
	}
	if res.Deleted.Runs != want {
		t.Fatalf("real pass deleted %d runs, plan promised %d", res.Deleted.Runs, want)
	}
	// Two hundred rows per batch means two deleting batches for 250 rows,
	// then the probe that comes back empty - plus one empty probe per
	// remaining rule.
	if res.Batches != 7 {
		t.Fatalf("pass made %d batches, want 3 for runs plus one per idle rule", res.Batches)
	}
}

// TestLogShardsGoWholeDirectories pins the RemoveAll contract: only date-named
// directories past the horizon go, unknown names stay, and recent shards stay.
func TestLogShardsGoWholeDirectories(t *testing.T) {
	s := migratedTempStore(t)
	ctx := context.Background()
	root := t.TempDir()

	old := time.Now().UTC().AddDate(0, 0, -30).Format(dateShardLayout)
	recent := time.Now().UTC().Format(dateShardLayout)
	for name, files := range map[string][]string{
		old:          {"a.ndjson", "b.ndjson"},
		recent:       {"c.ndjson"},
		"not-a-date": {"keep.txt"},
	} {
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	j := newTestJanitor(t, s, root, "")
	if _, err := j.Prune(ctx); err != nil {
		t.Fatalf("prune: %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, old)); !os.IsNotExist(err) {
		t.Fatalf("stale shard survived: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, recent)); err != nil {
		t.Fatalf("recent shard removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "not-a-date")); err != nil {
		t.Fatalf("unknown entry removed: %v", err)
	}
}

// TestCycleRecordsItsOutcomeForDoctor covers the meta side of the nightly
// sequence: a clean cycle stamps gc keys, and the backup status reads back.
func TestCycleRecordsItsOutcomeForDoctor(t *testing.T) {
	s := migratedTempStore(t)
	ctx := context.Background()
	bdir := t.TempDir()

	j := newTestJanitor(t, s, t.TempDir(), bdir)
	res, err := j.Cycle(ctx)
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}
	if len(res.Failures) != 0 {
		t.Fatalf("clean database produced failures: %v", res.Failures)
	}
	if res.Checkpoint != "ran" {
		t.Fatalf("idle database should truncate the WAL, got %q", res.Checkpoint)
	}
	if res.Backup.Status != store.BackupStatusVerified || res.Backup.Path == "" {
		t.Fatalf("backup did not verify: %+v", res.Backup)
	}
	info, err := s.BackupStatus(ctx)
	if err != nil {
		t.Fatalf("backup status: %v", err)
	}
	if !info.HasBackup || !info.Verified() {
		t.Fatalf("meta does not report a verified backup: %+v", info)
	}
	status, found, err := s.MetaValue(ctx, store.MetaKeyGCCycleLastStatus)
	if err != nil || !found || status != "ok" {
		t.Fatalf("gc status meta = %q found=%v err=%v", status, found, err)
	}

	generations, err := filepath.Glob(filepath.Join(bdir, "state-*.db"))
	if err != nil || len(generations) != 1 {
		t.Fatalf("want exactly one generation, got %v (%v)", generations, err)
	}
}

// TestFailingBackupVerificationDestroysTheCopy pins the rule that an
// unverified backup is a failed backup, not a warning: corrupting the copy
// must remove the file and record the failure in meta.
func TestFailingBackupVerificationDestroysTheCopy(t *testing.T) {
	s := migratedTempStore(t)
	ctx := context.Background()
	bdir := t.TempDir()

	j := newTestJanitor(t, s, t.TempDir(), bdir)

	// Sabotage every copy the moment VACUUM INTO produces it: verifyCopy
	// opens the file fresh, so garbage in the header region fails quick_check.
	original := j.st // keep for restore below
	saboteur := &backupSabotageStore{Store: original, dir: bdir}
	j.st = saboteur

	out := j.backup(ctx)
	if out.Status != store.BackupStatusFailed {
		t.Fatalf("corrupt copy passed verification: %+v", out)
	}
	matches, _ := filepath.Glob(filepath.Join(bdir, "state-*.db"))
	if len(matches) != 0 {
		t.Fatalf("failed copies must be removed, still have %v", matches)
	}
	info, err := s.BackupStatus(ctx)
	if err != nil {
		t.Fatalf("read backup status: %v", err)
	}
	if info.HasBackup && info.Verified() {
		t.Fatal("meta must not claim a verified backup after failed verification")
	}
	if !info.HasBackup || info.Status != store.BackupStatusFailed {
		t.Fatalf("meta must record the failure, got %+v", info)
	}
}

// backupSabotageStore corrupts the copy between VacuumInto and verification.
type backupSabotageStore struct {
	Store
	dir string
}

func (b *backupSabotageStore) VacuumInto(ctx context.Context, dst string) error {
	if err := b.Store.VacuumInto(ctx, dst); err != nil {
		return err
	}
	f, err := os.OpenFile(dst, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt([]byte("garbage-not-sqlite"), 100)
	return err
}

var _ Store = (*backupSabotageStore)(nil)

// TestRotationKeepsExactlyRetainGenerations pins the generation count.
func TestRotationKeepsExactlyRetainGenerations(t *testing.T) {
	dir := t.TempDir()
	for i := range 20 {
		name := filepath.Join(dir, fmt.Sprintf("state-%03d.db", i))
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := rotateGenerations(dir, "state-*.db", 14); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	left, err := filepath.Glob(filepath.Join(dir, "state-*.db"))
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 14 {
		t.Fatalf("rotation kept %d generations, want 14", len(left))
	}
	if left[0] != filepath.Join(dir, "state-006.db") {
		t.Fatalf("oldest survivor is %s, want state-006.db", left[0])
	}
}

// TestNightlyDueHonoursTheLastCycle pins the slot arithmetic around Due: a
// fresh installation runs at once, a daemon that ran inside today's slot
// waits for tomorrow's, and one from yesterday's slot runs tonight.
func TestNightlyDueHonoursTheLastCycle(t *testing.T) {
	now := time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)
	slotToday := time.Date(2026, 8, 25, 3, 0, 0, 0, time.UTC)

	if !Due(now, time.Time{}, NightlyHourDefault) {
		t.Fatal("a fresh installation must not wait a day for its first cycle")
	}
	if Due(now, slotToday.Add(time.Minute), NightlyHourDefault) {
		t.Fatal("a cycle inside today's slot must suppress tonight's rerun")
	}
	if !Due(now, slotToday.AddDate(0, 0, -1), NightlyHourDefault) {
		t.Fatal("yesterday's cycle must make tonight's slot due")
	}
}
