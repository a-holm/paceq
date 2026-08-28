package obs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The disk-guard's state machine (#44). Every reading is injected, so the
// hysteresis, the thresholds and the pruning order are tested without a
// filesystem and without a single wall-clock assumption: the clock is a
// fake and the disk is a function.

// fakeDisk is a statfs seam with a dial.
type fakeDisk struct {
	free  uint64
	total uint64
	err   error
}

func (f *fakeDisk) statfs(string) (uint64, uint64, error) {
	return f.free, f.total, f.err
}

// newTestGuard builds a guard on a 50 GiB disk at pct percent free.
func newTestGuard(t *testing.T, pct float64, mutate func(*GuardConfig)) (*Guard, *fakeDisk, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
	disk := &fakeDisk{free: uint64(50<<30) * uint64(pct) / 100, total: 50 << 30}
	cfg := GuardConfig{
		StateDir: t.TempDir(),
		LogDir:   filepath.Join(t.TempDir(), "logs"),
		Clock:    clk,
		Statfs:   disk.statfs,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return NewGuard(cfg), disk, clk
}

func TestThresholdsClassifyPure(t *testing.T) {
	limits := DiskLimits{MinFreePercent: 10, MinFreeBytes: 1 << 30}.WithDefaults()
	cases := []struct {
		name string
		free uint64
		want DiskState
	}{
		{"half empty", 25 << 30, DiskNormal},
		{"in the warning band", 7 << 30, DiskWarning},
		{"just over the percent floor", 5300 << 20, DiskWarning},
		{"under the percent floor", 4 << 30, DiskDegraded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyDisk(tc.free, 50<<30, limits); got != tc.want {
				t.Errorf("classify(%d GiB free) = %s, want %s", tc.free>>30, got, tc.want)
			}
		})
	}
	// The absolute floor is its own arm, independent of the percentage:
	// 10.5% free is over the percentage floor but under a 20 GiB absolute
	// one, and degraded is what protects the writes there.
	pctOnly := DiskLimits{MinFreePercent: 10, MinFreeBytes: 20 << 30}.WithDefaults()
	if got := classifyDisk(10500<<20, 100<<30, pctOnly); got != DiskDegraded {
		t.Errorf("classify above the percent floor but under the absolute one = %s, want degraded", got)
	}
}

func TestDegradedArrivesAtOnceAndLeavesAfterTwoClearReadings(t *testing.T) {
	g, disk, _ := newTestGuard(t, 19, nil) // 19% free: warning band, not degraded

	if g.Degraded() {
		t.Fatal("a guard that has not measured is degraded")
	}
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if g.State() != DiskWarning {
		t.Fatalf("19%% free on a 10%% floor is %s, want warning", g.State())
	}

	disk.free = disk.total * 8 / 100 // 8%: degraded
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if !g.Degraded() {
		t.Fatal("under the floor did not degrade the guard at once")
	}

	// One clear reading is not enough: the exit bar is one and a half
	// times the floor, twice in a row.
	disk.free = disk.total * 20 / 100 // 20% >= 15%
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if !g.Degraded() {
		t.Fatal("one clear reading lifted the degraded state; hysteresis failed")
	}
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if g.Degraded() {
		t.Fatal("two clear readings in a row did not lift the degraded state")
	}
	if free, ok := g.FreeBytes(); !ok || free <= 0 {
		t.Fatalf("the newest reading is %d (found %v), want the last statfs", free, ok)
	}
}

func TestOscillatingReadingsAroundTheFloorFlipOnce(t *testing.T) {
	g, disk, _ := newTestGuard(t, 11, nil) // just above the floor

	transitions := 0
	prev := g.State()
	observe := func(pct int) {
		disk.free = disk.total * uint64(pct) / 100
		_ = g.Step(context.Background())
		if g.State() != prev {
			transitions++
			prev = g.State()
		}
	}
	// One hundred checks swinging 9%-11%: the entry to degraded happens
	// once, and no reading pair above the 15% exit bar ever appears, so
	// the state must never leave degraded. That is the whole point of the
	// hysteresis: the daemon must not flap while retention frees space in
	// bursts.
	for i := range 100 {
		if i%2 == 0 {
			observe(9)
		} else {
			observe(11)
		}
	}
	if transitions != 1 {
		t.Fatalf("oscillating readings produced %d state changes, want exactly 1 (the entry)", transitions)
	}
	if !g.Degraded() {
		t.Fatalf("the guard ended %s, want still degraded", g.State())
	}
}

func TestByteCapPrunesOldestShardsFirstAndKeepsTheNewestTwo(t *testing.T) {
	logDir := t.TempDir()
	shards := []string{"2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28"}
	for _, name := range shards {
		dir := filepath.Join(logDir, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "step.1.ndjson"), make([]byte, 1000), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A non-date entry nobody may touch.
	if err := os.WriteFile(filepath.Join(logDir, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	var pruned []string
	g, disk, _ := newTestGuard(t, 90, func(c *GuardConfig) {
		c.LogDir = logDir
		// Five shards of 1000 bytes; the cap allows 4500: only the
		// oldest shard has to go, and nothing else may be touched.
		c.Limits.LogMaxBytes = 4500
		c.Pruner = prunerFunc(func(_ context.Context, shard string) (int64, error) {
			pruned = append(pruned, shard)
			return 0, nil
		})
	})
	disk.free = 40 << 30 // plenty of room: only the cap is at work

	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	for _, name := range shards {
		_, err := os.Stat(filepath.Join(logDir, name))
		gone := os.IsNotExist(err)
		switch name {
		case "2026-08-24":
			if !gone {
				t.Errorf("%s survived a cap that required its removal", name)
			}
		case "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28":
			if gone {
				t.Errorf("%s was removed although the cap only needed one shard", name)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(logDir, "notes.txt")); err != nil {
		t.Errorf("a non-date entry in the log root was removed: %v", err)
	}
	if len(pruned) != 1 || pruned[0] != "2026-08-24" {
		t.Errorf("the pruner was told about %v, want only 2026-08-24, oldest first", pruned)
	}
	if bytes, ok := g.LogDirBytes(); !ok || bytes != 4000 {
		t.Errorf("the cached byte count is %d (found %v), want 4000", bytes, ok)
	}
}

func TestTheCapNeverTakesTheNewestTwoShards(t *testing.T) {
	logDir := t.TempDir()
	for _, name := range []string{"2026-08-26", "2026-08-27", "2026-08-28"} {
		if err := os.MkdirAll(filepath.Join(logDir, name), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(logDir, name, "s.1.ndjson"), make([]byte, 100), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A cap nothing can satisfy: even with every shard gone the bytes
	// stay over it. The pruning must still stop at the protected pair.
	g, _, _ := newTestGuard(t, 90, func(c *GuardConfig) {
		c.LogDir = logDir
		c.Limits.LogMaxBytes = 1
	})
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if _, err := os.Stat(filepath.Join(logDir, "2026-08-27")); err != nil {
		t.Errorf("yesterday's shard was removed under an unsatisfiable cap: %v", err)
	}
	if _, err := os.Stat(filepath.Join(logDir, "2026-08-28")); err != nil {
		t.Errorf("today's shard was removed under an unsatisfiable cap: %v", err)
	}
	if _, err := os.Stat(filepath.Join(logDir, "2026-08-26")); !os.IsNotExist(err) {
		t.Errorf("the oldest shard survived an unsatisfiable cap: %v", err)
	}
}

func TestLogByteCountMeasuresOnlyTheGrowingShardAfterTheFirstCycle(t *testing.T) {
	logDir := t.TempDir()
	for _, name := range []string{"2026-08-27", "2026-08-28"} {
		if err := os.MkdirAll(filepath.Join(logDir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	measured := map[string]int{}
	g, _, _ := newTestGuard(t, 90, func(c *GuardConfig) {
		c.LogDir = logDir
		c.SizeOfShard = func(dir string) (int64, bool) {
			measured[filepath.Base(dir)]++
			return 10, true
		}
	})
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("first step: %v", err)
	}
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("second step: %v", err)
	}
	// The first cycle measures both shards; every later cycle re-measures
	// only the newest one, because the older ones cannot grow any more.
	// That is the incremental counting the cycle budget leans on: a
	// directory of 100 000 files costs one walk of today's shard, not the
	// whole tree, every thirty seconds.
	for name, n := range measured {
		if name == "2026-08-28" && n != 2 {
			t.Errorf("the newest shard was measured %d times over two cycles, want 2", n)
		}
		if name == "2026-08-27" && n != 1 {
			t.Errorf("an old shard was re-measured %d times, want 1 (cache hit afterwards)", n)
		}
	}
}

func TestAStatfsFailureKeepsThePreviousState(t *testing.T) {
	g, disk, _ := newTestGuard(t, 50, nil)
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	disk.err = errors.New("broken disk reading")
	if err := g.Step(context.Background()); err != nil {
		t.Fatalf("a statfs failure is not a fatal cycle: %v", err)
	}
	if g.State() != DiskNormal {
		t.Fatalf("after an unreadable disk the state is %s, want the previous normal", g.State())
	}
}

// prunerFunc adapts a function to the ShardPruner seam.
type prunerFunc func(ctx context.Context, shard string) (int64, error)

func (f prunerFunc) MarkLogShardPruned(ctx context.Context, shard string) (int64, error) {
	return f(ctx, shard)
}
