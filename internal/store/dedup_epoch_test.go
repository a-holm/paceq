package store

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The dedup epoch half of M3-04 (issue #6). The run_keys gate itself, the
// insert-first discipline and the deduped trigger row all landed with M3-03
// (#4) and are pinned by sensor_tick_test.go and core_schema_test.go. What
// these tests add is the epoch as something an operator can move: a reset
// bumps the epoch so the same run keys become new again, a cursor set moves
// the cursor without touching the epoch so old keys stay deduped, and both
// happen with one atomic decision in the store.

const resetSensor = "reset-sensor"

// commitKeys commits one triggered tick for the reset sensor with the given
// run keys and epoch, and reports the result.
func commitKeys(t *testing.T, s *Store, tickID string, cursorVersion, epoch int64, runKeys ...string) SensorTickCommitResult {
	t.Helper()
	triggers := make([]SensorTrigger, 0, len(runKeys))
	for _, k := range runKeys {
		triggers = append(triggers, SensorTrigger{RunKey: k})
	}
	out, err := s.CommitSensorTick(context.Background(), SensorTickCommitInput{
		TickID:        tickID,
		SensorName:    resetSensor,
		JobName:       sensorJob,
		CursorVersion: cursorVersion,
		CursorAfter:   "b",
		DedupEpoch:    epoch,
		Triggers:      triggers,
		Outcome:       OutcomeTriggered,
		NextEvalAt:    60000,
	})
	if err != nil {
		t.Fatalf("commit sensor tick: %v", err)
	}
	return out
}

// readResetSensor returns the dedup_epoch and cursor of one sensor.
func readResetSensor(t *testing.T, s *Store, name string) (epoch int64, cursor string) {
	t.Helper()
	var c sql.NullString
	if err := s.r.QueryRowContext(context.Background(),
		"SELECT dedup_epoch, cursor FROM sensors WHERE name = ?", name).Scan(&epoch, &c); err != nil {
		t.Fatalf("read sensor %s: %v", name, err)
	}
	return epoch, c.String
}

// runKeyCount counts the run_keys rows for one source id.
func runKeyCount(t *testing.T, s *Store, name string) int {
	t.Helper()
	var n int
	if err := s.r.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM run_keys WHERE source_id = ?", name).Scan(&n); err != nil {
		t.Fatalf("count run_keys for %s: %v", name, err)
	}
	return n
}

// TestDedupEpochBumpsOnReset makes the operator's decision: a reset raises the
// epoch, and the same run key that was a fingerprint against the old epoch
// becomes a new one in the new epoch. This is the "reset gives replay" half of
// the product sentence (50 files, 50 runs; rerun, 0; reset, 50 new).
func TestDedupEpochBumpsOnReset(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, resetSensor, "a", 0)
	ctx := context.Background()

	epoch, _ := readResetSensor(t, s, resetSensor)
	if epoch != 0 {
		t.Fatalf("seed epoch = %d, want 0", epoch)
	}

	// One run at epoch 0.
	b1, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out := commitKeys(t, s, b1.TickID, b1.CursorVersion, 0, "file:1"); out.Accepted != 1 {
		t.Fatalf("first commit accepted %d, want 1", out.Accepted)
	}
	if got := countSensorRuns(t, s); got != 1 {
		t.Fatalf("sensor runs before reset = %d, want 1", got)
	}

	// Reset: epoch bumps, and the result reports exactly the old and new
	// epoch (the value the M3-06 CLI will print), not just the bump happened.
	res, err := s.ResetSensor(ctx, ResetSensorInput{Name: resetSensor})
	if err != nil {
		t.Fatalf("reset sensor: %v", err)
	}
	if res.OldEpoch != 0 || res.NewEpoch != 1 || res.Sensor != resetSensor {
		t.Fatalf("reset result = old %d new %d sensor %s, want 0/1/%s",
			res.OldEpoch, res.NewEpoch, res.Sensor, resetSensor)
	}
	after, _ := readResetSensor(t, s, resetSensor)
	if after != 1 {
		t.Fatalf("epoch after reset = %d, want 1 (one bump)", after)
	}

	// The same key in the new epoch is a fresh fingerprint, not a replay.
	b2, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin after reset: %v", err)
	}
	out, err2 := s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        b2.TickID,
		SensorName:    resetSensor,
		JobName:       sensorJob,
		CursorVersion: b2.CursorVersion,
		CursorAfter:   "d",
		DedupEpoch:    after,
		Triggers:      []SensorTrigger{{RunKey: "file:1"}},
		Outcome:       OutcomeTriggered,
		NextEvalAt:    60000,
	})
	if err2 != nil {
		t.Fatalf("commit after reset: %v", err2)
	}
	if out.Accepted != 1 || out.Deduped != 0 {
		t.Fatalf("after reset = accepted %d deduped %d, want a fresh run (1/0)", out.Accepted, out.Deduped)
	}
	if got := countSensorRuns(t, s); got != 2 {
		t.Fatalf("sensor runs after reset = %d, want 2 (one new, not a replay)", got)
	}
}

// TestDedupEpochStaysOnCursorMove pins F4c: the cursor and the dedup key are
// two independent notions with two independent reset flags. Moving the cursor
// alone must leave the epoch untouched, and the old run keys keep dedupping.
func TestDedupEpochStaysOnCursorMove(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, resetSensor, "a", 0)
	ctx := context.Background()

	b1, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out := commitKeys(t, s, b1.TickID, b1.CursorVersion, 0, "file:1"); out.Accepted != 1 {
		t.Fatalf("accepted %d, want 1", out.Accepted)
	}
	if got := countSensorRuns(t, s); got != 1 {
		t.Fatalf("runs before cursor move = %d, want 1", got)
	}

	// Move the cursor without touching the epoch.
	if err := s.SetSensorCursor(ctx, CursorInput{Name: resetSensor, Cursor: "c"}); err != nil {
		t.Fatalf("set sensor cursor: %v", err)
	}
	epoch, _ := readResetSensor(t, s, resetSensor)
	if epoch != 0 {
		t.Fatalf("epoch after cursor move = %d, want 0 (a cursor move is not a reset)", epoch)
	}

	// A replay of the same key after the cursor move must still dedup.
	b2, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "c"})
	if err != nil {
		t.Fatalf("begin replay: %v", err)
	}
	out, err2 := s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        b2.TickID,
		SensorName:    resetSensor,
		JobName:       sensorJob,
		CursorVersion: b2.CursorVersion,
		CursorAfter:   "c",
		DedupEpoch:    0,
		Triggers:      []SensorTrigger{{RunKey: "file:1"}},
		Outcome:       OutcomeTriggered,
		NextEvalAt:    60000,
	})
	if err2 != nil {
		t.Fatalf("commit replay: %v", err2)
	}
	if out.Accepted != 0 || out.Deduped != 1 {
		t.Fatalf("after cursor move = accepted %d deduped %d, want a replay (0/1)", out.Accepted, out.Deduped)
	}
	if got := countSensorRuns(t, s); got != 1 {
		t.Fatalf("runs after cursor move replay = %d, want 1 (no new run)", got)
	}
}

// TestDedupReplaySameTickCollapsesToOneRun pins the intra-tick dedup: the same
// run key twice inside one evaluation collapses to exactly one run, not two.
func TestDedupReplaySameTickCollapsesToOneRun(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, resetSensor, "a", 0)

	b, err := s.BeginSensorTick(context.Background(), BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	out, err2 := s.CommitSensorTick(context.Background(), SensorTickCommitInput{
		TickID:        b.TickID,
		SensorName:    resetSensor,
		JobName:       sensorJob,
		CursorVersion: b.CursorVersion,
		CursorAfter:   "a",
		DedupEpoch:    0,
		Triggers:      []SensorTrigger{{RunKey: "dup:1"}, {RunKey: "dup:1"}, {RunKey: "dup:1"}},
		Outcome:       OutcomeTriggered,
		NextEvalAt:    60000,
	})
	if err2 != nil {
		t.Fatalf("commit: %v", err2)
	}
	if out.Accepted != 1 || out.Deduped != 2 || len(out.RunIDs) != 1 {
		t.Fatalf("intra-tick = accepted %d deduped %d runs %d, want 1/2/1", out.Accepted, out.Deduped, len(out.RunIDs))
	}
	if got := countSensorRuns(t, s); got != 1 {
		t.Fatalf("runs after intra-tick duplicates = %d, want exactly 1", got)
	}
}

// TestDedupManyKeysReplayResetIsTheProductSentence pins the headline sentence
// at store level: N run keys give N runs; the same keys replayed in the same
// epoch give 0 new runs, all deduped; after a reset the same keys give the N
// runs again. This is the M3 demo without the evaluator process.
func TestDedupManyKeysReplayResetIsTheProductSentence(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, resetSensor, "a", 0)
	ctx := context.Background()

	keys := make([]string, 0, 50)
	for i := 1; i <= 50; i++ {
		keys = append(keys, "file:"+string(rune(64+i)))
	}

	// First evaluation: 50 keys, 50 runs, 0 deduped.
	b1, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	out1 := commitKeys(t, s, b1.TickID, b1.CursorVersion, 0, keys...)
	if out1.Accepted != 50 || out1.Deduped != 0 {
		t.Fatalf("first pass = accepted %d deduped %d, want 50/0", out1.Accepted, out1.Deduped)
	}
	if got := countSensorRuns(t, s); got != 50 {
		t.Fatalf("runs after first pass = %d, want 50", got)
	}

	// Immediate rerun, same epoch: 0 new, 50 deduped.
	b2, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "b"})
	if err != nil {
		t.Fatalf("begin rerun: %v", err)
	}
	out2 := commitKeys(t, s, b2.TickID, b2.CursorVersion, 0, keys...)
	if out2.Accepted != 0 || out2.Deduped != 50 {
		t.Fatalf("rerun = accepted %d deduped %d, want 0/50", out2.Accepted, out2.Deduped)
	}
	if got := countSensorRuns(t, s); got != 50 {
		t.Fatalf("runs after rerun = %d, want still 50 (no new)", got)
	}

	// Reset bumps the epoch; the same keys become new fingerprints.
	if _, err := s.ResetSensor(ctx, ResetSensorInput{Name: resetSensor}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	epoch, _ := readResetSensor(t, s, resetSensor)
	b3, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "b"})
	if err != nil {
		t.Fatalf("begin after reset: %v", err)
	}
	out3 := commitKeys(t, s, b3.TickID, b3.CursorVersion, epoch, keys...)
	if out3.Accepted != 50 || out3.Deduped != 0 {
		t.Fatalf("after reset = accepted %d deduped %d, want 50/0", out3.Accepted, out3.Deduped)
	}
	if got := countSensorRuns(t, s); got != 100 {
		t.Fatalf("runs after reset replay = %d, want 100 (50 old + 50 new)", got)
	}

	// The sentence is only true if the database it leaves behind can be
	// served. A reset that replays into a state the boot gate calls critical
	// corruption is not a feature, so the product test owns the sweep too.
	requireFsckIsQuiet(t, s, "after the reset replay")
}

// requireFsckIsQuiet runs both sweeps, the operator's and the boot gate's,
// and fails on anything above a warning. Warnings alone exit paceq fsck 0;
// a serious finding exits 1, and a critical one also refuses the daemon's
// start with PSQ-FSCK-001.
func requireFsckIsQuiet(t *testing.T, s *Store, when string) {
	t.Helper()
	ctx := context.Background()
	full, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck %s: %v", when, err)
	}
	for _, v := range full {
		if v.Severity > Warning {
			t.Errorf("fsck %s reports %s %s (%s): %s", when, v.Severity, v.Check, v.Subject, v.Detail)
		}
	}
	quick, err := s.QuickFsck(ctx)
	if err != nil {
		t.Fatalf("quick fsck %s: %v", when, err)
	}
	for _, v := range quick {
		if v.Severity == Critical {
			t.Errorf("the boot gate %s refuses on %s (%s): %s", when, v.Check, v.Subject, v.Detail)
		}
	}
}

// TestAPrunedRunKeyRefiresWithoutACriticalFinding is the retention route to
// the same shape, with no reset involved. run_keys are pruned by age with no
// keep-minimum while runs keep a per-job minimum for ever, so a source that
// re-presents an old unit of work fires a second run under a run key whose
// first run is still there, inside one epoch. The runbook documents that
// re-firing as the price of a bounded table, so the sweep must not call it
// corruption.
func TestAPrunedRunKeyRefiresWithoutACriticalFinding(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, resetSensor, "a", 0)
	ctx := context.Background()

	b1, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "a"})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if out := commitKeys(t, s, b1.TickID, b1.CursorVersion, 0, "file:old"); out.Accepted != 1 {
		t.Fatalf("first pass accepted %d, want 1", out.Accepted)
	}

	// Retention deletes the key by age. The run it produced is younger than
	// the key's horizon and survives.
	if _, err := s.PruneRunKeysBatch(ctx, s.clk.Now().Add(24*time.Hour)); err != nil {
		t.Fatalf("prune the run keys: %v", err)
	}
	if got := runKeyCount(t, s, resetSensor); got != 0 {
		t.Fatalf("%d run keys survived the prune, want 0", got)
	}

	// The source re-presents the same unit of work in the SAME epoch.
	b2, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "b"})
	if err != nil {
		t.Fatalf("begin the refire: %v", err)
	}
	if out := commitKeys(t, s, b2.TickID, b2.CursorVersion, 0, "file:old"); out.Accepted != 1 {
		t.Fatalf("the refire accepted %d, want 1: the pruned key must fire again", out.Accepted)
	}
	if got := countSensorRuns(t, s); got != 2 {
		t.Fatalf("runs after the refire = %d, want 2", got)
	}

	requireFsckIsQuiet(t, s, "after a pruned key refired")
}

// TestResetCrashLeavesEitherOldOrNewEpoch proves a reset is atomic. A child
// kills itself inside ResetSensor after the epoch UPDATE bumped but before the
// transaction commits. The whole transaction must roll back: a crash mid reset
// leaves the old epoch and the old run keys, never a half-moved reset.
func TestResetCrashLeavesEitherOldOrNewEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reset.db")
	cmd := exec.Command(os.Args[0], "-test.run=TestResetCrashChild", "-test.count=1")
	cmd.Env = append(os.Environ(), "PACEQ_SENSOR_RESET_DB="+path)
	out, err := cmd.CombinedOutput()

	var status *exec.ExitError
	if !asExitError(err, &status) || status.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child exited with %v, want death by SIGKILL\n%s", err, out)
	}

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("reopen the crashed database: %v", err)
	}
	defer func() { _ = s.Close() }()

	// The whole reset rolled back: epoch never bumped, keys never forgotten.
	epoch, _ := readResetSensor(t, s, resetSensor)
	if epoch != 0 {
		t.Fatal("epoch survived the mid-reset crash, want 0 (rolled back)")
	}
	if keys := runKeyCount(t, s, resetSensor); keys != 0 {
		t.Error("run_keys survived the mid-reset crash, want 0 (rolled back)")
	}
}

// TestResetCrashChild is the child half of the crash test. It runs only under
// the parent, which sets the reset database path and expects a SIGKILL on the
// hook set below.
func TestResetCrashChild(t *testing.T) {
	path := os.Getenv("PACEQ_SENSOR_RESET_DB")
	if path == "" {
		t.Skip("driven by TestResetCrashLeavesEitherOldOrNewEpoch")
	}

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	seedSensorJob(t, s)
	seedSensor(t, s, resetSensor, "a", 0)

	// The half of the reset test runs only under the parent. Kill after the
	// cursor bump, before commit.
	sensorResetHook = func() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) }
	_, _ = s.ResetSensor(context.Background(), ResetSensorInput{Name: resetSensor, ForgetRunKeys: true})
	t.Fatal("the child survived its own SIGKILL")
}

// TestDedupConcurrentCommitsNeverSplitOneRun is the concurrency proof: two
// commits both seeing the same run key must never both create a run and never
// come back SQLITE_BUSY. The single-writer store plus the ON CONFLICT gate
// collapse them to one run and one deduped sibling.
func TestDedupConcurrentCommitsNeverSplitOneRun(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, resetSensor, "a", 0)
	ctx := context.Background()

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			begin, err := s.BeginSensorTick(ctx, BeginSensorTickInput{SensorName: resetSensor, CursorBefore: "a"})
			if err != nil {
				errs <- err
				return
			}
			if _, err := s.CommitSensorTick(ctx, SensorTickCommitInput{
				TickID:        begin.TickID,
				SensorName:    resetSensor,
				JobName:       sensorJob,
				CursorVersion: begin.CursorVersion,
				CursorAfter:   "b",
				DedupEpoch:    0,
				Triggers:      []SensorTrigger{{RunKey: "file:1"}},
				Outcome:       OutcomeTriggered,
				NextEvalAt:    60000,
			}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	for {
		select {
		case e := <-errs:
			t.Errorf("a concurrent commit failed: %v", e)
		default:
			goto done
		}
	}
done:
	if got := countSensorRuns(t, s); got != 1 {
		t.Fatalf("runs after %d concurrent commits = %d, want exactly 1", workers, got)
	}
}
