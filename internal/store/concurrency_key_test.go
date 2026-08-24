package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
)

// The concurrency key policy (#17). Creation is insert-first against the
// partial unique index ux_runs_conc_key: the insert result IS the conflict
// signal, and there is no count anywhere ahead of it. On a conflict the job's
// on_conflict policy decides: defer (the default) stores the loser queued but
// KEYLESS, naming the wanted key and the blocking run in reason_data; skip
// stores nothing and rejects the trigger.
//
// These tests pin the chosen index model: keyless deferral, keyed start.

// concApply applies a job whose spec carries exactly the given key and policy,
// so every assertion below reads its configuration from one place.
func concApply(t *testing.T, s *Store, name string, key *spec.ConcurrencyKey, onConflict string) {
	t.Helper()

	job := &spec.Job{
		Name:           name,
		MaxConcurrent:  4,
		Steps:          []spec.Step{{Name: "build", Run: []string{"/bin/true"}}},
		ConcurrencyKey: key,
	}
	if onConflict != "" {
		job.OnConflict = onConflict
	}
	h := spec.Compile(job)
	maxConcurrent := job.MaxConcurrent
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:       name,
		SpecHash:      h.Hash,
		SpecJSON:      string(h.Canonical),
		SourcePath:    "jobs/" + name + ".yaml",
		MaxConcurrent: maxConcurrent,
	}); err != nil {
		t.Fatalf("apply job %s: %v", name, err)
	}
}

func constKey(v string) *spec.ConcurrencyKey { return &spec.ConcurrencyKey{Constant: v} }

func TestAKeyedRunIsBornHoldingItsCanonicalKey(t *testing.T) {
	s := migratedStore(t)
	concApply(t, s, "keyed", constKey("k1"), "")
	row := concSchedule(t, s, "keyed")

	res := admitTick(t, s, row, 0)
	want := "keyed:k1"
	if res.Run.ConcurrencyKey != want {
		t.Fatalf("the run carries key %q, want %q", res.Run.ConcurrencyKey, want)
	}
	if res.Run.State != "queued" || res.Run.DeferReason != "" {
		t.Fatalf("an uncontested run must be plain queued, got %+v", res.Run)
	}
}

// The drop-index probe (AC 3): with the index gone, a plain writer puts two
// active runs on one key without a sound, which proves the index was the
// enforcement all along. With the index back, the same write is refused.
func TestDropTheIndexAndTheInvariantBreaks(t *testing.T) {
	s := migratedStore(t)
	concApply(t, s, "probe", constKey("db"), "")

	first := admitTick(t, s, concSchedule(t, s, "probe"), 0)
	if first.Run.ConcurrencyKey != "probe:db" {
		t.Fatalf("setup: the first run holds %q", first.Run.ConcurrencyKey)
	}

	dropIndex(t, s)
	keyedQueuedRun(t, s, "probe", "01J00000000000000000000002", "probe:db")

	// Both rows go terminal, which takes them out of the predicate and lets
	// the index come back.
	if _, err := s.w.ExecContext(context.Background(),
		`UPDATE runs SET state = 'succeeded', finished_at = ?,
		 reason_code = ?, updated_at = ?
		 WHERE job_name = 'probe'`, time.Now().UnixMilli(),
		string(reason.RUNSucceeded), time.Now().UnixMilli()); err != nil {
		t.Fatalf("close the probe runs: %v", err)
	}
	restoreIndex(t, s)

	// One fresh holder, and the next write on the same key is refused.
	keyedQueuedRun(t, s, "probe", "01J00000000000000000000003", "probe:db")
	var err error
	func() {
		defer func() { _ = recover() }()
		_, err = s.w.ExecContext(context.Background(), keyedInsertStmt,
			"01J00000000000000000000004", "probe", readVersion(t, s, "probe"),
			time.Now().UnixMilli(), time.Now().UnixMilli(), time.Now().UnixMilli(), "probe:db")
	}()
	if err == nil || !strings.Contains(err.Error(), "UNIQUE") {
		t.Fatalf("with the index back, the third write on one key was not refused: %v", err)
	}
}

const keyedInsertStmt = `INSERT INTO runs (id, job_name, job_version_id, origin, state,
available_at, created_at, updated_at, concurrency_key)
VALUES (?, ?, ?, 'schedule', 'queued', ?, ?, ?, ?)`

func readVersion(t *testing.T, s *Store, job string) string {
	t.Helper()
	var versionID string
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT current_version_id FROM jobs WHERE name = ?`, job).Scan(&versionID); err != nil {
		t.Fatalf("read the current version of %s: %v", job, err)
	}
	return versionID
}

// keyedQueuedRun plants a queued DUE run carrying a key, the way an
// uncontested materialisation would have left it.
func keyedQueuedRun(t *testing.T, s *Store, job, id, key string) {
	t.Helper()
	now := time.Now().UnixMilli()
	if _, err := s.w.ExecContext(context.Background(), keyedInsertStmt,
		id, job, readVersion(t, s, job), now, now, now, key); err != nil {
		t.Fatalf("seed a keyed queued run on %s: %v", job, err)
	}
}

func TestDeferPolicyStoresAKeylessDeferredRunNamingTheBlocker(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	concApply(t, s, "waiter", constKey("k"), "")
	row := concSchedule(t, s, "waiter")

	holder := admitTick(t, s, row, 0)
	loser := admitTick(t, s, row, 1)

	run := loser.Run
	if run.ID == "" {
		t.Fatal("a deferred conflict must still create the run row")
	}
	if run.State != "queued" {
		t.Fatalf("state %q, want queued (deferred is not a state)", run.State)
	}
	if run.ConcurrencyKey != "" {
		t.Fatalf("the deferred run carries key %q: holding the key while blocked would deadlock against itself", run.ConcurrencyKey)
	}
	if !run.AvailableAt.After(s.clk.Now()) {
		t.Fatalf("available_at %v is not in the future", run.AvailableAt)
	}
	if run.DeferReason != model.DeferReasonConcurrencyKey {
		t.Fatalf("defer_reason %q, want %q", run.DeferReason, model.DeferReasonConcurrencyKey)
	}
	if run.ReasonCode != string(reason.RUNDeferredConcurrencyKey) {
		t.Fatalf("reason_code %q, want %q", run.ReasonCode, reason.RUNDeferredConcurrencyKey)
	}
	for _, fragment := range []string{`"concurrency_key":"waiter:k"`, `"blocking_run_id":"` + holder.Run.ID + `"`} {
		if !strings.Contains(run.ReasonData, fragment) {
			t.Fatalf("reason_data %q lacks %s", run.ReasonData, fragment)
		}
	}

	events, err := s.RunEvents(ctx, run.ID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var deferred bool
	for _, e := range events {
		if e.Kind == "run.deferred" && e.ReasonCode == string(reason.RUNDeferredConcurrencyKey) {
			deferred = true
		}
	}
	if !deferred {
		t.Fatal("no run.deferred event names the key hold")
	}
}

func TestSkipPolicyRejectsTheTriggerWithoutCreatingARun(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	concApply(t, s, "skipper", constKey("k"), spec.OnConflictSkip)
	row := concSchedule(t, s, "skipper")

	holder := admitTick(t, s, row, 0)

	loser := admitTick(t, s, row, 1)
	if loser.Run.ID != "" {
		t.Fatalf("skip created run %s; a skipped fire creates nothing", loser.Run.ID)
	}

	var outcome, code, text, pointedAt string
	err := s.r.QueryRowContext(ctx,
		`SELECT outcome, reason_code, COALESCE(reason_text,''), COALESCE(run_id,'')
		 FROM triggers WHERE job_name = 'skipper' AND outcome = 'rejected'`).
		Scan(&outcome, &code, &text, &pointedAt)
	if err != nil {
		t.Fatalf("read the trigger: %v", err)
	}
	if outcome != "rejected" {
		t.Fatalf("trigger outcome %q, want rejected", outcome)
	}
	if code != string(reason.TRIGGERRejectedConcurrencyKey) {
		t.Fatalf("trigger reason_code %q, want %q", code, reason.TRIGGERRejectedConcurrencyKey)
	}
	if pointedAt != holder.Run.ID {
		t.Fatalf("the rejection points at %q, want the blocking run %q", pointedAt, holder.Run.ID)
	}
	if !strings.Contains(text, holder.Run.ID) {
		t.Fatalf("reason_text %q does not name the blocking run", text)
	}

	// Our own dedup registration went out with the refused run: the next
	// evaluation of the same event may try again instead of folding into a
	// run that never existed.
	var keys int
	if err := s.r.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM run_keys WHERE source_id = ?`, row.JobName+"/nightly").Scan(&keys); err != nil {
		t.Fatalf("count run_keys: %v", err)
	}
	// Exactly one remains: the holder's. The loser's is gone.
	if keys != 1 {
		t.Fatalf("%d run_keys rows survive, want exactly the holder's one", keys)
	}
}

// Fifty concurrent materialisations of one key under -race: exactly one run
// holds the key, every other run is a well formed keyless deferral, and no
// SQLITE_BUSY reaches a caller. Results come back over a channel; there is no
// sleep anywhere.
func TestFiftyMaterialisationsRaceOnOneKey(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	concApply(t, s, "racey", constKey("one"), "")
	row := concSchedule(t, s, "racey")

	const n = 50
	type outcome struct {
		run Run
		err error
	}
	results := make(chan outcome, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := s.MaterializeTick(ctx, TickInput{
				Schedule:       row,
				ScheduledFor:   admitFire(i),
				Outcome:        OutcomeTriggered,
				RunKey:         fmt.Sprintf("racey/nightly:%d", i),
				NextTickAt:     time.Now().Add(2 * time.Minute),
				UpdateProgress: true,
				Actor:          "scheduler",
			})
			if err != nil {
				results <- outcome{err: err}
				return
			}
			results <- outcome{run: res.Run}
		}(i)
	}
	wg.Wait()
	close(results)

	keyed, deferred := 0, 0
	for out := range results {
		if out.err != nil {
			if strings.Contains(out.err.Error(), "SQLITE_BUSY") || strings.Contains(out.err.Error(), "database is locked") {
				t.Fatalf("busy leaked to the caller: %v", out.err)
			}
			t.Fatalf("materialise: %v", out.err)
		}
		switch {
		case out.run.ConcurrencyKey == "racey:one":
			keyed++
			if out.run.DeferReason != "" {
				t.Fatalf("the key holder reads as deferred (%q)", out.run.DeferReason)
			}
		case out.run.DeferReason == model.DeferReasonConcurrencyKey && out.run.ConcurrencyKey == "":
			deferred++
			if !strings.Contains(out.run.ReasonData, `"concurrency_key":"racey:one"`) {
				t.Fatalf("deferral %s lost its wanted key: %q", out.run.ID, out.run.ReasonData)
			}
		default:
			t.Fatalf("run %+v is neither holder nor deferral", out.run)
		}
	}
	if keyed != 1 {
		t.Fatalf("%d runs hold the key, want exactly 1", keyed)
	}
	if deferred != n-1 {
		t.Fatalf("%d runs deferred, want %d", deferred, n-1)
	}
}

func TestFsckCatchesTwoActiveRunsOnOneKey(t *testing.T) {
	s := migratedStore(t)
	concApply(t, s, "fscky", constKey("k"), "")

	keyedActiveRun(t, s, "fscky", "01J00000000000000000000001", "fscky:k")
	// The index refuses a second active run on the key, so seeding the
	// violation needs the index gone first. That is the point: only a hand
	// edit or a broken migration can produce this state.
	dropIndex(t, s)
	keyedActiveRun(t, s, "fscky", "01J00000000000000000000002", "fscky:k")

	violations, err := s.ActiveConcurrencyKeyViolations(context.Background())
	if err != nil {
		t.Fatalf("fsck the keys: %v", err)
	}
	if len(violations) != 1 {
		t.Fatalf("%d violations, want 1: %+v", len(violations), violations)
	}
	if violations[0].Check != "I12" {
		t.Fatalf("check %q, want the I12 family", violations[0].Check)
	}
}

func TestFsckReportsNothingWhenEveryKeyIsClean(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	concApply(t, s, "clean", constKey("k"), "")
	row := concSchedule(t, s, "clean")

	admitTick(t, s, row, 0)
	loser := admitTick(t, s, row, 1)
	if loser.Run.ConcurrencyKey != "" {
		t.Fatalf("setup: the loser unexpectedly holds %q", loser.Run.ConcurrencyKey)
	}

	violations, err := s.ActiveConcurrencyKeyViolations(ctx)
	if err != nil {
		t.Fatalf("fsck the keys: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("a holder plus its keyless deferral is not a violation: %+v", violations)
	}
}

func TestAMissingParamLeavesEveryFireUnlimited(t *testing.T) {
	s := migratedStore(t)
	concApply(t, s, "paramed", &spec.ConcurrencyKey{Param: "kunde"}, "")
	row := concSchedule(t, s, "paramed")

	first := admitTick(t, s, row, 0)
	second := admitTick(t, s, row, 1)

	// A schedule fires with no params at all, so both runs resolve to NULL
	// keys and neither blocks the other. That is the documented warning
	// made real, not a bug.
	if first.Run.ConcurrencyKey != "" || second.Run.ConcurrencyKey != "" {
		t.Fatalf("unresolvable keys became %q and %q", first.Run.ConcurrencyKey, second.Run.ConcurrencyKey)
	}
	if second.Run.DeferReason != "" {
		t.Fatalf("an unlimited run stood down anyway: %q", second.Run.DeferReason)
	}
}

func TestFromRunKeyGivesEachFireItsOwnKey(t *testing.T) {
	s := migratedStore(t)
	concApply(t, s, "byfire", &spec.ConcurrencyKey{FromRunKey: true}, "")
	row := concSchedule(t, s, "byfire")

	first := admitTick(t, s, row, 0)
	second := admitTick(t, s, row, 1)

	if first.Run.ConcurrencyKey == "" || second.Run.ConcurrencyKey == "" {
		t.Fatalf("from: run_key resolved to empty keys: %q, %q",
			first.Run.ConcurrencyKey, second.Run.ConcurrencyKey)
	}
	if first.Run.ConcurrencyKey == second.Run.ConcurrencyKey {
		t.Fatalf("distinct fires collapsed onto one key %q", first.Run.ConcurrencyKey)
	}
	if !strings.HasPrefix(first.Run.ConcurrencyKey, "byfire:") {
		t.Fatalf("the key is not canonicalised under the job name: %q", first.Run.ConcurrencyKey)
	}
}

// --- probes and small helpers -------------------------------------------------

func concSchedule(t *testing.T, s *Store, job string) ScheduleRow {
	t.Helper()
	return admitSchedule(t, s, job, "")
}

func keyedActiveRun(t *testing.T, s *Store, job, id, key string) {
	t.Helper()

	ctx := context.Background()
	var versionID string
	if err := s.r.QueryRowContext(ctx,
		`SELECT current_version_id FROM jobs WHERE name = ?`, job).Scan(&versionID); err != nil {
		t.Fatalf("read the current version of %s: %v", job, err)
	}
	now := time.Now().UnixMilli()
	stmt := `INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,
		 lease_owner, lease_epoch, lease_expires_at, created_at, updated_at, concurrency_key)
	VALUES (?, ?, ?, 'schedule', 'running', ?, 'test-owner', 1, ?, ?, ?, ?)`
	expires := time.Now().Add(DefaultRunLeaseTTL).UnixMilli()
	if _, err := s.w.ExecContext(ctx, stmt, id, job, versionID, now, expires, now, now, key); err != nil {
		t.Fatalf("seed a keyed running run on %s: %v", job, err)
	}
}

func dropIndex(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.w.ExecContext(context.Background(), `DROP INDEX ux_runs_conc_key`); err != nil {
		t.Fatalf("drop the index: %v", err)
	}
}

func restoreIndex(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.w.ExecContext(context.Background(),
		`CREATE UNIQUE INDEX ux_runs_conc_key ON runs (concurrency_key)
		 WHERE concurrency_key IS NOT NULL AND state IN ('queued', 'running')`); err != nil {
		t.Fatalf("restore the index: %v", err)
	}
}

func readRunKey(t *testing.T, s *Store, runID string) string {
	t.Helper()
	var got any
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT concurrency_key FROM runs WHERE id = ?`, runID).Scan(&got); err != nil {
		t.Fatalf("read the run: %v", err)
	}
	if got == nil {
		return ""
	}
	return got.(string)
}
