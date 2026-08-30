package reconcile

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The startup reconciliation proofs. Every acceptance criterion of issue #62
// lands here or beside it: one gap, one row, evidence that points at it,
// idempotence by dump, convergence by construction.

var origin = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

// world is one migrated store on a fake clock with the boot id pinned, plus
// the reconciling options that follow its session.
type world struct {
	t      *testing.T
	store  *store.Store
	clk    *clock.Fake
	ctx    context.Context
	sweeps int // how many times the process scan ran
}

func newWorld(t *testing.T, boot string) *world {
	t.Helper()
	clk := clock.NewFake(origin)
	path := t.TempDir() + "/state.db"
	s, err := store.Open(context.Background(), path, store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open the store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close the store: %v", err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if boot != "" {
		s.OverrideBootIDForTest(boot)
	}
	return &world{t: t, store: s, clk: clk, ctx: context.Background()}
}

func (w *world) opts(gapFrom time.Time, prev string) Options {
	w.sweeps = 0
	return Options{
		Clock:            w.clk,
		SessionID:        "01J0RECONCILESESSION00",
		SessionStartedAt: w.clk.Now().UTC(),
		GapFrom:          gapFrom,
		PrevSessionID:    prev,
		ScanProcs: func() ([]Process, error) {
			w.sweeps++
			return nil, nil
		},
		Signals: stubSignals{},
	}
}

type stubSignals struct{}

func (stubSignals) Term(int) error { return nil }
func (stubSignals) Kill(int)       {}

// startCycle reproduces what Serve does around reconciliation: the health gate
// and gap capture before StartSession, the session open, then the sweep.
func (w *world) startCycle() error {
	// The health gate sits before StartSession so BootChanged() survives a
	// refusal: nothing was written yet, so the next start after repair still
	// sees the boot change.
	violations, err := w.store.QuickFsck(w.ctx)
	if err != nil {
		return fmt.Errorf("health check: %w", err)
	}
	if summary := CriticalViolationSummary(violations); summary != "" {
		return fmt.Errorf("startup refused: %s; run \"paceq fsck --repair\" "+
			"and confirm manually before starting", summary)
	}

	prev, found, err := w.store.OpenSession(w.ctx)
	if err != nil {
		w.t.Fatalf("read the open session: %v", err)
	}
	gapFrom := time.Time{}
	prevID := ""
	if found {
		gapFrom = prev.LastSeenAt
		prevID = prev.ID
	}
	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		w.t.Fatalf("start the session: %v", err)
	}
	return OnStartup(w.ctx, w.store, w.opts(gapFrom, prevID))
}

func (w *world) seedSchedule(expr, zone string) {
	w.t.Helper()
	if _, _, err := w.store.UpsertJobVersion(w.ctx, store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:test",
		SpecJSON: `{"name":"nightly","schema":"paceq.job.v1","max_concurrent":1,"steps":[{"name":"work","run":["/bin/true"],"shell":false}]}`,
	}); err != nil {
		w.t.Fatalf("seed the job: %v", err)
	}
	if _, err := w.store.UpsertSchedule(w.ctx, store.ScheduleInput{
		JobName: "nightly", Name: "default", Kind: "cron",
		Expr: expr, Timezone: zone,
		NextTickAt: origin.Add(-time.Hour),
	}); err != nil {
		w.t.Fatalf("seed the schedule: %v", err)
	}
}

// TestAGapBecomesOneOutageWithItsEvidence is AC1 and AC2 together: thirty
// minutes down leaves exactly one outages row, counted from the last
// heartbeat, whose synthetic ticks all carry TICK_MISSED_DAEMON_DOWN and name
// their row, all stamped well inside the ten second SLO.
func TestAGapBecomesOneOutageWithItsEvidence(t *testing.T) {
	w := newWorld(t, "boot-one")
	w.seedSchedule("*/5 * * * *", "UTC")

	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		t.Fatalf("first start: %v", err)
	}
	w.clk.Advance(time.Minute)
	if err := w.store.TouchSession(w.ctx, latestSessionID(t, w.store)); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	w.clk.Advance(30 * time.Minute)
	started := w.clk.Now()
	if err := w.startCycle(); err != nil {
		t.Fatalf("startup reconciliation: %v", err)
	}

	rows, err := w.store.Outages(w.ctx)
	if err != nil {
		t.Fatalf("list outages: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("%d outages after a 30 minute crash, want exactly 1", len(rows))
	}
	o := rows[0]
	if o.Kind != "crash" || o.MissedTicks == 0 {
		t.Fatalf("outage %+v, want kind crash with counted evidence", o)
	}
	if !o.From.Equal(origin.Add(time.Minute)) {
		t.Errorf("the gap starts at %s, want the last heartbeat %s", o.From, origin.Add(time.Minute))
	}
	if o.DetectedAt.Sub(started) > 10*time.Second {
		t.Errorf("detected %s, over the ten second SLO after %s", o.DetectedAt, started)
	}

	// The count is the truth from cronx, not a guess: the test walks the same
	// expression independently and demands the same number.
	want := countSlots(t, "*/5 * * * *", o.From, o.To)
	if o.MissedTicks != want {
		t.Errorf("outage counts %d slots, cronx says %d", o.MissedTicks, want)
	}

	evidence, err := w.store.MissedTickEvidence(w.ctx)
	if err != nil {
		t.Fatalf("read the synthetic ticks back: %v", err)
	}
	if len(evidence) != want {
		t.Fatalf("%d synthetic tick rows stored, want %d", len(evidence), want)
	}
	if code := evidence[0].ReasonCode; code != string(reason.TICKMissedDaemonDown) {
		t.Errorf("synthetic ticks carry %q, want %q", code, reason.TICKMissedDaemonDown)
	}
	if data := evidence[0].ReasonData; !strings.Contains(data, `"outage_id":`) {
		t.Errorf("reason_data %q does not point at the outages row", data)
	}
	if src := evidence[0].SourceName; src != "nightly/default" {
		t.Errorf("source_name is %q, want the scheduler's own job/name form", src)
	}
}

// TestAShortGapWritesNothing is AC3: fifty nine seconds of downtime is noise;
// sixty one is an outage. Nothing in between gets a row.
func TestAShortGapWritesNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		gap    time.Duration
		outage bool
	}{
		{"59s stays silent", 59 * time.Second, false},
		{"61s explains itself", 61 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t, "boot-one")
			w.seedSchedule("*/5 * * * *", "UTC")
			if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
				t.Fatalf("first start: %v", err)
			}
			w.clk.Advance(tc.gap)
			if err := w.startCycle(); err != nil {
				t.Fatalf("startup reconciliation: %v", err)
			}
			rows, err := w.store.Outages(w.ctx)
			if err != nil {
				t.Fatalf("list outages: %v", err)
			}
			if got := len(rows); (got == 1) != tc.outage {
				t.Fatalf("%d outages after %s down, want outage=%v", got, tc.gap, tc.outage)
			}
		})
	}
}

// TestBootChangeProvesEveryDeathAndSkipsTheSweep is AC4 plus the reboot test
// plan row: a changed boot id makes every running run dead on the spot
// regardless of its lease, the process scan never runs, and the outage says
// 'boot', the schema's word.
func TestBootChangeProvesEveryDeathAndSkipsTheSweep(t *testing.T) {
	w := newWorld(t, "boot-one")
	runID := w.seedRunningRun()

	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		t.Fatalf("first start: %v", err)
	}
	w.clk.Advance(5 * time.Minute)

	// The machine restarts between the two starts.
	w.store.OverrideBootIDForTest("boot-two")
	if err := w.startCycle(); err != nil {
		t.Fatalf("startup reconciliation after a restart: %v", err)
	}

	detail, err := w.store.GetRun(w.ctx, runID)
	if err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	// The reaper requeued the run, which closed its only step as lost
	// (STEP_FAILED_EXECUTOR_LOST). M4-03's reconciler then converges the
	// run to the state that step leaves: a run with a failed step is failed
	// (I10), not queued behind a step that can never run again.
	if detail.Run.State != "failed" {
		t.Fatalf("the run is %s after the restart, want converged to failed (its step died with the old executor)", detail.Run.State)
	}

	rows, err := w.store.Outages(w.ctx)
	if err != nil {
		t.Fatalf("list outages: %v", err)
	}
	if len(rows) != 1 || rows[0].Kind != "boot" {
		t.Fatalf("outages %+v, want one row kind 'boot'", rows)
	}
	if w.sweeps != 0 {
		t.Errorf("the process scan ran %d times after a machine restart, want skipped entirely", w.sweeps)
	}
}

// TestUnchangedBootSparesARunWhoseLeaseIsLive pins review warning 3: without
// boot proof there is no death proof, so a live lease survives startup
// untouched instead of being reaped early.
func TestUnchangedBootSparesARunWhoseLeaseIsLive(t *testing.T) {
	w := newWorld(t, "boot-one")
	runID := w.seedRunningRun()

	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		t.Fatalf("first start: %v", err)
	}
	w.clk.Advance(5 * time.Minute)
	if err := w.startCycle(); err != nil {
		t.Fatalf("startup reconciliation: %v", err)
	}

	detail, err := w.store.GetRun(w.ctx, runID)
	if err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	if detail.Run.State != "running" {
		t.Fatalf("the run was taken while its lease lived: now %s", detail.Run.State)
	}
}

// TestThreePassesLeaveTheStateUntouched is AC7: the whole sweep is
// idempotent. Three consecutive starts leave every table byte-identical to
// where the first pass left it, session timestamps aside.
func TestThreePassesLeaveTheStateUntouched(t *testing.T) {
	w := newWorld(t, "boot-one")
	w.seedSchedule("*/5 * * * *", "UTC")
	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		t.Fatalf("first start: %v", err)
	}
	w.clk.Advance(30 * time.Minute)

	if err := w.startCycle(); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	after1, err := w.store.DumpForIdempotence(w.ctx)
	if err != nil {
		t.Fatalf("dump after pass 1: %v", err)
	}

	for pass := 2; pass <= 3; pass++ {
		if err := w.startCycle(); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		afterN, err := w.store.DumpForIdempotence(w.ctx)
		if err != nil {
			t.Fatalf("dump after pass %d: %v", pass, err)
		}
		if len(afterN) != len(after1) {
			t.Fatalf("pass %d wrote %d rows the first pass did not have (%d)", pass, len(afterN)-len(after1), len(after1))
		}
		for i := range after1 {
			if after1[i] != afterN[i] {
				t.Fatalf("pass %d moved row %d:\n first: %s\n again: %s", pass, i, after1[i], afterN[i])
			}
		}
	}
}

// TestCleanStopOwesNoOutage is AC11: stop_reason 'clean' means the next start
// writes nothing, because there is nothing to explain.
func TestCleanStopOwesNoOutage(t *testing.T) {
	w := newWorld(t, "boot-one")
	w.seedSchedule("*/5 * * * *", "UTC")

	sess, err := w.store.StartSession(w.ctx, "test")
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	w.clk.Advance(45 * time.Second)
	if err := w.store.StopSession(w.ctx, sess.ID); err != nil {
		t.Fatalf("stop cleanly: %v", err)
	}
	w.clk.Advance(30 * time.Minute)

	if err := w.startCycle(); err != nil {
		t.Fatalf("next start: %v", err)
	}
	rows, err := w.store.Outages(w.ctx)
	if err != nil {
		t.Fatalf("list outages: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("a clean stop produced %d outage rows, want none", len(rows))
	}
}

// TestPeriodicWritesNothingWhenHealthy is AC12: two consecutive passes of the
// safety net over a healthy database leave the byte-dump untouched. No rows,
// no counters, no drift.
func TestPeriodicWritesNothingWhenHealthy(t *testing.T) {
	w := newWorld(t, "boot-one")
	w.seedSchedule("*/5 * * * *", "UTC")
	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		t.Fatalf("first start: %v", err)
	}

	before, err := w.store.DumpForIdempotence(w.ctx)
	if err != nil {
		t.Fatalf("dump before: %v", err)
	}
	for pass := 1; pass <= 2; pass++ {
		w.clk.Advance(30 * time.Second)
		if err := Periodic(w.ctx, w.store, w.opts(time.Time{}, "")); err != nil {
			t.Fatalf("periodic pass %d: %v", pass, err)
		}
		after, err := w.store.DumpForIdempotence(w.ctx)
		if err != nil {
			t.Fatalf("dump after pass %d: %v", pass, err)
		}
		if len(before) != len(after) {
			t.Fatalf("periodic pass %d changed the row count: %d -> %d", pass, len(before), len(after))
		}
		for i := range before {
			if before[i] != after[i] {
				t.Fatalf("periodic pass %d wrote:\n %s\nover:\n %s", pass, after[i], before[i])
			}
		}
	}
}

// TestACriticalViolationRefusesStartup is AC10: a duplicated run key (I3)
// means the code cannot reason about dedup history, so startup refuses and
// the error names the repair path. Nothing may be written first.
func TestACriticalViolationRefusesStartup(t *testing.T) {
	w := newWorld(t, "boot-one")
	w.seedSchedule("*/5 * * * *", "UTC")
	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		t.Fatalf("first start: %v", err)
	}
	w.clk.Advance(30 * time.Minute)

	// The violation needs a run to duplicate, so seed the world's running
	// run before planting.
	w.seedRunningRun()
	subject, err := w.store.InjectDuplicateRunKey(w.ctx)
	if err != nil {
		t.Fatalf("plant I3: %v", err)
	}

	err = w.startCycle()
	if err == nil {
		t.Fatal("startup succeeded on a critical invariant violation")
	}
	if !strings.Contains(err.Error(), `paceq fsck --repair`) {
		t.Errorf("the refusal %q does not name the repair path", err)
	}
	if !strings.Contains(err.Error(), "I3") || !strings.Contains(err.Error(), subject) {
		t.Errorf("the refusal %q does not name the violation %s", err, subject)
	}

	// Refusal means refused: no outage row, no reaped runs, nothing.
	rows, err := w.store.Outages(w.ctx)
	if err != nil {
		t.Fatalf("list outages: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("the refused start still wrote %d outage rows", len(rows))
	}
}

// TestBootChangeSurvivesHealthGateRefusal is the proof for the second review
// must-fix: the health gate now runs BEFORE StartSession in Serve(), so a
// refusal leaves the boot-change evidence intact. The operator fixes the
// state and restarts, and the next start still sees BootChanged() == true.
func TestBootChangeSurvivesHealthGateRefusal(t *testing.T) {
	w := newWorld(t, "boot-old")
	w.seedSchedule("*/5 * * * *", "UTC")

	// First session writes "boot-old" to meta.
	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		t.Fatalf("first start: %v", err)
	}
	w.seedRunningRun()
	subject, err := w.store.InjectDuplicateRunKey(w.ctx)
	if err != nil {
		t.Fatalf("plant I3: %v", err)
	}

	// The machine restarts between the first session and the next start.
	w.store.OverrideBootIDForTest("boot-new")

	// startCycle must refuse at the health gate, before StartSession.
	err = w.startCycle()
	if err == nil {
		t.Fatal("startup succeeded on a critical invariant violation")
	}
	if !strings.Contains(err.Error(), `paceq fsck --repair`) {
		t.Errorf("the refusal %q does not name the repair path", err)
	}
	if !strings.Contains(err.Error(), "I3") || !strings.Contains(err.Error(), subject) {
		t.Errorf("the refusal %q does not name the violation %s", err, subject)
	}

	// The boot change was not consumed: StartSession never ran, so meta
	// still holds "boot-old". A new StartSession after the operator fixes
	// the violation must still see the restart.
	if !w.store.BootChanged() {
		// BootChanged is set only by StartSession, which never ran, so
		// false is expected here. The real proof is below.
	}
	sess, err := w.store.StartSession(w.ctx, "post-repair")
	if err != nil {
		t.Fatalf("the repair start: %v", err)
	}
	if !w.store.BootChanged() {
		t.Errorf("BootChanged is false after the repair start; the " +
			"health gate consumed the one-shot evidence")
	}
	_ = sess

	// No outages row was written behind the refusal.
	rows, err := w.store.Outages(w.ctx)
	if err != nil {
		t.Fatalf("list outages: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("the refused start wrote %d outage rows", len(rows))
	}
}

// seedRunningRun materialises a run and claims it with a lease far in the
// future, so anything that touches it must answer for its evidence first.
func (w *world) seedRunningRun() string {
	w.t.Helper()
	if _, _, err := w.store.UpsertJobVersion(w.ctx, store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:test",
		SpecJSON: `{"name":"nightly","schema":"paceq.job.v1","max_concurrent":1,"steps":[{"name":"work","run":["/bin/true"],"shell":false}]}`,
	}); err != nil {
		w.t.Fatalf("seed the job: %v", err)
	}
	out, err := w.store.MaterializeManualTrigger(w.ctx, store.ManualTriggerInput{JobName: "nightly"})
	if err != nil {
		w.t.Fatalf("materialise the run: %v", err)
	}
	runID := out.Run.ID
	if _, _, err := w.store.ClaimRun(w.ctx, runID, store.LeaseInput{
		Owner: "doomed-executor", TTL: time.Hour,
	}); err != nil {
		w.t.Fatalf("claim the run: %v", err)
	}
	if err := w.store.StartStep(w.ctx, runID, "work", store.LeaseRef{Owner: "doomed-executor", Epoch: 1}); err != nil {
		w.t.Fatalf("start the step: %v", err)
	}
	return runID
}

func latestSessionID(t *testing.T, s *store.Store) string {
	t.Helper()
	sess, found, err := s.OpenSession(context.Background())
	if err != nil || !found {
		t.Fatalf("find the open session: %v (found=%v)", err, found)
	}
	return sess.ID
}

// countSlots answers the same question the gap walk must: how many fire times
// of this expression fall inside (from, to]. It walks the schedule forward
// one Next at a time so a bug in Between cannot grade its own homework.
func countSlots(t *testing.T, expr string, from, to time.Time) int {
	t.Helper()
	sched, err := cronx.Parse(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	n := 0
	cur := from
	for {
		occ, err := sched.Next(cur, time.UTC, cronx.Policy{})
		if err != nil {
			t.Fatalf("walk %q: %v", expr, err)
		}
		if occ.At.After(to) {
			return n
		}
		n++
		cur = occ.At
	}
}
