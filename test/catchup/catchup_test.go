// Package catchup proves the seam between startup reconciliation and the
// scheduler loop (#211). The gap walk fills every slot of an outage with a
// 'missed' tick before any loop runs, and those rows sit on the same slot
// uniqueness the loop claims through, so the first wake after a restart can
// only replay what catch-up owes by taking a slot back from the evidence.
//
// The proof lives here because internal/reconcile and internal/scheduler may
// not import each other, and the guard in internal/arch/deps_test.go has no
// exceptions: the gap walk must not start depending on the loop. Both halves
// run for real here, in the order a restart drives them: reconciliation
// first, while nobody else is running, then the loop's first wake.
//
// internal/scheduler holds the same rules against an outage staged by hand.
// This package is what keeps that staging honest, because nothing here writes
// a tick row itself.
package catchup

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reconcile"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// origin is the instant every scenario starts from.
var origin = testutil.Origin

// world is one migrated store on a fake clock with the boot id pinned: the
// one database both halves write to.
type world struct {
	t     *testing.T
	store *store.Store
	clk   *clock.Fake
	ctx   context.Context
}

func newWorld(t *testing.T, boot string) *world {
	t.Helper()
	clk := testutil.NewClock(t)
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

type stubSignals struct{}

func (stubSignals) Term(int) error { return nil }
func (stubSignals) Kill(int)       {}

func (w *world) opts(gapFrom time.Time, prev string) reconcile.Options {
	return reconcile.Options{
		Clock:            w.clk,
		SessionID:        "01J0RECONCILESESSION00",
		SessionStartedAt: w.clk.Now().UTC(),
		GapFrom:          gapFrom,
		PrevSessionID:    prev,
		ScanProcs:        func() ([]reconcile.Process, error) { return nil, nil },
		Signals:          stubSignals{},
	}
}

// startCycle reproduces what Serve does around reconciliation: the health
// gate and gap capture before StartSession, the session open, then the sweep.
func (w *world) startCycle() error {
	violations, err := w.store.QuickFsck(w.ctx)
	if err != nil {
		return err
	}
	if summary := reconcile.CriticalViolationSummary(violations); summary != "" {
		w.t.Fatalf("startup refused: %s", summary)
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
	return reconcile.OnStartup(w.ctx, w.store, w.opts(gapFrom, prevID))
}

// seedCatchupSchedule seeds a job and a schedule that replays the newest
// fire-time it missed, due exactly at the origin.
func (w *world) seedCatchupSchedule(catchup string) {
	w.t.Helper()
	if _, _, err := w.store.UpsertJobVersion(w.ctx, store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:catchup",
		SpecJSON: `{"name":"nightly","schema":"paceq.job.v1","max_concurrent":10,"steps":[{"name":"work","run":["/bin/true"],"shell":false}]}`,
	}); err != nil {
		w.t.Fatalf("seed the job: %v", err)
	}
	if _, err := w.store.UpsertSchedule(w.ctx, store.ScheduleInput{
		JobName: "nightly", Name: "default", Kind: "cron",
		Expr: "*/5 * * * *", Timezone: "UTC", Catchup: catchup,
		NextTickAt: origin,
	}); err != nil {
		w.t.Fatalf("seed the schedule: %v", err)
	}
}

// wake runs one scheduler pass against the same store reconciliation just
// wrote to.
func (w *world) wake() {
	w.t.Helper()
	src, err := scheduler.New(scheduler.Config{
		Store:  w.store,
		Clock:  w.clk,
		Holder: "test",
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		w.t.Fatalf("build the scheduler: %v", err)
	}
	if err := src.Tick(w.ctx); err != nil {
		w.t.Fatalf("scheduler wake: %v", err)
	}
}

func (w *world) queuedRuns() int {
	w.t.Helper()
	ids, err := w.store.ClaimableRunIDs(w.ctx)
	if err != nil {
		w.t.Fatalf("count claimable runs: %v", err)
	}
	return len(ids)
}

// crashAcross opens a session, lets the schedule tick once, heartbeats, then
// loses the daemon for the given stretch and reconciles the restart. It
// returns the moment the replacement came up.
func (w *world) crashAcross(down time.Duration) time.Time {
	w.t.Helper()
	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		w.t.Fatalf("first start: %v", err)
	}
	w.wake() // the honest tick that leaves a real cursor behind

	w.clk.Advance(time.Minute)
	if err := w.store.TouchSession(w.ctx, w.openSessionID()); err != nil {
		w.t.Fatalf("heartbeat: %v", err)
	}
	w.clk.Advance(down)
	if err := w.startCycle(); err != nil {
		w.t.Fatalf("startup reconciliation: %v", err)
	}
	return w.clk.Now().UTC()
}

func (w *world) openSessionID() string {
	w.t.Helper()
	sess, found, err := w.store.OpenSession(w.ctx)
	if err != nil || !found {
		w.t.Fatalf("find the open session: %v (found=%v)", err, found)
	}
	return sess.ID
}

// TestTheFirstWakeAfterAnOutageReplaysWhatCatchupOwes is the worked sequence
// of #211 end to end, through the real gap walk rather than a fixture of it.
func TestTheFirstWakeAfterAnOutageReplaysWhatCatchupOwes(t *testing.T) {
	w := newWorld(t, "boot-one")
	w.seedCatchupSchedule("last")

	back := w.crashAcross(30 * time.Minute)
	before := w.queuedRuns()

	// The gap walk owns every slot of the outage before the loop ever looks.
	evidence, err := w.store.MissedTickEvidence(w.ctx)
	if err != nil {
		t.Fatalf("read the evidence: %v", err)
	}
	if len(evidence) == 0 {
		t.Fatal("the gap walk wrote no missed ticks; the scenario proves nothing")
	}

	w.wake()

	if got := w.queuedRuns() - before; got != 1 {
		t.Fatalf("the first wake after the outage queued %d runs, want the one catchup=last owes", got)
	}
	// catchup=last replays the newest slot inside the window, which is the
	// last multiple of five minutes at or before the restart.
	newest := back.Truncate(5 * time.Minute)
	cursor, _, err := w.store.ScheduleCursor(w.ctx, "nightly", "default")
	if err != nil {
		t.Fatalf("read the cursor: %v", err)
	}
	if cursor == nil || !cursor.Equal(newest) {
		t.Fatalf("the cursor sits at %v, want the replayed fire-time %v", cursor, newest)
	}

	ticks, err := w.store.ScheduleTicks(w.ctx, "nightly", "default")
	if err != nil {
		t.Fatalf("read the ticks: %v", err)
	}
	var replayed, stillMissed int
	for _, tk := range ticks {
		switch {
		case tk.ScheduledFor.Equal(newest) && tk.Outcome == store.OutcomeTriggered:
			replayed++
		case tk.Outcome == store.OutcomeMissed:
			stillMissed++
		}
	}
	if replayed != 1 {
		t.Fatalf("the replayed fire-time %v is not a triggered row: %+v", newest, ticks)
	}
	if stillMissed != len(evidence)-1 {
		t.Fatalf("%d rows still explain the outage, want %d: the replay must claim its own slot and no other",
			stillMissed, len(evidence)-1)
	}
	assertClean(t, w)
}

// TestCatchupSkipLeavesTheWholeOutageExplained keeps the default honest: it
// wants no replay, so every slot must stay the gap walk's to explain.
func TestCatchupSkipLeavesTheWholeOutageExplained(t *testing.T) {
	w := newWorld(t, "boot-one")
	w.seedCatchupSchedule("skip")
	w.crashAcross(30 * time.Minute)

	evidence, err := w.store.MissedTickEvidence(w.ctx)
	if err != nil {
		t.Fatalf("read the evidence: %v", err)
	}
	before := w.queuedRuns()

	w.wake()

	if got := w.queuedRuns() - before; got != 0 {
		t.Fatalf("catchup=skip queued %d runs out of the outage, want none", got)
	}
	after, err := w.store.MissedTickEvidence(w.ctx)
	if err != nil {
		t.Fatalf("read the evidence back: %v", err)
	}
	if len(after) != len(evidence) {
		t.Fatalf("catchup=skip changed the outage evidence from %d rows to %d", len(evidence), len(after))
	}
	assertClean(t, w)
}

func assertClean(t *testing.T, w *world) {
	t.Helper()
	violations, err := w.store.Fsck(w.ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("fsck found %d violations: %+v", len(violations), violations)
	}
}
