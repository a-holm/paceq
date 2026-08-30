package reconcile

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
)

// The gap walk and the scheduler's catch-up meet on the same slot uniqueness
// (#211). These tests drive both for real, in the order a restart drives
// them: reconciliation first, under the comment that says nobody else is
// running yet, then the loop's first wake.

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
	if err := w.store.TouchSession(w.ctx, latestSessionID(w.t, w.store)); err != nil {
		w.t.Fatalf("heartbeat: %v", err)
	}
	w.clk.Advance(down)
	if err := w.startCycle(); err != nil {
		w.t.Fatalf("startup reconciliation: %v", err)
	}
	return w.clk.Now().UTC()
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

// TestASecondSweepOverAReplayedOutageWritesNothing is AC8. The gap walk is
// safe to run twice by construction, and a catch-up in between must not open
// a hole for it to fill a second time.
func TestASecondSweepOverAReplayedOutageWritesNothing(t *testing.T) {
	w := newWorld(t, "boot-one")
	w.seedCatchupSchedule("last")
	w.crashAcross(30 * time.Minute)
	w.wake()

	before, err := w.store.DumpForIdempotence(w.ctx)
	if err != nil {
		t.Fatalf("dump before the second sweep: %v", err)
	}

	outages, err := w.store.Outages(w.ctx)
	if err != nil {
		t.Fatalf("list outages: %v", err)
	}
	if len(outages) != 1 {
		t.Fatalf("%d outages, want exactly one to sweep again", len(outages))
	}
	n, err := fillOutage(w.ctx, w.store, w.opts(outages[0].From, ""), outages[0])
	if err != nil {
		t.Fatalf("the second sweep: %v", err)
	}
	if n != 0 {
		t.Fatalf("the second sweep inserted %d rows, want none: the replayed slot must not look empty again", n)
	}

	after, err := w.store.DumpForIdempotence(w.ctx)
	if err != nil {
		t.Fatalf("dump after the second sweep: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("the second sweep changed the row count from %d to %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("the second sweep moved row %d:\n before: %s\n  after: %s", i, before[i], after[i])
		}
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
