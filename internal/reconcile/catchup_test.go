package reconcile

import (
	"fmt"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// The gap walk has to stay idempotent over a slot the scheduler's catch-up
// took back from it (#211). The replay is the loop's decision, and the whole
// sequence is proved through both halves in test/catchup; the property
// asserted here is the walk's own, so the slot is converted with the one
// store call the loop makes rather than by driving the loop from here.

// seedCatchupSchedule seeds a job and a schedule that replays the newest
// fire-time it missed, due exactly at the origin.
func (w *world) seedCatchupSchedule(catchup string) store.ScheduleRow {
	w.t.Helper()
	if _, _, err := w.store.UpsertJobVersion(w.ctx, store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:catchup",
		SpecJSON: `{"name":"nightly","schema":"paceq.job.v1","max_concurrent":10,"steps":[{"name":"work","run":["/bin/true"],"shell":false}]}`,
	}); err != nil {
		w.t.Fatalf("seed the job: %v", err)
	}
	sched, err := w.store.UpsertSchedule(w.ctx, store.ScheduleInput{
		JobName: "nightly", Name: "default", Kind: "cron",
		Expr: "*/5 * * * *", Timezone: "UTC", Catchup: catchup,
		NextTickAt: origin,
	})
	if err != nil {
		w.t.Fatalf("seed the schedule: %v", err)
	}
	return sched
}

// crashAcross opens a session, heartbeats, then loses the daemon for the
// given stretch and reconciles the restart. It returns the moment the
// replacement came up.
func (w *world) crashAcross(down time.Duration) time.Time {
	w.t.Helper()
	if _, err := w.store.StartSession(w.ctx, "test"); err != nil {
		w.t.Fatalf("first start: %v", err)
	}
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

// replay takes one fire-time back from the gap walk's evidence exactly as a
// catch-up wake does: the store's tick gate, with the schedule row and the
// run key the loop would have used.
func (w *world) replay(sched store.ScheduleRow, fireAt time.Time) {
	w.t.Helper()
	res, err := w.store.MaterializeTick(w.ctx, store.TickInput{
		Schedule:       sched,
		ScheduledFor:   fireAt,
		Outcome:        store.OutcomeTriggered,
		RunKey:         fmt.Sprintf("%s/%s:%s", sched.JobName, sched.Name, fireAt.Format(time.RFC3339)),
		NextTickAt:     fireAt.Add(5 * time.Minute),
		UpdateProgress: true,
		Actor:          "scheduler",
	})
	if err != nil {
		w.t.Fatalf("replay the fire-time %v: %v", fireAt, err)
	}
	if !res.Replayed {
		w.t.Fatalf("the fire-time %v was not taken back from the gap walk: %+v", fireAt, res)
	}
}

// TestASecondSweepOverAReplayedOutageWritesNothing is AC8. The gap walk is
// safe to run twice by construction, and a catch-up in between must not open
// a hole for it to fill a second time.
func TestASecondSweepOverAReplayedOutageWritesNothing(t *testing.T) {
	w := newWorld(t, "boot-one")
	sched := w.seedCatchupSchedule("last")
	back := w.crashAcross(30 * time.Minute)
	w.replay(sched, back.Truncate(5*time.Minute))

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
