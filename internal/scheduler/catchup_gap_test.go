package scheduler_test

import (
	"bytes"
	"context"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// Catch-up against the gap walk (#211). Startup reconciliation fills every
// slot of an outage with a 'missed' tick before any loop runs, and those rows
// sit on the same UNIQUE (source_kind, source_name, scheduled_for) gate the
// scheduler claims through. The gap row is an explanation with no run behind
// it; a catch-up attempt for the same fire-time is entitled to convert it.

// gapDay is the outage these tests replay. Plain UTC: which slots the walk
// enumerates at a DST seam is a different question (#208), and none of these
// assertions depend on the answer.
var gapDay = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)

func at(min int) time.Time { return gapDay.Add(time.Duration(min) * time.Minute) }

// stageOutage writes exactly what reconcile's gap walk leaves behind: one
// outages row plus one 'missed' tick per slot nobody evaluated while the
// daemon was down.
func stageOutage(t *testing.T, ctx context.Context, s *store.Store, from, to time.Time, slots ...time.Time) {
	t.Helper()
	id, err := s.RecordOutage(ctx, store.OutageInput{From: from, To: to, Kind: "crash"})
	if err != nil {
		t.Fatalf("record the outage: %v", err)
	}
	var missed []store.MissedTick
	for _, sl := range slots {
		missed = append(missed, store.MissedTick{SourceName: "nightly/default", ScheduledFor: sl})
	}
	n, err := s.RecordMissedTicks(ctx, "01J0GAPSESSION000000000", id, missed)
	if err != nil {
		t.Fatalf("record the missed ticks: %v", err)
	}
	if n != len(slots) {
		t.Fatalf("the gap walk wrote %d missed ticks, want %d", n, len(slots))
	}
}

// tickAt indexes a schedule's recorded evaluations by fire-time.
func ticksByFireTime(t *testing.T, ctx context.Context, s *store.Store) map[time.Time]store.TickView {
	t.Helper()
	rows, err := s.ScheduleTicks(ctx, "nightly", "default")
	if err != nil {
		t.Fatalf("read the ticks back: %v", err)
	}
	out := make(map[time.Time]store.TickView, len(rows))
	for _, r := range rows {
		out[r.ScheduledFor] = r
	}
	return out
}

// primeCursor runs one honest tick at the schedule's first fire-time, so the
// outage that follows starts from a real cursor rather than a hand-written
// one.
func primeCursor(t *testing.T, ctx context.Context, s *store.Store, clk *clock.Fake, catchup string, limit int) {
	t.Helper()
	seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
		in.Expr = "*/5 * * * *"
		in.Timezone = "UTC"
		in.Catchup = catchup
		in.CatchupLimit = limit
		in.NextTickAt = gapDay
	})
	if err := newSource(s, clk).Tick(ctx); err != nil {
		t.Fatalf("prime the cursor: %v", err)
	}
	cursor := readCursor(t, ctx, s, "nightly", "default")
	if cursor == nil || !cursor.Equal(gapDay) {
		t.Fatalf("the priming tick left the cursor at %v, want %v", cursor, gapDay)
	}
}

// TestCatchupLastReplaysTheSlotTheGapWalkAlreadyExplained is AC1 through AC4.
// The daemon dies at 10:01 and comes back at 10:32. Reconciliation has
// already written 'missed' rows for 10:05 through 10:30. catchup=last owes
// exactly one replay, at 10:30, and it must survive the gap row sitting in
// its slot.
func TestCatchupLastReplaysTheSlotTheGapWalkAlreadyExplained(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	clk := clock.NewFake(gapDay)

	primeCursor(t, ctx, s, clk, "last", 10)
	before := queuedRuns(t, ctx, s)

	clk.Set(at(32))
	stageOutage(t, ctx, s, at(1), at(32), at(5), at(10), at(15), at(20), at(25), at(30))

	if err := newSource(s, clk).Tick(ctx); err != nil {
		t.Fatalf("the catch-up tick: %v", err)
	}

	if got := queuedRuns(t, ctx, s) - before; got != 1 {
		t.Fatalf("the catch-up tick queued %d runs, want exactly one (the 10:30 replay); "+
			"a gap row owns the slot and the catch-up decision was overruled", got)
	}

	rows := ticksByFireTime(t, ctx, s)
	replay, ok := rows[at(30)]
	if !ok {
		t.Fatalf("no tick row at %v at all", at(30))
	}
	if replay.Outcome != store.OutcomeTriggered || replay.TriggerCount != 1 {
		t.Fatalf("the %v row is %s/%s with trigger_count %d, want triggered with one trigger",
			at(30), replay.Outcome, replay.ReasonCode, replay.TriggerCount)
	}

	for _, slot := range []time.Time{at(5), at(10), at(15), at(20), at(25)} {
		r, ok := rows[slot]
		if !ok {
			t.Fatalf("the gap row at %v vanished", slot)
		}
		if r.Outcome != "missed" || r.ReasonCode != "TICK_MISSED_DAEMON_DOWN" {
			t.Fatalf("the gap row at %v became %s/%s, want it left as missed/TICK_MISSED_DAEMON_DOWN",
				slot, r.Outcome, r.ReasonCode)
		}
	}

	cursor := readCursor(t, ctx, s, "nightly", "default")
	if cursor == nil || !cursor.Equal(at(30)) {
		t.Fatalf("the cursor sits at %v, want %v: the replayed fire-time must move it", cursor, at(30))
	}
	testutil.AssertNoUnknownReasons(t, ctx, s)
}

// assertFsckClean is AC9: no scenario here may leave the database in a shape
// the integrity checks object to.
func assertFsckClean(t *testing.T, ctx context.Context, s *store.Store) {
	t.Helper()
	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("fsck found %d violations after the replay: %+v", len(violations), violations)
	}
}

// TestCatchupAllDripsThroughTheGapWalksEvidence is AC5. Every slot of the
// outage is owed, the limit doses three per wake, and each wake must move the
// cursor even though the gap walk holds every slot it wants.
func TestCatchupAllDripsThroughTheGapWalksEvidence(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	clk := clock.NewFake(gapDay)

	primeCursor(t, ctx, s, clk, "all", 3)
	before := queuedRuns(t, ctx, s)

	clk.Set(at(32))
	stageOutage(t, ctx, s, at(1), at(32), at(5), at(10), at(15), at(20), at(25), at(30))

	// First wake: the dose is three, oldest first, and the cursor lands on
	// the newest of them.
	if err := newSource(s, clk).Tick(ctx); err != nil {
		t.Fatalf("first wake: %v", err)
	}
	if got := queuedRuns(t, ctx, s) - before; got != 3 {
		t.Fatalf("the first wake queued %d runs, want the dose of three", got)
	}
	if cursor := readCursor(t, ctx, s, "nightly", "default"); cursor == nil || !cursor.Equal(at(15)) {
		t.Fatalf("after the first wake the cursor sits at %v, want %v", cursor, at(15))
	}

	// Second wake: the remainder, recomputed from the advanced cursor.
	if err := newSource(s, clk).Tick(ctx); err != nil {
		t.Fatalf("second wake: %v", err)
	}
	if got := queuedRuns(t, ctx, s) - before; got != 6 {
		t.Fatalf("after two wakes %d runs are queued, want all six owed fire-times", got)
	}
	if cursor := readCursor(t, ctx, s, "nightly", "default"); cursor == nil || !cursor.Equal(at(30)) {
		t.Fatalf("after the second wake the cursor sits at %v, want %v", cursor, at(30))
	}

	rows := ticksByFireTime(t, ctx, s)
	for _, slot := range []time.Time{at(5), at(10), at(15), at(20), at(25), at(30)} {
		r, ok := rows[slot]
		if !ok {
			t.Fatalf("no tick row at %v", slot)
		}
		if r.Outcome != store.OutcomeTriggered || r.TriggerCount != 1 {
			t.Fatalf("the %v row is %s/%s with trigger_count %d, want a triggered replay",
				slot, r.Outcome, r.ReasonCode, r.TriggerCount)
		}
	}
	assertFsckClean(t, ctx, s)
	testutil.AssertNoUnknownReasons(t, ctx, s)
}

// TestCatchupSkipLeavesTheOutageToTheGapWalk is AC6. The default policy wants
// no replay, so the gap walk's explanation must survive untouched: a slot
// nobody will ever run is better described as missed than as skipped.
func TestCatchupSkipLeavesTheOutageToTheGapWalk(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	clk := clock.NewFake(gapDay)

	primeCursor(t, ctx, s, clk, "skip", 10)
	before := queuedRuns(t, ctx, s)

	clk.Set(at(32))
	stageOutage(t, ctx, s, at(1), at(32), at(5), at(10), at(15), at(20), at(25), at(30))

	if err := newSource(s, clk).Tick(ctx); err != nil {
		t.Fatalf("the catch-up tick: %v", err)
	}
	if got := queuedRuns(t, ctx, s) - before; got != 0 {
		t.Fatalf("catchup=skip queued %d runs out of the outage, want none", got)
	}

	rows := ticksByFireTime(t, ctx, s)
	for _, slot := range []time.Time{at(5), at(10), at(15), at(20), at(25), at(30)} {
		r, ok := rows[slot]
		if !ok {
			t.Fatalf("the gap row at %v vanished", slot)
		}
		if r.Outcome != store.OutcomeMissed || r.ReasonCode != "TICK_MISSED_DAEMON_DOWN" {
			t.Fatalf("catchup=skip rewrote the gap row at %v into %s/%s", slot, r.Outcome, r.ReasonCode)
		}
	}
	assertFsckClean(t, ctx, s)
	testutil.AssertNoUnknownReasons(t, ctx, s)
}

// captureLog builds a logger a test can read back, at the level an operator
// actually watches.
func captureLog() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

func newSourceLogging(st scheduler.Store, clk clock.Clock, log *slog.Logger) *scheduler.Source {
	src, err := scheduler.New(scheduler.Config{Store: st, Clock: clk, Holder: "test", Log: log})
	if err != nil {
		panic(err)
	}
	return src
}

// lossStore answers every materialisation with one fixed result, so each
// shape of loss can be held to its own reporting contract.
type lossStore struct {
	*scriptedStore
	res store.TickResult
}

func (s *lossStore) MaterializeTick(ctx context.Context, in store.TickInput) (store.TickResult, error) {
	s.materialized = append(s.materialized, in)
	return s.res, nil
}

// backlogStore is a scripted wake owing six fire-times of one schedule.
func backlogStore(clk clock.Clock, res store.TickResult) *lossStore {
	now := clk.Now().UTC()
	return &lossStore{
		scriptedStore: &scriptedStore{
			grants: []bool{true},
			due: []store.ScheduleRow{{
				ID: "01BACKLOG", JobName: "nightly", Name: "default", Kind: "cron",
				Expr: "*/5 * * * *", Timezone: "UTC", Catchup: "all",
				CatchupLimit: 10, CatchupWindowMS: 86_400_000,
				NextTickAt: now.Add(-30 * time.Minute),
			}},
		},
		res: res,
	}
}

// TestALossToARivalHolderStaysSilent is the regression that keeps normal
// leader handover out of the log. The tick gate refuses foreign fire-times
// many times a minute in a two instance setup, and none of it is news.
func TestALossToARivalHolderStaysSilent(t *testing.T) {
	clk := clock.NewFake(gapDay)
	log, buf := captureLog()
	st := backlogStore(clk, store.TickResult{LostTo: store.LossEvaluation})

	if err := newSourceLogging(st, clk, log).Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(st.materialized) == 0 {
		t.Fatal("the scenario wrote nothing; it proves nothing about silence")
	}
	if buf.Len() != 0 {
		t.Fatalf("a rival holder loss produced log output at info or above:\n%s", buf.String())
	}
}

// TestALossToGapDetectionIsReportedOncePerSchedule is the other half. The
// decision was overruled by a row that only says nobody was there, and that
// used to leave no trace anywhere. One line per schedule, not one per slot:
// a day long outage of a minute schedule must not become a day of log.
func TestALossToGapDetectionIsReportedOncePerSchedule(t *testing.T) {
	clk := clock.NewFake(gapDay)
	log, buf := captureLog()
	st := backlogStore(clk, store.TickResult{LostTo: store.LossMissed})

	if err := newSourceLogging(st, clk, log).Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
	lines := strings.Count(strings.TrimSpace(buf.String()), "\n") + 1
	if buf.Len() == 0 {
		t.Fatal("losing every fire-time to gap detection produced no log line at all")
	}
	if lines != 1 {
		t.Fatalf("%d log lines for one schedule's outage, want exactly one:\n%s", lines, buf.String())
	}
	got := buf.String()
	for _, want := range []string{
		"schedule=nightly/default",
		"replayed=0",
		"left_as_missed=" + strconv.Itoa(len(st.materialized)),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("the report is missing %q:\n%s", want, got)
		}
	}
}

// TestAReplayIsReportedBesideWhatItLeftBehind is the line an operator
// following the troubleshooting page needs: the catch-up ran one fire-time
// and left the rest to the outage that explains them.
func TestAReplayIsReportedBesideWhatItLeftBehind(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	clk := clock.NewFake(gapDay)

	primeCursor(t, ctx, s, clk, "last", 10)
	clk.Set(at(32))
	stageOutage(t, ctx, s, at(1), at(32), at(5), at(10), at(15), at(20), at(25), at(30))

	log, buf := captureLog()
	if err := newSourceLogging(s, clk, log).Tick(ctx); err != nil {
		t.Fatalf("the catch-up tick: %v", err)
	}
	got := buf.String()
	for _, want := range []string{
		"catch-up met gap detection's evidence at the tick gate",
		"schedule=nightly/default",
		"replayed=1",
		"left_as_missed=5",
		"oldest=2026-05-01T10:05:00Z",
		"newest=2026-05-01T10:30:00Z",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("the report is missing %q:\n%s", want, got)
		}
	}
}
