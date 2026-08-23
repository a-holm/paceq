package scheduler_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// The loop's contract, one test per behaviour: admit first, write nothing
// when nothing is due, survive one broken schedule, explain every skip,
// advance the cursor into the future, and stop between transactions when
// leadership is lost.

// scriptedStore records every call so a test can hold the loop to exactly
// the reads and writes its scenario allows.
type scriptedStore struct {
	store.Store // nil panics on any unscripted embedded call

	grants []bool // one answer per AcquireOrRenew call; last repeats
	due    []store.ScheduleRow

	dueCalls      int
	materialized  []store.TickInput
	acquireCalls  int
	failDue       error
	failAdmission error
}

func (s *scriptedStore) AcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (store.LeaseGrant, bool, error) {
	s.acquireCalls++
	if s.failAdmission != nil {
		return store.LeaseGrant{}, false, s.failAdmission
	}
	if len(s.grants) == 0 {
		// Script exhausted: leadership is gone, which is what a mid-pass
		// takeover looks like to every later renewal.
		return store.LeaseGrant{}, false, nil
	}
	ok := s.grants[0]
	if len(s.grants) > 1 {
		s.grants = s.grants[1:]
	}
	if !ok {
		return store.LeaseGrant{}, false, nil
	}
	return store.LeaseGrant{Name: name, Epoch: 1}, true, nil
}

func (s *scriptedStore) DueSchedules(ctx context.Context, nowMilli int64, max int) ([]store.ScheduleRow, error) {
	s.dueCalls++
	if s.failDue != nil {
		return nil, s.failDue
	}
	return s.due, nil
}

func (s *scriptedStore) MaterializeTick(ctx context.Context, in store.TickInput) (store.TickResult, error) {
	s.materialized = append(s.materialized, in)
	return store.TickResult{Claimed: true}, nil
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func newSource(st scheduler.Store, clk clock.Clock) *scheduler.Source {
	src, err := scheduler.New(scheduler.Config{Store: st, Clock: clk, Holder: "test", Log: quietLog()})
	if err != nil {
		panic(err)
	}
	return src
}

// newSourceRenew builds a source whose in-pass leadership recheck fires on
// every materialisation, so a scripted store can take leadership away
// mid pass deterministically.
func newSourceRenew(st scheduler.Store, clk clock.Clock) *scheduler.Source {
	src, err := scheduler.New(scheduler.Config{Store: st, Clock: clk, Holder: "test", Log: quietLog(), Renew: -time.Second})
	if err != nil {
		panic(err)
	}
	return src
}

func TestNewRefusesToRunWithoutAHolder(t *testing.T) {
	if _, err := scheduler.New(scheduler.Config{Clock: testutil.NewClock(t)}); err == nil {
		t.Fatal("New accepted a source with no holder identity")
	}
}

func TestAnotherHolderLeadsSoNothingIsReadOrWritten(t *testing.T) {
	st := &scriptedStore{grants: []bool{false}}
	err := newSource(st, testutil.NewClock(t)).Tick(context.Background())
	if err != nil {
		t.Fatalf("a lost lease is silence, not an error: %v", err)
	}
	if st.dueCalls != 0 || len(st.materialized) != 0 {
		t.Fatalf("a follower read due rows (%d) or wrote ticks (%d)", st.dueCalls, len(st.materialized))
	}
}

func TestAnEmptyWakeWritesNothing(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	clk := testutil.NewClock(t)

	// A schedule that is not due for a year, so every wake finds nothing.
	seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
		in.NextTickAt = clk.Now().Add(365 * 24 * time.Hour)
	})

	src := newSource(s, clk)
	for i := 0; i < 3; i++ {
		if err := src.Tick(ctx); err != nil {
			t.Fatalf("wake %d: %v", i, err)
		}
		clk.Advance(time.Minute)
	}

	ticks, err := s.ScheduleTicks(ctx, "nightly", "default")
	if err != nil || len(ticks) != 0 {
		t.Fatalf("an empty wake wrote %d tick rows; there is no such thing as NOT_DUE (%v)", len(ticks), err)
	}
	if n := queuedRuns(t, ctx, s); n != 0 {
		t.Fatalf("an empty wake wrote %d runs", n)
	}
}

func TestOneBrokenScheduleLeavesTheOthersRunning(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	clk := testutil.NewClock(t)

	brokenAt := clk.Now().Add(-time.Hour)
	good := seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
		in.Name = "good"
		in.Expr = "*/5 * * * *"
		in.NextTickAt = clk.Now().Add(-time.Hour)
	})
	seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
		in.Name = "broken"
		in.Expr = "*/5 * * * *"
		in.Timezone = "Mars/Olympus"
		in.NextTickAt = brokenAt
	})

	if err := newSource(s, clk).Tick(ctx); err != nil {
		t.Fatalf("one broken schedule killed the pass: %v", err)
	}

	ticks, err := s.ScheduleTicks(ctx, "nightly", "broken")
	if err != nil || len(ticks) != 1 {
		t.Fatalf("the broken schedule has %d ticks, want exactly the config error row (%v)", len(ticks), err)
	}
	if ticks[0].Outcome != "error" || ticks[0].ReasonCode != "TICK_ERROR_CONFIG" ||
		ticks[0].TriggerCount != 0 || !ticks[0].ScheduledFor.Equal(brokenAt.Truncate(time.Second)) {
		t.Fatalf("the config row is wrong: %+v (want TICK_ERROR_CONFIG at %s)", ticks[0], brokenAt)
	}
	after := readCursor(t, ctx, s, "nightly", "broken")
	if after != nil {
		t.Fatalf("the broken schedule's cursor moved to %v; it must stay due until fixed", after)
	}

	if _, err := s.ScheduleTicks(ctx, "nightly", good.Name); err != nil {
		t.Fatalf("the healthy schedule was never processed: %v", err)
	}
	if n := queuedRuns(t, ctx, s); n == 0 {
		t.Fatal("the healthy schedule produced no run through the same wake")
	}
}

func TestSkippedEvaluationsAllCarryReasonsAndTheCursorLandsAhead(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	clk := testutil.NewClock(t)

	// Half an hour of backlog ends three minutes ago: owed, but not fresh-now.
	now := time.Date(2026, 3, 15, 12, 28, 0, 0, time.UTC)
	seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
		in.Expr = "*/5 * * * *"
		in.NextTickAt = now.Add(-30 * time.Minute)
	})
	clk.Set(now)

	if err := newSource(s, clk).Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}

	ticks, err := s.ScheduleTicks(ctx, "nightly", "default")
	if err != nil {
		t.Fatalf("read the ticks back: %v", err)
	}
	// */5 over the half open window (11:58, 12:28]: six owed fire-times,
	// all identical DISABLED skips, so they fold into ONE row with the
	// whole backlog counted on it.
	if len(ticks) != 1 {
		t.Fatalf("%d rows were recorded for the backlog, want 1 coalesced row: %+v", len(ticks), ticks)
	}
	for _, v := range ticks {
		if v.Outcome != "skipped" || v.ReasonCode == "" || v.ReasonCode == "UNKNOWN" {
			t.Fatalf("backlog evaluation %+v carries no usable reason code", v)
		}
		if v.ReasonCode != "TICK_SKIPPED_CATCHUP_DISABLED" {
			t.Fatalf("skip policy recorded %s, want CATCHUP_DISABLED", v.ReasonCode)
		}
	}
	if ticks[0].RepeatCount != 6 {
		t.Fatalf("repeat_count is %d, want 6: every identical skip must be counted on the row", ticks[0].RepeatCount)
	}
	testutil.AssertNoUnknownReasons(t, ctx, s)

	next := readCursorNext(t, ctx, s, "nightly", "default")
	if !next.After(now) {
		t.Fatalf("after a drained pass next_tick_at is %v, which is not in the future of %v", next, now)
	}
	if n := queuedRuns(t, ctx, s); n != 0 {
		t.Fatalf("catchup=skip produced %d runs", n)
	}
}

func TestThePassStopsBetweenTransactionsWhenTheLeaseIsLost(t *testing.T) {
	clk := testutil.NewClock(t)
	// Leadership holds for the admission and the first materialisation, then
	// a rival takes over mid pass.
	st := &scriptedStore{grants: []bool{true, true, false}}
	base := clk.Now().Add(-time.Hour)
	sched, _ := cronx.Parse("*/5 * * * *")
	occs, err := sched.Between(base.Add(-15*time.Minute), base, time.UTC, cronx.Policy{})
	if err != nil || len(occs) < 2 {
		t.Fatalf("fixture owes %d occurrences (%v)", len(occs), err)
	}
	st.due = []store.ScheduleRow{{
		ID: "01SCHED", JobName: "nightly", Name: "default",
		Kind: "cron", Expr: "*/5 * * * *", Timezone: "UTC",
		Catchup: "all", CatchupLimit: 10, CatchupWindowMS: 86_400_000,
		NextTickAt: occs[0].At,
	}}

	if err := newSourceRenew(st, clk).Tick(context.Background()); err != nil {
		t.Fatalf("the pass errored instead of stopping quietly: %v", err)
	}
	if len(st.materialized) < 1 {
		t.Fatal("the pass stopped before doing any work at all")
	}
	if len(st.materialized) > 1 {
		t.Fatalf("after losing the lease the pass still materialised %d fire-times; it must stop between transactions", len(st.materialized))
	}
	first := st.materialized[0]
	if !first.ScheduledFor.Equal(occs[0].At) {
		t.Fatalf("the pass started at %v, want the oldest owed fire-time %v", first.ScheduledFor, occs[0].At)
	}
}

func TestDSTSeamsMapOntoRecordedOutcomesNotInventedBehaviour(t *testing.T) {
	ctx := context.Background()

	oslo, err := cronx.LoadZone("Europe/Oslo")
	if err != nil {
		t.Fatalf("load Oslo: %v", err)
	}
	cases := []struct {
		name      string
		expr      string
		from      time.Time
		to        time.Time
		wantCodes map[string]int // reason code -> rows
	}{
		{
			name:      "spring gap skips the nonexistent wall",
			expr:      "0 2 * * *",
			from:      time.Date(2026, 3, 29, 0, 0, 0, 0, oslo),
			to:        time.Date(2026, 3, 29, 4, 0, 0, 0, oslo),
			wantCodes: map[string]int{"TICK_SKIPPED_DST_NONEXISTENT": 1},
		},
		{
			name:      "fall back keeps the first pass and explains the second",
			expr:      "0 2 * * *",
			from:      time.Date(2026, 10, 25, 0, 0, 0, 0, oslo),
			to:        time.Date(2026, 10, 25, 4, 0, 0, 0, oslo),
			wantCodes: map[string]int{"TICK_SKIPPED_DST_DUPLICATE": 1},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := testutil.TempStore(t)
			clk := testutil.NewClock(t)
			clk.Set(tc.to)
			seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
				in.Expr = tc.expr
				in.Timezone = "Europe/Oslo"
				in.SpringForward = "skip"
				in.FallBack = "first"
				in.Catchup = "all"
				in.NextTickAt = tc.from
			})

			if err := newSource(s, clk).Tick(ctx); err != nil {
				t.Fatalf("tick across the seam: %v", err)
			}

			ticks, err := s.ScheduleTicks(ctx, "nightly", "default")
			if err != nil {
				t.Fatalf("read the seam ticks: %v", err)
			}
			got := make(map[string]int)
			for _, v := range ticks {
				if v.ReasonCode != "" {
					got[v.ReasonCode]++
				}
			}
			for code, n := range tc.wantCodes {
				if got[code] != n {
					t.Fatalf("seam produced %v, want %s=%d among %+v", got, code, n, ticks)
				}
			}

			// Completeness: every emission the iterator owes the window has
			// exactly one recorded evaluation, whatever its outcome.
			parsed, err := cronx.Parse(tc.expr)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			occs, err := parsed.Between(tc.from, tc.to, oslo, cronx.Policy{})
			if err != nil && len(occs) == 0 {
				t.Fatalf("iterator refused the fixture window: %v", err)
			}
			if len(ticks) != len(occs) {
				t.Fatalf("%d evaluations were recorded for %d emissions the iterator owes: %+v",
					len(ticks), len(occs), ticks)
			}
			testutil.AssertNoUnknownReasons(t, ctx, s)
		})
	}
}

// seedSchedule creates the nightly job plus one schedule, letting the case
// override whatever it needs.
func seedSchedule(t *testing.T, ctx context.Context, s *store.Store, mutate func(*store.ScheduleInput)) store.ScheduleRow {
	t.Helper()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:sched",
		SpecJSON: `{"schema":"paceq.job.v1","name":"nightly","max_concurrent":200,"steps":[{"name":"build","run":["true"]}]}`,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	clkNow := time.Now().UTC()
	in := store.ScheduleInput{
		JobName:    "nightly",
		Name:       "default",
		Kind:       "cron",
		Expr:       "* * * * *",
		Timezone:   "UTC",
		Catchup:    "skip",
		NextTickAt: clkNow,
	}
	if mutate != nil {
		mutate(&in)
	}
	row, err := s.UpsertSchedule(ctx, in)
	if err != nil {
		t.Fatalf("seed the schedule: %v", err)
	}
	return row
}

// readCursor returns the schedule's last_tick_at (nil when never ticked).
func readCursor(t *testing.T, ctx context.Context, s *store.Store, job, name string) *time.Time {
	t.Helper()
	last, _, err := s.ScheduleCursor(ctx, job, name)
	if err != nil {
		t.Fatalf("read the cursor of %s/%s: %v", job, name, err)
	}
	return last
}

// readCursorNext returns the schedule's next_tick_at.
func readCursorNext(t *testing.T, ctx context.Context, s *store.Store, job, name string) time.Time {
	t.Helper()
	_, next, err := s.ScheduleCursor(ctx, job, name)
	if err != nil {
		t.Fatalf("read the cursor of %s/%s: %v", job, name, err)
	}
	return next
}

// queuedRuns counts runs waiting to be claimed.
func queuedRuns(t *testing.T, ctx context.Context, s *store.Store) int {
	t.Helper()
	ids, err := s.ClaimableRunIDs(ctx)
	if err != nil {
		t.Fatalf("count claimable runs: %v", err)
	}
	return len(ids)
}
