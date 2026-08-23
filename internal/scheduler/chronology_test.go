package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// The batch contract this file pins: evaluations are written strictly in
// fire-time order across the whole wake, and every transaction boundary is a
// chronological cut. Before this held, the loop wrote every DST skip before
// every other decision of its schedule, so the first committed transaction
// could push the cursor past older backlog that then was buried: never
// materialised, never explained, invisible to the next pass.

// stopAfterFirstCommit delegates to a real store and cancels the pass the
// moment the first transaction has committed: a daemon killed between
// transactions.
type stopAfterFirstCommit struct {
	scheduler.Store
	cancel context.CancelFunc
	fired  bool
}

func (s *stopAfterFirstCommit) MaterializeTick(ctx context.Context, in store.TickInput) (store.TickResult, error) {
	res, err := s.Store.MaterializeTick(ctx, in)
	if err == nil && !s.fired {
		s.fired = true
		s.cancel()
	}
	return res, err
}

func TestAStopBetweenTransactionsCannotBuryAnOwedFireTime(t *testing.T) {
	ctx := context.Background()
	oslo, err := cronx.LoadZone("Europe/Oslo")
	if err != nil {
		t.Fatalf("load Oslo: %v", err)
	}

	// The October fallback night watched hourly: four real walls around a
	// duplicated hour whose second pass carries the DST mark. Six owed
	// fire-times, all at distinct instants, one decision each under
	// catchup=all.
	now := time.Date(2025, 10, 26, 4, 0, 0, 0, oslo)
	want := []struct {
		at      time.Time
		outcome string
		code    string
	}{
		{time.Date(2025, 10, 25, 22, 0, 0, 0, time.UTC), "triggered", ""},
		{time.Date(2025, 10, 25, 23, 0, 0, 0, time.UTC), "triggered", ""},
		{time.Date(2025, 10, 26, 0, 0, 0, 0, time.UTC), "triggered", ""},
		{time.Date(2025, 10, 26, 1, 0, 0, 0, time.UTC), "skipped", "TICK_SKIPPED_DST_DUPLICATE"},
		{time.Date(2025, 10, 26, 2, 0, 0, 0, time.UTC), "triggered", ""},
		{time.Date(2025, 10, 26, 3, 0, 0, 0, time.UTC), "triggered", ""},
	}

	s := testutil.TempStore(t)
	clk := clock.NewFake(now.UTC())
	seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
		in.Expr = "0 * * * *"
		in.Timezone = "Europe/Oslo"
		in.Catchup = "all"
		in.CatchupLimit = 10
		in.NextTickAt = want[0].at
	})

	// First pass: killed between transactions, right after the first commit.
	passCtx, cancel := context.WithCancel(ctx)
	stopped := &stopAfterFirstCommit{Store: s, cancel: cancel}
	if stopErr := newSource(stopped, clk).Tick(passCtx); stopErr == nil {
		t.Fatal("the cancelled pass reported success; the stop never happened")
	}
	cancel()

	ticks, err := s.ScheduleTicks(ctx, "nightly", "default")
	if err != nil {
		t.Fatalf("read back the stopped pass: %v", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("the stop left %d tick rows, want exactly the one committed transaction: %+v",
			len(ticks), ticks)
	}
	if !ticks[0].ScheduledFor.Equal(want[0].at) {
		t.Fatalf("the surviving transaction fired at %v, want the oldest owed fire-time %v; evaluation is not chronological",
			ticks[0].ScheduledFor, want[0].at)
	}

	// Restart: the daemon comes back and drains what is left. Whatever the
	// stop interrupted, the remaining window is replanned from the cursor
	// and honoured under the same policy.
	if err := newSource(s, clk).Tick(ctx); err != nil {
		t.Fatalf("restart: %v", err)
	}

	ticks, err = s.ScheduleTicks(ctx, "nightly", "default")
	if err != nil {
		t.Fatalf("read back after restart: %v", err)
	}
	if len(ticks) != len(want) {
		t.Fatalf("after the restart %d evaluations are recorded, want one per owed fire-time (%d): %+v",
			len(ticks), len(want), ticks)
	}
	seen := make(map[time.Time]string)
	for _, v := range ticks {
		seen[v.ScheduledFor] = v.Outcome + "|" + v.ReasonCode
	}
	for _, w := range want {
		got, ok := seen[w.at]
		if !ok {
			t.Fatalf("owed fire-time %v has no tick row after the restart; it was buried by the stop", w.at)
		}
		if got != w.outcome+"|"+w.code {
			t.Fatalf("fire-time %v became %q, want %s/%s", w.at, got, w.outcome, w.code)
		}
	}

	if runs := queuedRuns(t, ctx, s); runs != 5 {
		t.Fatalf("%d runs were queued after the drain, want the five real walls (one fire-time is a DST skip)", runs)
	}
	testutil.AssertNoUnknownReasons(t, ctx, s)
}

func TestTheBatchWritesOldestFireTimeFirstAcrossSchedules(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 5, 1, 11, 30, 0, 0, time.UTC))

	// Two schedules sharing the wake, handed to the loop newest-first. Each
	// owes two interleaved fire-times, so only a globally sorted batch can
	// produce the right order; processing schedule by schedule cannot.
	mk := func(name, expr string, next time.Time) store.ScheduleRow {
		return store.ScheduleRow{
			ID:              "01" + name,
			JobName:         "nightly",
			Name:            name,
			Kind:            "cron",
			Expr:            expr,
			Timezone:        "UTC",
			Catchup:         "all",
			CatchupLimit:    10,
			CatchupWindowMS: 86_400_000,
			NextTickAt:      next,
		}
	}
	st := &scriptedStore{grants: []bool{true}}
	st.due = []store.ScheduleRow{
		mk("late", "30 * * * *", clk.Now().Add(-time.Hour)),
		mk("early", "0 * * * *", clk.Now().Add(-90*time.Minute)),
	}

	if err := newSource(st, clk).Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	type wrote struct {
		name string
		at   time.Time
	}
	var got []wrote
	for _, in := range st.materialized {
		got = append(got, wrote{name: in.Schedule.Name, at: in.ScheduledFor})
	}
	want := []wrote{
		{"early", time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)},
		{"late", time.Date(2026, 5, 1, 10, 30, 0, 0, time.UTC)},
		{"early", time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC)},
		{"late", time.Date(2026, 5, 1, 11, 30, 0, 0, time.UTC)},
	}
	if len(got) != len(want) {
		t.Fatalf("the wake materialised %d fire-times (%v), want %d", len(got), got, len(want))
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("write %d went to %s at %v, want %s at %v; the batch is not in fire-time order",
				i+1, got[i].name, got[i].at, w.name, w.at)
		}
	}
}
