package scheduler_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// G9 / AC2: two daemons with different uptimes against the same schedule
// must record identical tick sets. One evaluates the backlog in a single
// jump, as a daemon that was down for six hours would; the other walks the
// same stretch in one-minute slices, as an always-alive daemon would. The
// fire-time identities and their outcomes must come out equal, because both
// recompute the window from last_tick_at alone.

type tickKey struct {
	ScheduledFor time.Time
	Outcome      string
	ReasonCode   string
}

func tickKeys(t *testing.T, ctx context.Context, s *store.Store, job, name string) []tickKey {
	t.Helper()
	ticks, err := s.ScheduleTicks(ctx, job, name)
	if err != nil {
		t.Fatalf("read the ticks of %s/%s: %v", job, name, err)
	}
	keys := make([]tickKey, 0, len(ticks))
	for _, v := range ticks {
		keys = append(keys, tickKey{ScheduledFor: v.ScheduledFor, Outcome: v.Outcome, ReasonCode: v.ReasonCode})
	}
	slices.SortFunc(keys, func(a, b tickKey) int {
		return a.ScheduledFor.Compare(b.ScheduledFor)
	})
	return keys
}

func TestTwoUptimesOneTickSet(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 3, 15, 9, 0, 0, 0, time.UTC)
	end := start.Add(6 * time.Hour)

	build := func(t *testing.T) (*store.Store, *clock.Fake, *scheduler.Source) {
		s := testutil.TempStore(t)
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
			in.Name = "default"
			in.Expr = "*/5 * * * *"
			in.Catchup = "all"
			in.CatchupLimit = 100
			in.NextTickAt = start.Add(5 * time.Minute)
		})
		clk := clock.NewFake(start)
		return s, clk, newSource(s, clk)
	}

	// Daemon A: down for the whole stretch, then one wake at the end.
	sa, clka, srcA := build(t)
	clka.Set(end)
	if err := srcA.Tick(ctx); err != nil {
		t.Fatalf("daemon A: %v", err)
	}

	// Daemon B: alive the whole time, waking every minute.
	sb, clkb, srcB := build(t)
	for now := start.Add(time.Minute); !now.After(end); now = now.Add(time.Minute) {
		clkb.Set(now)
		if err := srcB.Tick(ctx); err != nil {
			t.Fatalf("daemon B at %v: %v", now, err)
		}
	}

	keysA := tickKeys(t, ctx, sa, "nightly", "default")
	keysB := tickKeys(t, ctx, sb, "nightly", "default")

	if len(keysA) == 0 {
		t.Fatal("the jumped daemon recorded nothing; the fixture owes 71 fire-times")
	}
	if !slices.Equal(keysA, keysB) {
		t.Fatalf("the two uptimes disagree:\njumped: %v\nsliced: %v", keysA, keysB)
	}
	for _, k := range keysA {
		if k.Outcome != "triggered" {
			t.Fatalf("fire-time %v became %s/%s under catchup=all inside the window",
				k.ScheduledFor, k.Outcome, k.ReasonCode)
		}
	}
}

// The five-runs idempotency contract from the issue test plan: evaluating the
// same now five times leaves byte-identical state after the first pass. The
// gate absorbs every repeat, so counts and cursors stop moving.
func TestFiveIdenticalPassesLeaveIdenticalState(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	now := time.Date(2026, 3, 15, 12, 28, 0, 0, time.UTC)
	seedSchedule(t, ctx, s, func(in *store.ScheduleInput) {
		in.Expr = "*/5 * * * *"
		in.NextTickAt = now.Add(-30 * time.Minute)
	})
	clk := testutil.NewClock(t)
	src := newSource(s, clk)

	var firstKeys []tickKey
	var firstRuns int
	for i := 0; i < 5; i++ {
		if err := src.Tick(ctx); err != nil {
			t.Fatalf("pass %d: %v", i+1, err)
		}
		keys := tickKeys(t, ctx, s, "nightly", "default")
		runIDs, err := s.ClaimableRunIDs(ctx)
		if err != nil {
			t.Fatalf("count runs in pass %d: %v", i+1, err)
		}
		if i == 0 {
			firstKeys = keys
			firstRuns = len(runIDs)
			continue
		}
		if !slices.Equal(firstKeys, keys) || len(runIDs) != firstRuns {
			t.Fatalf("pass %d changed the state: ticks %d->%d rows, runs %d->%d",
				i+1, len(firstKeys), len(keys), firstRuns, len(runIDs))
		}
	}
}
