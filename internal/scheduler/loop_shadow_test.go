package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// Shadow mode equivalence (#32), the load-bearing test: identical planning,
// identical per-fire-time decisions, only the materialisation differs. Two
// sources built on identically seeded stores walk the same fourteen days with
// a fake clock - one real, one shadow - and every (scheduled_for, reason_code)
// pair must match one to one, while the shadow side carries outcome
// 'shadow_triggered' where the real side carries 'triggered'.
//
// The strongest guarantee in the issue rides here too: after any number of
// shadow passes, not one run can exist for the shadowed schedule. A run
// cannot start unless it is queued first; if nothing was ever queued,
// nothing was ever executed.

const shadowSimDays = 14

// shadowSeed schedules one cron definition under a ceiling no fixture can
// trip, so the comparison below isolates planning behaviour: with an absent
// executor pool even the REAL branch would otherwise throttle itself at its
// own job ceiling once queued runs pile up - true production behaviour, but
// poison for a planning-diff.
func shadowSeed(t *testing.T, ctx context.Context, s *store.Store,
	job, name, expr string, next time.Time, extra func(*store.ScheduleInput),
) {
	t.Helper()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	spec := `{"schema":"paceq.job.v1","name":"` + job +
		`","max_concurrent":1000000,"steps":[{"name":"build","run":["true"]}]}`
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  job,
		SpecHash: "sha256:" + name,
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	in := store.ScheduleInput{
		JobName:      job,
		Name:         name,
		Kind:         "cron",
		Expr:         expr,
		Timezone:     "UTC",
		Catchup:      "all",
		CatchupLimit: 100000,
		NextTickAt:   next,
	}
	if extra != nil {
		extra(&in)
	}
	if _, err := s.UpsertSchedule(ctx, in); err != nil {
		t.Fatalf("seed the schedule: %v", err)
	}
}

func TestShadowDecisionsMatchRealOnesTickForTick(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC)
	end := start.Add(shadowSimDays * 24 * time.Hour)

	build := func(t *testing.T, name string) (*store.Store, *clock.Fake, *scheduler.Source) {
		s := testutil.TempStore(t)
		shadowSeed(t, ctx, s, "nightly", name, "*/7 * * * *",
			start.Add(5*time.Minute), nil)
		return s, nil, nil
	}

	realStore, _, _ := build(t, "real")
	clkReal := clock.NewFake(start)
	srcReal := newSource(realStore, clkReal)

	shadowStore, _, _ := build(t, "shadow")
	clkShadow := clock.NewFake(start)
	srcShadow := newShadowSource(shadowStore, clkShadow)

	// Both walk the window waking hourly: enough granularity to interleave
	// DST-free single fire-times, cheap enough for 14 simulated days.
	for now := start.Add(time.Hour); !now.After(end); now = now.Add(time.Hour) {
		clkReal.Set(now)
		if err := srcReal.Tick(ctx); err != nil {
			t.Fatalf("real pass at %v: %v", now, err)
		}
		clkShadow.Set(now)
		if err := srcShadow.Tick(ctx); err != nil {
			t.Fatalf("shadow pass at %v: %v", now, err)
		}
	}

	realKeys := tickKeys(t, ctx, realStore, "nightly", "real")
	shadowKeys := tickKeys(t, ctx, shadowStore, "nightly", "shadow")
	if len(realKeys) == 0 {
		t.Fatal("the real pass recorded nothing; the fixture owes hundreds of fire-times")
	}
	if len(realKeys) != len(shadowKeys) {
		t.Fatalf("tick counts diverge: real %d vs shadow %d", len(realKeys), len(shadowKeys))
	}
	for i := range realKeys {
		wantOutcome := realKeys[i].Outcome
		if wantOutcome == "triggered" {
			wantOutcome = "shadow_triggered"
		}
		got := shadowKeys[i]
		if got.ScheduledFor.Compare(realKeys[i].ScheduledFor) != 0 || got.ReasonCode != realKeys[i].ReasonCode ||
			got.Outcome != wantOutcome {
			t.Fatalf("pair %d disagrees:\n real %s/%s/%s\nshadow %s/%s/%s",
				i,
				realKeys[i].ScheduledFor.Format(time.RFC3339), realKeys[i].Outcome, realKeys[i].ReasonCode,
				got.ScheduledFor.Format(time.RFC3339), got.Outcome, got.ReasonCode)
		}
	}

	// Cursor equality: shadow advanced exactly where the real loop did.
	lastReal := readCursor(t, ctx, realStore, "nightly", "real")
	nextReal := readCursorNext(t, ctx, realStore, "nightly", "real")
	lastShadow := readCursor(t, ctx, shadowStore, "nightly", "shadow")
	nextShadow := readCursorNext(t, ctx, shadowStore, "nightly", "shadow")
	realLast, shadowLast := time.Time{}, time.Time{}
	if lastReal != nil {
		realLast = *lastReal
	}
	if lastShadow != nil {
		shadowLast = *lastShadow
	}
	if realLast.Compare(shadowLast) != 0 || nextReal.Compare(nextShadow) != 0 {
		t.Fatalf("cursors diverged: real (%v -> %v) shadow (%v -> %v)",
			realLast, nextReal, shadowLast, nextShadow)
	}

	// The shadow marker is the outcome of a would-run decision; the fixture's
	// catchup=all window inside bounds means triggered dominates.
	var wouldRuns int
	for _, k := range shadowKeys {
		switch k.Outcome {
		case "shadow_triggered":
			wouldRuns++
		case "triggered":
			t.Fatalf("a shadow pass materialised a plain 'triggered' row at %v",
				k.ScheduledFor)
		case "skipped", "error":
			// recorded stand-downs are legal and equal on both sides
		default:
			t.Fatalf("unexpected outcome %q in the shadow set", k.Outcome)
		}
	}
	if wouldRuns == 0 {
		t.Fatal("no would-run was ever recorded; the fixture proves nothing")
	}
}

func newShadowSource(st scheduler.Store, clk clock.Clock) *scheduler.Source {
	src, err := scheduler.New(scheduler.Config{Store: st, Clock: clk, Holder: "test", Log: quietLog(), Shadow: true})
	if err != nil {
		panic(err)
	}
	return src
}

func TestShadowNeverMaterialisesAnythingExecutionShaped(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 6, 1, 3, 0, 0, 0, time.UTC)

	s := testutil.TempStore(t)
	shadowSeed(t, ctx, s, "nightly", "quiet", "* * * * *",
		start.Add(time.Minute), nil)
	clk := clock.NewFake(start)
	src := newShadowSource(s, clk)
	clk.Set(start.Add(10 * time.Minute))
	if err := src.Tick(ctx); err != nil {
		t.Fatalf("the pass errored: %v", err)
	}

	keys := tickKeys(t, ctx, s, "nightly", "quiet")
	if len(keys) == 0 {
		t.Fatal("nothing recorded: every due evaluation owes its tick even in shadow")
	}
	for _, k := range keys {
		if k.Outcome == "triggered" {
			t.Fatalf("fire-time %v is a real trigger inside a shadow instance", k.ScheduledFor)
		}
	}
	runIDs, err := s.ClaimableRunIDs(ctx)
	if err != nil {
		t.Fatalf("read claimable runs: %v", err)
	}
	if len(runIDs) != 0 {
		t.Fatalf("%d runs exist under shadow mode: nothing may ever be queued", len(runIDs))
	}
}

func TestScheduleLevelShadowFlagShadowsOnlyThatSchedule(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 4, 10, 8, 0, 0, 0, time.UTC)

	s := testutil.TempStore(t)
	shadowSeed(t, ctx, s, "nightly", "normal", "*/4 * * * *",
		start.Add(time.Minute), func(in *store.ScheduleInput) { in.Shadow = false })
	shadowSeed(t, ctx, s, "nightly", "ghost", "*/4 * * * *",
		start.Add(time.Minute), func(in *store.ScheduleInput) { in.Shadow = true })
	clk := clock.NewFake(start)
	// No global switch: only the row flag shadows.
	src := newSource(s, clk)
	clk.Set(start.Add(20 * time.Minute))
	if err := src.Tick(ctx); err != nil {
		t.Fatalf("the pass errored: %v", err)
	}

	assertOutcomes := func(name string, want map[string]bool) {
		t.Helper()
		keys := tickKeys(t, ctx, s, "nightly", name)
		if len(keys) == 0 {
			t.Fatalf("schedule %s recorded nothing", name)
		}
		outcomes := map[string]bool{}
		for _, k := range keys {
			outcomes[k.Outcome] = true
		}
		for o := range outcomes {
			if !want[o] {
				t.Fatalf("schedule %s produced unexpected outcome %q", name, o)
			}
		}
	}
	assertOutcomes("normal", map[string]bool{"triggered": true})
	assertOutcomes("ghost", map[string]bool{"shadow_triggered": true})
}
