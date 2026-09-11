// Package shadow proves the seam between paceq apply and the scheduler loop
// (#203). A job file that carries a top-level shadow flag and repeats it on
// none of its schedules must record every fire-time and execute nothing, with
// no daemon-wide switch anywhere.
//
// The proof lives here because internal/scheduler may not import internal/spec,
// and the guard in internal/arch/deps_test.go has no exceptions: the loop plans
// from schedule rows and must never learn the job grammar. Driving a real apply
// needs that grammar, because store.JobVersionInput carries []spec.Schedule, so
// the two halves can only meet outside internal/.
//
// internal/store/schedulesync_test.go holds the write half against the rows an
// apply leaves behind, and internal/scheduler holds the read half against rows
// staged by hand. This package is what keeps both honest, because nothing here
// writes a schedule row itself.
package shadow

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// storeOnClock opens a store that stamps from the same clock the scheduler
// reads. An apply computes next_tick_at from the store's own clock, so a test
// that applies a job and then ticks it needs both ends on one timeline.
func storeOnClock(t *testing.T, ctx context.Context, clk clock.Clock) *store.Store {
	t.Helper()

	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"), store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open the test store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close the test store: %v", err)
		}
	})
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// newSource builds the loop with no instance-wide shadow switch, so a job file
// is the only thing in this package that can ask for shadow at all.
func newSource(t *testing.T, s *store.Store, clk clock.Clock) *scheduler.Source {
	t.Helper()

	src, err := scheduler.New(scheduler.Config{
		Store:  s,
		Clock:  clk,
		Holder: "test",
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("build the scheduler: %v", err)
	}
	return src
}

// applyJob applies one job the way paceq apply does, so the schedule rows under
// test are the rows an apply writes and not rows a fixture seeded by hand.
func applyJob(t *testing.T, ctx context.Context, s *store.Store,
	job string, jobShadow bool, schedules ...spec.Schedule,
) {
	t.Helper()

	if _, err := s.ApplyJobs(ctx, []store.JobVersionInput{{
		JobName:       job,
		MaxConcurrent: 1000,
		SpecHash:      "sha256:" + job,
		SpecJSON: `{"schema":"paceq.job.v1","name":"` + job +
			`","max_concurrent":1000,"steps":[{"name":"build","run":["true"]}]}`,
		Shadow:    jobShadow,
		Schedules: schedules,
	}}); err != nil {
		t.Fatalf("apply %s: %v", job, err)
	}
}

// tickKey is one recorded decision, reduced to what a shadow proof compares.
type tickKey struct {
	ScheduledFor time.Time
	Outcome      string
}

func tickKeys(t *testing.T, ctx context.Context, s *store.Store, job, name string) []tickKey {
	t.Helper()

	ticks, err := s.ScheduleTicks(ctx, job, name)
	if err != nil {
		t.Fatalf("read the ticks of %s/%s: %v", job, name, err)
	}
	keys := make([]tickKey, 0, len(ticks))
	for _, v := range ticks {
		keys = append(keys, tickKey{ScheduledFor: v.ScheduledFor, Outcome: v.Outcome})
	}
	slices.SortFunc(keys, func(a, b tickKey) int {
		return a.ScheduledFor.Compare(b.ScheduledFor)
	})
	return keys
}

// queuedRuns counts runs waiting to be claimed. A run cannot start unless it is
// queued first, so zero here is the whole zero-execution guarantee.
func queuedRuns(t *testing.T, ctx context.Context, s *store.Store) int {
	t.Helper()

	ids, err := s.ClaimableRunIDs(ctx)
	if err != nil {
		t.Fatalf("count claimable runs: %v", err)
	}
	return len(ids)
}

// TestJobLevelShadowShadowsEveryScheduleOfThatJob is the #203 guard. A job file
// that says shadow: true and repeats it on none of its schedules must record
// its fire-times and execute nothing.
//
// The loud job beside it is the control: the same fixture, the same fire-time,
// no flag, and a real run comes out. A green result here therefore cannot come
// from a fixture that never triggers in the first place.
func TestJobLevelShadowShadowsEveryScheduleOfThatJob(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 5, 4, 2, 0, 0, 0, time.UTC)

	clk := clock.NewFake(start)
	s := storeOnClock(t, ctx, clk)
	applyJob(t, ctx, s, "quiet", true,
		spec.Schedule{Name: "early", Cron: "* * * * *"},
		spec.Schedule{Name: "late", Cron: "* * * * *"})
	applyJob(t, ctx, s, "loud", false,
		spec.Schedule{Name: "early", Cron: "* * * * *"})

	src := newSource(t, s, clk)
	clk.Set(start.Add(60*time.Second + 500*time.Millisecond))
	if err := src.Tick(ctx); err != nil {
		t.Fatalf("the pass errored: %v", err)
	}

	for _, name := range []string{"early", "late"} {
		keys := tickKeys(t, ctx, s, "quiet", name)
		if len(keys) == 0 {
			t.Fatalf("schedule quiet/%s recorded nothing: a shadowed schedule still owes its ticks", name)
		}
		shadowed := 0
		for _, k := range keys {
			if k.Outcome == "triggered" {
				t.Errorf("quiet/%s fired for real at %s: the job file said shadow",
					name, k.ScheduledFor.Format(time.RFC3339))
			}
			if k.Outcome == "shadow_triggered" {
				shadowed++
			}
		}
		if shadowed == 0 {
			t.Errorf("quiet/%s recorded no would-run: the fixture proves nothing", name)
		}
	}

	loud := tickKeys(t, ctx, s, "loud", "early")
	fired := 0
	for _, k := range loud {
		if k.Outcome == "triggered" {
			fired++
		}
	}
	if fired != 1 {
		t.Fatalf("the control job recorded %d real triggers, want 1: the fixture is not firing", fired)
	}

	// One run, and it belongs to the job that asked for one. Two more would be
	// the shadowed job executing what the operator told it not to.
	if got := queuedRuns(t, ctx, s); got != 1 {
		t.Fatalf("%d runs exist, want the control job's one alone", got)
	}
}
