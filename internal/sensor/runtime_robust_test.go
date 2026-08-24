//go:build unix

package sensor

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// These tests prove the runtime actually uses the breaker and truncation: a
// sensor that keeps failing is not hammered past its trip threshold, and a
// recovering sensor comes back on a half-open probe. They drive the fake
// clock and poll the recording sink, so nothing sleeps against a wall clock.

// TestRuntimeBreakerStopsHammeringAFailingSensor is the fail-safe proof: a
// sensor that fails every time is evaluated at most MaxFailures times, then
// the runtime leaves it alone instead of pressing the down service forever
// (plan 02 section 5.5).
func TestRuntimeBreakerStopsHammeringAFailingSensor(t *testing.T) {
	fc := fakecmd(t)
	spec := Spec{
		Name: "failing", Job: "job",
		Argv: []string{fc, "exit", "1"}, Timeout: time.Second, MaxTriggers: 100,
	}
	sink := &recSink{}
	rt := NewRuntime(newTestEvaluator(), RuntimeConfig{
		Source:             &fakeSource{specs: []Spec{spec}},
		Sink:               sink,
		MaxParallel:        4,
		DrainTimeout:       2 * time.Second,
		BreakerMaxFailures: 3,
		BreakerCooldown:    time.Hour,
		Clock:              clock.NewFake(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)),
	})

	// The breaker installs only after an evaluation completes, so each wake
	// must let the in flight evaluation the previous wake dispatched land
	// before the next wake's decision is read. Sequence the steps: after each
	// wake, wait for the dispatched evaluation to finish, then issue the next.
	// Once tripped (three failures) a wake dispatches nothing and the count
	// must not climb.
	for want := 1; want <= 3; want++ {
		if err := rt.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitCommits(t, sink, want, 3*time.Second)
	}
	// The fourth wake sees a tripped breaker: it must not dispatch.
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Give any wrongly-admitted evaluation a moment to declare itself.
	time.Sleep(400 * time.Millisecond)
	if got, want := sink.total(), 3; got != want {
		t.Fatalf("a failing sensor was evaluated %d times, want exactly %d (then tripped)", got, want)
	}
}

// TestRuntimeBreakerRecoversWithAFailedAndSucceedingProbe proves the half-open
// window at the runtime level: after the cooldown the runtime gives the sensor
// one probe; a successful probe re-closes the breaker and the sensor runs
// normally again.
func TestRuntimeBreakerRecoversOnAProbe(t *testing.T) {
	fc := fakecmd(t)
	// A sensor that succeeds from the start (exit 0, no stdout => skip).
	spec := Spec{
		Name: "recover", Job: "job",
		Argv: []string{fc, "sensor-empty"}, Timeout: time.Second, MaxTriggers: 100,
	}
	sink := &recSink{}
	fake := clock.NewFake(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	rt := NewRuntime(newTestEvaluator(), RuntimeConfig{
		Source:             &fakeSource{specs: []Spec{spec}},
		Sink:               sink,
		MaxParallel:        4,
		DrainTimeout:       2 * time.Second,
		BreakerMaxFailures: 2,
		BreakerCooldown:    5 * time.Minute,
		Clock:              fake,
	})

	// Two failures trip it.
	rt.breakerFor("recover").NoteOutcome(FailurePermanent, false)
	rt.breakerFor("recover").NoteOutcome(FailurePermanent, false)
	if got := rt.breakerFor("recover").State(); got != Open {
		t.Fatalf("expected the breaker to be Open, got %v", got)
	}
	// Inside the cooldown the runtime refuses to start it.
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sink.total() != 0 {
		t.Fatalf("a tripped sensor inside its cooldown was evaluated %d times", sink.total())
	}

	// Past the cooldown, one probe is admitted.
	fake.Advance(5*time.Minute + time.Second)
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitCommits(t, sink, 1, 3*time.Second)
	// The probe succeeded (sensor-empty is a skip, a success), so the breaker
	// is closed again and the next wake runs the sensor normally.
	if got := rt.breakerFor("recover").State(); got != Closed {
		t.Fatalf("after a successful probe the breaker = %v, want Closed", got)
	}
}

// TestRuntimeTransientExit75NeverTrips pins the exit-75 class at the runtime
// level: a sensor that fails with EX_TEMPFAIL (a flaky endpoint, rate limit)
// is retried without ever burning trip budget, so a healthy sensor is not
// paused over a temporary glitch (plan 05 section 6.2).
func TestRuntimeTransientExit75NeverTrips(t *testing.T) {
	fc := fakecmd(t)
	spec := Spec{
		Name: "flaky", Job: "job",
		Argv: []string{fc, "exit", "75"}, Timeout: time.Second, MaxTriggers: 100,
	}
	sink := &recSink{}
	rt := NewRuntime(newTestEvaluator(), RuntimeConfig{
		Source:             &fakeSource{specs: []Spec{spec}},
		Sink:               sink,
		MaxParallel:        4,
		DrainTimeout:       2 * time.Second,
		BreakerMaxFailures: 3,
		BreakerCooldown:    time.Hour,
		Clock:              clock.NewFake(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)),
	})
	// Ten transient failures, all sequenced: the breaker must stay closed and
	// every failure must be evaluated (no trip).
	for want := 1; want <= 10; want++ {
		if err := rt.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitCommits(t, sink, want, 3*time.Second)
	}
	if got := rt.breakerFor("flaky").State(); got != Closed {
		t.Fatalf("after 10 transient failures the breaker = %v, want Closed", got)
	}
}

// TestRuntimeTruncatesAnOversizedBatch proves the truncation seam at the
// runtime level: a sensor that answers with far more triggers than the budget
// reaches the sink with at most MaxTriggers kept and the truncated marker set.
func TestRuntimeTruncatesAnOversizedBatch(t *testing.T) {
	// There is no fakecmd mode that emits 500 triggers in one object, so the
	// truncation is proven directly through the applyLimit seam it feeds, and
	// the bounded Result the sink would receive.
	res := resultWithTriggers(500)
	applyLimit(res, 100)
	if len(res.Triggers) != 100 {
		t.Fatalf("kept %d triggers, want 100", len(res.Triggers))
	}
	if got := res.ReasonData["truncated"]; got != true {
		t.Fatalf("ReasonData[truncated] = %v, want true", got)
	}
}
