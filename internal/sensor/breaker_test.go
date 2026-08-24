package sensor

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The breaker tests drive a fake clock (no sleeps). The cooldown timing is
// proven by advancing the fake clock, never by waiting on real time.

func newTestBreaker(t *testing.T, max int, cool time.Duration) (*Breaker, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	b := NewBreaker(BreakerConfig{MaxFailures: max, Cooldown: cool, Clock: fc})
	if got := b.State(); got != Closed {
		t.Fatalf("a fresh breaker starts %v, want Closed", got)
	}
	return b, fc
}

// TestBreakerTripsAfterNFailures is the M3-05 breaker kernel: nine permanent
// failures leave it closed (evaluating), the tenth opens it and refuses work.
func TestBreakerTripsAfterNFailures(t *testing.T) {
	b, _ := newTestBreaker(t, 10, time.Hour)
	for i := 1; i <= 9; i++ {
		if got := b.NoteOutcome(FailurePermanent, false); got != Closed {
			t.Fatalf("after %d permanent failures state = %v, want Closed", i, got)
		}
		if !b.Admit() {
			t.Fatalf("after %d failures Admit refused an evaluation", i)
		}
	}
	if got := b.NoteOutcome(FailurePermanent, false); got != Open {
		t.Fatalf("the 10th permanent failure state = %v, want Open", got)
	}
	if b.Admit() {
		t.Fatal("a tripped breaker still admits an evaluation inside its cooldown")
	}
}

// TestTransientFailuresNeverTrip pins exit 75: fifty transient failures never
// open the breaker, because a flaky endpoint must not pause a healthy sensor
// over night (plan 05 section 6.2).
func TestTransientFailuresNeverTrip(t *testing.T) {
	b, _ := newTestBreaker(t, 10, time.Hour)
	for i := 0; i < 50; i++ {
		if got := b.NoteOutcome(FailureTransient, false); got != Closed {
			t.Fatalf("transient failure %d state = %v, want Closed (no trip)", i, got)
		}
	}
	if b.State() != Closed {
		t.Fatal("50 transient failures tripped the breaker")
	}
}

// TestSuccessResetsBreakerCounter pins that a success clears the consecutive
// failure budget, so the breaker only counts uninterrupted consecutive
// failures (plan 02 section 5.5).
func TestSuccessResetsBreakerCounter(t *testing.T) {
	b, _ := newTestBreaker(t, 10, time.Hour)
	for i := 0; i < 9; i++ {
		b.NoteOutcome(FailurePermanent, false)
	}
	// One success resets the budget: nine more failures must not hit the
	// threshold of ten.
	if got := b.NoteOutcome(FailurePermanent, true); got != Closed {
		t.Fatalf("after success the state = %v, want Closed", got)
	}
	for i := 0; i < 9; i++ {
		if got := b.NoteOutcome(FailurePermanent, false); got != Closed {
			t.Fatalf("after success then %d failures state = %v, want Closed", i+1, got)
		}
	}
}

// TestBreakerRecoversHalfOpen is the automatic-recovery path the task names:
// after the cooldown the breaker admits exactly one probe, and a successful
// probe re-closes it.
func TestBreakerRecoversHalfOpen(t *testing.T) {
	b, fc := newTestBreaker(t, 3, 10*time.Minute)
	// Trip it.
	b.NoteOutcome(FailurePermanent, false)
	b.NoteOutcome(FailurePermanent, false)
	if got := b.NoteOutcome(FailurePermanent, false); got != Open {
		t.Fatalf("3rd failure state = %v, want Open", got)
	}
	// During the cooldown no evaluation is admitted.
	if b.Admit() {
		t.Fatal("an open breaker admitted an evaluation inside its cooldown")
	}
	// Advance past the cooldown; the single probe is admitted (half-open).
	fc.Advance(10*time.Minute + time.Second)
	if !b.Admit() {
		t.Fatal("an open breaker past its cooldown refused the probe")
	}
	if got := b.State(); got != HalfOpen {
		t.Fatalf("after admitting the probe state = %v, want HalfOpen", got)
	}
	// No more than one evaluation per cooldown, even while the probe is in
	// flight.
	if b.Admit() {
		t.Fatal("a half-open breaker admitted a second evaluation before its probe reported")
	}
	// A successful probe recovers the sensor: back to closed.
	if got := b.NoteOutcome(FailurePermanent, true); got != Closed {
		t.Fatalf("successful probe state = %v, want Closed", got)
	}
	if !b.Admit() {
		t.Fatal("a recovered breaker refuses an evaluation")
	}
}

// TestFailedProbeReopens pins that a probe hitting a still-down sensor
// re-trips the breaker and restarts its cooldown.
func TestFailedProbeReopens(t *testing.T) {
	b, fc := newTestBreaker(t, 3, 10*time.Minute)
	for range 3 {
		b.NoteOutcome(FailurePermanent, false)
	}
	fc.Advance(10*time.Minute + time.Second)
	if !b.Admit() {
		t.Fatal("cooldown probe not admitted")
	}
	if got := b.NoteOutcome(FailurePermanent, false); got != Open {
		t.Fatalf("failed probe state = %v, want Open again", got)
	}
	// The new cooldown restarts: right now no probe is admitted.
	if b.Admit() {
		t.Fatal("after a failed probe the breaker admitted another row too soon")
	}
}

// TestResumeClearsBreakerState pins the operator recovery: resume re-closes
// the breaker and wipes the trip budget and the cooldown in one step (M3-06
// `sensors resume`).
func TestResumeClearsBreakerState(t *testing.T) {
	b, _ := newTestBreaker(t, 3, 10*time.Minute)
	for range 3 {
		b.NoteOutcome(FailurePermanent, false)
	}
	if got := b.State(); got != Open {
		t.Fatalf("state before resume = %v, want Open", got)
	}
	if got := b.Resume(); got != Closed {
		t.Fatalf("Resume state = %v, want Closed", got)
	}
	if b.State() != Closed || !b.Admit() {
		t.Fatal("a resumed breaker does not admit evaluations")
	}
	// The budget is cleared: three failures after resume open it again (not
	// zero), which is the proof that resume reset the count to a full budget.
	for range 3 {
		b.NoteOutcome(FailurePermanent, false)
	}
	if got := b.State(); got != Open {
		t.Fatalf("after resume the breaker did not trip at a fresh full budget; state = %v, want Open", got)
	}
}
