package sensor

import (
	"testing"
	"time"
)

// Fixed jitter: a frac of 0.0 pins the delay to its smallest value and 1.0 to
// its full mathematical value. The tests pin exact times this way so the
// assertion is on the formula, never on randomness. A full-jitter contract is
// rand(0, backoff], so 1.0 is the endpoint the monotonic and cap proofs need.

func TestClassifyFailure(t *testing.T) {
	cases := []struct {
		name     string
		exit     int
		wantKind FailureKind
	}{
		{"exit 75 is transient", 75, FailureTransient},
		{"exit 64 is config", 64, FailureConfig},
		{"exit 0 is not perm-failed but classified permanent when errored", 0, FailurePermanent},
		{"exit 1 is permanent", 1, FailurePermanent},
		{"exit 2 is permanent", 2, FailurePermanent},
		{"exit 137 (killed) is permanent", 137, FailurePermanent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyFailure(c.exit); got != c.wantKind {
				t.Fatalf("ClassifyFailure(%d) = %v, want %v", c.exit, got, c.wantKind)
			}
		})
	}
}

func TestNextEvalAtExponentialDoubling(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	interval := time.Minute
	// Formula: interval * 2^n   (60s, 2m, 4m, 8m, 16m, 32m, then cap 1h for n>=6)
	want := []time.Duration{
		time.Minute,
		2 * time.Minute,
		4 * time.Minute,
		8 * time.Minute,
	}
	for n, wantDelay := range want {
		got := nextEvalAt(now, interval, 0, n, FailurePermanent, 1)
		wantAt := now.Add(wantDelay)
		if !got.Equal(wantAt) {
			t.Errorf("n=%d: nextEvalAt = %s, want %s (+%s)", n, got, wantAt, wantDelay)
		}
	}
}

func TestNextEvalAtCapsAtOneHour(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	interval := time.Minute
	// n>=6: 2^6=64m, capped at 60m.
	for _, n := range []int{6, 7, 8, 100} {
		got := nextEvalAt(now, interval, 0, n, FailurePermanent, 1)
		want := now.Add(time.Hour)
		if !got.Equal(want) {
			t.Errorf("n=%d: nextEvalAt = %s, want %s (cap 1h)", n, got, want)
		}
	}
}

func TestNextEvalAtIsMonotonicInN(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	prev := time.Time{}
	for n := 0; n <= 20; n++ {
		got := nextEvalAt(now, time.Minute, 0, n, FailurePermanent, 1)
		// Monotonic non-strict: the cap means several n share the one-hour
		// ceiling. What must never happen is a regression backwards.
		if got.Before(prev) {
			t.Errorf("n=%d: nextEvalAt went backwards (%s then %s)", n, prev, got)
		}
		prev = got
	}
}

func TestNextEvalAtNeverBelowMinInterval(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	// A transient failure with frac 0 picks a zero delay; the absolute floor
	// must still hold. A huge interval must also be floored by min_interval
	// when jitter lands small.
	if got := nextEvalAt(now, time.Minute, time.Second, 0, FailureTransient, 0); got != now.Add(time.Second) {
		t.Errorf("transient frac0 nextEvalAt = %s, want now+1s floor", got)
	}
	if got := nextEvalAt(now, 30*time.Minute, 5*time.Second, 0, FailurePermanent, 0); !got.Equal(now.Add(5 * time.Second)) {
		t.Errorf("large-interval low-jitter nextEvalAt = %s, want now+5s", got)
	}
	t.Log("full jitter bounds: transient frac1 must not exceed base")
	if got := nextEvalAt(now, time.Minute, time.Second, 0, FailureTransient, 1); !got.Equal(now.Add(time.Minute)) {
		t.Errorf("transient frac1 nextEvalAt = %s, want now+1m", got)
	}
}

func TestNextEvalAtConfigSchedulesFloor(t *testing.T) {
	// A config failure must never schedule a normal backoff; the caller pauses
	// instead. The function returns the floor so forgetting to pause can never
	// hot-loop (plan 05 section 6.2).
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	if got := nextEvalAt(now, time.Minute, time.Second, 5, FailureConfig, 0); !got.Equal(now.Add(time.Second)) {
		t.Errorf("config nextEvalAt = %s, want now+1s floor", got)
	}
}

// TestNextEvalAtKeepsWallClockAndZoneIndependent pins that the result is UTC,
// so a schedule comparing evaluation times never pays a timezone tax.
func TestNextEvalAtReturnsUTC(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.Local)
	got := nextEvalAt(now, time.Minute, 0, 3, FailurePermanent, 0)
	if _, offset := got.Zone(); offset != 0 {
		t.Errorf("nextEvalAt returned a non-UTC time (zone offset %d)", offset)
	}
}
