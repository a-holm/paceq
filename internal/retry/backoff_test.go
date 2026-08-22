package retry_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/retry"
)

// The backoff table. Delay is a pure function, so every property the issue
// promises is a plain assertion here: no sleeps, no wall clock, no flakes.

func exponential(initial, maxDelay time.Duration) retry.Policy {
	return retry.Policy{
		Backoff:  retry.Exponential,
		Initial:  initial,
		MaxDelay: maxDelay,
		Jitter:   retry.JitterNone,
	}
}

func flat(initial, maxDelay time.Duration) retry.Policy {
	return retry.Policy{
		Backoff:  retry.Fixed,
		Initial:  initial,
		MaxDelay: maxDelay,
		Jitter:   retry.JitterNone,
	}
}

func TestExponentialBackoffDoublesUntilTheCap(t *testing.T) {
	p := exponential(10*time.Millisecond, 100*time.Millisecond)
	want := map[int]time.Duration{
		1: 10 * time.Millisecond,
		2: 20 * time.Millisecond,
		3: 40 * time.Millisecond,
		4: 80 * time.Millisecond,
		5: 100 * time.Millisecond, // capped after doubling past it
		6: 100 * time.Millisecond,
		7: 100 * time.Millisecond,
	}
	for attempt, d := range want {
		if got := retry.Delay(p, attempt, nil); got != d {
			t.Errorf("Delay(attempt %d) = %s, want %s", attempt, got, d)
		}
	}
}

// A cap that only trims the initial value before growing would answer 120ms
// here; the cap applies to the result of the growth, not its input.
func TestTheCapAppliesAfterTheGrowth(t *testing.T) {
	p := exponential(60*time.Millisecond, 100*time.Millisecond)
	if got := retry.Delay(p, 1, nil); got != 60*time.Millisecond {
		t.Errorf("Delay(1) = %s, want 60ms below the cap", got)
	}
	if got := retry.Delay(p, 2, nil); got != 100*time.Millisecond {
		t.Errorf("Delay(2) = %s, want the 100ms cap, not the doubled 120ms", got)
	}
}

func TestFixedBackoffStaysFlat(t *testing.T) {
	p := flat(250*time.Millisecond, time.Second)
	for attempt := 1; attempt <= 12; attempt++ {
		if got := retry.Delay(p, attempt, nil); got != 250*time.Millisecond {
			t.Errorf("Delay(attempt %d) = %s, want a flat 250ms", attempt, got)
		}
	}
}

func TestAnInitialAboveTheCapIsCappedAtOnce(t *testing.T) {
	p := exponential(2*time.Second, time.Second)
	for attempt := 1; attempt <= 4; attempt++ {
		if got := retry.Delay(p, attempt, nil); got != time.Second {
			t.Errorf("Delay(attempt %d) = %s, want the 1s cap", attempt, got)
		}
	}
}

func TestHugeAttemptNumbersNeitherOverflowNorPanic(t *testing.T) {
	p := exponential(time.Nanosecond, time.Hour)
	want := map[int]time.Duration{
		1:         time.Nanosecond, // the first attempt has not doubled yet
		1_000:     time.Hour,
		65_535:    time.Hour,
		1_000_000: time.Hour,
	}
	for attempt, d := range want {
		got := retry.Delay(p, attempt, nil)
		if got != d {
			t.Errorf("Delay(attempt %d) = %s, want %s", attempt, got, d)
		}
		if got < 0 {
			t.Errorf("Delay(attempt %d) = %s, negative", attempt, got)
		}
	}
}

func TestMonotonicAcrossTheWholeSweep(t *testing.T) {
	for name, p := range map[string]retry.Policy{
		"exponential": exponential(time.Millisecond, time.Minute),
		"fixed":       flat(30*time.Millisecond, time.Minute),
	} {
		prev := retry.Delay(p, 1, nil)
		for attempt := 2; attempt <= 64; attempt++ {
			got := retry.Delay(p, attempt, nil)
			if got < prev {
				t.Errorf("%s: Delay(%d) = %s < Delay(%d) = %s, must not decrease",
					name, attempt, got, attempt-1, prev)
			}
			if got > p.MaxDelay {
				t.Errorf("%s: Delay(%d) = %s above the %s cap",
					name, attempt, got, p.MaxDelay)
			}
			prev = got
		}
	}
}

func TestAttemptsBeforeTheFirstCountAsTheFirst(t *testing.T) {
	p := exponential(10*time.Millisecond, time.Minute)
	first := retry.Delay(p, 1, nil)
	for _, attempt := range []int{0, -1, -100} {
		if got := retry.Delay(p, attempt, nil); got != first {
			t.Errorf("Delay(attempt %d) = %s, want the first attempt's %s",
				attempt, got, first)
		}
	}
}

func TestFullJitterDrawsInsideTheCappedBaseAndSpreads(t *testing.T) {
	p := retry.Policy{
		Backoff:  retry.Exponential,
		Initial:  40 * time.Millisecond,
		MaxDelay: 400 * time.Millisecond,
		Jitter:   retry.JitterFull,
	}
	// Attempt 3 doubles twice from 40ms to 160ms, so the draws live in
	// [0, 160ms].
	const base = 160 * time.Millisecond
	const draws = 10_000
	rnd := rand.New(rand.NewPCG(67, 1))

	lowest, highest := base+1, time.Duration(-1)
	for i := 0; i < draws; i++ {
		got := retry.Delay(p, 3, rnd)
		if got < 0 || got > base {
			t.Fatalf("draw %d = %s, outside [0, %s]", i, got, base)
		}
		if got < lowest {
			lowest = got
		}
		if got > highest {
			highest = got
		}
	}
	if lowest >= base/10 {
		t.Errorf("lowest of %d draws = %s, want under 10%% of %s: the spread collapsed",
			draws, lowest, base)
	}
	if highest <= base-base/10 {
		t.Errorf("highest of %d draws = %s, want over 90%% of %s: the spread collapsed",
			draws, highest, base)
	}
}

func TestAnEmptyJitterFieldMeansFullJitter(t *testing.T) {
	p := retry.Policy{Backoff: retry.Exponential, Initial: time.Second, MaxDelay: time.Second}
	rnd := rand.New(rand.NewPCG(67, 2))
	const cap = time.Second
	lowest, highest := cap+1, time.Duration(-1)
	for i := 0; i < 1000; i++ {
		got := retry.Delay(p, 1, rnd)
		if got < 0 || got > cap {
			t.Fatalf("draw %d = %s, outside [0, %s]", i, got, cap)
		}
		if got < lowest {
			lowest = got
		}
		if got > highest {
			highest = got
		}
	}
	if lowest >= cap/10 || highest <= cap-cap/10 {
		t.Errorf("jitter unset spread [%s, %s] over 1000 draws, want a full-jitter spread inside [0, %s]",
			lowest, highest, cap)
	}
}

func TestDeterministicGivenTheSameSeed(t *testing.T) {
	p := retry.Policy{
		Backoff:  retry.Exponential,
		Initial:  40 * time.Millisecond,
		MaxDelay: 400 * time.Millisecond,
		Jitter:   retry.JitterFull,
	}
	draw := func() []time.Duration {
		rnd := rand.New(rand.NewPCG(67, 3))
		out := make([]time.Duration, 0, 50)
		for attempt := 1; len(out) < 50; attempt++ {
			out = append(out, retry.Delay(p, attempt, rnd))
		}
		return out
	}
	first, second := draw(), draw()
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("draw %d differs: %s vs %s, the same seed must replay exactly",
				i, first[i], second[i])
		}
	}
}

func TestJitterNoneNeverTouchesTheSource(t *testing.T) {
	p := exponential(15*time.Millisecond, time.Second)
	// A nil source must survive: jitter none has nothing to draw.
	for attempt := 1; attempt <= 5; attempt++ {
		retry.Delay(p, attempt, nil)
	}
}
