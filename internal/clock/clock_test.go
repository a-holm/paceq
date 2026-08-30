package clock_test

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// t0 is an arbitrary but fixed wall time. Nothing in these tests depends on the
// value, only on the arithmetic around it.
var t0 = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

// contractCase pairs a clock with the only operation that differs between
// implementations: making real or simulated time pass.
type contractCase struct {
	name  string
	clk   clock.Clock
	pass  func()
	realT bool
}

func contractCases() []contractCase {
	fake := clock.NewFake(t0)
	sys := clock.System()
	return []contractCase{
		{name: "system", clk: sys, pass: func() {}, realT: true},
		{name: "fake", clk: fake, pass: func() { fake.Advance(time.Second) }},
	}
}

func TestNowIsUTC(t *testing.T) {
	for _, tc := range contractCases() {
		t.Run(tc.name, func(t *testing.T) {
			now := tc.clk.Now()
			if now.Location() != time.UTC {
				t.Errorf("Now() location = %v, want UTC", now.Location())
			}
			if now.IsZero() {
				t.Error("Now() returned the zero time")
			}
		})
	}
}

func TestSinceIsNeverNegativeAndGrows(t *testing.T) {
	for _, tc := range contractCases() {
		t.Run(tc.name, func(t *testing.T) {
			mark := tc.clk.Mark()
			if d := tc.clk.Since(mark); d < 0 {
				t.Fatalf("Since(Mark()) = %v, want >= 0", d)
			}

			tc.pass()
			if tc.realT {
				// Real time needs no help beyond waiting for the next tick of
				// the monotonic clock. No sleep: the loop is nanoseconds long.
				for tc.clk.Since(mark) == 0 {
				}
			}

			if d := tc.clk.Since(mark); d <= 0 {
				t.Errorf("Since(Mark()) after time passed = %v, want > 0", d)
			}
			if d := tc.clk.Since(tc.clk.Mark()); d < 0 {
				t.Errorf("Since of a mark taken just now = %v, want >= 0", d)
			}
		})
	}
}

// TestSinceOrdersMarksTakenInSequence states the ordering of two marks in the
// only form a running clock can answer. "The later mark has less elapsed than
// the earlier one" is a claim about a single instant, and two reads of a
// running clock never share an instant, so the claim is put over a window
// instead: the elapsed time reported for the later mark cannot exceed the
// growth of the earlier mark's elapsed time measured across the moment the
// later mark was taken.
//
// The bound follows from Since being non-decreasing and from nothing else, so
// delay at any of the four reads only widens the window. That is what makes it
// safe on a loaded machine: load cannot turn the inequality around.
func TestSinceOrdersMarksTakenInSequence(t *testing.T) {
	for _, tc := range contractCases() {
		t.Run(tc.name, func(t *testing.T) {
			first := tc.clk.Mark()

			opened := tc.clk.Since(first)
			second := tc.clk.Mark()
			tc.pass()
			gap := tc.clk.Since(second)
			closed := tc.clk.Since(first)

			if gap < 0 {
				t.Errorf("Since(later mark) = %v, want >= 0", gap)
			}
			if window := closed - opened; gap > window {
				t.Errorf("Since(later mark) = %v, want <= the growth of Since(earlier mark) across it = %v",
					gap, window)
			}
		})
	}
}

func TestFakeNowStartsAtTheGivenTime(t *testing.T) {
	f := clock.NewFake(t0)
	if got := f.Now(); !got.Equal(t0) {
		t.Errorf("Now() = %v, want %v", got, t0)
	}
}
