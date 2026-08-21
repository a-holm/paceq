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

			later := tc.clk.Since(mark)
			if later <= 0 {
				t.Fatalf("Since(Mark()) after time passed = %v, want > 0", later)
			}

			second := tc.clk.Mark()
			if d := tc.clk.Since(second); d < 0 {
				t.Errorf("Since of a later mark = %v, want >= 0", d)
			}
			if tc.clk.Since(second) > later {
				t.Errorf("Since(later mark) = %v, want <= Since(earlier mark) = %v",
					tc.clk.Since(second), later)
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
