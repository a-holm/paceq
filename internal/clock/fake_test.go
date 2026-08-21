package clock_test

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

func TestAdvanceMovesWallAndMonotonicTogether(t *testing.T) {
	f := clock.NewFake(t0)
	mark := f.Mark()

	f.Advance(90 * time.Second)

	if got, want := f.Now(), t0.Add(90*time.Second); !got.Equal(want) {
		t.Errorf("Now() = %v, want %v", got, want)
	}
	if got := f.Since(mark); got != 90*time.Second {
		t.Errorf("Since(mark) = %v, want 90s", got)
	}
}

// TestWallJumpsDoNotMoveDurations is the test that protects lease logic from
// false fencing: an owner renews on Since(Mark), so an NTP correction of an hour
// in either direction must leave the elapsed duration untouched.
func TestWallJumpsDoNotMoveDurations(t *testing.T) {
	jumps := []struct {
		name string
		move func(f *clock.Fake)
		want time.Time
	}{
		{"forward one hour", func(f *clock.Fake) { f.JumpWall(time.Hour) }, t0.Add(time.Hour + 30*time.Second)},
		{"backward one hour", func(f *clock.Fake) { f.JumpWall(-time.Hour) }, t0.Add(-time.Hour + 30*time.Second)},
		{"set to a past instant", func(f *clock.Fake) { f.Set(t0.Add(-25 * time.Hour)) }, t0.Add(-25 * time.Hour)},
		{"set to a future instant", func(f *clock.Fake) { f.Set(t0.Add(25 * time.Hour)) }, t0.Add(25 * time.Hour)},
	}

	for _, tc := range jumps {
		t.Run(tc.name, func(t *testing.T) {
			f := clock.NewFake(t0)
			mark := f.Mark()
			f.Advance(30 * time.Second)

			tc.move(f)

			if got := f.Since(mark); got != 30*time.Second {
				t.Errorf("Since(mark) after wall jump = %v, want 30s", got)
			}
			if got := f.Now(); !got.Equal(tc.want) {
				t.Errorf("Now() = %v, want %v", got, tc.want)
			}

			// A mark taken after the jump still measures forwards.
			after := f.Mark()
			f.Advance(time.Second)
			if got := f.Since(after); got != time.Second {
				t.Errorf("Since(mark taken after the jump) = %v, want 1s", got)
			}
		})
	}
}

func TestSinceIsMonotonicAcrossASetToThePast(t *testing.T) {
	f := clock.NewFake(t0)
	mark := f.Mark()
	f.Advance(time.Minute)

	f.Set(t0.Add(-time.Hour))
	f.Advance(time.Minute)

	if got := f.Since(mark); got != 2*time.Minute {
		t.Fatalf("Since(mark) = %v, want 2m: a wall clock Set must not reset monotonic time", got)
	}
}
