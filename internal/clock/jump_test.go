package clock_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

func TestDetectorReportsWallJumpsOnly(t *testing.T) {
	cases := []struct {
		name     string
		move     func(f *clock.Fake)
		want     time.Duration
		reported bool
	}{
		{"time passing normally", func(f *clock.Fake) { f.Advance(time.Minute) }, 0, false},
		{"nothing at all", func(f *clock.Fake) {}, 0, false},
		{"ntp shaving 900ms", func(f *clock.Fake) { f.JumpWall(-900 * time.Millisecond) }, 0, false},
		{"ntp adding 900ms", func(f *clock.Fake) { f.JumpWall(900 * time.Millisecond) }, 0, false},
		{"exactly at the threshold", func(f *clock.Fake) { f.JumpWall(5 * time.Second) }, 0, false},
		{"forward 6s", func(f *clock.Fake) { f.JumpWall(6 * time.Second) }, 6 * time.Second, true},
		{"backward 6s", func(f *clock.Fake) { f.JumpWall(-6 * time.Second) }, -6 * time.Second, true},
		{"forward an hour", func(f *clock.Fake) { f.JumpWall(time.Hour) }, time.Hour, true},
		{"backward an hour", func(f *clock.Fake) { f.JumpWall(-time.Hour) }, -time.Hour, true},
		{"forward 25 hours", func(f *clock.Fake) { f.JumpWall(25 * time.Hour) }, 25 * time.Hour, true},
		{"backward 25 hours", func(f *clock.Fake) { f.JumpWall(-25 * time.Hour) }, -25 * time.Hour, true},
		{
			"a jump while time also passes",
			func(f *clock.Fake) { f.Advance(time.Minute); f.JumpWall(-time.Hour) },
			-time.Hour, true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := clock.NewFake(t0)
			d := clock.NewDetector(f, 5*time.Second)

			if jump, ok := d.Check(); ok {
				t.Fatalf("Check() straight after NewDetector reported %+v, want no jump", jump)
			}

			tc.move(f)

			jump, ok := d.Check()
			if ok != tc.reported {
				t.Fatalf("Check() reported = %v (delta %v), want %v", ok, jump.Delta, tc.reported)
			}
			if !tc.reported {
				return
			}
			if jump.Delta != tc.want {
				t.Errorf("jump.Delta = %v, want %v", jump.Delta, tc.want)
			}
			if !jump.At.Equal(f.Now()) {
				t.Errorf("jump.At = %v, want the wall clock reading %v", jump.At, f.Now())
			}
		})
	}
}

func TestDetectorReportsEachJumpOnce(t *testing.T) {
	f := clock.NewFake(t0)
	d := clock.NewDetector(f, 5*time.Second)
	d.Check()

	f.JumpWall(time.Hour)
	if _, ok := d.Check(); !ok {
		t.Fatal("Check() missed the jump")
	}
	f.Advance(time.Minute)
	if jump, ok := d.Check(); ok {
		t.Errorf("Check() reported %+v after the jump was already reported, want no jump", jump)
	}
}

// TestDetectorRunReportsOnATicker is also the smoke test for the synctest setup:
// the ticker inside Run is a real one, and the bubble makes it virtual, so a
// loop that samples once a second finishes in microseconds.
func TestDetectorRunReportsOnATicker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := clock.NewFake(t0)
		d := clock.NewDetector(f, 5*time.Second)
		out := make(chan clock.Jump)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- d.Run(ctx, time.Second, out) }()

		synctest.Wait()
		f.JumpWall(-2 * time.Hour)

		jump := <-out
		if jump.Delta != -2*time.Hour {
			t.Errorf("jump.Delta = %v, want -2h", jump.Delta)
		}

		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run returned %v, want nil after cancellation", err)
		}
	})
}

// TestBackoffLoopOverThirtySimulatedDays proves the other half of the decision
// table: code that only needs timers takes clock.System() into a bubble and gets
// deterministic time for free, no Fake involved.
func TestBackoffLoopOverThirtySimulatedDays(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := clock.System()
		start := c.Now()
		mark := c.Mark()

		var slept time.Duration
		for backoff := time.Second; slept < 30*24*time.Hour; backoff *= 2 {
			if backoff > 6*time.Hour {
				backoff = 6 * time.Hour
			}
			timer := c.NewTimer(backoff)
			<-timer.C
			slept += backoff
		}

		if got := c.Now().Sub(start); got != slept {
			t.Errorf("wall clock advanced %v, want %v", got, slept)
		}
		if got := c.Since(mark); got != slept {
			t.Errorf("Since(mark) = %v, want %v", got, slept)
		}
		if slept < 30*24*time.Hour {
			t.Errorf("simulated %v, want at least 30 days", slept)
		}
	})
}
