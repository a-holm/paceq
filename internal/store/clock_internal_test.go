package store

import (
	"database/sql"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// recordingClock is a real clock that remembers every backoff it was asked to
// time. It is the only way to see which clock the retry loop actually used: a
// store that ignored Options.Clock would wait on exactly the same wall time.
type recordingClock struct {
	clock.Clock

	mu    sync.Mutex
	waits []time.Duration
}

func (c *recordingClock) NewTimer(d time.Duration) *time.Timer {
	c.mu.Lock()
	c.waits = append(c.waits, d)
	c.mu.Unlock()
	return c.Clock.NewTimer(d)
}

func (c *recordingClock) recorded() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.waits)
}

// TestWithTxBacksOffOnTheOptionsClock pins Options.Clock as the seam it claims
// to be. Without it, a store built by Open would keep its own clock and the
// option would be decoration.
func TestWithTxBacksOffOnTheOptionsClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := &recordingClock{Clock: clock.System()}
		s := openTestStore(t, Options{Clock: clk})

		attempts := 0
		err := s.withTx(t.Context(), func(*sql.Tx) error {
			attempts++
			if attempts < maxWriteAttempts {
				return busySnapshotStub()
			}
			return nil
		})
		if err != nil {
			t.Fatalf("withTx: %v", err)
		}
		if attempts != maxWriteAttempts {
			t.Fatalf("withTx ran the callback %d times, want %d", attempts, maxWriteAttempts)
		}

		want := []time.Duration{retryBackoff, 2 * retryBackoff}
		if got := clk.recorded(); !slices.Equal(got, want) {
			t.Errorf("the injected clock timed %v, want %v: the retry backoff is not using Options.Clock", got, want)
		}
	})
}

func TestOpenDefaultsToTheSystemClock(t *testing.T) {
	s := openTestStore(t, Options{})

	if s.clk == nil {
		t.Fatal("Open left the store without a clock")
	}
	if _, ok := s.clk.(*recordingClock); ok {
		t.Fatal("Open picked up a clock nobody passed")
	}
	if s.clk.Now().IsZero() {
		t.Error("the default clock reads the zero time")
	}
}
