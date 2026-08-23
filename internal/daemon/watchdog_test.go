package daemon

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/notify"
)

// fakeWatchdogNotifier records sent notifications via a buffered channel.
type fakeWatchdogNotifier struct {
	ch chan string
}

func newFakeWatchdogNotifier() *fakeWatchdogNotifier {
	return &fakeWatchdogNotifier{ch: make(chan string, 20)}
}

func (f *fakeWatchdogNotifier) add(msg string) { f.ch <- msg }

func (f *fakeWatchdogNotifier) received() []string {
	var out []string
	for {
		select {
		case s := <-f.ch:
			out = append(out, s)
		default:
			return out
		}
	}
}

// fakeNotifierSender implements WatchdogNotifier.
type fakeNotifierSender struct{ add func(string) }

func (f *fakeNotifierSender) Send(string) error {
	f.add("WATCHDOG=1")
	return nil
}

// Ensure sync/atomic is used.
var _ = atomic.Int64{}

func testHeart() *Heart {
	return NewHeart(clock.System())
}

// --- Heart tests ---

func TestHeartBeatStoresTime(t *testing.T) {
	h := testHeart()
	if h.TickCount() != 0 {
		t.Error("fresh Heart should have TickCount 0")
	}
	h.Beat()
	if h.TickCount() != 1 {
		t.Error("after one Beat, TickCount should be 1")
	}
}

func TestHeartLastTickAfterBeat(t *testing.T) {
	h := testHeart()
	before := time.Now()
	h.Beat()
	after := time.Now()
	tick := h.LastTick()
	if tick.Before(before) || tick.After(after) {
		t.Errorf("LastTick %v should be between %v and %v", tick, before, after)
	}
}

func TestHeartTickCount(t *testing.T) {
	h := testHeart()
	if h.TickCount() != 0 {
		t.Error("fresh Heart should have 0 ticks")
	}
	h.Beat()
	if h.TickCount() != 1 {
		t.Error("one Beat should give TickCount 1")
	}
	h.Beat()
	if h.TickCount() != 2 {
		t.Error("two Beats should give TickCount 2")
	}
}

func TestHeartConcurrentAccess(t *testing.T) {
	h := testHeart()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				h.Beat()
			}
		}()
	}
	wg.Wait()
	if h.TickCount() != 1000 {
		t.Errorf("TickCount = %d, want 1000", h.TickCount())
	}
}

func TestHeartSinceAfterBeat(t *testing.T) {
	h := testHeart()
	h.Beat()
	time.Sleep(50 * time.Millisecond)
	since := h.SinceLastBeat()
	if since < 40*time.Millisecond || since > 120*time.Millisecond {
		t.Errorf("since last beat: got %v, want ~50ms", since)
	}
}

// --- Watchdog loop tests ---

func TestWatchdogLoopSendsOnFreshHeartbeat(t *testing.T) {
	h := testHeart()
	h.Beat()
	notifier := newFakeWatchdogNotifier()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchdogLoop(ctx, clock.System(), h, 50*time.Millisecond,
		&fakeNotifierSender{add: notifier.add})

	select {
	case msg := <-notifier.ch:
		if msg != "WATCHDOG=1" {
			t.Errorf("got %q, want WATCHDOG=1", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watchdog ping")
	}
}

func TestWatchdogLoopSkipsOnStaleHeartbeat(t *testing.T) {
	h := testHeart()
	// Never beaten: SinceLastBeat returns time.Hour.
	notifier := newFakeWatchdogNotifier()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		watchdogLoop(ctx, clock.System(), h, 10*time.Millisecond,
			&fakeNotifierSender{add: notifier.add})
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if msgs := notifier.received(); len(msgs) > 0 {
		t.Errorf("stale heartbeat should not ping, got %v", msgs)
	}
}

func TestWatchdogLoopNoopZeroInterval(t *testing.T) {
	h := testHeart()
	notifier := newFakeWatchdogNotifier()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	watchdogLoop(ctx, clock.System(), h, 0, &fakeNotifierSender{add: notifier.add})

	if len(notifier.received()) > 0 {
		t.Error("watchdog should not send when interval is 0")
	}
}

// --- Env parsing tests ---

func TestWatchdogUSecFromEnv(t *testing.T) {
	t.Setenv("WATCHDOG_USEC", "30000000")
	if got := watchdogUSecFromEnv(); got != 30*time.Second {
		t.Errorf("got %v, want 30s", got)
	}
}

func TestWatchdogIntervalHalf(t *testing.T) {
	if got := watchdogPingInterval(30 * time.Second); got != 15*time.Second {
		t.Errorf("got %v, want 15s", got)
	}
}

func TestWatchdogUSecFromEnvEmpty(t *testing.T) {
	os.Unsetenv("WATCHDOG_USEC")
	if got := watchdogUSecFromEnv(); got != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

func TestWatchdogUSecFromEnvInvalid(t *testing.T) {
	t.Setenv("WATCHDOG_USEC", "not-a-number")
	if got := watchdogUSecFromEnv(); got != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

// countingSource is a real (non-nil) schedule source that does nothing but
// answer, so the scheduler loop runs its work body and reaches the Heart beat.
type countingSource struct{ ticks int }

func (c *countingSource) Tick(context.Context) error {
	c.ticks++
	return nil
}

// TestSchedulerStallDoesNotExtendWatchdogDeadline pins the wiring review found
// missing: the Heart is tied to the scheduler loop tick, so a live scheduler
// keeps the watchdog fed, and a stalled one lets the watchdog go quiet once the
// deadline window passes instead of extending it. The mutant this test kills is
// the one that decouples the Heart from the tick (removes the Heart beat from
// schedulerLoop): with that mutation the scheduler tick looks dead and no
// watchdog ping arrives while the loop runs.
func TestSchedulerStallDoesNotExtendWatchdogDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := notify.New()
		_, logger := newRecLog()
		sts := newStatuses(func() time.Time { return clock.System().Now() })
		clk := clock.System()

		heart := NewHeart(clk)
		d := testLoops(t, logger, bus, sts, clk)
		d.heart = heart

		const (
			tickEvery = 20 * time.Millisecond  // how often the scheduler beats
			pingEvery = 100 * time.Millisecond // watchdog ping rate
			grace     = 3 * (2 * pingEvery)    // settle time well past a window
		)

		notifier := newFakeWatchdogNotifier()

		schedCtx, schedCancel := context.WithCancel(context.Background())
		watchCtx, watchCancel := context.WithCancel(context.Background())
		defer watchCancel()

		src := &countingSource{}
		go func() { _ = schedulerLoop(schedCtx, d, tickEvery, src) }()
		go watchdogLoop(watchCtx, clk, heart, pingEvery, &fakeNotifierSender{add: notifier.add})

		// The scheduler is alive and beats the Heart every 20ms, so the
		// watchdog has a fresh beat to extend the deadline with and pings. If
		// the Heart is not tied to the loop tick this is where the bug shows:
		// a scheduler that is demonstrably ticking still looks dead to the
		// watchdog, so no ping ever arrives.
		time.Sleep(400 * time.Millisecond)
		if got := len(notifier.received()); got == 0 {
			t.Fatalf("watchdog never pinged while the scheduler loop ticked: the Heart is not tied to the scheduler tick")
		}

		// Stall the scheduler. The watchdog must keep its own clock: the last
		// beat was at most one tick ago, so pings may continue for one window
		// and then must stop. If a stalled loop still fed the watchdog, pings
		// would keep arriving and the systemd deadline would be extended
		// forever by a scheduler that is not actually scheduling.
		schedCancel()
		time.Sleep(grace)
		settled := len(notifier.received())

		time.Sleep(grace)
		later := len(notifier.received())
		watchCancel()
		synctest.Wait()

		// The stall must not extend the deadline: pings may continue for one
		// window after the last beat (that is the grace a watchdog legitimately
		// has), but nothing new may arrive once that grace has passed. A later
		// count of zero is what proves the stuck loop cannot dodge a restart.
		if settled == 0 {
			t.Fatalf("watchdog went quiet before the grace window passed: the Heart is tied to the tick but the window logic is wrong")
		}
		if later != 0 {
			t.Fatalf("a stalled scheduler kept feeding the watchdog: %d pings arrived after %v of grace, so a loop stuck past the deadline would dodge restarts", later, grace)
		}
	})
}
