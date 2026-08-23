package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The release path of #68: when a run leaves the pool, whatever deferred
// behind its concurrency slot hears about it at once. The notify is an
// optimisation; the ticker is the safety net, and that half is already proven
// by TestLoopsRunOnTickersAlone.

func TestARunLeavingThePoolWakesTheDispatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := notify.New()
		wake := bus.Subscribe(notify.TopicRunQueued)

		eng := &stubRunDriver{state: "succeeded"}
		pool := newExecutorPool(eng, eng,
			slog.New(slog.NewTextHandler(io.Discard, nil)), 2)
		pool.afterRun = func() { bus.Notify(notify.TopicRunQueued) }

		if !pool.submit(context.Background(), "run-1") {
			t.Fatal("the pool refused a run with a free slot")
		}
		synctest.Wait()

		select {
		case <-wake:
			// The wake arrived in the same instant the run finished: no
			// virtual time passed, which is what "within one second"
			// means with room to spare.
		default:
			t.Fatal("the dispatcher was not woken when the run left the pool")
		}
	})
}

func TestAFailingExecutionStillWakesTheDispatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := make(chan struct{}, 4)
		eng := &stubRunDriver{err: errors.New("boom")}
		pool := newExecutorPool(eng, eng,
			slog.New(slog.NewTextHandler(io.Discard, nil)), 1)
		pool.afterRun = func() { calls <- struct{}{} }

		pool.submit(context.Background(), "run-1")
		synctest.Wait()

		select {
		case <-calls:
		default:
			t.Fatal("a failed execution did not free its slot's wake")
		}
	})
}

// stubRunDriver answers every execution with a fixed verdict or error, holds
// no leases, and so never hands anything back.
type stubRunDriver struct {
	state string
	err   error
}

func (s *stubRunDriver) ExecuteRun(context.Context, string) (string, error) {
	return s.state, s.err
}

func (s *stubRunDriver) HeldLease(string) (store.LeaseRef, bool) { return store.LeaseRef{}, false }

func (s *stubRunDriver) DrainRun(context.Context, string, store.LeaseRef, reason.Code) (bool, error) {
	return false, nil
}

// The reaper frees slots too: a reap must reach the dispatcher as a wake.
func TestTheReaperWakeReachesTheDispatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := notify.New()
		wake := bus.Subscribe(notify.TopicRunQueued)
		_, logger := newRecLog()
		sts := newStatuses(func() time.Time { return clock.System().Now() })
		d := testLoops(t, logger, bus, sts, clock.System())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sweeps := 0
		done := make(chan struct{})
		go func() {
			_ = reaperLoop(ctx, d, time.Hour, stubSweeper{
				fn: func() int { sweeps++; return sweeps },
			})
			close(done)
		}()
		synctest.Wait()

		// First sweep: nothing expired, no wake.
		time.Sleep(time.Hour)
		synctest.Wait()
		select {
		case <-wake:
			t.Fatal("a sweep that reaped nothing woke the dispatcher")
		default:
		}

		// Second sweep: something expired, the dispatcher hears about it.
		time.Sleep(time.Hour)
		synctest.Wait()
		select {
		case <-wake:
		default:
			t.Fatal("the reaper freed a slot and the dispatcher never heard")
		}
		cancel()
		<-done
	})
}

type stubSweeper struct {
	fn func() int
}

func (s stubSweeper) ReapExpiredRuns(context.Context) ([]store.ReapedRun, error) {
	n := s.fn()
	if n < 2 {
		return nil, nil
	}
	return []store.ReapedRun{{ID: "reaped-1"}}, nil
}
