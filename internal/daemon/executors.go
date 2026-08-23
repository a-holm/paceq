package daemon

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// executorPool runs the claimed work. There are no worker processes (00
// section 3.21): the steps themselves are processes, and the pool is N
// goroutines each driving engine.ExecuteRun, which is the same code the run
// command uses in its own process.
type executorPool struct {
	eng   runDriver
	drain leaseDrainer
	clk   clock.Clock
	log   *slog.Logger
	slots chan struct{}
	wg    sync.WaitGroup

	// afterRun runs once per finished execution, success or failure (#68).
	// Serve wires the notify bus into it: a run going terminal frees its
	// concurrency slot, and whatever the slot freed, a deferred run waits
	// for exactly this wake. The ticker covers it if the wake never comes,
	// which makes this an optimisation like every other use of the bus.
	afterRun func()
}

// runDriver is what the pool needs of the engine. The interface keeps the
// seam wide enough for tests to replay how an executor can die mid-flight
// without dragging a second engine implementation into production.
type runDriver interface {
	ExecuteRun(ctx context.Context, runID string) (string, error)
}

// leaseDrainer names what a clean handback needs from the engine and the
// store. The engine supplies the fencing token it believes it holds; the
// store checks that belief against the row inside its own transaction, so a
// frozen daemon thawing mid drain can never disturb whatever took its place.
type leaseDrainer interface {
	HeldLease(runID string) (store.LeaseRef, bool)
	DrainRun(ctx context.Context, runID string, ref store.LeaseRef, code reason.Code) (bool, error)
}

// newExecutorPool wires the pool. The engine fills both seams: it is the run
// driver and the lease drainer.
func newExecutorPool(eng runDriver, drain leaseDrainer, log *slog.Logger, workers int) *executorPool {
	return &executorPool{
		eng:   eng,
		drain: drain,
		clk:   clock.System(),
		log:   log,
		slots: make(chan struct{}, workers),
	}
}

// submit starts one run if a slot is free. It reports false when the pool is
// full, and the dispatcher simply waits for its next wake; a queued run loses
// nothing by waiting.
func (p *executorPool) submit(ctx context.Context, runID string) bool {
	select {
	case p.slots <- struct{}{}:
	default:
		return false
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.slots }()
		p.execute(ctx, runID)
	}()
	return true
}

// execute drives one run and then closes the pool's one promise: no goroutine
// leaves while its run is still claimed. The promise is checked against the
// row, not against this goroutine's context and not against the engine's
// word. Both can lie. A cancelled context says nothing about whether the
// verdict write landed before or after the cut, and an engine that stopped
// early has left the claim standing behind it; only the row knows which world
// we are in, and the row is also the thing the next start reads.
func (p *executorPool) execute(ctx context.Context, runID string) {
	if _, err := p.eng.ExecuteRun(ctx, runID); err != nil {
		// The expected shape here is the drain itself: the kill
		// answered a dead context, and the verdict write answers to
		// the same dead context and loses. A lost lease reads the same
		// way and stays quiet too; the discard event already tells the
		// story. Any other error means the engine gave up on its own
		// and someone should hear why.
		if errors.Is(err, context.Canceled) || errors.Is(err, store.ErrLeaseLost) {
			p.log.Debug("the executor stopped without a verdict",
				"run", runID, "error", err)
		} else {
			p.log.Warn("the executor stopped with an error",
				"run", runID, "error", err)
		}
	}
	if err := p.handBackWhenOwed(context.WithoutCancel(ctx), runID); err != nil {
		p.log.Error("could not hand the run back during the drain",
			"run", runID, "error", err)
	}
	// The run has left the pool one way or another: finished, failed,
	// cancelled, or handed back. Whatever waited for its slot may start.
	if p.afterRun != nil {
		p.afterRun()
	}
}

// handBackAttempts bounds how often the handback tries to land. One attempt
// per shutdown used to be the whole story, and a single lost store call then
// left a claimed run behind a clean exit forever. Three tries with tiny gaps
// ride out a transient without letting a wedged writer hold the stop open.
const handBackAttempts = 3

// handBackWhenOwed hands the run back when, and only when, the engine still
// believes it holds the lease and the row agrees. It returns nil once nothing
// more is owed: finished runs stay finished, runs another holder took were
// never ours to give back. An error means the row may still say running after
// every try, which the caller reports loudly.
func (p *executorPool) handBackWhenOwed(ctx context.Context, runID string) error {
	var lastErr error
	for attempt := 0; attempt < handBackAttempts; attempt++ {
		if attempt > 0 {
			// A short, clock-measured gap between tries. Long enough
			// that a transient hiccup is a different instant; short
			// enough that three tries stay invisible next to the
			// drain budget.
			timer := p.clk.NewTimer(time.Duration(attempt) * 5 * time.Millisecond)
			<-timer.C
		}
		ref, held := p.drain.HeldLease(runID)
		if !held {
			return nil
		}
		handed, err := p.drain.DrainRun(ctx, runID, ref, reason.RUNInterruptedShutdown)
		if err != nil {
			lastErr = err
			continue
		}
		if !handed {
			// The row moved under us between the run ending and this
			// call: nothing of ours is standing any more.
			return nil
		}
		return nil
	}
	return lastErr
}

// drained returns a channel closed when every submitted run has finished,
// including its handback writes. The shutdown waits on it inside the drain
// budget, never unbounded.
func (p *executorPool) drained() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	return done
}
