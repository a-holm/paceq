package daemon

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// executorPool runs the claimed work. There are no worker processes (00
// section 3.21): the steps themselves are processes, and the pool is N
// goroutines each driving engine.ExecuteRun, which is the same code the run
// command uses in its own process.
type executorPool struct {
	st    drainStore
	eng   runDriver
	clk   clock.Clock
	log   *slog.Logger
	slots chan struct{}
	wg    sync.WaitGroup
}

// runDriver is what the pool needs of the engine. The interface keeps the
// seam wide enough for tests to replay how an executor can die mid-flight
// without dragging a second engine implementation into production.
type runDriver interface {
	ExecuteRun(ctx context.Context, runID string) (string, error)
}

// drainStore names what handing a run back needs from the store: read the
// row, put a running step back, requeue the run. Narrow enough to fake in
// tests, wide enough for the whole handback.
type drainStore interface {
	GetRun(ctx context.Context, runID string) (store.RunDetail, error)
	InterruptStepForShutdown(ctx context.Context, runID, name string, code reason.Code) error
	RequeueRunAfterDrain(ctx context.Context, runID string) error
}

func newExecutorPool(st *store.Store, eng *engine.Engine, log *slog.Logger, workers int) *executorPool {
	return &executorPool{
		st:    st,
		eng:   eng,
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
		// the same dead context and loses. That is why the handback
		// below exists, so it stays quiet. Any other error means the
		// engine gave up on its own and someone should hear why.
		if errors.Is(err, context.Canceled) {
			p.log.Debug("the executor stopped on a cancelled context",
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
}

// handBackAttempts bounds how often the handback tries to land. One attempt
// per shutdown used to be the whole story, and a single lost store call then
// left a claimed run behind a clean exit forever. Three tries with tiny gaps
// ride out a transient without letting a wedged writer hold the stop open.
const handBackAttempts = 3

// handBackWhenOwed hands the run back when, and only when, the row says the
// claim is still standing. It returns nil once the row is out of "running":
// finished runs stay finished, queued runs were never ours, and a run another
// attempt already handed back needs nothing. An error means the row may still
// say running after every try, which the caller reports loudly.
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
		detail, err := p.st.GetRun(ctx, runID)
		if err != nil {
			lastErr = err
			continue
		}
		state, err := model.ParseRunState(detail.Run.State)
		if err != nil {
			return err
		}
		if state != model.RunRunning {
			return nil
		}
		if err := p.releaseForDrain(ctx, runID); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// releaseForDrain interrupts every step still running and requeues the run.
// The two writes go through the machines, in this order: steps first, so the
// intermediate state is an ordinary between-steps pause, then the run.
func (p *executorPool) releaseForDrain(ctx context.Context, runID string) error {
	detail, err := p.st.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if detail.Run.State != string(model.RunRunning) {
		// The executor never got to claim it, or the run already
		// ended; there is nothing of ours to hand back.
		return nil
	}
	for _, step := range detail.Steps {
		if model.StepState(step.State) != model.StepRunning {
			continue
		}
		if err := p.st.InterruptStepForShutdown(ctx, runID, step.Name,
			reason.RUNInterruptedShutdown); err != nil {
			return err
		}
	}
	return p.st.RequeueRunAfterDrain(ctx, runID)
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
