package daemon

import (
	"context"
	"log/slog"
	"sync"

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
	st    *store.Store
	eng   *engine.Engine
	log   *slog.Logger
	slots chan struct{}
	wg    sync.WaitGroup
}

func newExecutorPool(st *store.Store, eng *engine.Engine, log *slog.Logger, workers int) *executorPool {
	return &executorPool{
		st:    st,
		eng:   eng,
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

// execute drives one run and hands it back untouched when the pool's context
// was cut by the stop. That handback is the heart of the clean shutdown: the
// process group is already gone, so what remains is bookkeeping, and the
// bookkeeping must say "nothing happened" rather than invent a verdict.
func (p *executorPool) execute(ctx context.Context, runID string) {
	_, _ = p.eng.ExecuteRun(ctx, runID)
	if ctx.Err() == nil {
		return // the run ended on its own; its verdict is already written
	}
	if err := p.releaseForDrain(context.WithoutCancel(ctx), runID); err != nil {
		p.log.Error("could not hand the run back during the drain", "run", runID, "error", err)
	}
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
// including its release writes. The shutdown waits on it inside the drain
// budget, never unbounded.
func (p *executorPool) drained() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	return done
}
