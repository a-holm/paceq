package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/store"
)

// Executor runs one step the run's claim loop admitted, to a verdict.
// Returning nil promises the claim's outcome is already recorded (or was
// deliberately refused and no retry is owed).
type Executor func(ctx context.Context, step *store.ClaimedStep) error

// ExecError stops the claim loop because an executor could not run a step.
// The error is not recorded as a verdict; it is a fault of the executor.
type ExecError struct {
	Step *store.ClaimedStep
	Err  error
}

func (e ExecError) Error() string { return e.Err.Error() }
func (e ExecError) Unwrap() error { return e.Err }

// ClaimTick is how long a worker that finds no claim waits before looking
// again. The bus wakes it the moment a claim becomes possible; the ticker is
// the correctness path that works with the bus off (the whole DAG still runs
// on tickers alone, just more slowly).
const ClaimTick = 2 * time.Second

// Pool is the M4-02 step executor. It drives the claim predicate of one
// already-claimed run: a coordinator claims steps one at a time through the
// store gate and hands each to a bounded set of worker goroutines that run it
// through the Executor and record the verdict. The pool keeps no agenda of
// its own: whatever the claim admits next is what it runs, and the run's own
// max_parallel, enforced inside the claim, is the real concurrency bound.
//
// The pool holds no state about the DAG: the claim predicate is the only
// place order is decided, and restarting a crashed pool just re-runs the
// claims, because the steps that already committed are no longer pending.
type Pool struct {
	clk     clock.Clock
	store   *store.Store
	bus     *notify.Bus // nil bus: a ticker alone is the correctness path
	execute Executor
	workers int // goroutine ceiling; the DB cap is the true bound

	// Tick is how long a worker with no claim sleeps before looking again.
	// Zero means ClaimTick. Tests shorten it so a bus-off run still finishes
	// fast; production keeps the safety net wide enough to be cheap.
	Tick time.Duration
}

// New builds a pool. workers is the goroutine ceiling: at least one, and it
// may exceed the run's max_parallel, which is fine because the claim gate
// bounds how many steps are running regardless.
func New(st *store.Store, bus *notify.Bus, execute Executor, clk clock.Clock, workers int) *Pool {
	if workers < 1 {
		workers = 1
	}
	return &Pool{clk: clk, store: st, bus: bus, execute: execute, workers: workers}
}

func (p *Pool) tick() time.Duration {
	if p.Tick > 0 {
		return p.Tick
	}
	return ClaimTick
}

// ErrLeaseLostAtStart reports a run that stopped being ours between the
// caller's claim and the pool's first step claim.
var ErrLeaseLostAtStart = errors.New("the run lease was lost before any step was claimed")

// Run drives one claimed run's steps to a drained point: it claims and runs
// steps in parallel until no step is left pending, then returns. A run whose
// steps are all terminal is the caller's to close; a run that parked a retry
// is the caller's to wait out.
//
// The pool returns nil once every pending step has been admitted. A step
// whose upstream never succeeded cannot be admitted, so the run has to be
// closed (skip + finish) by whoever drove it; the pool does not guess.
func (p *Pool) Run(ctx context.Context, runID string, ref store.LeaseRef) error {
	if ref.Owner == "" {
		return errors.New("run the steps of a run with no holder named")
	}

	// The wake the coordinator sleeps on when nothing is claimable. With a
	// live bus the executor publishes on TopicStepReady when a step finishes;
	// with no bus the ticker alone carries correctness.
	var wake <-chan struct{}
	if p.bus != nil {
		wake = p.bus.Subscribe(notify.TopicStepReady)
		defer p.bus.Unsubscribe(notify.TopicStepReady, wake)
	}

	// slots bounds the number of concurrently executing steps to workers.
	slots := make(chan struct{}, p.workers)
	var wg sync.WaitGroup
	fail := make(chan error, 1)
	failOnce := sync.Once{}
	report := func(err error) {
		if err == nil {
			return
		}
		failOnce.Do(func() { fail <- err })
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Dispatch phase: claim one step per free worker slot until the
		// predicate admits nothing more.
		dispatched := false
		for {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			c, err := p.store.ClaimNextStep(ctx, runID, ref)
			if err != nil {
				<-slots
				return err
			}
			if c == nil {
				<-slots
				break
			}
			dispatched = true
			wg.Add(1)
			go func(step *store.ClaimedStep) {
				defer wg.Done()
				defer func() { <-slots }()
				if err := p.execute(ctx, step); err != nil {
					report(ExecError{Step: step, Err: err})
				}
			}(c)
		}
		if !dispatched {
			// Nothing was claimable this cycle and nothing ran: this is the
			// pool at a still point.
			pending, err := p.store.PendingSteps(ctx, runID)
			if err != nil {
				return err
			}
			if len(pending) == 0 {
				// Every step is terminal; the caller closes the run.
				wg.Wait()
				return nil
			}
			// Some step is parked or waiting on a running upstream. Sleep
			// until a wake or the tick says to look again.
			if err := p.sleep(ctx, wake); err != nil {
				wg.Wait()
				return err
			}
			continue
		}
		// The dispatch phase found work. Let it settle, then loop: the
		// finished steps may have unblocked downstream ones.
		wg.Wait()
		select {
		case err := <-fail:
			return err
		default:
		}
	}
}

// sleep waits for the next look: a bus wake when one exists, otherwise just
// the tick. A closed context ends the wait.
func (p *Pool) sleep(ctx context.Context, wake <-chan struct{}) error {
	t := p.clk.NewTimer(p.tick())
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	case <-t.C:
		return nil
	}
}
