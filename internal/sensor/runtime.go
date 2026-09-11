package sensor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
)

// Source lists which sensors are due in one wake. M3-01 supplies the
// store-backed implementation that reads the sensors table; the runtime only
// knows the seam, so it can run and be tested before any store row exists.
// Access is never parallel: the runtime calls Due serially at the top of a
// wake.
type Source interface {
	Due(ctx context.Context, limit int) ([]Spec, error)
}

// Sink receives each finished evaluation. M3-03 implements it with the atomic
// commit transaction that turns a Result into cursor, tick and trigger rows;
// this milestone ships the recording test double, because the runtime must
// prove what it hands over without depending on a write layer that is not
// here yet.
type Sink interface {
	// Begin opens the tick before the sensor runs. The version it returns is
	// the one the commit fences against, so a cursor reset that lands while
	// the sensor is running loses that race instead of being overwritten by
	// a result computed from the cursor it replaced. Opening first also
	// leaves a tick on the row for recovery to close when a daemon dies
	// mid-evaluation.
	Begin(ctx context.Context, spec Spec) (Ticket, error)
	// Commit closes the tick Begin opened.
	Commit(ctx context.Context, spec Spec, tk Ticket, r Result) error
}

// Ticket is an opened tick: what the commit closes, and the version it fences
// against. The runtime carries it from Begin to Commit and never reads inside
// it.
type Ticket struct {
	ID      string
	Version int64
}

// Runtime is the long-lived evaluator context: it finds due sensors under the
// seam, claims each one so no sensor ever runs twice concurrently, bounds the
// global concurrency with a semaphore, and runs every evaluation in its own
// goroutine so a hanging sensor never blocks a loop. It never writes to the
// database; the Result travels to the Sink, which is M3-03's.
//
// It applies neither of the two sensor-robustness rules (M3-05) itself. Both
// are decided where the evaluation is committed, which is the one point a
// daemon wake and a forced `paceq sensors tick` both pass through: ApplyLimit
// bounds the batch there, so the forced evaluation is held to the same budget
// and the same cursor rule as a daemon wake (#215), and the breaker count is
// folded onto the sensor's row there, in the same transaction as the
// evaluation it counts (#220).
//
// What the runtime still does about the breaker is refuse to start. A tripped
// sensor is not evaluated until a probe after its cooldown recovers it or an
// operator resumes it, so the loop stops hammering a service that is down. It
// reads that state off the sensor's row and keeps no count of its own, which
// is what makes a restart harmless and the reported number the same one work
// is refused on.
//
// It is not a Go errgroup and does not own a goroutine of selector logic on
// purpose: the daemon's loop shell already select-s on the context, the ticker
// and the notify bus, and calls Step once per wake. The runtime only carries
// what is in flight right now and the bounded workers the evaluations share.
type Runtime struct {
	source Source
	sink   Sink
	ev     *Evaluator
	clk    clock.Clock
	log    *slog.Logger

	maxParallel  int
	drainTimeout time.Duration

	// breakerCooldown is how long a tripped sensor stays down before a probe
	// is admitted. The count it is measured against is on the sensor's row,
	// not here: this runtime remembers nothing about any breaker.
	breakerCooldown time.Duration

	mu      sync.Mutex
	active  map[string]struct{}
	permits chan struct{}
}

// RuntimeConfig wires a runtime.
type RuntimeConfig struct {
	Source Source
	Sink   Sink
	// MaxParallel is the global semaphore size. Zero means 4.
	MaxParallel int
	// DrainTimeout bounds how long a cancelled runtime waits for in flight
	// evaluations to release their process groups. Zero means 30 seconds.
	DrainTimeout time.Duration
	Clock        clock.Clock
	Log          *slog.Logger

	// BreakerCooldown is how long a tripped sensor stays closed before a
	// half-open probe. Zero means the backoff default (one hour).
	BreakerCooldown time.Duration
}

// NewRuntime builds a runtime. A nil clock means the system clock; a nil log
// means slog's default.
func NewRuntime(ev *Evaluator, cfg RuntimeConfig) *Runtime {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.System()
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	maxP := cfg.MaxParallel
	if maxP <= 0 {
		maxP = 4
	}
	drain := cfg.DrainTimeout
	if drain <= 0 {
		drain = 30 * time.Second
	}
	cooldown := cfg.BreakerCooldown
	if cooldown <= 0 {
		cooldown = BackoffCap
	}
	return &Runtime{
		source:          cfg.Source,
		sink:            cfg.Sink,
		ev:              ev,
		clk:             clk,
		log:             log,
		maxParallel:     maxP,
		drainTimeout:    drain,
		breakerCooldown: cooldown,
		active:          make(map[string]struct{}),
		permits:         make(chan struct{}, maxP),
	}
}

// admits reports whether the sensor's circuit breaker lets this evaluation
// start. The whole answer is on the row the spec was read from, so a restart
// cannot rearm a tripped sensor and the number refused here is the number
// every health surface prints (#220).
func (rt *Runtime) admits(spec Spec) bool {
	return model.SensorBreakerAdmits(spec.ConsecutiveFailures, spec.BreakerOpenedAt,
		rt.clk.Now(), rt.breakerCooldown)
}

// Step is one run of the loop seam: admit due sensors, claim the ones that
// are not in flight and whose breaker admits them, hand each a permit, and
// dispatch it to a goroutine. It never blocks on a slow sensor, which is the
// whole point of running them off the loop. A cancelled context is the
// shutdown signal: every dispatched evaluation is already bound to it, so
// each one kills its own process group and drains; Step waits for them so the
// daemon's loop returns only when no sensor subprocess is left behind.
//
// A sensor whose breaker is open is left due for a later wake, exactly as a
// sensor with no free permit is; it is never started. Its probe, once the
// cooldown has elapsed, is admitted like any other evaluation and is held to
// one at a time by the same claim: no second probe starts until the first has
// committed its verdict onto the row.
func (rt *Runtime) Step(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		rt.drain()
		return err
	}
	if rt.source == nil {
		return nil // the store-backed source lands with M3-01; idle until then
	}
	due, err := rt.source.Due(ctx, 100)
	if err != nil {
		return err
	}
	for _, spec := range due {
		if err := ctx.Err(); err != nil {
			rt.drain()
			return err
		}
		if !rt.admits(spec) {
			continue // tripped: refuse work until the cooldown offers a probe
		}
		if !rt.claim(spec.Name) {
			continue // already running this sensor; never two of the same
		}
		if !rt.tryPermit() {
			rt.unclaim(spec.Name)
			continue // no worker free; leave it due for the next wake
		}
		go rt.evaluate(ctx, spec)
	}
	return nil
}

// evaluate runs one sensor to a Result and hands it to the sink, then feeds
// the verdict into the breaker so the next Step knows whether to keep
// pressing this sensor. All three releases are deferred so not even a panic
// leaks the claim or the permit.
func (rt *Runtime) evaluate(ctx context.Context, spec Spec) {
	defer rt.ReleasePermit()
	defer rt.unclaim(spec.Name)
	// The tick opens before the sensor runs, never after: see Sink.Begin.
	// A sensor whose tick cannot be opened is not evaluated at all, because
	// an evaluation nothing can close is work with no record of it.
	var tk Ticket
	if rt.sink != nil {
		opened, err := rt.sink.Begin(ctx, spec)
		if err != nil {
			if ctx.Err() == nil {
				rt.log.Warn("sensor tick was not opened", "sensor", spec.Name, "error", err.Error())
			}
			return
		}
		tk = opened
	}
	in := rt.inputFor(spec)
	res := rt.ev.Evaluate(ctx, spec, in)
	// The Result reaches the sink whole and unjudged. Its batch is bounded by
	// max_triggers_per_tick where it is committed, so the forced CLI
	// evaluation is bound by the same ceiling and the same cursor rule
	// (#215), and its verdict is folded into the breaker count by the same
	// transaction, so a crash between the two cannot leave a sensor stopped
	// with nothing recording why (#220). Truncation moves neither the outcome
	// nor the exit code, so the verdict is the same on either side of it.
	if rt.sink != nil {
		if err := rt.sink.Commit(ctx, spec, tk, res); err != nil && ctx.Err() == nil {
			rt.log.Warn("sensor result was not committed", "sensor", spec.Name, "error", err.Error())
		}
	}
}

// inputFor builds the inbound contract object for a sensor.
func (rt *Runtime) inputFor(spec Spec) Input {
	now := rt.clk.Now()
	max := spec.MaxTriggers
	if max <= 0 {
		max = 100
	}
	return Input{
		Sensor:      spec.Name,
		Job:         spec.Job,
		Cursor:      spec.Cursor,
		LastTickAt:  spec.LastTickAt,
		Now:         now.UnixMilli(),
		MaxTriggers: max,
		DeadlineMS:  now.Add(spec.Timeout).UnixMilli(),
		DryRun:      false,
	}
}

// claim marks a sensor as in flight. It succeeds once and fails while the
// sensor is still running, which is the whole of the per-sensor
// serialisation guarantee.
func (rt *Runtime) claim(name string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if _, ok := rt.active[name]; ok {
		return false
	}
	rt.active[name] = struct{}{}
	return true
}

func (rt *Runtime) unclaim(name string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	delete(rt.active, name)
}

// tryPermit borrows one worker without blocking.
func (rt *Runtime) tryPermit() bool {
	select {
	case rt.permits <- struct{}{}:
		return true
	default:
		return false
	}
}

// ReleasePermit returns a worker to the semaphore.
func (rt *Runtime) ReleasePermit() { <-rt.permits }

// drain waits, bounded by drain timeout, until every in flight evaluation has
// released its process group. The evaluations themselves are already bound to
// a cancelled context, so each is killing its own group right now; drain only
// holds the daemon's loop open until none is left.
func (rt *Runtime) drain() {
	deadline := rt.clk.Now().Add(rt.drainTimeout)
	ticker := rt.clk.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		rt.mu.Lock()
		n := len(rt.active)
		rt.mu.Unlock()
		if n == 0 {
			return
		}
		if !rt.clk.Now().Before(deadline) {
			rt.log.Warn("sensor drain timed out with evaluations still in flight",
				"in_flight", n)
			return
		}
		<-ticker.C
	}
}
