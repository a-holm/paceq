package sensor

import (
	"sync"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// State is how open (tripped) a breaker is.
type State int

const (
	// Closed means the sensor may evaluate normally.
	Closed State = 0
	// Open means the sensor is tripped and must not be evaluated until the
	// cooldown elapses and a half-open probe is admitted.
	Open State = 1
	// HalfOpen is the probe slot reached after the cooldown: one evaluation
	// is admitted to test whether the sensor recovered. A success re-closes
	// the breaker; a failure trips it open again.
	HalfOpen State = 2
)

// Breaker holds the trip discipline for one sensor. A breaker starts closed,
// opens after enough consecutive permanent failures, waits out a cooldown,
// then admits a single probe; the probe either recovers the sensor (closed)
// or trips it open again. Transient failures never burn trip budget and an
// explicit operator resume re-closes it (plan 05 section 6.2 / plan 02
// section 5.5).
//
// The breaker is deliberately separable from the store: it owns the timing
// (cooldown) and the counting, and any caller can persist the resulting
// transition to a sensors row. The injected clock keeps every timing proof
// deterministic; no test sleeps.
type Breaker struct {
	// MaxFailures is how many consecutive permanent failures open it.
	MaxFailures int
	// Cooldown is how long an open breaker stays closed to attempts before a
	// probe is admitted.
	Cooldown time.Duration
	clk      clock.Clock

	mu    sync.Mutex
	state State
	// count is consecutive permanent failures since the last success or
	// resume.
	count int
	// openedAt is a monotonic mark taken when the breaker last entered Open.
	// It is always set whenever state is Open; zero Mono is never compared,
	// because the monotonic clock starts at zero the moment a Fake is born.
	openedAt clock.Mono
}

// BreakerConfig wires a breaker. A nil clock means the system clock.
type BreakerConfig struct {
	MaxFailures int
	Cooldown    time.Duration
	Clock       clock.Clock
}

// NewBreaker builds a closed breaker.
func NewBreaker(cfg BreakerConfig) *Breaker {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.System()
	}
	max := cfg.MaxFailures
	if max <= 0 {
		max = MaxConsecutiveFailures
	}
	cool := cfg.Cooldown
	if cool <= 0 {
		cool = BackoffCap
	}
	return &Breaker{
		MaxFailures: max,
		Cooldown:    cool,
		clk:         clk,
	}
}

// State reports the current breaker state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// openLocked trips the breaker open and marks the cooldown start.
func (b *Breaker) openLocked() {
	b.state = Open
	b.openedAt = b.clk.Mark()
}

// NoteOutcome feeds one finished evaluation's outcome into the breaker and
// returns the resulting state. kind is how the evaluation ended and success
// is whether the sensor produced a triggered or skipped verdict, rather than
// an error.
func (b *Breaker) NoteOutcome(kind FailureKind, success bool) State {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch {
	case success:
		// A success in any state re-closes the breaker and resets the
		// counter: the sensor proved it works. This is what a probe
		// recovery looks like as well.
		b.state = Closed
		b.count = 0
		return b.state
	case kind == FailureTransient || kind == FailureConfig:
		// A transient failure never counts toward the breaker and a config
		// failure pauses via the store rather than via counting here. In a
		// trip, neither re-arms: the sensor is still down, the cooldown just
		// continues.
		return b.state
	case b.state == HalfOpen:
		// A failed probe re-opens the breaker and restarts its cooldown.
		b.count = 1
		b.openLocked()
		return b.state
	default:
		// Closed: count toward the threshold; trip when reached.
		b.count++
		if b.count >= b.MaxFailures {
			b.openLocked()
		}
		return b.state
	}
}

// Resume forces the breaker back to closed, clearing the trip budget and the
// cooldown in one step. It is the operator's explicit recovery (`sensors
// resume`, M3-06), as distinct from the automatic half-open probe.
func (b *Breaker) Resume() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = Closed
	b.count = 0
	return b.state
}

// Admit decides whether an evaluation may run now.
//
//   - Closed admits always.
//   - Open refuses while the cooldown has not elapsed, and admits exactly one
//     probe once it has: the breaker transitions to HalfOpen, so the probe is
//     the only evaluation allowed in that window. This is the fail-safe
//     guarantee that a tripped sensor is never hammered (plan 02 section
//     5.5): it is pressed at most once per cooldown, and only a success
//     recovers it.
//   - HalfOpen admits nothing further, because the probe is in flight and its
//     verdict must land through NoteOutcome before the breaker decides again.
//
// This single gate is deliberately the whole engine-facing surface: an
// evaluation that is not admitted must simply not start.
func (b *Breaker) Admit() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Closed:
		return true
	case Open:
		// The cooldown may still be running (refuse), or it may have
		// elapsed (admit the one probe and go half-open). b.openedAt is
		// always valid here because state Open is only ever reached through
		// openLocked, which sets it.
		if elapsed := b.clk.Since(b.openedAt); elapsed >= b.Cooldown {
			b.state = HalfOpen
			return true
		}
		return false
	case HalfOpen:
		// The probe is in flight; admit nothing else until its verdict
		// lands through NoteOutcome.
		return false
	}
	return false
}
