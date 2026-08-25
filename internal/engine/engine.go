package engine

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/store"
)

// DefaultPollInterval is how often a running step's cancellation request is
// re-read while the process runs. Between steps the request is read directly,
// so this only bounds how long a cancel can stay invisible mid-run.
const DefaultPollInterval = 2 * time.Second

// Engine executes runs. It owns no state of its own beyond its wiring: every
// fact about a run lives in the store, and every decision about a transition
// lives in internal/model. What is left here is sequencing and the one thing
// nothing else may do, running processes, which happens strictly between
// transactions, never inside one.
type Engine struct {
	// Store is the database. All reads and writes go through it; the
	// engine holds no handle of its own and opens no transaction.
	Store *store.Store

	// LogRoot is where step logs are written. Paths stored in the
	// database are relative to it.
	LogRoot logsink.Root

	// StateDir is the state directory this executor serves. Per-run
	// working directories live under <StateDir>/runs/<run id> (#13); a
	// step's $PACEQ_OUTPUT lands inside its own run's directory. Empty
	// disables the output contract.
	StateDir string

	// Clock drives every timing decision: poll ticks, deadlines, the
	// stamps handed to the runner. Never time directly.
	Clock clock.Clock

	// Owner is the name this executor claims leases under. Every write
	// the engine makes on a claimed run must come from the same name.
	Owner string

	// PollInterval bounds how long a cancellation can wait to be seen
	// while a step runs. Zero means DefaultPollInterval.
	PollInterval time.Duration

	// StepTimeoutDefault applies when neither the job nor the step names
	// a timeout. Zero means runner.DefaultTimeout.
	StepTimeoutDefault time.Duration

	LeaseTTL time.Duration

	// RenewInterval is how often held leases renew, in one batched
	// transaction for all of them. Zero means a third of the ttl, which is
	// the ratio that tolerates two lost renewals before ownership is even in
	// question.
	RenewInterval time.Duration

	// ClockSkewAllowance is how long past the expiry the reaper waits before
	// it takes a run from its holder. Zero means the store's default.
	ClockSkewAllowance time.Duration

	// RequeueBackoff is how long a reaped run waits before it is due again.
	// Zero means the store's default.
	RequeueBackoff time.Duration

	// MaxCrashCount is the poison quarantine line: a run that has outlived
	// this many executors fails on the next reap instead of requeueing.
	// Zero means the store's default of five.
	MaxCrashCount int

	// Rnd is the source full-jitter draws from. Nil means one seeded
	// from system entropy on first use. Tests inject a seeded source so
	// backoff sequences replay exactly.
	Rnd *rand.Rand

	rndOnce sync.Once

	// mu and held are the lease keeper: every claim this process holds, with
	// the fencing token it was granted. The renewal goroutine reads them; an
	// executor registers on claim and forgets on exit.
	mu   sync.Mutex
	held map[string]*heldRun
}

func (e *Engine) ttl() time.Duration {
	if e.LeaseTTL > 0 {
		return e.LeaseTTL
	}
	return store.DefaultRunLeaseTTL
}

func (e *Engine) renewEvery() time.Duration {
	if e.RenewInterval > 0 {
		return e.RenewInterval
	}
	return e.ttl() / 3
}

// rnd returns the injected jitter source, creating the default one on first
// use. The seed comes from crypto/rand, not the time and not a package
// global, so two engines never share a draw sequence.
func (e *Engine) rnd() *rand.Rand {
	e.rndOnce.Do(func() {
		if e.Rnd == nil {
			var seed [16]byte
			if _, err := crand.Read(seed[:]); err != nil {
				panic("engine: seed the jitter source: " + err.Error())
			}
			// The source only spaces retry attempts out; nothing
			// security relevant reads its output, and a fast non
			// cryptographic generator is exactly what jitter wants.
			e.Rnd = rand.New(rand.NewPCG( // #nosec G404 - backoff spacing, not a secret
				binary.LittleEndian.Uint64(seed[:8]),
				binary.LittleEndian.Uint64(seed[8:]),
			))
		}
	})
	return e.Rnd
}

func (e *Engine) pollInterval() time.Duration {
	if e.PollInterval > 0 {
		return e.PollInterval
	}
	return DefaultPollInterval
}
