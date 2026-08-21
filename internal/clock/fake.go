package clock

import (
	"sync"
	"time"
)

// Fake is a deterministic Clock for tests. Wall time and monotonic time move
// separately, which is the point: it can simulate an NTP correction that moves
// the wall clock while leaving durations alone.
//
// Fake does not fake timers. NewTimer and NewTicker return real ones, which a
// testing/synctest bubble virtualises anyway. See doc.go.
type Fake struct {
	mu   sync.Mutex
	now  time.Time
	mono time.Duration
}

// NewFake returns a Fake whose wall clock reads t0. The monotonic clock starts
// at zero, so the zero Mono is the moment the Fake was created.
func NewFake(t0 time.Time) *Fake { return &Fake{now: t0.UTC()} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) Mark() Mono {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Mono{d: f.mono}
}

func (f *Fake) Since(m Mono) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mono - m.d
}

func (f *Fake) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

func (f *Fake) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }

// Advance moves wall time and monotonic time by the same amount: ordinary time
// passing.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	f.mono += d
}

// JumpWall moves wall time only, forwards or backwards. This is an NTP
// correction: durations taken from Mark and Since must not notice it.
func (f *Fake) JumpWall(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set puts wall time at an absolute instant, leaving monotonic time alone. It is
// JumpWall for tests that find the destination easier to write than the delta.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t.UTC()
}
