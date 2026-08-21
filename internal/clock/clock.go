package clock

import "time"

// Mono is a monotonic reference point. It is a distinct type, not a time.Time,
// so the compiler refuses the mistake the project cannot afford: a monotonic
// reading stored in the database or sent to another process. Its zero value is
// the moment the clock was created.
//
// What the type prevents: a Mono cannot be assigned to a time.Time field, cannot
// be passed to a database/sql query (the driver rejects an unknown struct type),
// and carries no data through encoding/json because it has no exported fields
// and no marshaller.
//
// What it does not prevent: fmt verbs still print it, unsafe and reflection
// still reach the field, and json.Marshal produces the empty object "{}" rather
// than an error. The guarantee is that no Mono value can silently become a
// timestamp somewhere else, not that the value is unreachable.
type Mono struct{ d time.Duration }

// Clock is the whole time surface domain code may use. Wall time is logical
// time: scheduled_for, created_at, anything a human or another process reads.
// Monotonic time is duration: lease deadlines, timeouts, backoff. Mixing them is
// how an NTP correction becomes a false lease expiry.
type Clock interface {
	// Now is wall time in UTC, with no monotonic reading attached.
	Now() time.Time
	// Mark takes a monotonic reference point.
	Mark() Mono
	// Since is the monotonic duration elapsed since a mark. It is unaffected by
	// wall clock changes.
	Since(Mono) time.Duration
	// NewTimer and NewTicker are the package time constructors. Inside a
	// testing/synctest bubble they run on the bubble's fake clock; see doc.go
	// for when to use synctest and when to use Fake.
	NewTimer(time.Duration) *time.Timer
	NewTicker(time.Duration) *time.Ticker
}

// systemClock reads the operating system clock. base carries a monotonic
// reading, so every duration derived from it survives a wall clock jump.
type systemClock struct{ base time.Time }

// System returns the clock every production entry point passes down.
func System() Clock { return systemClock{base: time.Now()} }

func (c systemClock) Now() time.Time { return time.Now().UTC() }

func (c systemClock) Mark() Mono { return Mono{d: time.Since(c.base)} }

func (c systemClock) Since(m Mono) time.Duration { return time.Since(c.base) - m.d }

func (c systemClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

func (c systemClock) NewTicker(d time.Duration) *time.Ticker { return time.NewTicker(d) }
