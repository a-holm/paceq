// Package clock defines the Clock interface and its fake, so time is injectable in tests.
//
// # Wall time and monotonic time are different things
//
// Now is wall time: logical time, the kind that is stored and shown. Mark and
// Since are monotonic: duration, the kind that decides whether a lease has
// expired, a timeout has elapsed or a backoff is over. Wall time can move
// sideways when NTP corrects the machine; monotonic time cannot. Measuring a
// duration with wall time is how an NTP correction turns into a lease that
// looks expired while its owner is still working.
//
// Mono is a distinct type so that mistake is a compile error rather than a
// review comment. A deadline held as a Mono cannot be written to a column or
// sent to another process, which is the rule this project needs: lease time is
// never compared across processes. Every Store method that involves a lease
// takes its time from the database or from the process itself, never from a now
// value handed in by a caller.
//
// # Choosing between synctest and Fake
//
// Both exist. They answer different questions, and the choice is not open:
//
//	Kind of test                                     Tool
//	-------------------------------------------------------------------------
//	Loop, timeout, retry or backoff logic, no DB     testing/synctest
//	Timers and tickers firing in order               testing/synctest
//	Anything that reaches internal/store             clock.Fake and real SQLite
//	A wall clock jump, an NTP correction, DST        clock.Fake
//
// The reason for the split is that a synctest bubble only advances its clock
// when every goroutine in it is durably blocked, and a goroutine blocked on
// SQLite file I/O is not durably blocked. The paths that touch disk, which is
// most of this scheduler, cannot rely on the bubble. They take a Fake and step
// it themselves.
//
// The reason Fake does not fake timers is that same split seen from the other
// side. A time.Timer is driven by the runtime through a channel this package
// cannot fill, so a hand-built fake timer would be a poor copy of what the
// bubble already does properly. Fake.NewTimer and Fake.NewTicker return real
// timers, which are virtual inside a bubble and real outside it. Test timer
// behaviour in a bubble; test wall clock behaviour with a Fake.
package clock
