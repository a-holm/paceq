// Package leases runs the leader and follower loop around one role lease.
//
// A role is a singleton decision: exactly one instance evaluates due
// schedules, one reaps dead runs. The store owns the admission decision (one
// idempotent statement) and the event trail; this package owns the loop that
// calls it every renew interval, starts and stops the body as leadership comes
// and goes, and records each transition with its reason code.
//
// The loop is deliberately dumb about time: it reads the clock it was handed,
// never compares lease timestamps across processes, and treats an empty
// renewal answer as "someone else leads" rather than an error. The body
// contract: check ctx between transactions, never hold a write transaction
// across a renew interval, expect to be cancelled at any moment and to be
// started again with a higher epoch after a takeover.
package leases
