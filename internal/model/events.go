package model

// Event is something that happened to a run or to a step. Events are the whole
// input alphabet of both machines: nothing changes state except by one of them,
// and a machine that does not name a pair (state, event) refuses it.
//
// The set is shared by the two machines on purpose. A pair such as
// (run, step_succeeded) is then a refusal the cross table proves, not a name
// that happens not to exist.
type Event string

const (
	// EvClaim is an executor taking a queued run.
	EvClaim Event = "claim"
	// EvDeferred is the concurrency gate or a backoff pushing a queued run
	// forward in time. It is the event behind "deferred", which is not a
	// state (00 section 3.14).
	EvDeferred Event = "deferred"
	// EvStepStarted is a step beginning an attempt.
	EvStepStarted Event = "step_started"
	// EvStepSucceeded is a step attempt that exited cleanly.
	EvStepSucceeded Event = "step_succeeded"
	// EvStepFailed is a step attempt that did not.
	EvStepFailed Event = "step_failed"
	// EvUpstreamFailed is a step that will never run because a step it needs
	// failed. The model exposes the transition; which steps it applies to is
	// the DAG closure in M4-03.
	EvUpstreamFailed Event = "upstream_failed"
	// EvAllStepsDone is the run's steps having all reached a terminal state.
	EvAllStepsDone Event = "all_steps_done"
	// EvCancelObserved is the owner or the reaper seeing cancel_requested_at.
	// The request is durable before anything is killed (02 section 5.8), so
	// requesting and observing are two different things and only the second
	// one is an event here.
	EvCancelObserved Event = "cancel_observed"
	// EvLeaseExpired is the reaper finding a run whose lease ran out.
	EvLeaseExpired Event = "lease_expired"
	// EvOperatorRetry is a person reopening a finished run. It is the only
	// event that leads out of a terminal state (02 T14).
	EvOperatorRetry Event = "operator_retry"
)

func (e Event) String() string { return string(e) }

// AllEvents is the closed input alphabet, in the order the cross table prints
// it. Like the state sets it is a function, so the alphabet cannot be edited
// underneath a caller.
func AllEvents() []Event {
	return []Event{
		EvClaim,
		EvDeferred,
		EvStepStarted,
		EvStepSucceeded,
		EvStepFailed,
		EvUpstreamFailed,
		EvAllStepsDone,
		EvCancelObserved,
		EvLeaseExpired,
		EvOperatorRetry,
	}
}
