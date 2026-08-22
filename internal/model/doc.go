// Package model holds the run and step state machines as pure functions.
//
// Nothing here reads a database, a network, a file or a clock, and the package
// imports nothing under internal/ (05 section 4). Time arrives as
// Guards.Now, in unix milliseconds UTC, because internal/clock owns time.Now
// (M0-09). Every function is deterministic given its arguments, which is what
// makes the cross table below provable rather than merely tested.
//
// # The two machines
//
// A run is queued, running, succeeded, failed or cancelled. A step is pending,
// running, succeeded, failed, skipped or cancelled. NextRunState and
// NextStepState take the current state, an event and the guards, and return the
// next state, the effects the caller has to perform, and an error. The drawn
// form of both machines, generated from the code, is transitions.golden.md.
//
// Two simplifications shrink the model, and both come from the plans:
//
//   - Deferred is not a state (00 section 3.14, 01 section 4.1). A deferred run
//     is queued, with available_at in the future and a defer reason beside it.
//     IsDeferred computes it, so the command line still shows "deferred
//     (reason)" without two extra states and every transition around them.
//   - Retry is not a state either (05 section 6.6). A failed attempt with
//     retries left goes back to pending with a next_attempt_at, which makes the
//     claim gate in SQL the whole retry scheduler.
//
// # Effects
//
// A transition returns what has to happen, and the caller performs it inside
// its own transaction. That keeps the model pure while it stays in charge of
// the writes a transition implies, including the single run_events row every
// transition commits with itself (G10). An effect names the column to write;
// the value comes from the caller when it depends on a clock or on backoff,
// which the model must not compute.
//
// # Refusals
//
// A refusal moves nothing: the state comes back as it went in and no effect is
// demanded. Refusals carry identity, so callers match them with errors.Is and
// never with a string:
//
//   - ErrIllegalTransition is a pair the machine does not name at all.
//   - ErrNotAvailable, ErrMissingReasonCode, ErrMissingDeferReason,
//     ErrStepsNotTerminal, ErrStaleLease and ErrLeaseStillValid are pairs the
//     machine names and the guards deny.
//
// errors.As on IllegalTransitionError or GuardError gives the state, the event
// and the outcome that was denied, so a log line and an explain output stand on
// their own.
//
// # The invariants this package holds
//
//	I2   A terminal run has no step in running. EvAllStepsDone is refused
//	     unless Guards.AllStepsTerminal says every step has finished.
//	I10  A succeeded run has no failed step, and a failed run has one.
//	     RunAggregate is the single function that decides it, used by the
//	     engine (M1-08) and by fsck (M1-12).
//	I14  A deferred run has a defer reason. EvDeferred is refused without
//	     one, and a requeue after a crash writes DeferReasonAfterCrash.
//	     06 section 2.1: a terminal state has a reason code. Every
//	     transition into one is refused without it.
//	     02 T14: only EvOperatorRetry leads out of a terminal run. A
//	     terminal step accepts no event at all.
//
// The same rules are enforced a second time as CHECK constraints in the schema
// (07 section 7). Two independent enforcements of one rule are cheap, and they
// catch the case where one side is edited and the other is not.
package model
