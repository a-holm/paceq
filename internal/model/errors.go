package model

import (
	"errors"
	"fmt"
)

// The sentinels are what callers match with errors.Is. Every refusal the
// machines make has one, so no caller has to compare error text, and the
// detailed types below carry the state and the event that explain the refusal
// to a person reading a log or running explain.
var (
	// ErrIllegalTransition is a pair (state, event) the machine does not
	// name at all.
	ErrIllegalTransition = errors.New("illegal transition")
	// ErrNotAvailable is a claim on a run whose available_at is still in the
	// future. The pair is legal; the run is simply deferred.
	ErrNotAvailable = errors.New("run is not available yet")
	// ErrMissingReasonCode is a transition that needs an explanation and did
	// not get one (06 section 2.1).
	ErrMissingReasonCode = errors.New("missing reason code")
	// ErrMissingDeferReason is a deferral without a reason (I14).
	ErrMissingDeferReason = errors.New("missing defer reason")
	// ErrStepsNotTerminal is a run being finished while a step of it is
	// still active (I2).
	ErrStepsNotTerminal = errors.New("steps are not all terminal")
	// ErrStaleLease is a writer without the run's lease trying to move it.
	ErrStaleLease = errors.New("lease is not held")
	// ErrLeaseStillValid is a lease expiry reported for a lease that has not
	// expired.
	ErrLeaseStillValid = errors.New("lease has not expired")
	// ErrUnknownState is a stored name outside the closed set.
	ErrUnknownState = errors.New("unknown state")
)

// IllegalTransitionError is a refusal that names both sides of it, so the
// message stands on its own in a log and in explain (M5-01).
type IllegalTransitionError struct {
	From  State
	Event Event
}

func (e IllegalTransitionError) Error() string {
	return fmt.Sprintf("%s state %q does not accept event %q: %s",
		e.From.Kind(), e.From.String(), e.Event, ErrIllegalTransition)
}

func (e IllegalTransitionError) Unwrap() error { return ErrIllegalTransition }

// GuardError is a transition the machine names but the guards refuse. Want is
// the sentinel the refusal matches, and the fields say which transition asked
// for what, so the message names the outcome that was denied rather than only
// the rule that denied it.
type GuardError struct {
	From  State
	Event Event
	To    State
	Want  error
}

func (e GuardError) Error() string {
	return fmt.Sprintf("%s state %q cannot take event %q to %q: %s",
		e.From.Kind(), e.From.String(), e.Event, e.To.String(), e.Want)
}

func (e GuardError) Unwrap() error { return e.Want }

// UnknownStateError is a name that is not in the closed set of either machine.
type UnknownStateError struct {
	Kind string
	Name string
}

func (e UnknownStateError) Error() string {
	return fmt.Sprintf("unknown %s state %q: %s", e.Kind, e.Name, ErrUnknownState)
}

func (e UnknownStateError) Unwrap() error { return ErrUnknownState }
