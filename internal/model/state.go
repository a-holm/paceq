package model

// RunState is where a run is in its life. The set is closed, and it is the same
// set the runs table's CHECK constraint spells out: two independent
// enforcements of one rule (07 section 7).
//
// There are five states, not six. A deferred run is not a state of its own: it
// is a queued run whose available_at lies in the future, with a defer reason
// beside it (00 section 3.14, 01 section 4.1). Two states and every transition
// around them disappear, and the command line still prints "deferred (reason)"
// because IsDeferred computes it.
type RunState string

const (
	RunQueued    RunState = "queued"
	RunRunning   RunState = "running"
	RunSucceeded RunState = "succeeded"
	RunFailed    RunState = "failed"
	RunCancelled RunState = "cancelled"
)

// StepState is where one step of a run is in its life. Six states, and the same
// closed set as the steps table's CHECK constraint.
type StepState string

const (
	StepPending   StepState = "pending"
	StepRunning   StepState = "running"
	StepSucceeded StepState = "succeeded"
	StepFailed    StepState = "failed"
	StepSkipped   StepState = "skipped"
	StepCancelled StepState = "cancelled"
)

// State is what the two machines have in common. It exists so one error type
// can name the state it refused, whichever machine produced it, and so a caller
// that only asks "is this finished" does not need to know which machine it
// holds.
type State interface {
	// String is the name stored in the database and printed to people.
	String() string
	// Kind is the machine the state belongs to, "run" or "step".
	Kind() string
	// IsTerminal reports whether the state accepts no further work.
	IsTerminal() bool
	// RequiresReasonCode reports whether a transition into this state has to
	// carry a reason code.
	RequiresReasonCode() bool
}

// The two state types are the only implementations, and both are checked here
// rather than at every use.
var (
	_ State = RunQueued
	_ State = StepPending
)

func (s RunState) String() string { return string(s) }

func (s RunState) Kind() string { return "run" }

// IsTerminal is the end of the run. Only EvOperatorRetry leads out of it
// (02 T14), which NextRunState enforces and TestOnlyOperatorRetryLeavesTerminal
// proves for every other event.
func (s RunState) IsTerminal() bool {
	return s == RunSucceeded || s == RunFailed || s == RunCancelled
}

// RequiresReasonCode is the rule from 06 section 2.1: nothing reaches an end
// without an explanation. It is a separate predicate from IsTerminal even
// though the two agree today, because callers that write "a reason code is
// required here" should not have to know that terminality is the reason.
func (s RunState) RequiresReasonCode() bool { return s.IsTerminal() }

func (s StepState) String() string { return string(s) }

func (s StepState) Kind() string { return "step" }

// IsTerminal is the end of the step. Unlike a run, a terminal step accepts no
// event at all: an operator reopens a run, and the engine materialises its
// steps again.
func (s StepState) IsTerminal() bool {
	return s == StepSucceeded || s == StepFailed || s == StepSkipped || s == StepCancelled
}

func (s StepState) RequiresReasonCode() bool { return s.IsTerminal() }

// AllRunStates is the closed set, in life order. It is a function rather than a
// variable so no caller can rewrite the set for everybody else, and so the
// package holds no mutable global state at all.
func AllRunStates() []RunState {
	return []RunState{RunQueued, RunRunning, RunSucceeded, RunFailed, RunCancelled}
}

// AllStepStates is the closed set, in life order.
func AllStepStates() []StepState {
	return []StepState{StepPending, StepRunning, StepSucceeded, StepFailed, StepSkipped, StepCancelled}
}

// ParseRunState turns a stored name back into a state. Anything outside the
// closed set is refused rather than carried, because a row the model cannot
// interpret is a row nothing downstream can explain.
func ParseRunState(name string) (RunState, error) {
	for _, s := range AllRunStates() {
		if string(s) == name {
			return s, nil
		}
	}
	return "", UnknownStateError{Kind: "run", Name: name}
}

// ParseStepState turns a stored name back into a step state.
func ParseStepState(name string) (StepState, error) {
	for _, s := range AllStepStates() {
		if string(s) == name {
			return s, nil
		}
	}
	return "", UnknownStateError{Kind: "step", Name: name}
}

// IsDeferred is what the six state description called "deferred", computed
// instead of stored: a queued run that is not allowed to start yet. The caller
// passes the timestamps because the model never reads a clock (M0-09); both are
// unix milliseconds UTC, the unit every time column in the database uses.
//
// A run that is exactly available now is not deferred, which is the same
// comparison the claim gate in SQL makes.
func IsDeferred(state RunState, availableAt, now int64) bool {
	return state == RunQueued && availableAt > now
}
