package model

// EffectKind is one thing a transition requires its caller to do. The machine
// decides what has to happen and returns it; the caller performs it inside its
// own transaction (05 section 4). That is what keeps the model pure while still
// leaving it in charge of the writes a transition implies.
type EffectKind string

const (
	// EffectBumpEpoch raises the run's fencing epoch, so any writer holding
	// the old one is shut out.
	EffectBumpEpoch EffectKind = "bump_epoch"
	// EffectTakeLease and EffectReleaseLease are the ownership of a running
	// run. Only a run that holds a lease releases one.
	EffectTakeLease    EffectKind = "take_lease"
	EffectReleaseLease EffectKind = "release_lease"
	// EffectSetStarted and EffectSetFinished stamp started_at and
	// finished_at, together with the reason code the transition validated.
	// The values come from the caller's clock, never from here.
	EffectSetStarted  EffectKind = "set_started"
	EffectSetFinished EffectKind = "set_finished"
	// EffectKillProcessGroup ends the running process group. It is listed
	// before the writes because a cancelled run that is still executing is
	// the one thing worse than a slow cancel.
	EffectKillProcessGroup EffectKind = "kill_process_group"
	// EffectIncCrashCount counts a run against its crash budget.
	EffectIncCrashCount EffectKind = "inc_crash_count"
	// EffectIncAttempt opens the next attempt of a step.
	EffectIncAttempt EffectKind = "inc_attempt"
	// EffectRestoreAttempt takes an interrupted attempt's increment back off
	// the step. Only the shutdown drain uses it: the attempt never got to
	// produce a verdict, so the retry budget it would have spent stays
	// unspent. It is deliberately distinct from EffectIncAttempt so no caller
	// can "restore" an attempt nothing opened.
	EffectRestoreAttempt EffectKind = "restore_attempt"
	// EffectSetNextAttemptAt and EffectSetAvailableAt write the time a retry
	// or a deferred run becomes runnable. The model names the column; the
	// caller computes the value, because that is backoff (M1-09) and a clock.
	EffectSetNextAttemptAt EffectKind = "set_next_attempt_at"
	EffectSetAvailableAt   EffectKind = "set_available_at"
	// EffectSetDeferReason writes why a run is not running now. Its argument
	// is the reason, so a deferred run can never end up without one (I14).
	EffectSetDeferReason EffectKind = "set_defer_reason"
	// EffectEmit is the run_events row for the transition, exactly one per
	// transition, committed with it (G10). Its argument is the event name.
	EffectEmit EffectKind = "emit"
)

// Effect is one required action. Arg carries the value the model itself
// determined: the event name for EffectEmit and the reason for
// EffectSetDeferReason. It is empty for every other kind, because those name a
// column to write rather than a value to write into it.
//
// The type is comparable, so a test compares whole effect lists with
// slices.Equal and a caller can switch on Kind without a type assertion.
type Effect struct {
	Kind EffectKind
	Arg  string
}

// effects is the list constructor the machines use, so a transition reads as
// the sequence of things it demands.
func effects(list ...Effect) []Effect { return list }

// act is an effect with no argument.
func act(kind EffectKind) Effect { return Effect{Kind: kind} }

// emit is the run_events row a transition writes.
func emit(name string) Effect { return Effect{Kind: EffectEmit, Arg: name} }

// deferReason is the reason a run is not running now.
func deferReason(reason string) Effect {
	return Effect{Kind: EffectSetDeferReason, Arg: reason}
}
