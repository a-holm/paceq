package model

// DeferReasonAfterCrash is what a run requeued after a lost lease says about
// itself. The reason code catalogue is M1-05; this one string is the model's
// own, because the run it describes is put back by the reaper rather than by
// anything that could pass a reason in.
const DeferReasonAfterCrash = "reconciled_after_crash"

// DeferReasonAfterShutdown is what a run requeued by its own executor's clean
// stop says about itself. Like DeferReasonAfterCrash it lives here because the
// machine demands the effect and nothing else owns the word. A drained run
// carries no crash: the executor left on purpose, and counting one would make
// every ordinary restart look like a failing run.
const DeferReasonAfterShutdown = "requeued_after_shutdown"

// DeferReasonConcurrency is what a run materialised under overlap: queue says
// about itself: it was born queued, but every concurrency slot of its job was
// already held, so available_at points into the future and this reason names
// why (#68). It lives beside the other two defer reasons so all three words
// the schema's CHECK and fsck I14 look for come from one file.
const DeferReasonConcurrency = "concurrency"

// NextRunState is the run machine, and the only place that decides what a run
// may do next. It reads nothing but its arguments: no database, no clock, no
// package level state, so the same three inputs always give the same three
// results. The effects it returns say what the caller has to write; the caller
// performs them in its own transaction (05 section 4).
//
// A refusal moves nothing. The state comes back exactly as it went in and no
// effect is demanded, so a caller that acts on the state without checking the
// error cannot half apply a transition.
//
// Guards are read in one order everywhere: fencing first, because a writer that
// has lost the lease must be shut out before anything it reports is believed;
// then the precondition of the event itself; then the reason code, which is the
// last thing that can be missing from an otherwise legal transition.
func NextRunState(cur RunState, ev Event, g Guards) (RunState, []Effect, error) {
	switch {
	case cur == RunQueued && ev == EvClaim:
		if g.CancelRequested {
			// Cancellation observed before the run ever started. There is
			// no lease to release and no process group to kill, which
			// makes this the cheapest cancellation in the system.
			if err := requireReason(cur, ev, RunCancelled, g); err != nil {
				return cur, nil, err
			}
			return RunCancelled, effects(act(EffectSetFinished), emit("run.cancelled")), nil
		}
		if !g.available() {
			// This is the deferred run: queued, with an available_at that
			// has not arrived. Nothing moves, and the refusal says why.
			return cur, nil, GuardError{From: cur, Event: ev, To: RunRunning, Want: ErrNotAvailable}
		}
		return RunRunning, effects(
			act(EffectBumpEpoch), act(EffectTakeLease), act(EffectSetStarted), emit("run.started")), nil

	case cur == RunQueued && ev == EvDeferred:
		// A deferral writes two things or it writes neither: without the
		// reason, I14 is broken the moment the row is committed.
		if g.DeferReason == "" {
			return cur, nil, GuardError{From: cur, Event: ev, To: RunQueued, Want: ErrMissingDeferReason}
		}
		return RunQueued, effects(
			act(EffectSetAvailableAt), deferReason(g.DeferReason), emit("run.deferred")), nil

	case cur == RunRunning && ev == EvAllStepsDone:
		next, name := RunSucceeded, "run.succeeded"
		if g.AnyStepFailed {
			next, name = RunFailed, "run.failed"
		}
		if !g.LeaseValid {
			return cur, nil, GuardError{From: cur, Event: ev, To: next, Want: ErrStaleLease}
		}
		if !g.AllStepsTerminal {
			// I2: a terminal run has no step still running. The run
			// machine holds it without reading a single step row.
			return cur, nil, GuardError{From: cur, Event: ev, To: next, Want: ErrStepsNotTerminal}
		}
		if err := requireReason(cur, ev, next, g); err != nil {
			return cur, nil, err
		}
		return next, effects(act(EffectSetFinished), act(EffectReleaseLease), emit(name)), nil

	case cur == RunRunning && ev == EvCancelObserved:
		if !g.LeaseValid {
			return cur, nil, GuardError{From: cur, Event: ev, To: RunCancelled, Want: ErrStaleLease}
		}
		if err := requireReason(cur, ev, RunCancelled, g); err != nil {
			return cur, nil, err
		}
		return RunCancelled, effects(act(EffectKillProcessGroup), act(EffectSetFinished),
			act(EffectReleaseLease), emit("run.cancelled")), nil

	case cur == RunRunning && ev == EvLeaseExpired:
		next, name := RunQueued, "run.requeued"
		if !g.CrashBudgetLeft {
			next, name = RunFailed, "run.poisoned"
		}
		if g.LeaseValid {
			return cur, nil, GuardError{From: cur, Event: ev, To: next, Want: ErrLeaseStillValid}
		}
		if next == RunFailed {
			// Poison quarantine (02 section 5.7): a run that keeps
			// killing its executor stops being requeued.
			if err := requireReason(cur, ev, next, g); err != nil {
				return cur, nil, err
			}
			return next, effects(act(EffectSetFinished), emit(name)), nil
		}
		return next, effects(act(EffectBumpEpoch), act(EffectIncCrashCount),
			deferReason(DeferReasonAfterCrash), emit(name)), nil

	case cur == RunRunning && ev == EvShutdownDrain:
		// The mirror of lease_expired: the owner is still holding the
		// lease and is handing the run back itself. The epoch still goes
		// up, so a late writer from this attempt stays fenced out, and
		// available_at moves to now so the next executor can claim at
		// once. No crash count: nothing died.
		if !g.LeaseValid {
			return cur, nil, GuardError{From: cur, Event: ev, To: RunQueued, Want: ErrStaleLease}
		}
		return RunQueued, effects(act(EffectBumpEpoch), act(EffectReleaseLease),
			act(EffectSetAvailableAt), deferReason(DeferReasonAfterShutdown), emit("run.drained")), nil

	case cur.IsTerminal() && ev == EvOperatorRetry:
		// 02 T14, the only way out of a terminal state. The epoch is
		// bumped so anything still holding the old one stays shut out.
		return RunQueued, effects(act(EffectBumpEpoch), emit("run.reopened")), nil
	}

	return cur, nil, IllegalTransitionError{From: cur, Event: ev}
}

// requireReason is the rule from 06 section 2.1: nothing ends without an
// explanation. The retry transition takes it too, because a scheduled retry
// records an attempt that failed.
func requireReason(from State, ev Event, to State, g Guards) error {
	if g.ReasonCode != "" {
		return nil
	}
	return GuardError{From: from, Event: ev, To: to, Want: ErrMissingReasonCode}
}
