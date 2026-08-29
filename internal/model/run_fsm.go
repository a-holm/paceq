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

// DeferReasonConcurrencyKey is what a run materialised under a held
// concurrency_key says about itself (#17). It is a different word from
// DeferReasonConcurrency on purpose: the two deferrals look alike in the row
// (queued, available_at ahead) but release differently. A job-overlap deferral
// claims like any queued run once it is due; a key deferral may not claim at
// all until the claim pass gives it its key back. The claim predicate tells
// the two apart by this word.
const DeferReasonConcurrencyKey = "concurrency_key"

// DeferReasonUnspecified is what fsck --repair stamps onto a queued run held
// into the future that carries no defer reason of its own (I14, M6-06). The
// repair never invents the true reason - there is none to recover - so the
// word it writes is the honest "unspecified", which the CLI then reports as
// held for an unknown reason instead of a hold with no story at all.
const DeferReasonUnspecified = "unspecified"

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
		// The same verdict the cancel arm takes. A finish that ranked its
		// own way could write failed over steps that only cancelled, which
		// is what I10 then reports and nothing ever repairs (#188).
		next, name := TerminalVerdict(g)
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
		// The steps hold the verdict, not the request (I10). A run that
		// failed and was then cancelled still failed, and a run whose steps
		// all succeeded succeeded, however late the cancel arrived. The
		// request stays on the row either way.
		next, name := TerminalVerdict(g)
		if !g.LeaseValid {
			return cur, nil, GuardError{From: cur, Event: ev, To: next, Want: ErrStaleLease}
		}
		if !g.AllStepsTerminal {
			// I2, and the ordering the verdict depends on: the caller closes
			// the open steps out first, then asks what they add up to. A step
			// still open here means it asked in the wrong order.
			return cur, nil, GuardError{From: cur, Event: ev, To: next, Want: ErrStepsNotTerminal}
		}
		if err := requireReason(cur, ev, next, g); err != nil {
			return cur, nil, err
		}
		return next, effects(act(EffectKillProcessGroup), act(EffectSetFinished),
			act(EffectReleaseLease), emit(name)), nil

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

// TerminalVerdict ranks a run's closed-out steps in the same order RunAggregate
// does, from the two facts the guards carry: a failure outranks a cancellation,
// and a cancellation outranks the plain success of steps that were all done
// before the cancel landed. Both endings take it, the finish and the cancel, so
// the machine holds one order rather than one per arm.
//
// Keeping that order the same as RunAggregate's is what makes I10 hold without
// the machine reading a step row. TestTerminalVerdictMatchesRunAggregate proves
// the two stay in step.
func TerminalVerdict(g Guards) (RunState, string) {
	switch {
	case g.AnyStepFailed:
		return RunFailed, "run.failed"
	case g.AnyStepCancelled:
		return RunCancelled, "run.cancelled"
	default:
		return RunSucceeded, "run.succeeded"
	}
}
