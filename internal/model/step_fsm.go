package model

// NextStepState is the step machine. It has the same shape and the same rules
// as NextRunState: pure, refusals move nothing, and every terminal outcome
// carries a reason code.
//
// Two things it deliberately does not do. It does not read the lease: a step is
// moved by whoever owns the run, and that ownership is checked once, on the run
// (I1). It does not know the step graph either: EvUpstreamFailed is the hook a
// skip arrives through, and which steps it applies to is the transitive closure
// in M4-03.
func NextStepState(cur StepState, ev Event, g Guards) (StepState, []Effect, error) {
	switch {
	case cur == StepPending && ev == EvStepStarted:
		return StepRunning, effects(act(EffectIncAttempt), act(EffectSetStarted), emit("step.started")), nil

	case cur == StepPending && ev == EvUpstreamFailed:
		if err := requireReason(cur, ev, StepSkipped, g); err != nil {
			return cur, nil, err
		}
		return StepSkipped, effects(act(EffectSetFinished), emit("step.skipped")), nil

	case cur == StepRunning && ev == EvStepSucceeded:
		if err := requireReason(cur, ev, StepSucceeded, g); err != nil {
			return cur, nil, err
		}
		return StepSucceeded, effects(act(EffectSetFinished), emit("step.succeeded")), nil

	case cur == StepRunning && ev == EvStepFailed:
		if g.AttemptsLeft {
			// Retry is a transition, not a state (05 section 6.6). The
			// step goes back to pending with a time it becomes runnable,
			// and the claim gate in SQL is the whole retry scheduler.
			if err := requireReason(cur, ev, StepPending, g); err != nil {
				return cur, nil, err
			}
			return StepPending, effects(act(EffectSetNextAttemptAt), emit("step.retry_scheduled")), nil
		}
		if err := requireReason(cur, ev, StepFailed, g); err != nil {
			return cur, nil, err
		}
		return StepFailed, effects(act(EffectSetFinished), emit("step.failed")), nil

	case cur == StepRunning && ev == EvCancelObserved:
		if err := requireReason(cur, ev, StepCancelled, g); err != nil {
			return cur, nil, err
		}
		return StepCancelled, effects(act(EffectKillProcessGroup), act(EffectSetFinished),
			emit("step.cancelled")), nil

	case cur == StepRunning && ev == EvShutdownDrain:
		// The daemon is stopping and this attempt was cut short by it.
		// The attempt goes back to pending with the start's increment
		// restored (05 section 3.2, point 4): a restart of paceq is not
		// the user's fault, so it must not spend a retry.
		if err := requireReason(cur, ev, StepPending, g); err != nil {
			return cur, nil, err
		}
		return StepPending, effects(act(EffectRestoreAttempt), act(EffectSetNextAttemptAt),
			emit("step.interrupted")), nil

	case (cur == StepFailed || cur == StepSkipped) && ev == EvOperatorRetry:
		// M4-04: the step half of T14. An operator reopening the run puts
		// its failed and skipped steps back in front of the claim gate,
		// runnable at once. The attempt counter is history, not budget: it
		// stays as it stands here, and whoever reopens raises max_attempts
		// so the next start is a real new attempt. A succeeded step never
		// takes this event; its work is what the retry builds on.
		return StepPending, effects(act(EffectSetNextAttemptAt), emit("step.reopened")), nil
	}

	return cur, nil, IllegalTransitionError{From: cur, Event: ev}
}
