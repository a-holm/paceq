package model

// RunAggregate is the run state a set of steps implies (I10). fsck checks by
// calling it, and every writer of a terminal run state takes the same order,
// either by calling it directly or through TerminalVerdict, which reads the
// order off the guards instead of the step rows.
// TestTerminalVerdictMatchesRunAggregate holds the two to one answer, so the
// computed state and the checked state cannot disagree.
//
// The order of the tests is the meaning. An active step outranks everything,
// because a run with work left has not ended, whatever else happened. A failure
// outranks a cancellation, because a run that failed and was then cancelled
// still failed. Skipped counts as success: a skip is a decision, not a failure.
//
// runLevelFailure is the second input, and it exists because a run can fail for
// a reason no step can express. The reaper quarantines a poisoned run and fails
// one whose attempt budget is spent; when that happens before any step ran,
// every step ends skipped and the step fold alone would read the run as a
// success. The flag outranks the steps, which is the only way the fold can
// answer for both kinds of ending without a caller deciding on its own.
//
// A run with no steps has nothing outstanding and nothing failed, so it
// succeeded. Nothing materialises such a run, because a job with no steps does
// not validate, and this is what the model says if one ever appears.
func RunAggregate(steps []StepState, runLevelFailure bool) RunState {
	if runLevelFailure {
		return RunFailed
	}
	var anyActive, anyFailed, anyCancelled bool
	for _, s := range steps {
		switch s {
		case StepPending, StepRunning:
			anyActive = true
		case StepFailed:
			anyFailed = true
		case StepCancelled:
			anyCancelled = true
		}
	}
	switch {
	case anyActive:
		return RunRunning
	case anyFailed:
		return RunFailed
	case anyCancelled:
		return RunCancelled
	default:
		return RunSucceeded
	}
}
