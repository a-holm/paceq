package model

// RunAggregate is the run state a set of steps implies (I10). It is the same
// function in the engine (M1-08) and in fsck (M1-12): one that computes the run
// state and one that checks it cannot disagree if they are the same code.
//
// The order of the tests is the meaning. An active step outranks everything,
// because a run with work left has not ended, whatever else happened. A failure
// outranks a cancellation, because a run that failed and was then cancelled
// still failed. Skipped counts as success: a skip is a decision, not a failure.
//
// A run with no steps has nothing outstanding and nothing failed, so it
// succeeded. Nothing materialises such a run, because a job with no steps does
// not validate, and this is what the model says if one ever appears.
func RunAggregate(steps []StepState) RunState {
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
