package model_test

import (
	"slices"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

// TestRunAggregateOverEveryCombinationOfUpToFourSteps is exhaustive rather than
// sampled: every list of up to four steps, 1555 of them, checked against an
// oracle written the other way round from the implementation. The invariants
// the aggregate exists to hold, I2 and I10, are asserted on every list.
func TestRunAggregateOverEveryCombinationOfUpToFourSteps(t *testing.T) {
	const maxSteps = 4

	combinations := 0
	var walk func(prefix []model.StepState)
	walk = func(prefix []model.StepState) {
		combinations++

		got := model.RunAggregate(prefix)
		if want := aggregateOracle(prefix); got != want {
			t.Fatalf("RunAggregate(%v) = %q, want %q", prefix, got, want)
		}
		if got == model.RunSucceeded && slices.Contains(prefix, model.StepFailed) {
			t.Fatalf("RunAggregate(%v) = %q with a failed step, which breaks I10", prefix, got)
		}
		if got == model.RunFailed && !slices.Contains(prefix, model.StepFailed) {
			t.Fatalf("RunAggregate(%v) = %q with no failed step, which breaks I10", prefix, got)
		}
		active := slices.Contains(prefix, model.StepPending) || slices.Contains(prefix, model.StepRunning)
		if active && got.IsTerminal() {
			t.Fatalf("RunAggregate(%v) = %q with an active step, which breaks I2", prefix, got)
		}
		if !slices.Contains(model.AllRunStates(), got) {
			t.Fatalf("RunAggregate(%v) = %q, which is not a run state", prefix, got)
		}

		if len(prefix) == maxSteps {
			return
		}
		for _, s := range model.AllStepStates() {
			next := make([]model.StepState, 0, len(prefix)+1)
			next = append(next, prefix...)
			walk(append(next, s))
		}
	}
	walk(nil)

	// 1 + 6 + 36 + 216 + 1296. A smaller number means the walk lost a state
	// and the sweep would be proving less than it claims.
	if want := 1555; combinations != want {
		t.Errorf("walked %d step lists, want %d", combinations, want)
	}
}

// aggregateOracle is the rule stated as membership tests rather than as one
// pass over the list, so an error in the implementation's loop does not repeat
// itself here.
func aggregateOracle(steps []model.StepState) model.RunState {
	switch {
	case slices.Contains(steps, model.StepPending), slices.Contains(steps, model.StepRunning):
		return model.RunRunning
	case slices.Contains(steps, model.StepFailed):
		return model.RunFailed
	case slices.Contains(steps, model.StepCancelled):
		return model.RunCancelled
	default:
		return model.RunSucceeded
	}
}

// TestRunAggregateOfNoStepsSucceeds pins the vacuous case. A run with no steps
// has nothing outstanding and nothing failed, so it is succeeded. The engine
// never materialises a run without steps, because a job without steps does not
// validate, and this is what the model says if one ever appears.
func TestRunAggregateOfNoStepsSucceeds(t *testing.T) {
	if got := model.RunAggregate(nil); got != model.RunSucceeded {
		t.Errorf("RunAggregate(nil) = %q, want %q", got, model.RunSucceeded)
	}
	if got := model.RunAggregate([]model.StepState{}); got != model.RunSucceeded {
		t.Errorf("RunAggregate(empty) = %q, want %q", got, model.RunSucceeded)
	}
}

// TestRunAggregateReadsSkippedStepsAsSuccess is the rule from I10 spelled out:
// a run whose steps all succeeded or were skipped succeeded. A skip is a
// decision, not a failure.
func TestRunAggregateReadsSkippedStepsAsSuccess(t *testing.T) {
	steps := []model.StepState{model.StepSucceeded, model.StepSkipped, model.StepSkipped}
	if got := model.RunAggregate(steps); got != model.RunSucceeded {
		t.Errorf("RunAggregate(%v) = %q, want %q", steps, got, model.RunSucceeded)
	}
}

// TestRunAggregateDoesNotTouchItsInput is what makes the function safe to hand
// a slice the caller keeps using.
func TestRunAggregateDoesNotTouchItsInput(t *testing.T) {
	steps := []model.StepState{model.StepFailed, model.StepPending, model.StepSkipped}
	before := slices.Clone(steps)

	model.RunAggregate(steps)

	if !slices.Equal(steps, before) {
		t.Errorf("RunAggregate rewrote its input to %v, want %v", steps, before)
	}
}
