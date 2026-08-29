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

		got := model.RunAggregate(prefix, false)
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
	if got := model.RunAggregate(nil, false); got != model.RunSucceeded {
		t.Errorf("RunAggregate(nil) = %q, want %q", got, model.RunSucceeded)
	}
	if got := model.RunAggregate([]model.StepState{}, false); got != model.RunSucceeded {
		t.Errorf("RunAggregate(empty) = %q, want %q", got, model.RunSucceeded)
	}
}

// TestRunAggregateReadsSkippedStepsAsSuccess is the rule from I10 spelled out:
// a run whose steps all succeeded or were skipped succeeded. A skip is a
// decision, not a failure.
func TestRunAggregateReadsSkippedStepsAsSuccess(t *testing.T) {
	steps := []model.StepState{model.StepSucceeded, model.StepSkipped, model.StepSkipped}
	if got := model.RunAggregate(steps, false); got != model.RunSucceeded {
		t.Errorf("RunAggregate(%v) = %q, want %q", steps, got, model.RunSucceeded)
	}
}

// TestRunAggregateDoesNotTouchItsInput is what makes the function safe to hand
// a slice the caller keeps using.
func TestRunAggregateDoesNotTouchItsInput(t *testing.T) {
	steps := []model.StepState{model.StepFailed, model.StepPending, model.StepSkipped}
	before := slices.Clone(steps)

	model.RunAggregate(steps, false)

	if !slices.Equal(steps, before) {
		t.Errorf("RunAggregate rewrote its input to %v, want %v", steps, before)
	}
}

// TestTerminalVerdictMatchesRunAggregate proves the claim run_fsm.go makes: the
// machine ranks a terminal run's steps in the same order the fold does. The
// machine never reads a step row, so the two can only agree by holding the same
// order, and nothing but this test holds them to it.
//
// The comment asserted this test existed before the test did (#188).
func TestTerminalVerdictMatchesRunAggregate(t *testing.T) {
	t.Parallel()

	terminal := []model.StepState{
		model.StepSucceeded, model.StepFailed,
		model.StepCancelled, model.StepSkipped,
	}
	var walk func(prefix []model.StepState, depth int)
	walk = func(prefix []model.StepState, depth int) {
		if len(prefix) > 0 {
			want := model.RunAggregate(prefix, false)
			g := model.Guards{
				AnyStepFailed:    contains(prefix, model.StepFailed),
				AnyStepCancelled: contains(prefix, model.StepCancelled),
			}
			got, _ := model.TerminalVerdict(g)
			if got != want {
				t.Fatalf("TerminalVerdict over %v = %q, RunAggregate = %q", prefix, got, want)
			}
		}
		if depth == 0 {
			return
		}
		for _, s := range terminal {
			walk(append(prefix, s), depth-1)
		}
	}
	walk(nil, 4)
}

// TestRunLevelFailureOutranksEverySetOfSteps pins the second input: a run that
// failed for a reason no step can express is failed whatever its steps say,
// including the all-skipped shape the reaper leaves behind.
func TestRunLevelFailureOutranksEverySetOfSteps(t *testing.T) {
	t.Parallel()

	for _, steps := range [][]model.StepState{
		nil,
		{model.StepSkipped},
		{model.StepSkipped, model.StepSkipped, model.StepSkipped},
		{model.StepSucceeded, model.StepSucceeded},
		{model.StepCancelled},
		{model.StepPending},
	} {
		if got := model.RunAggregate(steps, true); got != model.RunFailed {
			t.Errorf("RunAggregate(%v, true) = %q, want %q", steps, got, model.RunFailed)
		}
	}
}

func contains(steps []model.StepState, want model.StepState) bool {
	for _, s := range steps {
		if s == want {
			return true
		}
	}
	return false
}
