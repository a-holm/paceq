package model

// Input is one event together with the guards it is judged under. A sequence of
// them is a history, which is what makes replay possible.
type Input struct {
	Event  Event
	Guards Guards
}

// ApplyRun folds a history through the run machine and returns where it ended,
// every effect it earned on the way, and the first refusal that stopped it. A
// refused input stops the fold, and the state returned is the last one the run
// legally reached.
//
// It exists so a property test can drive the model and a real store through the
// same sequence and compare them (M3-08), and so explain can replay a history
// without reimplementing the machine.
func ApplyRun(cur RunState, inputs []Input) (RunState, []Effect, error) {
	var all []Effect
	for _, in := range inputs {
		next, fx, err := NextRunState(cur, in.Event, in.Guards)
		if err != nil {
			return cur, all, err
		}
		cur = next
		all = append(all, fx...)
	}
	return cur, all, nil
}

// ApplyStep is ApplyRun for the step machine.
func ApplyStep(cur StepState, inputs []Input) (StepState, []Effect, error) {
	var all []Effect
	for _, in := range inputs {
		next, fx, err := NextStepState(cur, in.Event, in.Guards)
		if err != nil {
			return cur, all, err
		}
		cur = next
		all = append(all, fx...)
	}
	return cur, all, nil
}
