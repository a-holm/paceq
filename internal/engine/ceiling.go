package engine

import (
	"time"

	"github.com/a-holm/paceq/internal/spec"
)

// stepCeiling answers how long one attempt of a step may run, and whether the
// run's own budget is what set that answer.
//
// A step that names no timeout is bounded by the job's. That is the file
// format's rule (spec.Step.Timeout) and the reason the canonical document
// leaves the key out instead of materialising the job's value into every step,
// so the executor reads it off the document it was handed rather than
// restating it as a constant of its own. The deadline is that job timeout
// anchored at the moment the run started, so what is left of it at now is the
// ceiling such a step runs under. A step that names its own runs under that
// instead, and still cannot outlive the run it belongs to.
//
// The second result is the attribution the kill carries afterwards: true means
// the run's budget set the ceiling and a timeout reads as scope: run, false
// means the step overran a ceiling it asked for itself. Two incidents, two
// different fixes, so both come from the same facts in the same place.
//
// A zero deadline is a job version with no timeout at all. The decoder
// materialises spec.DefaultTimeout before hashing, so a validated job never
// arrives that way; the fallback is what keeps an unvalidated one from running
// with no ceiling.
func stepCeiling(stepTimeout time.Duration, deadline, now time.Time) (time.Duration, bool) {
	if deadline.IsZero() {
		if stepTimeout > 0 {
			return stepTimeout, false
		}
		return spec.DefaultTimeout, false
	}
	remaining := deadline.Sub(now)
	if stepTimeout > 0 && stepTimeout <= remaining {
		return stepTimeout, false
	}
	return remaining, true
}
