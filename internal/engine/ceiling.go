package engine

import (
	"time"

	"github.com/a-holm/paceq/internal/runner"
)

// stepCeiling answers how long one attempt of a step may run, and whether the
// run's own budget is what set that answer.
//
// The second result is the attribution a timeout carries afterwards: true
// means the kill belongs to the run's deadline and reads as scope: run, false
// means the step overran a ceiling it asked for itself. The two are different
// incidents with different fixes, so they are decided here, once, from the
// same inputs that decide the duration.
func stepCeiling(stepTimeout time.Duration, deadline, now time.Time) (time.Duration, bool) {
	timeout := stepTimeout
	if timeout <= 0 {
		timeout = runner.DefaultTimeout
	}
	runDeadlineHit := false
	if !deadline.IsZero() {
		if remaining := deadline.Sub(now); remaining < timeout {
			timeout = remaining
			runDeadlineHit = true
		}
	}
	return timeout, runDeadlineHit
}
