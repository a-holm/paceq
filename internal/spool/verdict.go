package spool

import (
	"fmt"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// EventAndReason translates a result's outcome into the step machine's
// vocabulary: the model event the store's transition will take, and the
// reason code that explains it. The mapping mirrors the daemon-side
// classifier's verdict table exactly — the same exit fact must read the same
// way whether the executor saw it itself or read it back from this file
// after a crash.
//
// An outcome word this package does not know is a format error: the file
// claims a verdict the vocabulary has no row for, and the caller archives it
// rather than guessing.
func (r Result) EventAndReason() (event string, code reason.Code, err error) {
	switch r.Outcome {
	case OutcomeSucceeded:
		return string(model.EvStepSucceeded), reason.STEPSucceeded, nil
	case OutcomeFailed:
		return string(model.EvStepFailed), reason.STEPFailedNonzeroExit, nil
	case OutcomeTimedOut:
		return string(model.EvStepFailed), reason.STEPFailedTimeout, nil
	case OutcomeSignalled:
		return string(model.EvStepFailed), reason.STEPFailedSignal, nil
	case OutcomeSpawnFailed:
		return string(model.EvStepFailed), reason.STEPFailedSpawn, nil
	default:
		return "", "", fmt.Errorf("%w: unknown outcome %q", ErrFormat, r.Outcome)
	}
}
