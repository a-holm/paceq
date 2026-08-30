package store

import (
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spool"
)

// StepVerdict is the one table from what happened to an attempt to what the
// step machine is told about it. Every writer of a step verdict reads its row
// here: the live executor with what it watched itself, recovery with what the
// dead attempt's shim wrote down. One table is the only way the same exit fact
// can be made to read the same way on both, which is precisely what the spool
// file exists to promise — a verdict that survives an executor's death is only
// worth having if it is the verdict that executor would have committed (#204).
//
// The arguments are the two facts the answer turns on and nothing else. The
// outcome word is the spool format's vocabulary, because that is the vocabulary
// that outlives the process that observed it. cancelled says the signal which
// ended the attempt was this system answering a cancellation; it is the only
// row that branches, and it is the row that matters, because the machine
// retries a failure and never retries a cancellation, and a run with a failed
// step outranks a run with a cancelled one (I10). Getting it wrong therefore
// reruns work an operator asked to stop and calls the run a failure.
//
// ok is false for a word the vocabulary has no row for. Only a file can carry
// one, and the reader of that file archives it rather than guessing.
func StepVerdict(outcome string, cancelled bool) (event string, code reason.Code, ok bool) {
	switch outcome {
	case spool.OutcomeSucceeded:
		return string(model.EvStepSucceeded), reason.STEPSucceeded, true
	case spool.OutcomeFailed:
		return string(model.EvStepFailed), reason.STEPFailedNonzeroExit, true
	case spool.OutcomeTimedOut:
		return string(model.EvStepFailed), reason.STEPFailedTimeout, true
	case spool.OutcomeSignalled:
		if cancelled {
			return string(model.EvCancelObserved), reason.STEPCancelled, true
		}
		return string(model.EvStepFailed), reason.STEPFailedSignal, true
	case spool.OutcomeSpawnFailed:
		// The process never existed, so nothing about the job failed;
		// the launch did.
		return string(model.EvStepFailed), reason.STEPFailedSpawn, true
	default:
		return "", "", false
	}
}
