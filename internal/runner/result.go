package runner

import (
	"os"
	"os/exec"
	"time"
)

// Outcome is the five value verdict a run can have. The taxonomy is a
// contract: explain and retry logic read it, so two different incidents must
// never share a value. The executable table lives in result_test.go.
type Outcome int

const (
	Succeeded Outcome = iota
	Failed
	TimedOut
	SpawnFailed
	Signalled
)

func (o Outcome) String() string {
	switch o {
	case Succeeded:
		return "Succeeded"
	case Failed:
		return "Failed"
	case TimedOut:
		return "TimedOut"
	case SpawnFailed:
		return "SpawnFailed"
	case Signalled:
		return "Signalled"
	default:
		return "Unknown"
	}
}

// The reason codes this package emits. They follow the event catalogue in the
// observability plan; when the reason package lands (M1-05) these constants
// become its values without a contract change.
const (
	ReasonSucceeded   = "STEP_SUCCEEDED"
	ReasonNonzeroExit = "STEP_FAILED_NONZERO_EXIT"
	ReasonTimeout     = "STEP_FAILED_TIMEOUT"
	ReasonSpawn       = "STEP_FAILED_SPAWN"
	ReasonSignal      = "STEP_FAILED_SIGNAL"
)

// Result is the full verdict on one attempt. StartedAt and FinishedAt are
// unix milliseconds read from the Spec's clock.
type Result struct {
	ExitCode   int
	Signal     string // "SIGKILL" and friends, empty when the process exited on its own
	StartedAt  int64
	FinishedAt int64
	Pgid       int
	Outcome    Outcome
	ReasonCode string
	ReasonData map[string]any
	Err        error // the operating system cause, set for SpawnFailed only
}

// classify turns what os/exec observed into the taxonomy. The order carries
// meaning: an exit status is direct evidence that the program completed, so a
// clean or nonzero exit wins even if the deadline fired in the same instant;
// a signal death under an active deadline is the runner's own TERM or KILL
// and reports as TimedOut.
func classify(cmd *exec.Cmd, timedOut bool, ctxErr error, timeout time.Duration) Result {
	st := cmd.ProcessState
	if st == nil {
		return Result{Outcome: SpawnFailed, ReasonCode: ReasonSpawn}
	}
	code, sig, signaled := exitStatus(st)
	switch {
	case signaled && timedOut:
		return Result{
			Outcome:    TimedOut,
			ReasonCode: ReasonTimeout,
			ReasonData: map[string]any{"timeout_ms": timeout.Milliseconds()},
		}
	case signaled:
		data := map[string]any{
			"signal":    sigName(sig),
			"exit_code": 128 + int(sig),
		}
		if ctxErr != nil {
			// The parent context cancelled the run; the signal came from us.
			data["cancelled"] = true
		}
		return Result{
			Outcome:    Signalled,
			ReasonCode: ReasonSignal,
			Signal:     sigName(sig),
			ExitCode:   128 + int(sig),
			ReasonData: data,
		}
	case code == 0:
		return Result{Outcome: Succeeded, ReasonCode: ReasonSucceeded}
	default:
		return Result{
			Outcome:    Failed,
			ReasonCode: ReasonNonzeroExit,
			ExitCode:   code,
			ReasonData: map[string]any{
				"exit_code": code,
				"transient": code == 75, // EX_TEMPFAIL: always retryable
			},
		}
	}
}

// ExitStatus is the public reading of a finished process's wait status: the
// exit code for a normal exit, or the canonical signal name for a signal
// death. The sensor evaluator reads the same facts through this seam instead
// of re-deriving the platform split the classifier above keeps private.
func ExitStatus(st *os.ProcessState) (code int, signal string, signalled bool) {
	if st == nil {
		return 0, "", false
	}
	c, sig, signaled := exitStatus(st)
	if !signaled {
		return c, "", false
	}
	return c, sigName(sig), true
}
