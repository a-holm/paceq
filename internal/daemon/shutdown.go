package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"syscall"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/obs/sdnotify"
	"github.com/a-holm/paceq/internal/runner"
)

// The stop sequence (05 section 3.2). Two phases, one rule: nothing in flight
// is abandoned with a verdict it never wrote.
//
//   - Phase one closes the intake. Scheduler, sensor and dispatcher take no
//     new work; each acknowledges by returning, and the whole phase has a
//     100 millisecond budget measured on the clock.
//   - Phase two drains. The executors' context ends, which the runner answers
//     by killing each step's process group (SIGTERM, then SIGKILL after the
//     grace), and every executor whose run was cut short hands it back to the
//     queue through the store's drain transitions. Then the drain looks:
//     an executor that has returned proves its goroutine is gone and nothing
//     more, so the groups themselves are read off /proc and escalated until
//     they are (issue #232).
//
// After both phases the session row is closed as clean, the wal is
// checkpointed as the last write, and the pools close. A stop that could not
// end a step's process group closes none of that as clean: it says what
// survived and reports the stop as failed, because an operator told the stop
// succeeded is an operator who does not go looking.

// intakeBudget bounds phase one. Stopping reading is a context cancellation
// away for every loop, so a healthy daemon acks in microseconds; the budget
// exists so a wedged loop can delay, but never prevent, the drain.
const intakeBudget = 100 * time.Millisecond

// shutdown carries what the stop needs from startup.
type shutdown struct {
	cfg      Config
	clk      clock.Clock
	log      *slog.Logger
	statuses *statuses

	stopIntake   context.CancelFunc
	intakeAcks   []<-chan struct{}
	stopExec     context.CancelFunc
	execDrained  <-chan struct{}
	apiStopped   func(context.Context)
	loopsDrained <-chan struct{}

	// runsInFlight names the runs whose process groups phase two answers
	// for. It is read once, before the executors are told to stop, because
	// a run leaves the pool's book the moment its goroutine returns, and
	// that return is exactly the event the drain must not take as proof.
	// Nil means the drain has no census to make.
	runsInFlight func() []string

	closeSession func(context.Context) error
	checkpoint   func(context.Context) error
	closeStore   func() error
}

// run drives the two phases after the caller's context ended, then finishes
// the bookkeeping. It returns the error Serve reports: the reason the daemon
// stopped, with a clean stop reported as clean.
func (sd *shutdown) run(cause error) error {
	base := context.WithoutCancel(context.Background())

	// Notify systemd we are stopping. This prevents a restart during clean shutdown.
	_ = sdnotify.Stopping("draining")

	// Phase one: no new work enters. Measured, because "the intake stops at
	// once" is a promise an operator can hold us to (05 section 3.2).
	started := sd.clk.Mark()
	sd.stopIntake()
	if !sd.waitAcked(base, intakeBudget) {
		sd.log.Warn("an intake loop missed its acknowledgement deadline",
			"budget", intakeBudget.String())
	}
	took := sd.clk.Since(started)
	sd.log.Info("intake closed", "took", took.String())

	// Phase two: the executors end their process groups and hand their runs
	// back. Bounded by the drain timeout; a step that ignores everything
	// still cannot hold the daemon past its budget.
	budget := sd.cfg.drainTimeout()
	drainCtx, cancelDrain := context.WithTimeout(base, budget)
	// The census is taken before the executors are told anything: from the
	// cancel onwards a run can leave the pool's book while its process
	// group is still on the machine, which is the whole defect.
	owed := sd.owedRuns()
	sd.stopExec()
	// The drain announces itself. An operator watching a deploy needs to
	// know the daemon has stopped taking work and is now waiting for the
	// steps in flight, and how long it will wait before it gives up.
	sd.log.Info("draining running work", "timeout", budget.String())
	select {
	case <-sd.execDrained:
	case <-drainCtx.Done():
		sd.log.Warn("executors did not finish inside the drain timeout",
			"timeout", budget.String())
	}
	cancelDrain()

	// The executors are done with their goroutines. Whether they are done
	// with their processes is a question only the machine can answer, so
	// the drain asks it, and escalates whatever answers back.
	survived := reapRunGroups(base, sd.clk, sd.log, owed, sd.cfg.killGrace())

	// The remaining loops are selects; they need no budget of their own, but
	// one bounds them anyway so a stuck writer cannot hang the exit.
	restCtx, cancelRest := context.WithTimeout(base, intakeBudget)
	select {
	case <-sd.loopsDrained:
	case <-restCtx.Done():
		sd.log.Warn("background loops missed their exit deadline")
	}
	cancelRest()
	if sd.apiStopped != nil {
		sd.apiStopped(base)
	}

	// Last writes, in this order: the session says goodbye as clean, then
	// the checkpoint moves every committed frame into the database, then the
	// pools close. Nothing may write after the checkpoint, or the wal grows
	// again and the next start replays it.
	if sd.closeSession != nil && len(survived) == 0 {
		if err := sd.closeSession(base); err != nil {
			sd.log.Error("could not close the session row as clean", "error", err)
		}
	}
	if sd.checkpoint != nil {
		if err := sd.checkpoint(base); err != nil {
			sd.log.Error("the final checkpoint failed", "error", err)
		}
	}
	if sd.closeStore != nil {
		if err := sd.closeStore(); err != nil {
			sd.log.Error("closing the store failed", "error", err)
		}
	}

	if len(survived) > 0 {
		// The line an operator has to be able to trust. The stop did not
		// do what a stop promises, so it says which processes it left and
		// reports failure; the session row stays open, which is what the
		// next start reads as work this daemon never finished with.
		sd.log.Error("the stop left a job process running; this daemon did not stop cleanly",
			"processes", describeStepProcesses(survived),
			"count", len(survived))
		return fmt.Errorf("%w: %s", ErrStepProcessSurvived, describeStepProcesses(survived))
	}
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		sd.log.Info("daemon stopped cleanly")
		return nil
	}
	return cause
}

// owedRuns is the census the drain answers for: every run with an execution
// in the pool at the moment phase two begins.
func (sd *shutdown) owedRuns() []string {
	if sd.runsInFlight == nil {
		return nil
	}
	return sd.runsInFlight()
}

// waitAcked waits until every intake loop has returned, or until the budget
// runs out, whichever comes first. The waits are all bounded on channels.
func (sd *shutdown) waitAcked(ctx context.Context, within time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	for _, ack := range sd.intakeAcks {
		select {
		case <-ack:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// watchHardStop answers insistence. The first signal starts the graceful path
// wherever the process wired its context; copies of every signal also arrive
// here, so the second copy is by definition the second request. That one gets
// SIGKILL into every live process group and an immediate exit: an operator who
// asks twice has said the polite path is over.
func watchHardStop(signals <-chan os.Signal, log *slog.Logger, onHardStop func()) {
	<-signals // the graceful path is already running
	second, ok := <-signals
	if !ok {
		return
	}
	log.Warn("second stop signal: killing every process group now",
		"signal", second.String())
	hardKill(syscall.SIGKILL)
	if onHardStop == nil {
		os.Exit(ExitHardStop)
	}
	onHardStop()
}

// hardKill is the seam between the decision and the delivery. Production uses
// the runner's registry sweep; a test records the signal instead of firing it.
var hardKill = runner.KillAllProcessGroups
