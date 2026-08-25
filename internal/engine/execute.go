package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/retry"
	"github.com/a-holm/paceq/internal/runner"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// ExecuteRun takes one queued run and drives it to a terminal state: claim,
// then steps one at a time in index order, each requiring its every upstream
// step to have succeeded, then the run's own verdict.
//
// The claim is where ownership becomes real: one statement hands back the
// fencing token, and every write this attempt makes afterwards carries that
// token. A write refused for a lost lease ends the attempt with its result
// discarded and an event saying so; worst case is duplicate work, never
// duplicate state.
//
// The shape of the loop is where the other hard rules live. Every state
// change is a store method, and each of those commits the state change with
// exactly one event row. The process runs strictly between those
// transactions: the transaction that starts the step is closed before the
// command is spawned, and the transaction that records its verdict is opened
// after it is reaped.
func (e *Engine) ExecuteRun(ctx context.Context, runID string) (string, error) {
	detail, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return "", fmt.Errorf("execute run %s: %w", runID, err)
	}
	runID = detail.Run.ID

	job, err := e.frozenSpec(ctx, detail.Run.JobVersionID)
	if err != nil {
		return "", err
	}
	stepsByName := make(map[string]spec.Step, len(job.Steps))
	for _, st := range job.Steps {
		stepsByName[st.Name] = st
	}

	var deadline time.Time
	if job.Timeout > 0 {
		deadline = e.Clock.Now().Add(job.Timeout)
	}

	state, epoch, err := e.Store.ClaimRun(ctx, runID, store.LeaseInput{Owner: e.Owner, TTL: e.ttl()})
	if err != nil {
		return "", fmt.Errorf("execute run %s: %w", runID, err)
	}
	if state == string(model.RunCancelled) {
		// The cancellation arrived before the run ever started; there is
		// no process group to kill and nothing else to do.
		return state, nil
	}

	d := drive{
		runID:       runID,
		ref:         store.LeaseRef{Owner: e.Owner, Epoch: epoch},
		job:         job,
		stepsByName: stepsByName,
		run:         detail.Run,
		deadline:    deadline,
	}
	return d.execute(ctx, e)
}

// drive is one claimed attempt at one run: the lease it holds, the frozen
// spec it works from, and nothing else worth keeping.
type drive struct {
	runID       string
	ref         store.LeaseRef
	job         *spec.Job
	stepsByName map[string]spec.Step
	run         store.Run
	deadline    time.Time
}

// execute runs the step loop under the lease. The keeper handle h carries the
// two signals the renewal goroutine can raise while work runs: a cancellation
// request seen in a renewal answer, and the lease itself being lost.
func (d *drive) execute(ctx context.Context, e *Engine) (string, error) {
	h, release := e.hold(d.runID, d.ref.Epoch)
	defer release()

	var discarded bool
	lostLease := func(why string) (string, error) {
		if !discarded {
			discarded = true
			if err := e.discardResult(context.WithoutCancel(ctx), d.runID, d.ref, why); err != nil {
				return "", err
			}
		}
		return "", fmt.Errorf("execute run %s: %w (%s)", d.runID, store.ErrLeaseLost, why)
	}

	timedOut := false
	for {
		select {
		case <-h.lost:
			// The renewal heard the reaper before our process did. There is
			// no verdict to record: the successor owns the row.
			return lostLease("lease lost")
		default:
		}

		// Between steps is where a cancellation is cheapest to observe:
		// nothing is running, so nothing has to be killed. The poll reads the
		// row directly, which also covers executors without a renewal loop.
		requested, by, err := e.Store.CancelRequested(ctx, d.runID)
		if err != nil && !errors.Is(err, store.ErrRunNotFound) {
			return "", fmt.Errorf("execute run %s: %w", d.runID, err)
		}
		if requested {
			return d.observeCancel(ctx, e, by)
		}

		name, ok, err := e.Store.NextRunnableStep(ctx, d.runID)
		if err != nil {
			return "", fmt.Errorf("execute run %s: %w", d.runID, err)
		}
		if !ok {
			// Nothing is runnable right now. That is either the end or
			// a parked retry; the store can tell them apart. Sleeping
			// on its answer is the whole waiting strategy in process:
			// the claim gate stays the scheduler, and no retry state
			// lives here.
			wait, waiting, err := e.Store.NextRetryWait(ctx, d.runID)
			if err != nil {
				return "", fmt.Errorf("execute run %s: %w", d.runID, err)
			}
			if !waiting {
				break
			}
			timer := e.Clock.NewTimer(wait)
			select {
			case <-timer.C:
				continue
			case <-h.lost:
				timer.Stop()
				return lostLease("lease lost")
			case <-ctx.Done():
				timer.Stop()
				return "", fmt.Errorf("execute run %s: %w", d.runID, ctx.Err())
			}
		}

		stepHitRunDeadline, err := e.runStep(ctx, d, name, h)
		if err != nil {
			if errors.Is(err, store.ErrLeaseLost) {
				return lostLease("lease lost")
			}
			return "", err
		}
		if stepHitRunDeadline {
			// The run's own budget is spent. Whatever else is pending
			// cannot fit inside it either, so the loop ends and the
			// verdict below is TIMED_OUT.
			timedOut = true
			break
		}
	}

	// Whatever is still pending never became runnable: some upstream ended
	// failed, cancelled or skipped, so it never will be. Each leaves
	// through the machine's skip transition, with its own event, in index
	// order.
	if err := e.skipPending(ctx, d.runID, d.ref); err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			return lostLease("lease lost")
		}
		return "", err
	}

	finish, err := e.finishReason(ctx, d.runID, timedOut)
	if err != nil {
		return "", err
	}
	state, err := e.Store.FinishRun(ctx, d.runID, d.ref, finish)
	if errors.Is(err, store.ErrLeaseLost) {
		return lostLease("verdict refused")
	}
	if err != nil {
		return "", err
	}
	return state, nil
}

// observeCancel closes a run whose cancellation somebody requested: the
// pending steps leave through their own events and the run follows, both
// inside the store method's one transaction.
func (d *drive) observeCancel(ctx context.Context, e *Engine, by string) (string, error) {
	if err := e.Store.ObserveRunCancel(ctx, d.runID, d.ref, by, reason.RUNCancelledManual); err != nil {
		return "", fmt.Errorf("cancel run %s: %w", d.runID, err)
	}
	return string(model.RunCancelled), nil
}

// skipPending closes out every step still waiting, in index order.
func (e *Engine) skipPending(ctx context.Context, runID string, ref store.LeaseRef) error {
	pending, err := e.Store.PendingSteps(ctx, runID)
	if err != nil {
		return fmt.Errorf("skip the pending steps of run %s: %w", runID, err)
	}
	now := e.Clock.Now()
	for _, p := range pending {
		err := e.Store.RecordStepOutcome(ctx, runID, p.Name, store.StepOutcome{
			Event:      string(model.EvUpstreamFailed),
			ReasonCode: reason.STEPSkippedUpstreamFailed,
			FinishedAt: now,
		}, ref)
		if err != nil {
			return fmt.Errorf("skip step %s of run %s: %w", p.Name, runID, err)
		}
	}
	return nil
}

// frozenSpec reads the version this run points at out of the database and
// decodes it. It is called once per execution; everything downstream works
// from what it returned.
func (e *Engine) frozenSpec(ctx context.Context, versionID string) (*spec.Job, error) {
	version, err := e.Store.JobVersionByID(ctx, versionID)
	if err != nil {
		return nil, fmt.Errorf("read the frozen spec: %w", err)
	}
	job, err := spec.FromIR([]byte(version.SpecJSON))
	if err != nil {
		return nil, fmt.Errorf("decode the frozen spec of version %s: %w", versionID, err)
	}
	return job, nil
}

// runStep carries one step from pending to terminal: start it in one
// transaction, run the process outside any transaction, record the verdict in
// another. The bool reports whether the step ended because the run's own
// deadline ran out, which is the engine's signal to stop scheduling work.
//
// The lease rides along twice: as the fence on both transactions, and as the
// two signals the watcher selects on while the process runs. A loss kills the
// process group through the same context cancel a cancellation uses; the
// difference shows afterwards, when there is nothing left worth writing.
func (e *Engine) runStep(ctx context.Context, d *drive, name string, h *heldRun) (bool, error) {
	run := d.run
	fail := func(err error) (bool, error) {
		if errors.Is(err, store.ErrLeaseLost) {
			return false, err
		}
		return false, fmt.Errorf("run step %s of run %s: %w", name, run.ID, err)
	}

	current, err := e.Store.GetRun(ctx, run.ID)
	if err != nil {
		return fail(err)
	}
	var before store.Step
	for _, s := range current.Steps {
		if s.Name == name {
			before = s
			break
		}
	}

	if err := e.Store.StartStep(ctx, run.ID, name, d.ref); err != nil {
		return fail(fmt.Errorf("%w", err))
	}
	attempt := before.Attempt + 1

	sink, err := logsink.Open(e.LogRoot, run.ID, name, attempt,
		logsink.Options{Clock: e.Clock})
	if err != nil {
		return fail(fmt.Errorf("open the log: %w", err))
	}

	// The output file of the attempt (#13): created empty, 0600, before
	// exec, under the run's own directory. Creation failures refuse the
	// step exactly like log failures do: the process never ran.
	outputPath, err := prepareStepOutput(e.StateDir, run.ID, name, attempt)
	if err != nil {
		return fail(err)
	}

	// The merged upstream references of the attempt (#13): read from the
	// frozen graph before exec, outside any transaction. A spill to a
	// file happens here too, while nothing is running.
	inputsJSON, inputsFile, err := e.prepareStepInputs(ctx, e.StateDir, run.ID, name, attempt)
	if err != nil {
		return fail(err)
	}

	// The crash window between the committed start and the spawn. A
	// process killed here leaves a running step whose command never ran,
	// which is the safest window to recover: retrying cannot duplicate an
	// effect that never happened.
	faults.Point("M1:step:before_exec")

	timeout := d.stepsByName[name].Timeout
	if timeout <= 0 {
		timeout = runner.DefaultTimeout
	}
	runDeadlineHit := false
	if !d.deadline.IsZero() {
		if remaining := d.deadline.Sub(e.Clock.Now()); remaining < timeout {
			timeout = remaining
			runDeadlineHit = true
		}
	}

	// The poll: while the process runs, its cancellation request is
	// re-read on the clock, and the renewal goroutine's signals are
	// selected beside it. Seeing a request cancels the step's context,
	// and the runner answers a dead context by killing the whole
	// process group. Seeing a lost lease cancels too: we are not the
	// owner any more, and our output stopped mattering the moment the
	// token moved. No sleep touches the wall clock directly.
	watchCtx, cancelWatch := context.WithCancel(ctx)
	cancelledBy := make(chan string, 1)
	abandoned := make(chan struct{})
	go func() {
		defer close(cancelledBy)
		ticker := e.Clock.NewTicker(e.pollInterval())
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				requested, by, err := e.Store.CancelRequested(watchCtx, run.ID)
				if err != nil || !requested {
					continue
				}
				// The request is durable in the database; killing is
				// the answer to it. The context cancel is what takes
				// the process group down.
				cancelWatch()
				select {
				case cancelledBy <- by:
				default:
				}
				return
			case <-h.cancel:
				// The renewal answer carried the request first. Read who
				// asked once, then kill exactly as the poll would have.
				_, by, _ := e.Store.CancelRequested(watchCtx, run.ID)
				cancelWatch()
				select {
				case cancelledBy <- by:
				default:
				}
				return
			case <-h.lost:
				close(abandoned)
				cancelWatch()
				return
			}
		}
	}()

	result, runErr := runner.Run(watchCtx, runner.Spec{
		Argv:       d.stepsByName[name].Run,
		Shell:      d.stepsByName[name].Shell,
		Workdir:    d.stepsByName[name].Workdir,
		Env:        d.job.Env,
		InheritEnv: d.job.InheritEnv,
		Timeout:    timeout,
		Clock:      e.Clock,
		OutputPath: outputPath,
		InputsJSON: inputsJSON,
		InputsFile: inputsFile,
		Stdout:     crashOnFirstWrite(sink.Writer(logsink.StreamStdout), "M1:step:under_exec"),
		Stderr:     sink.Writer(logsink.StreamStderr),
		// The process baseline is the orphan sweep's evidence (issue #62):
		// pid plus /proc start ticks on file the moment the spawn succeeds.
		// It must survive this call's own cancellation, so it writes under a
		// context that outlives watchCtx, and it never fails the step: an
		// unrecorded pid only makes the sweep refuse to touch that process.
		OnStart: func(pid int) {
			ticks, ok := store.ReadProcessStartTicks(pid)
			if !ok {
				slog.Warn("could not read the start ticks of a spawned step; the orphan sweep will not be able to verify it",
					"run", run.ID, "step", name, "pid", pid)
				return
			}
			if err := e.Store.RecordAttemptProcess(context.WithoutCancel(watchCtx), run.ID, name, d.ref, pid, ticks); err != nil {
				slog.Warn("could not record the process baseline of a spawned step",
					"run", run.ID, "step", name, "pid", pid, "error", err)
			}
		},
		Ctx: runner.RunContext{
			RunID:   run.ID,
			Job:     run.JobName,
			Step:    name,
			Attempt: attempt,
			RunKey:  run.RunKey,
			Params:  paramsMap(run.ParamsJSON),
		},
	})
	cancelWatch()
	select {
	case <-abandoned:
		// The lease died while the process ran. The process group is
		// dead; the verdict belongs in the bin, not in the database.
		return false, fmt.Errorf("run step %s of run %s: %w", name, run.ID, store.ErrLeaseLost)
	default:
	}
	by := <-cancelledBy

	tail, bytes, truncated, err := sink.Finish()
	if err != nil {
		return fail(fmt.Errorf("close the log: %w", err))
	}
	// The crash window between a finalized log file and the metadata write
	// that records it. The bytes are on disk, the database knows nothing
	// yet; recovery has to leave both sides telling the same story.
	faults.Point("M1:step:after_log_finish")
	if runErr != nil {
		return fail(fmt.Errorf("the process could not start: %w", runErr))
	}

	outcome := verdictFor(result, tail, bytes, truncated)
	outcome.LogMeta.RelPath = sink.RelPath()
	switch {
	case outcome.Event == string(model.EvCancelObserved):
		// The kill happened outside any transaction, as it must. What
		// remains is bookkeeping, and the bookkeeping names who asked.
		outcome.DetailJSON = mergeDetail(outcome.DetailJSON,
			map[string]any{"requested_by": by})
	case outcome.Event == string(model.EvStepFailed):
		// The order matters below: a kill by the run's own budget is
		// attributed to that budget before anything else names the
		// attempt, because no further attempt can fit anyway.
		if result.Outcome == runner.TimedOut && runDeadlineHit {
			outcome.DetailJSON = mergeDetail(outcome.DetailJSON, map[string]any{
				"scope":      "run",
				"timeout_ms": timeout.Milliseconds(),
			})
			break
		}
		// A failed attempt under a retry budget either buys another
		// one or spends the last one. Which of the two is arithmetic
		// on the row, not a decision of its own: attempts left
		// schedules the next try at now plus the policy's delay through
		// a RetryPlan, none spent closes the step with the exhausted
		// code. The machine still owns the transition; this only names
		// it. Without a budget there is nothing to schedule or spend,
		// so the runner's own verdict stands untouched.
		if before.MaxAttempts <= 1 {
			break
		}
		if attempt < before.MaxAttempts {
			policy := retryPolicyOf(d.stepsByName[name].Retry)
			delayed := retry.Delay(policy, attempt, e.rnd())
			finished := outcome.FinishedAt
			if finished.IsZero() {
				finished = e.Clock.Now()
			}
			next := finished.Add(delayed)
			facts := map[string]any{
				"attempt":         attempt,
				"backoff_ms":      delayed.Milliseconds(),
				"next_attempt_at": next.UnixMilli(),
			}
			if result.Outcome == runner.Failed {
				// Exit 75 is EX_TEMPFAIL: the program itself claims
				// the failure was transient (00-SYNTESE 4.5).
				facts["transient"] = result.ExitCode == 75
			}
			outcome.Retry = &store.RetryPlan{
				NextAttemptAt: next,
				ReasonCode:    reason.STEPRetryScheduled,
				DetailJSON:    detailJSON(facts),
			}
		} else {
			outcome.ReasonCode = reason.STEPRetriesExhausted
			outcome.DetailJSON = mergeDetail(outcome.DetailJSON, map[string]any{
				"attempt":      attempt,
				"max_attempts": before.MaxAttempts,
			})
		}
	}

	// Publication (#13): read what the step wrote, strictly after exit
	// and outside any transaction. Only a succeeded verdict publishes;
	// a failed or cancelled step's file is nobody's input.
	if result.Outcome == runner.Succeeded && outputPath != "" {
		parsed, err := runner.ReadStepOutput(outputPath)
		if err != nil {
			return fail(fmt.Errorf("read the step output: %w", err))
		}
		arts, warnings, err := e.collectPublications(ctx, run.ID, name, stepIndexOf(current.Steps, name), parsed)
		if err != nil {
			return fail(err)
		}
		outcome.Artifacts = arts
		outcome.DetailJSON = publicationDetail(outcome.DetailJSON, parsed.Params, warnings)
	}

	if err := e.Store.RecordStepOutcome(ctx, run.ID, name, outcome, d.ref); err != nil {
		return fail(fmt.Errorf("record the verdict: %w", err))
	}
	return runDeadlineHit && result.Outcome == runner.TimedOut, nil
}

// stepIndexOf finds a step's position in the spec order.
func stepIndexOf(steps []store.Step, name string) int {
	for i := range steps {
		if steps[i].Name == name {
			return steps[i].Index
		}
	}
	return 0
}

// finishReason decides why the run ended. A step failure fails the run and
// names the step; a spent run budget ends it as TIMED_OUT; a cancelled step
// means the run was cancelled; otherwise the run succeeded, a skip counting
// as success.
func (e *Engine) finishReason(ctx context.Context, runID string, timedOut bool) (store.FinishReason, error) {
	detail, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return store.FinishReason{}, fmt.Errorf("finish run %s: %w", runID, err)
	}
	if timedOut {
		return store.FinishReason{Code: reason.RUNTimedOut}, nil
	}
	for _, s := range detail.Steps {
		switch model.StepState(s.State) {
		case model.StepFailed:
			return store.FinishReason{
				Code: reason.RUNFailedStep,
				Data: detailJSON(map[string]any{"step": s.Name}),
			}, nil
		case model.StepCancelled:
			return store.FinishReason{Code: reason.RUNCancelledManual}, nil
		}
	}
	return store.FinishReason{Code: reason.RUNSucceeded}, nil
}

// verdictFor translates what the runner observed into the step machine's
// vocabulary. The mapping is total: every Outcome the runner can report has a
// row here, so an unclassified result cannot reach the store. The log's
// relative path is filled in by the caller, which is the one holding the sink.
func verdictFor(res runner.Result, tail string, bytes int64, truncated bool) store.StepOutcome {
	out := store.StepOutcome{
		LogMeta:    store.LogMeta{Bytes: bytes, Truncated: truncated, ErrorTail: tail},
		FinishedAt: msToTime(res.FinishedAt),
	}
	code := res.ExitCode
	switch res.Outcome {
	case runner.Succeeded:
		out.Event = string(model.EvStepSucceeded)
		out.ReasonCode = reason.STEPSucceeded
		out.ExitCode = &code
	case runner.Failed:
		out.Event = string(model.EvStepFailed)
		out.ReasonCode = reason.STEPFailedNonzeroExit
		out.ExitCode = &code
		out.DetailJSON = detailJSON(res.ReasonData)
	case runner.TimedOut:
		out.Event = string(model.EvStepFailed)
		out.ReasonCode = reason.STEPFailedTimeout
		out.Signal = res.Signal
		out.DetailJSON = detailJSON(res.ReasonData)
	case runner.Signalled:
		if res.ReasonData["cancelled"] == true {
			// The signal came from us answering a cancellation.
			out.Event = string(model.EvCancelObserved)
			out.ReasonCode = reason.STEPCancelled
		} else {
			out.Event = string(model.EvStepFailed)
			out.ReasonCode = reason.STEPFailedSignal
			out.Signal = res.Signal
		}
		out.DetailJSON = detailJSON(res.ReasonData)
	default:
		// SpawnFailed: the process never existed, so nothing about the
		// job failed; the launch did.
		out.Event = string(model.EvStepFailed)
		out.ReasonCode = reason.STEPFailedSpawn
		out.DetailJSON = detailJSON(res.ReasonData)
	}
	return out
}

// paramsMap decodes a run's parameter object for the runner's environment
// contract. Empty or malformed JSON is no parameters; the store only ever
// writes what materialisation canonicalised, so this is a formality.
func paramsMap(paramsJSON string) map[string]any {
	if paramsJSON == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(paramsJSON), &m); err != nil {
		return nil
	}
	return m
}

// detailJSON renders a detail object canonically. encoding/json sorts map
// keys, so the same facts always read the same way back.
func detailJSON(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// mergeDetail decodes a canonical detail object, adds facts to it and
// renders it back. An empty or malformed base counts as no base: nothing in
// this file writes malformed JSON, but a verdict with no detail at all is
// ordinary.
func mergeDetail(baseJSON string, extra map[string]any) string {
	m := map[string]any{}
	if err := json.Unmarshal([]byte(baseJSON), &m); err != nil {
		m = map[string]any{}
	}
	for k, v := range extra {
		m[k] = v
	}
	return detailJSON(m)
}

func msToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// crashOnFirstWrite arms the under_exec crash point on a step's stdout. The
// point fires while the command is alive and producing output, which is what
// makes the kill land inside execution rather than beside it: the first line
// a job prints proves a process exists to print it. In a build without the
// pulseq_faults tag Point does nothing and the wrapper is pass through.
type firstWriteWriter struct {
	next io.Writer
	name string
}

func crashOnFirstWrite(next io.Writer, name string) io.Writer {
	return &firstWriteWriter{next: next, name: name}
}

func (w *firstWriteWriter) Write(p []byte) (int, error) {
	faults.Point(w.name)
	return w.next.Write(p)
}
