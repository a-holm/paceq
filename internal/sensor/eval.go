package sensor

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"sync/atomic"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/runner"
)

// Evaluator runs one sensor command as a subprocess in its own process group
// and reads exactly one JSON object off stdout within the resource limits. It
// never writes to the database: the value it returns is the whole of its
// effect, and the commit transaction that turns it into state is M3-03's.
type Evaluator struct {
	clk clock.Clock
	cfg Config
}

// NewEvaluator wires an evaluator onto a clock. A nil clock means the system
// clock.
func NewEvaluator(cfg Config, clk clock.Clock) *Evaluator {
	if clk == nil {
		clk = clock.System()
	}
	return &Evaluator{clk: clk, cfg: cfg}
}

// Evaluate starts the sensor, feeds it the inbound contract on stdin and as
// environment, reads its one JSON object off stdout, and classifies the whole
// run into a Result. The context is the daemon's shutdown handle as well, so
// a cancelled daemon kills every sensor subprocess; the sensor's own timeout
// is enforced inside on top of it.
func (e *Evaluator) Evaluate(ctx context.Context, spec Spec, in Input) Result {
	before := spec.Cursor
	startedAt := e.clk.Now().UnixMilli()

	fail := func(code reason.Code, data map[string]any) Result {
		return errored(code, data, 0, "", before, "")
	}
	if err := spec.validate(); err != nil {
		return fail(reason.TICKErrorSensorFailed, map[string]any{"err": err.Error()})
	}
	env, err := buildEnv(spec, in)
	if err != nil {
		return fail(reason.TICKErrorConfig, map[string]any{"err": err.Error()})
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return fail(reason.TICKErrorSensorFailed, map[string]any{"err": err.Error()})
	}

	grace := e.cfg.killGrace()
	esc := runner.NewEscalator(grace, e.clk)
	defer esc.Stop()

	out := newCappedStdout(e.cfg.maxStdout())
	terr := newTailStderr(e.cfg.stderrTail())

	cmd := exec.CommandContext(ctx, spec.Argv[0], spec.Argv[1:]...) // #nosec G204 - the configured sensor argv is the contract, see doc.go
	cmd.Dir = spec.Workdir
	cmd.Env = env
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = out
	cmd.Stderr = terr
	cmd.SysProcAttr = runner.SysProcAttr()
	cmd.WaitDelay = grace
	cmd.Cancel = esc.Fire // SIGTERM to the whole group, grace before SIGKILL

	if err := cmd.Start(); err != nil {
		return fail(reason.TICKErrorSensorFailed, map[string]any{"err": err.Error()})
	}
	esc.SetGroup(cmd.Process.Pid)
	release := runner.RegisterProcessGroup(cmd.Process.Pid)
	defer release()

	var timedOut atomic.Bool
	timer := e.clk.NewTimer(spec.Timeout)
	defer timer.Stop()
	stopWatcher := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-timer.C:
			timedOut.Store(true)
			_ = esc.Fire() // fire never fails; the error satisfies exec only
		case <-ctx.Done():
			_ = esc.Fire()
		case <-stopWatcher:
		}
	}()

	_ = cmd.Wait()
	finishedAt := e.clk.Now().UnixMilli()
	close(stopWatcher)
	<-watcherDone

	return e.classify(cmd, out, terr, timedOut.Load(), spec, before, startedAt, finishedAt)
}

// classify turns what the subprocess did into the frozen tick verdicts. The
// order of the reason catalogue's row table is kept exactly here so every row
// is exercised by a test. A timeout owns every other reading: a process that
// had to be killed to stop it is a timeout, not a crash.
func (e *Evaluator) classify(cmd *exec.Cmd, out *cappedStdout, terr *tailStderr,
	timedOut bool, spec Spec, before *string, startedAt, finishedAt int64,
) Result {
	code, sig, signalled := runner.ExitStatus(cmd.ProcessState)
	excerpt := terr.String()

	switch {
	case timedOut:
		r := errored(reason.TICKErrorSensorTimeout,
			map[string]any{"timeout_ms": spec.Timeout.Milliseconds()},
			code, sig, before, excerpt)
		r.TimedOut = true
		return e.done(r, out, startedAt, finishedAt)
	case signalled:
		return e.done(errored(reason.TICKErrorSensorFailed,
			map[string]any{"signal": sig, "exit_code": code},
			code, sig, before, excerpt), out, startedAt, finishedAt)
	case out.overflow:
		return e.done(errored(reason.TICKErrorSensorOutput,
			map[string]any{
				"bytes": out.received, "limit": e.cfg.maxStdout(),
				"output_limit_exceeded": true,
			},
			code, "", before, excerpt), out, startedAt, finishedAt)
	}

	switch code {
	case 75: // EX_TEMPFAIL: transient, counts nothing against a breaker (M3-05)
		return e.done(errored(reason.TICKErrorSensorFailed,
			map[string]any{"exit_code": code, "transient": true},
			code, "", before, excerpt), out, startedAt, finishedAt)
	case 64: // EX_USAGE: configuration is wrong, the sensor asks to be paused
		return e.done(errored(reason.TICKErrorConfig,
			map[string]any{"exit_code": code},
			code, "", before, excerpt), out, startedAt, finishedAt)
	case 0:
	default:
		return e.done(errored(reason.TICKErrorSensorFailed,
			map[string]any{"exit_code": code},
			code, "", before, excerpt), out, startedAt, finishedAt)
	}

	// exit 0: the sensor answered and the output decides the verdict. Silence
	// is a skip with a fixed explanation, not a parse error; anything on the
	// wire after that is the JSON the contract names.
	if len(out.buf) == 0 {
		return e.done(skipped(reason.TICKSkippedSensor, "no output from sensor", nil, before),
			out, startedAt, finishedAt)
	}
	var obj Output
	if err := json.Unmarshal(out.buf, &obj); err != nil {
		return e.done(errored(reason.TICKErrorSensorOutput,
			map[string]any{"err": err.Error()},
			0, "", before, excerpt), out, startedAt, finishedAt)
	}
	if len(obj.Triggers) > 0 {
		// Triggers win even when a skip_reason is also set; the reason text is
		// carried as a note for the commit layer to keep (03 section 4.3).
		res := Result{
			Outcome:       Triggered,
			CursorBefore:  before,
			CursorAfter:   obj.Cursor,
			Triggers:      obj.Triggers,
			ExitCode:      code,
			Signal:        sig,
			StderrExcerpt: excerpt,
			ReasonText:    deref(obj.SkipReason),
		}
		return e.done(res, out, startedAt, finishedAt)
	}
	// No triggers. A named skip reason is the sensor's own words; silence gets
	// a fixed explanation, both landing on the same skipped verdict.
	if obj.SkipReason != nil {
		return e.done(skipped(reason.TICKSkippedSensor, *obj.SkipReason, obj.Cursor, before),
			out, startedAt, finishedAt)
	}
	return e.done(skipped(reason.TICKSkippedSensor, "no output from sensor", obj.Cursor, before),
		out, startedAt, finishedAt)
}

// done stamps the duration, the stdout byte count and the overflow flag onto
// a verdict.
func (e *Evaluator) done(r Result, out *cappedStdout, startedAt, finishedAt int64) Result {
	r.DurationMS = finishedAt - startedAt
	r.StdoutBytes = out.received
	r.OutputOverflow = out.overflow
	return r
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
