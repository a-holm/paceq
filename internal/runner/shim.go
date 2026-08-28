package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/procfs"
	"github.com/a-holm/paceq/internal/spool"
)

// The exec shim (issue #39). A step's process tree is two processes deep:
// the daemon spawns this binary as `paceq exec`, and the shim spawns the
// user's command. The shim owns the three facts a daemon can lose in a
// crash: it sits beside the attempt's process group, it watches a pipe whose
// write end dies with the daemon, and it writes the attempt's result
// durably to the spool before it exits.
//
// The result file is the point. A daemon that dies in the microseconds
// between its child's exit and the verdict's commit used to leave an attempt
// with no known outcome, and recovery had to assume the worst and re-run a
// job that may have spent an hour building its effect. With the spool, the
// child itself — not the process that can crash — holds the knowledge, and
// recovery commits what really happened instead of guessing (crash window
// W8, docs/plans 02 section 6).
//
// The command is hidden and explicitly not a public contract: it is an
// implementation detail between two versions of the same binary. Its
// argument format may change at any release; what must survive is the spool
// file's format, which carries its own version field.

// ShimConfig is one shim execution. The daemon builds it from the same Spec
// it would have used to run the command itself; the CLI command parses it
// from flags. Timeout is mandatory: a shim that can be launched without a
// deadline is a hang one daemon death away from being forever.
type ShimConfig struct {
	RunID      string
	Step       string
	Attempt    int
	ClaimEpoch int64
	SpoolDir   string
	Workdir    string
	Argv       []string
	Env        []string // nil inherits the shim's own environment (manual runs)

	Timeout   time.Duration
	KillGrace time.Duration // zero means DefaultKillGrace

	// WatchFD is the descriptor the daemon passes the read end of its
	// watchdog pipe on (always 3 via ExtraFiles). Negative means no
	// watchdog: a human running the command by hand gets no phantom EOF
	// from an unopened descriptor.
	WatchFD int
	// BaseFD is the descriptor the shim writes the child's process baseline
	// on, one JSON line, the instant the spawn succeeds. Negative disables
	// it, which only costs the orphan sweep its evidence for the attempt.
	BaseFD int

	Stdout io.Writer // nil is /dev/null
	Stderr io.Writer // nil is /dev/null

	// Clock drives the timeout and the kill grace. Nil means the system
	// clock; tests inject so timing decisions stay deterministic.
	Clock clock.Clock
}

// baseline is what crosses the baseline pipe: the child's pid and the
// kernel's start time for it, read at spawn. It is the orphan sweep's
// evidence and must be on file while the attempt runs, not after.
type baseline struct {
	PID        int   `json:"pid"`
	StartTicks int64 `json:"start_ticks"`
}

// ShimMain is the shim's whole life: run the command, watch the pipe, write
// the spool, and come back with the exit code the caller should exit with.
// It never returns an error — every outcome, including a refused spawn, is a
// spool entry — because an outcome that only lives in an error return dies
// with the process that would have logged it.
func ShimMain(ctx context.Context, cfg ShimConfig) (exitCode int) {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.System()
	}
	grace := cfg.KillGrace
	if grace <= 0 {
		grace = DefaultKillGrace
	}

	res := spool.Result{
		V:          spool.Version,
		RunID:      cfg.RunID,
		Step:       cfg.Step,
		Attempt:    cfg.Attempt,
		ClaimEpoch: cfg.ClaimEpoch,
		BootID:     procfs.BootID(),
		StartedAt:  clk.Now().UnixMilli(),
	}
	fail := func(code int, killedBy, outcome string, cause error) int {
		res.EndedAt = clk.Now().UnixMilli()
		res.ExitCode = code
		res.KilledBy = killedBy
		res.Outcome = outcome
		if cause != nil && res.PID == 0 {
			res.ReasonData = mergeReasonData(nil, map[string]any{"error": cause.Error()})
		}
		writeShimSpool(cfg, res, cause)
		return code
	}

	if len(cfg.Argv) == 0 {
		return fail(127, "spawn_failed", spool.OutcomeSpawnFailed, errors.New("argv is empty"))
	}
	if !strings.Contains(cfg.Argv[0], "/") {
		return fail(127, "spawn_failed", spool.OutcomeSpawnFailed,
			fmt.Errorf("argv[0] %q has no path separator", cfg.Argv[0]))
	}
	if cfg.Timeout <= 0 {
		return fail(127, "spawn_failed", spool.OutcomeSpawnFailed,
			errors.New("a shim must be launched with a timeout"))
	}

	// The daemon filtered the environment deny-by-default; the child gets
	// exactly what is on this command. argv[0] carries a separator (checked
	// above), so no PATH lookup happens here either.
	cmd := exec.Command(cfg.Argv[0], cfg.Argv[1:]...) // #nosec G204 - the configured job argv is the contract, validated above
	cmd.Dir = cfg.Workdir
	cmd.Env = cfg.Env
	cmd.Stdin = nil
	cmd.Stdout = cfg.Stdout
	cmd.Stderr = cfg.Stderr
	cmd.SysProcAttr = sysProcAttr() // the child leads its own group

	releaseCore := zeroCoreLimit()

	if err := cmd.Start(); err != nil {
		releaseCore()
		return fail(127, "spawn_failed", spool.OutcomeSpawnFailed, err)
	}
	res.PID = cmd.Process.Pid
	if ticks, ok := procfs.ProcStartTicks(res.PID); ok {
		res.PIDStartTicks = ticks
	}
	writeBaseline(cfg.BaseFD, res.PID, res.PIDStartTicks)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	// The watchdog. The daemon holds the write end for as long as it lives;
	// the kernel closes it on any death, including SIGKILL, and the read
	// here sees EOF — the one signal that needs no cooperation from the
	// dying process (issue #39, design choice 2).
	eof := make(chan struct{})
	if cfg.WatchFD >= 0 {
		watch := os.NewFile(uintptr(cfg.WatchFD), "watchdog")
		go func() {
			defer close(eof)
			_, _ = io.Copy(io.Discard, watch)
			_ = watch.Close()
		}()
	}

	// Forwarded signals. A TERM aimed at the shim's group does not reach
	// the child (the child leads a different group), so a trapped signal is
	// forwarded to the child's group first and recorded second. The
	// forwarder stops with this function: the channel closes, the
	// goroutine returns, and an in-process caller finds nothing left
	// behind.
	fwd := newSignalForwarder(res.PID)
	defer fwd.stop()

	timer := clk.NewTimer(cfg.Timeout)
	defer timer.Stop()

	timedOut := false
	killedBy := ""
waitLoop:
	for {
		select {
		case waitErr := <-done:
			_ = waitErr // classification reads the process state, not the wrapper
			if name := fwd.first(); name != "" {
				killedBy = "signal:" + name
			}
			break waitLoop
		case <-eof:
			killedBy = "daemon_gone"
			_ = VerifiedGroupKill(res.PID, res.PIDStartTicks, grace, clk)
			<-done
			break waitLoop
		case <-timer.C:
			timedOut = true
			killedBy = "timeout"
			_ = VerifiedGroupKill(res.PID, res.PIDStartTicks, grace, clk)
			<-done
			break waitLoop
		case name := <-fwd.received:
			// The child has the signal already. Give it the grace to
			// react, then escalate exactly once, so a child that
			// ignores the signal cannot pin the shim until the
			// daemon's own SIGKILL lands with the result unwritten.
			// Half the grace here, half in the kill: the whole
			// detour fits inside the escalation the caller may be
			// running against this process.
			timer.Stop()
			forwardGrace := grace / 2
			forwardTimer := clk.NewTimer(forwardGrace)
			select {
			case waitErr := <-done:
				forwardTimer.Stop()
				_ = waitErr
				killedBy = "signal:" + name
			case <-forwardTimer.C:
				_ = VerifiedGroupKill(res.PID, res.PIDStartTicks, forwardGrace, clk)
				<-done
				killedBy = "signal:" + name
			}
			break waitLoop
		}
	}

	releaseCore()
	classified := classify(cmd, timedOut, nil, cfg.Timeout)
	res.EndedAt = clk.Now().UnixMilli()
	res.Outcome = outcomeName(classified.Outcome)
	res.ExitCode = classified.ExitCode
	res.Signal = classified.Signal
	if killedBy != "" {
		res.KilledBy = killedBy
		res.ReasonData = mergeReasonData(classified.ReasonData, map[string]any{"killed_by": killedBy})
	}
	writeShimSpool(cfg, res, nil)

	switch {
	case classified.Outcome == TimedOut:
		// The escalation opened with SIGTERM; the shell convention for
		// "died to TERM" is what a caller reading only the exit code
		// expects.
		return 128 + int(syscall.SIGTERM)
	case classified.Outcome == SpawnFailed:
		return 127
	case classified.Signal != "":
		return 128 + signalNumber(classified.Signal)
	}
	return classified.ExitCode
}

// signalForwarder owns the shim's signal handling for one attempt: trap the
// stop signals, forward each to the child's process group at once, and
// remember the first one so the spool can name what stopped the job.
type signalForwarder struct {
	received chan string
	firstOne string
	mu       sync.Mutex

	sigs    chan os.Signal
	stopped chan struct{}
	stopPwd sync.Once
}

func newSignalForwarder(childPID int) *signalForwarder {
	f := &signalForwarder{
		received: make(chan string, 4),
		sigs:     make(chan os.Signal, 4),
		stopped:  make(chan struct{}),
	}
	signal.Notify(f.sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for {
			select {
			case sig := <-f.sigs:
				name := signalNameOf(sig)
				_ = killProcessGroup(-childPID, signalOf(sig))
				f.mu.Lock()
				if f.firstOne == "" {
					f.firstOne = name
				}
				f.mu.Unlock()
				select {
				case f.received <- name:
				default:
				}
			case <-f.stopped:
				return
			}
		}
	}()
	return f
}

// first names the first forwarded signal, empty for none.
func (f *signalForwarder) first() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.firstOne
}

func (f *signalForwarder) stop() {
	f.stopPwd.Do(func() {
		close(f.stopped)
		signal.Stop(f.sigs)
	})
}

func signalNameOf(sig os.Signal) string {
	if s, ok := sig.(syscall.Signal); ok {
		return sigName(s)
	}
	return sig.String()
}

func signalOf(sig os.Signal) syscall.Signal {
	if s, ok := sig.(syscall.Signal); ok {
		return s
	}
	return syscall.SIGTERM
}

// writeShimSpool writes the result file, or says so as loudly as stderr
// allows. A missing result file is not an error the shim can retry: the
// daemon falls back to what it can see, and recovery treats the attempt as
// unknown — the honest outcome when the whole point of the file was to make
// the outcome known.
func writeShimSpool(cfg ShimConfig, res spool.Result, cause error) {
	if err := spool.WriteResult(cfg.SpoolDir, res); err != nil {
		fmt.Fprintf(os.Stderr, "paceq: writing the result spool failed: %v", err)
		if cause != nil {
			fmt.Fprintf(os.Stderr, " (spawn failed: %v)", cause)
		}
		fmt.Fprintln(os.Stderr)
	}
}

// writeBaseline hands the child's process identity to the daemon over the
// baseline pipe. Every failure is ignored on purpose: a baseline that never
// arrived only means the orphan sweep has no opinion about this attempt, the
// same degradation a failed OnStart has today.
func writeBaseline(fd, pid int, ticks int64) {
	if fd < 0 {
		return
	}
	f := os.NewFile(uintptr(fd), "baseline")
	defer f.Close()
	b, err := json.Marshal(baseline{PID: pid, StartTicks: ticks})
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
}

// outcomeName maps the classifier's verdict onto the spool format's words.
func outcomeName(o Outcome) string {
	switch o {
	case Succeeded:
		return spool.OutcomeSucceeded
	case Failed:
		return spool.OutcomeFailed
	case TimedOut:
		return spool.OutcomeTimedOut
	case SpawnFailed:
		return spool.OutcomeSpawnFailed
	case Signalled:
		return spool.OutcomeSignalled
	default:
		return spool.OutcomeSignalled
	}
}

func mergeReasonData(base map[string]any, extra map[string]any) json.RawMessage {
	out := make(map[string]any, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	b, err := json.Marshal(out)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}

// signalNumber reads a canonical signal name back into its number for the
// 128+n shell convention. Unknown names return 0 and the caller keeps the
// child's own exit code instead.
func signalNumber(name string) int {
	for sig, canonical := range signalNames {
		if canonical == name {
			return int(sig)
		}
	}
	return 0
}

// ShimTarget is what the daemon needs to launch the shim: which binary to
// exec (its own), where the spool directory is, and the fencing token the
// result must carry.
type ShimTarget struct {
	Executable string
	SpoolDir   string
	ClaimEpoch int64
}

// childBaseline is the received side of the baseline pipe, plus the cleanup
// duty the daemon owes a child whose shim died before finishing the story.
type childBaseline struct {
	mu    sync.Mutex
	pid   int
	ticks int64
}

func (c *childBaseline) set(pid int, ticks int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pid, c.ticks = pid, ticks
}

func (c *childBaseline) get() (int, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pid, c.ticks
}

// SpawnViaShim is the daemon side of the process chain: instead of running
// the command itself, it runs its own binary as the shim and lets the shim
// run the command. The watchdog pipe's write end stays in this process, so
// its death — however sudden — is the shim's kill order, and the spool file
// the shim leaves behind is this process's answer to a crash between the
// child's exit and the verdict's commit.
//
// The returned Result is read from the spool whenever the shim managed to
// write one, so the verdict always rests on the same facts recovery would
// see. The file is deliberately NOT removed here: the caller removes it only
// after the verdict's transaction has committed, which is exactly the window
// the spool exists to protect.
func SpawnViaShim(ctx context.Context, s Spec, t ShimTarget) (Result, error) {
	if err := s.validate(); err != nil {
		return Result{}, err
	}
	if t.Executable == "" || t.SpoolDir == "" {
		return Result{}, errors.New("spawn via shim: no executable or spool directory was named")
	}
	clk := s.Clock
	if clk == nil {
		clk = clock.System()
	}
	grace := s.KillGrace
	if grace <= 0 {
		grace = DefaultKillGrace
	}

	workdir, err := resolveWorkdir(s.Workdir)
	if err != nil {
		return spawnFailure(err, "workdir")
	}
	env, err := buildEnv(s, workdir)
	if err != nil {
		return spawnFailure(err, "environment")
	}
	closeOutput, err := createOutput(workdir, s.OutputPath)
	if err != nil {
		return spawnFailure(err, "output")
	}
	defer closeOutput()

	argv := s.Argv
	if s.Shell {
		argv = append([]string{"/bin/sh", "-c"}, strings.Join(s.Argv, " "))
	}

	// Watchdog: we hold the write end; the shim reads fd 3. Baseline: the
	// shim writes fd 4; we read. Both pipes exist only for this attempt.
	watchR, watchW, err := os.Pipe()
	if err != nil {
		return spawnFailure(err, "watchdog pipe")
	}
	baseR, baseW, err := os.Pipe()
	if err != nil {
		_ = watchR.Close()
		_ = watchW.Close()
		return spawnFailure(err, "baseline pipe")
	}
	defer func() {
		// Closing the write end is a no-op for a live attempt (the shim
		// has already exited by the time this defer runs) and is the
		// kill order for one that somehow lingered. A failed close on
		// the way out names nothing an operator could act on.
		_ = watchW.Close()
		_ = watchR.Close()
		_ = baseR.Close()
		_ = baseW.Close()
	}()

	shimArgs := []string{
		"exec",
		"--run-id", s.Ctx.RunID,
		"--step", s.Ctx.Step,
		"--attempt", strconv.Itoa(s.Ctx.Attempt),
		"--claim-epoch", strconv.FormatInt(t.ClaimEpoch, 10),
		"--spool-dir", t.SpoolDir,
		"--timeout", s.Timeout.String(),
		"--kill-grace", grace.String(),
		"--watch-fd", "3",
		"--base-fd", "4",
	}
	if workdir != "" {
		shimArgs = append(shimArgs, "--workdir", workdir)
	}
	shimArgs = append(shimArgs, "--")
	shimArgs = append(shimArgs, argv...)

	cmd := exec.CommandContext(ctx, t.Executable, shimArgs...) // #nosec G204 - the executable is this process's own binary, the argv is the contract
	cmd.Dir = workdir
	cmd.Env = env
	cmd.Stdin = nil
	cmd.Stdout = s.Stdout
	cmd.Stderr = s.Stderr
	cmd.ExtraFiles = []*os.File{watchR, baseW}
	cmd.WaitDelay = grace
	cmd.SysProcAttr = sysProcAttr() // the shim leads the attempt's outer group

	esc := newEscalation(nil, grace, clk)
	defer esc.stop()
	cmd.Cancel = esc.fire

	var timedOut atomic.Bool
	timer := clk.NewTimer(s.Timeout)
	defer timer.Stop()

	releaseCoreLimit := zeroCoreLimit()

	if err := cmd.Start(); err != nil {
		releaseCoreLimit()
		return spawnFailure(err, "")
	}
	shimPID := cmd.Process.Pid
	esc.setGroup(shimPID)
	releaseShimGroup := registerGroup(shimPID)
	faults.Point("M6:step:after_spawn")

	// The child's baseline arrives on the pipe the moment the shim has it.
	// It is recorded through the same OnStart seam the direct path uses, so
	// the orphan sweep's evidence keeps its shape, and the child's group is
	// registered so a hard stop reaches it directly.
	var child childBaseline
	baselineDone := make(chan struct{})
	go func() {
		defer close(baselineDone)
		releaseChild := func() {}
		defer func() { releaseChild() }()
		sc := bufio.NewScanner(baseR)
		sc.Buffer(make([]byte, 0, 256), 4096)
		if !sc.Scan() {
			return
		}
		var b baseline
		if err := json.Unmarshal(sc.Bytes(), &b); err != nil || b.PID <= 0 {
			return
		}
		child.set(b.PID, b.StartTicks)
		releaseChild = registerGroup(b.PID)
		if s.OnStart != nil {
			s.OnStart(b.PID)
		}
	}()

	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-timer.C:
			timedOut.Store(true)
			_ = esc.fire()
		case <-esc.done:
		}
	}()

	waitErr := cmd.Wait()
	finishedAt := clk.Now().UnixMilli()
	releaseShimGroup()
	releaseCoreLimit()
	esc.stop()
	<-watcherDone
	<-baselineDone
	_ = waitErr // the spool, or the fallback below, carries the verdict

	if r, err := spool.ReadResult(filepath.Join(t.SpoolDir, spool.FileName(s.Ctx.RunID, s.Ctx.Step, s.Ctx.Attempt))); err == nil {
		res := resultFromSpool(r, shimPID)
		if res.Outcome == Signalled && ctx.Err() != nil {
			// The kill was this process answering a cancellation or a
			// lost lease; the verdict says so, exactly as the direct
			// path's classifier does with the context it was given.
			mergeCancelled(res)
		}
		return res, nil
	}

	// No result file: the shim died before it could write one, or could
	// not. The best left is the shim's own wait status and a cleanup of the
	// child it may have left behind.
	if pid, ticks := child.get(); pid > 0 {
		_ = VerifiedGroupKill(pid, ticks, grace, clk)
	}
	res := classify(cmd, timedOut.Load(), ctx.Err(), s.Timeout)
	res.Pgid = shimPID
	res.FinishedAt = finishedAt
	extra := make(map[string]any, len(res.ReasonData)+1)
	for k, v := range res.ReasonData {
		extra[k] = v
	}
	extra["spool"] = "missing"
	res.ReasonData = extra
	return res, nil
}

func resultFromSpool(r spool.Result, shimPID int) Result {
	data := map[string]any{}
	if len(r.ReasonData) > 0 {
		_ = json.Unmarshal(r.ReasonData, &data)
	}
	if r.KilledBy != "" {
		data["killed_by"] = r.KilledBy
	}
	out := Result{
		ExitCode:   r.ExitCode,
		Signal:     r.Signal,
		StartedAt:  r.StartedAt,
		FinishedAt: r.EndedAt,
		Pgid:       shimPID,
		ReasonData: data,
	}
	switch r.Outcome {
	case spool.OutcomeSucceeded:
		out.Outcome, out.ReasonCode = Succeeded, ReasonSucceeded
	case spool.OutcomeTimedOut:
		out.Outcome, out.ReasonCode = TimedOut, ReasonTimeout
	case spool.OutcomeSpawnFailed:
		out.Outcome, out.ReasonCode = SpawnFailed, ReasonSpawn
	case spool.OutcomeSignalled:
		out.Outcome, out.ReasonCode = Signalled, ReasonSignal
	default:
		out.Outcome, out.ReasonCode = Failed, ReasonNonzeroExit
	}
	return out
}

func mergeCancelled(res Result) {
	if res.ReasonData == nil {
		res.ReasonData = map[string]any{}
	}
	res.ReasonData["cancelled"] = true
}
