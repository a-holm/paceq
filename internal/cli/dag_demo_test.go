package cli

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
	"github.com/rogpeppe/go-internal/txtar"

	"github.com/a-holm/paceq/internal/store"
)

// TestDAGDemoScripts runs the M4 exit demo rows of issue #20: the diamond
// lifecycle in both IPC modes. One file per mode, each readable as
// documentation of what the milestone promises. The parallel half of the
// criterion - the branches' windows actually overlapping - lives in
// test/demo, because its proof drives internal/runner and internal/worker,
// and this package's test imports are pinned by the dependency rules.
func TestDAGDemoScripts(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir: "testdata/dagdemo",
		Cmds: map[string]func(*testscript.TestScript, bool, []string){
			"wantexit":      cmdWantExit,
			"maskfile":      cmdMaskFile,
			"writeid":       cmdWriteID,
			"mark":          cmdMark,
			"wantexitwall":  cmdWantExitWall,
			"startdaemon":   cmdStartDaemon,
			"stopdaemon":    cmdStopDaemon,
			"waitrun":       cmdWaitRun,
			"assertafter":   cmdAssertAfter,
			"expecteffects": cmdExpectEffects,
		},
		Setup:               setupScriptEnv,
		RequireExplicitExec: true,
	})
}

// TestDiamondExampleMatchesTheDemoFixture is the mirror rule as a gate: the
// shipped example and the fixture inside the demo rows must stay the same
// job, so a user who runs scripts/demo-m4.sh sees exactly what CI proves.
func TestDiamondExampleMatchesTheDemoFixture(t *testing.T) {
	example, err := os.ReadFile(filepath.Join("..", "..", "examples", "dag", "diamond.yaml"))
	if err != nil {
		t.Fatalf("the example job is missing: %v", err)
	}
	script, err := os.ReadFile(filepath.Join("testdata", "dagdemo", "dag_demo_down.txtar"))
	if err != nil {
		t.Fatalf("the demo row is missing: %v", err)
	}
	fixture, ok := txtarFile(script, "diamond.yaml")
	if !ok {
		t.Fatal("the demo row carries no diamond.yaml section")
	}
	if specBody(example) != specBody(fixture) {
		t.Fatalf("examples/dag/diamond.yaml and the dag_demo_down.txtar fixture differ in substance\nexample:\n%s\nfixture:\n%s", example, fixture)
	}
}

// specBody strips comment lines and blank lines, so the two copies may
// carry different amounts of prose around the same job.
func specBody(data []byte) string {
	var kept []string
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		kept = append(kept, trimmed)
	}
	return strings.Join(kept, "\n")
}

// txtarFile returns one named file section of a txtar document.
func txtarFile(data []byte, name string) ([]byte, bool) {
	for _, file := range txtar.Parse(data).Files {
		if file.Name == name {
			return file.Data, true
		}
	}
	return nil, false
}

// --------------- daemon in the background ---------------

// dagPaceqOnce guards the one build of the real daemon binary the suite
// needs. The sandbox's own paceq command is the test binary, whose entry
// point carries no signal wiring of its own; serve must be the shipped
// main, so a stop signal takes the exact graceful path an operator's take.
var (
	dagPaceqOnce sync.Once
	dagPaceqPath string
	dagPaceqErr  error
)

// dagPaceqBinary builds cmd/paceq once and returns its path, mirroring the
// test/serve harness pattern.
func dagPaceqBinary(ts *testscript.TestScript) string {
	dagPaceqOnce.Do(func() {
		dir, err := os.MkdirTemp("", "paceq-dagdemo-bin")
		if err != nil {
			dagPaceqErr = err
			return
		}
		path := filepath.Join(dir, "paceq")
		build := exec.Command("go", "build", "-o", path, "./cmd/paceq")
		build.Dir = dagModuleRoot(ts)
		if out, buildErr := build.CombinedOutput(); buildErr != nil {
			dagPaceqErr = fmt.Errorf("%v\n%s", buildErr, out)
			return
		}
		dagPaceqPath = path
	})
	if dagPaceqErr != nil {
		ts.Fatalf("could not build the daemon binary: %v", dagPaceqErr)
	}
	return dagPaceqPath
}

// dagModuleRoot walks up from the working directory until go.mod names the
// module root, so the build runs from the right place whatever the suite's
// working directory is.
func dagModuleRoot(ts *testscript.TestScript) string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			ts.Fatalf("no go.mod above the dag demo harness")
		}
		dir = parent
	}
}

// demoDaemon is one real paceq serve process, started in the script's work
// directory the way an operator starts it. With a socket it is the endpoint
// the CLI dials; without one it is an executor only, and every CLI write
// keeps taking the flock path. It is a subprocess, not a goroutine of this
// test: the steps it executes must run with the project as their working
// directory, exactly as they would for a user.
type demoDaemon struct {
	cmd    *exec.Cmd
	log    *os.File
	socket string
}

// cmdStartDaemon starts the project's daemon and waits until it is
// reachable before the row continues.
//
//	startdaemon            # an executor only: no socket, writes stay on flock
//	startdaemon -socket    # serves .paceq/paceq.sock; CLI writes dial it
func cmdStartDaemon(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) > 1 {
		ts.Fatalf("usage: startdaemon [-socket]")
	}
	withSocket := len(args) == 1 && args[0] == "-socket"
	if len(args) == 1 && !withSocket {
		ts.Fatalf("%q is not a startdaemon option", args[0])
	}
	key := "dagdaemon/" + ts.Name()
	if harnessGet(key) != nil {
		ts.Fatalf("the daemon is already started in this script; stopdaemon it first")
	}
	work := workDirOf(ts)
	var socketPath string
	if withSocket {
		socketPath = filepath.Join(stateDirName, "paceq.sock")
	}
	bin := dagPaceqBinary(ts)
	env, err := wallClockEnv()
	if err != nil {
		ts.Fatalf("%v", err)
	}
	logFile, err := os.OpenFile(filepath.Join(work, "serve.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		ts.Fatalf("could not open serve.log: %v", err)
	}
	serveArgs := []string{"serve", "--workers", "1"}
	if withSocket {
		serveArgs = append(serveArgs, "--socket", socketPath)
	}
	cmd := exec.Command(bin, serveArgs...)
	cmd.Dir = work
	cmd.Env = env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		ts.Fatalf("could not start paceq serve: %v", err)
	}
	d := &demoDaemon{cmd: cmd, log: logFile, socket: socketPath}
	harnessSet(key, d)
	ts.Defer(func() { stopDemoDaemon(ts) })
	if withSocket {
		waitForSocketLive(ts, filepath.Join(work, stateDirName, "paceq.sock"))
	} else {
		waitForDaemonReady(ts, work)
	}
}

// waitForDaemonReady waits until the executor-only daemon has opened its
// session, so the rows after it see a daemon that owns the state. The
// readiness line is what serve writes when every loop is running; the wait
// is bounded and event driven, never a sleep.
func waitForDaemonReady(ts *testscript.TestScript, work string) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		if live(ts) {
			data, err := os.ReadFile(filepath.Join(work, "serve.log"))
			if err == nil && bytes.Contains(data, []byte(`"msg":"daemon ready"`)) {
				return
			}
		}
		if !time.Now().Before(deadline) {
			ts.Fatalf("paceq serve was never ready within 30s\n%s", serveLogTail(ts))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// live reports whether the script's daemon process is still running.
func live(ts *testscript.TestScript) bool {
	value := harnessGet("dagdaemon/" + ts.Name())
	d, ok := value.(*demoDaemon)
	return ok && d.cmd.Process != nil
}

// cmdStopDaemon stops the script's daemon cleanly and waits until it is
// gone, so the rows after it see an ordinary unlocked directory.
//
//	stopdaemon
func cmdStopDaemon(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 0 {
		ts.Fatalf("usage: stopdaemon")
	}
	if harnessGet("dagdaemon/"+ts.Name()) == nil {
		ts.Fatalf("no daemon is started; startdaemon begins one")
	}
	stopDemoDaemon(ts)
}

// stopDemoDaemon is the body of stopdaemon and the safety net behind
// startdaemon's Defer: SIGTERM exactly as a stop would deliver it, then a
// bounded wait inside the drain budget.
func stopDemoDaemon(ts *testscript.TestScript) {
	key := "dagdaemon/" + ts.Name()
	value := harnessGet(key)
	d, ok := value.(*demoDaemon)
	if !ok {
		return
	}
	harnessDelete(key)
	if d.cmd.Process == nil {
		return
	}
	if err := d.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		ts.Logf("the stop signal could not be delivered: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- d.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		procState := "(gone)"
		if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/wchan", d.cmd.Process.Pid)); err == nil {
			procState = fmt.Sprintf("wchan=%s", bytes.TrimSpace(data))
			if status, sErr := os.ReadFile(fmt.Sprintf("/proc/%d/status", d.cmd.Process.Pid)); sErr == nil {
				for _, line := range strings.Split(string(status), "\n") {
					if strings.HasPrefix(line, "State:") || strings.HasPrefix(line, "SigBlk") ||
						strings.HasPrefix(line, "SigIgn") || strings.HasPrefix(line, "SigCgt") {
						procState += "\n" + line
					}
				}
			}
		}
		_ = d.cmd.Process.Kill()
		<-done
		ts.Fatalf("the daemon did not stop within 30s (%s)\n%s", procState, serveLogTail(ts))
	}
	_ = d.log.Close()
}

// waitForSocketLive polls /livez over the daemon's unix socket until it
// answers 200, so the first write after startdaemon really travels the
// socket instead of falling back to a lock this process still holds.
func waitForSocketLive(ts *testscript.TestScript, socketPath string) {
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socketPath)
			},
		},
		Timeout: 2 * time.Second,
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodGet, "http://unix/livez", nil)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if !time.Now().Before(deadline) {
			ts.Fatalf("the daemon's socket at %s never answered within 30s", socketPath)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// cmdMark creates a one line marker file. The fixture steps read the
// filesystem, not the environment, so a demo can flip their behaviour
// between phases no matter which process spawns them.
//
//	mark marks/fail-warehouse
func cmdMark(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 1 {
		ts.Fatalf("usage: mark FILE")
	}
	ts.Check(os.WriteFile(ts.MkAbs(args[0]), []byte("go-ahead\n"), 0o644))
}

// cmdWantExitWall is wantexit with one difference: the paceq process runs
// on the wall clock, whatever the suite's frozen clock says. The demo rows
// need it because a run stamped in the suite's frozen 2026 would queue
// itself weeks into every dispatcher's future: available_at comes from the
// creation stamp, and a queued run dated ahead of now is invisible to the
// claim query until that date arrives.
//
//	wantexitwall 5 failed.json failed_err.txt run diamond
func cmdWantExitWall(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 3 {
		ts.Fatalf("usage: wantexitwall CODE STDOUT-FILE STDERR-FILE ARGS...")
	}
	code, err := strconv.Atoi(args[0])
	if err != nil {
		ts.Fatalf("%q is not an exit code", args[0])
	}
	paceqArgs, err := expandFileArgs(ts, args[3:])
	if err != nil {
		ts.Fatalf("%v", err)
	}
	bin, err := exec.LookPath("paceq")
	if err != nil {
		ts.Fatalf("the paceq command is not on PATH: %v", err)
	}
	env, err := wallClockEnv()
	if err != nil {
		ts.Fatalf("%v", err)
	}
	cmd := exec.Command(bin, paceqArgs...)
	cmd.Dir = ts.MkAbs(".")
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	got := 0
	if runErr != nil {
		exitErr, ok := runErr.(*exec.ExitError)
		if !ok {
			ts.Fatalf("paceq could not be started: %v", runErr)
		}
		got = exitErr.ExitCode()
	}
	if got != code {
		ts.Fatalf("%v exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			args[3:], got, code, stdout.String(), stderr.String())
	}
	if args[1] != "-" {
		ts.Check(os.WriteFile(ts.MkAbs(args[1]), stdout.Bytes(), 0o644))
	}
	if args[2] != "-" {
		ts.Check(os.WriteFile(ts.MkAbs(args[2]), stderr.Bytes(), 0o644))
	}
}

// wallClockEnv builds a paceq process environment without the frozen clock,
// keeping the rest of the determinism layer.
func wallClockEnv() ([]string, error) {
	drop := map[string]bool{
		"PULSEQ_FAKE_CLOCK": true, "LC_ALL": true, "NO_COLOR": true,
		"CLICOLOR_FORCE": true, "COLUMNS": true,
	}
	var env []string
	for _, entry := range os.Environ() {
		key := entry[:strings.IndexByte(entry, '=')]
		if !drop[key] {
			env = append(env, entry)
		}
	}
	return append(env, "LC_ALL=C", "NO_COLOR=1", "COLUMNS=100"), nil
}

// --------------- bounded waits and window assertions ---------------

// cmdWaitRun polls the state database until one named run reaches the named
// state, or fails naming what it waited for. It is how a row follows work
// another process executes, without sleeping a fixed amount.
//
//	waitrun @failed.id succeeded 60s
func cmdWaitRun(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 3 {
		ts.Fatalf("usage: waitrun IDFILE STATE DEADLINE")
	}
	expanded, err := expandFileArgs(ts, args[:1])
	if err != nil {
		ts.Fatalf("%v", err)
	}
	runID := expanded[0]
	deadline, err := time.ParseDuration(args[2])
	if err != nil || deadline <= 0 {
		ts.Fatalf("%q is not a positive deadline such as 60s", args[2])
	}
	ctx := context.Background()
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	giveUp := time.Now().Add(deadline)
	lastState := "(unreadable)"
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ro, openErr := store.OpenReadOnly(ctx, dbPath, store.Options{}); openErr == nil {
			detail, getErr := ro.GetRun(ctx, runID)
			_ = ro.Close()
			if getErr == nil {
				lastState = detail.State
				if lastState == args[1] {
					return
				}
			}
		}
		if !time.Now().Before(giveUp) {
			ts.Fatalf("run %.8s never reached %q within %s (last state %s)\n%s\n%s",
				runID, args[1], args[2], lastState, queueDump(ts), serveLogTail(ts))
		}
		<-ticker.C
	}
}

// queueDump lists the runs a fresh reader sees and asks the store's own
// claimable query what it would hand a dispatcher, so a stuck wait names
// which side disagrees.
func queueDump(ts *testscript.TestScript) string {
	ctx := context.Background()
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return fmt.Sprintf("(could not open a reader for the dump: %v)", err)
	}
	defer func() { _ = ro.Close() }()
	rows, err := ro.ListRuns(ctx, store.RunFilter{Limit: 10})
	if err != nil {
		return fmt.Sprintf("(ListRuns failed: %v)", err)
	}
	lines := []string{"queue dump (id state available_at):"}
	for _, row := range rows {
		lines = append(lines, fmt.Sprintf("  %.8s %s avail=%s created=%s",
			row.ID, row.State, row.AvailableAt.UTC().Format(time.RFC3339Nano),
			row.CreatedAt.UTC().Format(time.RFC3339Nano)))
	}
	if ids, err := ro.ClaimableRunIDs(ctx); err != nil {
		lines = append(lines, fmt.Sprintf("  ClaimableRunIDs failed: %v", err))
	} else {
		short := make([]string, len(ids))
		for i, id := range ids {
			short[i] = id[:8]
		}
		lines = append(lines, fmt.Sprintf("  ClaimableRunIDs now: %v", short))
	}
	return strings.Join(lines, "\n")
}

// serveLogTail returns the daemon's log for a row that waited in vain, so a
// failure names what the executor was doing instead of leaving silence.
func serveLogTail(ts *testscript.TestScript) string {
	data, err := os.ReadFile(filepath.Join(workDirOf(ts), "serve.log"))
	if err != nil {
		return "(no serve.log)"
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) > 12 {
		lines = lines[len(lines)-12:]
	}
	return "serve.log tail:\n" + strings.Join(lines, "\n")
}

// stepWindow reads one step's recorded window out of one run, ready for the
// interval comparisons the demo rows assert.
func stepWindow(ts *testscript.TestScript, runID, name string) (start, end time.Time) {
	ctx := context.Background()
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		ts.Fatalf("could not read the state to compare windows: %v", err)
	}
	defer func() { _ = ro.Close() }()
	detail, err := ro.GetRun(ctx, runID)
	if err != nil {
		ts.Fatalf("could not read run %s: %v", runID, err)
	}
	for _, step := range detail.Steps {
		if step.Name != name {
			continue
		}
		if step.StartedAt.IsZero() || step.FinishedAt.IsZero() {
			ts.Fatalf("step %s has no complete window: start %v end %v", name, step.StartedAt, step.FinishedAt)
		}
		return step.StartedAt, step.FinishedAt
	}
	ts.Fatalf("run %.26s has no step named %s", runID, name)
	return time.Time{}, time.Time{}
}

// cmdAssertAfter demands that step AFTER began no earlier than the moment
// step BEFORE finished. Millisecond equality passes, because the store
// records whole milliseconds.
//
//	assertafter @green.id transform load-warehouse
func cmdAssertAfter(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 3 {
		ts.Fatalf("usage: assertafter IDFILE BEFORE AFTER")
	}
	expanded, err := expandFileArgs(ts, args[:1])
	if err != nil {
		ts.Fatalf("%v", err)
	}
	_, beforeEnd := stepWindow(ts, expanded[0], args[1])
	afterStart, _ := stepWindow(ts, expanded[0], args[2])
	if afterStart.Before(beforeEnd) {
		ts.Fatalf("%s started at %v, before %s finished at %v: the dependency order is broken",
			args[2], afterStart, args[1], beforeEnd)
	}
}

// --------------- effect counting ---------------

// cmdExpectEffects compares the per-run effect files the fixture steps
// append to against the counts the row promises. A count is how reuse is
// proved without artifact surfaces: a step that ran again wrote again.
//
//	expecteffects @failed.id extract=1 transform=1 load-warehouse=2
func cmdExpectEffects(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 2 {
		ts.Fatalf("usage: expecteffects IDFILE NAME=COUNT ...")
	}
	expanded, err := expandFileArgs(ts, args[:1])
	if err != nil {
		ts.Fatalf("%v", err)
	}
	runID := expanded[0]
	work := workDirOf(ts)
	var problems []string
	for _, want := range args[1:] {
		name, countText, found := strings.Cut(want, "=")
		if !found {
			ts.Fatalf("%q is not a NAME=COUNT pair", want)
		}
		count, err := strconv.Atoi(countText)
		if err != nil || count < 0 {
			ts.Fatalf("%q is not a count", countText)
		}
		data, readErr := os.ReadFile(filepath.Join(work, "effects", fmt.Sprintf("%s.%s.txt", runID, name)))
		got := 0
		if readErr == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.TrimSpace(line) != "" {
					got++
				}
			}
		}
		if got != count {
			problems = append(problems, fmt.Sprintf("%s wrote %d effects, want %d", name, got, count))
		}
	}
	if len(problems) > 0 {
		ts.Fatalf("the effect files disagree:\n%s", strings.Join(problems, "\n"))
	}
}
