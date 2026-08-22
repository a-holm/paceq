package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

var (
	fakeCmdOnce sync.Once
	fakeCmdPath string
)

// fakecmd builds the testdata/fakecmd fixture and returns its path. Building
// happens once per test binary; every test shares the read only artifact.
func fakecmd(t *testing.T) string {
	t.Helper()

	fakeCmdOnce.Do(func() {
		dir, err := os.MkdirTemp("", "paceq-runner-fakecmd-")
		if err != nil {
			t.Fatalf("tempdir for fakecmd: %v", err)
		}
		path := filepath.Join(dir, "fakecmd")
		build := exec.Command("go", "build", "-o", path, "./testdata/fakecmd")
		// A test binary runs with its working directory set to the package
		// directory, where the fixture lives.
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build fakecmd: %v\n%s", err, out)
		}
		fakeCmdPath = path
	})
	if fakeCmdPath == "" {
		t.Fatal("fakecmd was not built")
	}
	return fakeCmdPath
}

// baseSpec returns a Spec whose defaults are right for most tests: a real
// binary, a short but generous timeout and a small kill grace so escalation
// proofs stay fast while leaving orders of magnitude of slack against slow
// machines.
func baseSpec(t *testing.T, argv ...string) Spec {
	t.Helper()

	return Spec{
		Argv:       argv,
		Timeout:    30 * time.Second,
		KillGrace:  150 * time.Millisecond,
		Ctx:        RunContext{RunID: "01JTEST0000000000000000000", Job: "job", Step: "step", Attempt: 1},
		Stdout:     io.Discard,
		Stderr:     io.Discard,
		OutputPath: filepath.Join(t.TempDir(), "output.ndjson"),
	}
}

// runBounded runs Run inside a watchdog so a mutant that hangs can turn a red
// test into a hung gate. The bound exists only to fail loudly; every value is
// at least two orders of magnitude above what the assertion needs.
func runBounded(t *testing.T, bound time.Duration, ctx context.Context, s Spec) (Result, error) {
	t.Helper()

	type outcome struct {
		res Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := Run(ctx, s)
		done <- outcome{res, err}
	}()
	select {
	case o := <-done:
		return o.res, o.err
	case <-time.After(bound):
		t.Fatalf("Run did not return within %s; termination is broken", bound)
		return Result{}, nil
	}
}

// waitForGroupGone polls until no member of the process group exists, zombies
// included, and fails loudly with everything it saw if members remain.
func waitForGroupGone(t *testing.T, pgid int, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			live := listGroupMembers(pgid)
			t.Fatalf("process group %d still alive after %s: members %v", pgid, within, live)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// listGroupMembers scans /proc for processes whose pgid matches. Used only to
// make a failure message useful; the assertion itself is the ESRCH probe.
func listGroupMembers(pgid int) []string {
	if runtime.GOOS != "linux" {
		return nil
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var found []string
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			continue
		}
		fields := strings.Fields(string(stat))
		if len(fields) < 5 {
			continue
		}
		if fields[4] == strconv.Itoa(pgid) {
			cmdline, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
			found = append(found, fmt.Sprintf("pid=%d cmd=%q", pid, strings.ReplaceAll(string(cmdline), "\x00", " ")))
		}
	}
	return found
}

// scanProcFor finds /proc entries whose cmdline contains marker. It is the
// direct implementation of the acceptance criterion "verified via a /proc
// scan"; linux only, skipped elsewhere with the capability that is missing.
func scanProcFor(t *testing.T, marker string) []string {
	t.Helper()

	if runtime.GOOS != "linux" {
		t.Skip("needs /proc, which " + runtime.GOOS + " does not have")
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatalf("read /proc: %v", err)
	}
	var found []string
	for _, e := range entries {
		cmdline, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue // raced an exit; that process cannot be ours
		}
		if bytes.Contains(cmdline, []byte(marker)) {
			found = append(found, e.Name())
		}
	}
	return found
}

func TestRunExitZeroIsSucceeded(t *testing.T) {
	res, err := runBounded(t, time.Minute, context.Background(), baseSpec(t, "/bin/true"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != Succeeded {
		t.Errorf("outcome = %v, want Succeeded", res.Outcome)
	}
	if res.ReasonCode != ReasonSucceeded {
		t.Errorf("reason = %q, want %q", res.ReasonCode, ReasonSucceeded)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code = %d, want 0", res.ExitCode)
	}
	if res.Signal != "" {
		t.Errorf("signal = %q, want empty", res.Signal)
	}
	if res.StartedAt <= 0 || res.FinishedAt < res.StartedAt {
		t.Errorf("timestamps not ordered: started %d finished %d", res.StartedAt, res.FinishedAt)
	}
	if res.Pgid <= 0 {
		t.Errorf("pgid = %d, want the child's process group", res.Pgid)
	}
}

func TestRunExitNIsFailedWithTransientFalse(t *testing.T) {
	res, err := runBounded(t, time.Minute, context.Background(), baseSpec(t, "/bin/sh", "-c", "exit 7"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != Failed {
		t.Errorf("outcome = %v, want Failed", res.Outcome)
	}
	if res.ReasonCode != ReasonNonzeroExit {
		t.Errorf("reason = %q, want %q", res.ReasonCode, ReasonNonzeroExit)
	}
	if res.ExitCode != 7 {
		t.Errorf("exit code = %d, want 7", res.ExitCode)
	}
	if got, ok := res.ReasonData["transient"].(bool); !ok || got {
		t.Errorf("reason_data transient = %#v, want false", res.ReasonData["transient"])
	}
}

func TestRunExitSeventyFiveMarksTransient(t *testing.T) {
	res, err := runBounded(t, time.Minute, context.Background(), baseSpec(t, "/bin/sh", "-c", "exit 75"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != Failed || res.ExitCode != 75 {
		t.Fatalf("outcome = %v exit = %d, want Failed 75", res.Outcome, res.ExitCode)
	}
	if got, ok := res.ReasonData["transient"].(bool); !ok || !got {
		t.Errorf("reason_data transient = %#v, want true: exit 75 is the retryable convention", res.ReasonData["transient"])
	}
	if res.ReasonCode != ReasonNonzeroExit {
		t.Errorf("reason = %q, want %q", res.ReasonCode, ReasonNonzeroExit)
	}
}

func TestRunSignalDeathReportsNameAnd128PlusSig(t *testing.T) {
	// KILL is the one signal a Go program cannot catch, so raising it at
	// itself is the reliable way for the fixture to die by signal.
	res, err := runBounded(t, time.Minute, context.Background(), baseSpec(t, fakecmd(t), "signal-self", "KILL"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != Signalled {
		t.Fatalf("outcome = %v, want Signalled", res.Outcome)
	}
	if res.Signal != "SIGKILL" {
		t.Errorf("signal = %q, want SIGKILL", res.Signal)
	}
	want := 128 + int(syscall.SIGKILL)
	if res.ExitCode != want {
		t.Errorf("exit code = %d, want %d", res.ExitCode, want)
	}
	if res.ReasonCode != ReasonSignal {
		t.Errorf("reason = %q, want %q", res.ReasonCode, ReasonSignal)
	}
	if got := res.ReasonData["signal"]; got != "SIGKILL" {
		t.Errorf("reason_data signal = %v, want SIGKILL", got)
	}
}

func TestRunMissingBinaryIsSpawnFailedNotExit127(t *testing.T) {
	s := baseSpec(t, "/nonexistent/definitely-not-here")
	res, err := runBounded(t, time.Minute, context.Background(), s)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != SpawnFailed {
		t.Fatalf("outcome = %v, want SpawnFailed", res.Outcome)
	}
	if res.ReasonCode != ReasonSpawn {
		t.Errorf("reason = %q, want %q", res.ReasonCode, ReasonSpawn)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code = %d, want 0: the command never ran so there is no exit status", res.ExitCode)
	}
	if !errors.Is(res.Err, syscall.ENOENT) && !strings.Contains(fmt.Sprint(res.Err), "no such file") {
		t.Errorf("err = %v, want an ENOENT underneath", res.Err)
	}
	if res.ReasonData["errno"] == nil {
		t.Errorf("reason_data errno missing, want the operating system error number")
	}
}

func TestRunNonExecutableIsSpawnFailedWithErrno(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root, which ignores execute permission bits")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "tool")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := runBounded(t, time.Minute, context.Background(), baseSpec(t, bin))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != SpawnFailed || res.ReasonCode != ReasonSpawn {
		t.Fatalf("outcome = %v reason = %q, want SpawnFailed %q", res.Outcome, res.ReasonCode, ReasonSpawn)
	}
	if res.ReasonData["errno"] == nil {
		t.Errorf("reason_data errno missing")
	}
}

func TestRunMissingWorkdirIsSpawnFailedWithErrno(t *testing.T) {
	s := baseSpec(t, "/bin/true")
	s.Workdir = filepath.Join(t.TempDir(), "does-not-exist")
	res, err := runBounded(t, time.Minute, context.Background(), s)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != SpawnFailed || res.ReasonCode != ReasonSpawn {
		t.Fatalf("outcome = %v reason = %q, want SpawnFailed %q", res.Outcome, res.ReasonCode, ReasonSpawn)
	}
	if res.ReasonData["errno"] == nil {
		t.Errorf("reason_data errno missing")
	}
}

func TestRunTimeoutEscalatesTermThenKillAcrossTheGroup(t *testing.T) {
	// The marker embeds this test binary's pid, so a stray left behind by an
	// earlier crashed run can never be mistaken for ours.
	marker := fmt.Sprintf("paceq-zombie-%d-%s", os.Getpid(), t.Name())
	// tree mode: the direct child ignores SIGTERM and a grandchild in the
	// same group ignores it too. Only the SIGKILL escalation can end either.
	s := baseSpec(t, fakecmd(t), "tree", "5m", marker)
	s.Timeout = 250 * time.Millisecond
	s.KillGrace = 200 * time.Millisecond

	start := time.Now()
	res, err := runBounded(t, 15*time.Second, context.Background(), s)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != TimedOut {
		t.Fatalf("outcome = %v, want TimedOut", res.Outcome)
	}
	if res.ReasonCode != ReasonTimeout {
		t.Errorf("reason = %q, want %q", res.ReasonCode, ReasonTimeout)
	}
	if ms, ok := res.ReasonData["timeout_ms"].(int64); !ok || ms != 250 {
		t.Errorf("reason_data timeout_ms = %#v, want 250", res.ReasonData["timeout_ms"])
	}
	// The ignore-term grandchild survives SIGTERM, so only the SIGKILL
	// escalation can end it. It must die within grace of the timeout, with a
	// wide margin for machine load.
	if max := s.Timeout + s.KillGrace + time.Second; elapsed > max {
		t.Errorf("group took %s to die, want under %s", elapsed, max)
	}
	waitForGroupGone(t, res.Pgid, 15*time.Second)
	if left := scanProcFor(t, marker); len(left) > 0 {
		t.Errorf("grandchild survived as pids %v; the group was not killed", left)
	}
}

func TestRunIgnoreTermSurvivesTermDiesToKill(t *testing.T) {
	// Same proof as the grandchild test but for the direct child: SIGTERM is
	// ignored, so surviving past the timeout proves the TERM went out, and
	// dying within grace proves the KILL went out.
	s := baseSpec(t, fakecmd(t), "ignore-term", "5m")
	s.Timeout = 250 * time.Millisecond
	s.KillGrace = 200 * time.Millisecond

	start := time.Now()
	res, err := runBounded(t, 15*time.Second, context.Background(), s)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != TimedOut {
		t.Fatalf("outcome = %v, want TimedOut", res.Outcome)
	}
	if elapsed < s.Timeout {
		t.Errorf("returned after %s, before the %s deadline: the deadline never fired", elapsed, s.Timeout)
	}
	if max := s.Timeout + s.KillGrace + time.Second; elapsed > max {
		t.Errorf("took %s, want under %s: SIGKILL escalation did not arrive within grace", elapsed, max)
	}
	waitForGroupGone(t, res.Pgid, 15*time.Second)
}

func TestRunParentCancelSignalsTheGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := baseSpec(t, fakecmd(t), "ignore-term", "5m")
	s.Timeout = time.Hour // the deadline must not be what ends this
	s.KillGrace = 200 * time.Millisecond

	type boxed struct {
		res Result
		err error
	}
	done := make(chan boxed, 1)
	go func() {
		res, err := Run(ctx, s)
		done <- boxed{res, err}
	}()

	time.Sleep(300 * time.Millisecond) // let the child reach its signal handler
	cancel()
	select {
	case o := <-done:
		if o.err != nil {
			t.Fatalf("Run: %v", o.err)
		}
		if o.res.Outcome != Signalled {
			t.Fatalf("outcome = %v, want Signalled: a cancelled run that died by signal is not a timeout", o.res.Outcome)
		}
		if o.res.ReasonCode != ReasonSignal {
			t.Errorf("reason = %q, want %q", o.res.ReasonCode, ReasonSignal)
		}
		waitForGroupGone(t, o.res.Pgid, 15*time.Second)
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after parent cancel")
	}
}

func TestRunEnvironmentIsExactlyTheContract(t *testing.T) {
	t.Setenv("DAEMON_SECRET_TOKEN", "must-not-leak")
	t.Setenv("PACEQ_EVIL_INJECTION", "must-not-leak")
	t.Setenv("INHERITED_OK", "from-daemon")
	t.Setenv("INHERITED_NO", "from-daemon")
	t.Setenv("HOME", "/home/tester")

	workdir := t.TempDir()
	envFile := filepath.Join(workdir, "job.env")
	content := "# comment line\nFILE_VAR=from-file\nSHARED_LAYER=from-file\n\r\nCRLF_VAR=crlf-value\n"
	if err := os.WriteFile(envFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	s := baseSpec(t, fakecmd(t), "env-dump")
	s.Stdout = &bytes.Buffer{}
	s.Env = map[string]string{"JOB_VAR": "from-job", "SHARED_LAYER": "from-job"}
	s.EnvFile = envFile
	s.InheritEnv = []string{"INHERITED_OK"}
	s.Workdir = workdir
	s.Ctx.Params = map[string]any{"alpha": float64(1)}
	s.Ctx.RunKey = "rk-123"
	s.Ctx.ScheduledFor = time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)

	res, err := runBounded(t, time.Minute, context.Background(), s)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != Succeeded {
		t.Fatalf("outcome = %v stderr buffer holds the failure", res.Outcome)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(s.Stdout.(*bytes.Buffer).String())), &got); err != nil {
		t.Fatalf("child env is not a JSON object: %v\ngot: %q", err, s.Stdout.(*bytes.Buffer).String())
	}

	key := sha256.Sum256([]byte(s.Ctx.RunID + ":" + s.Ctx.Step))
	want := map[string]string{
		// Fixed baseline, never inherited.
		"PATH": DefaultPath,
		"HOME": "/home/tester",
		// Job layers, in precedence order.
		"JOB_VAR":      "from-job",
		"SHARED_LAYER": "from-job", // job env beats env_file beats inherit_env
		"FILE_VAR":     "from-file",
		"CRLF_VAR":     "crlf-value",
		"INHERITED_OK": "from-daemon",
		// The frozen context contract, spelled out in full.
		"PACEQ_RUN_ID":          s.Ctx.RunID,
		"PACEQ_JOB":             s.Ctx.Job,
		"PACEQ_STEP":            s.Ctx.Step,
		"PACEQ_ATTEMPT":         "1",
		"PACEQ_RUN_KEY":         "rk-123",
		"PACEQ_IDEMPOTENCY_KEY": hex.EncodeToString(key[:])[:32],
		"PACEQ_SCHEDULED_FOR":   "2026-08-22T10:00:00Z",
		"PACEQ_PARAMS":          `{"alpha":1}`,
		"PACEQ_INPUTS":          `{}`,
		"PACEQ_OUTPUT":          s.OutputPath,
	}
	// TZ and LANG pass through under their known names when the runner has
	// them; whether the machine running the test has them must not change the
	// shape of the assertion.
	for _, name := range []string{"TZ", "LANG"} {
		if v, ok := os.LookupEnv(name); ok {
			want[name] = v
		}
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("env[%q] = %q, want %q", k, got[k], v)
		}
		delete(got, k)
	}
	for k, v := range got {
		t.Errorf("unexpected env[%q] = %q leaked into the job", k, v)
	}
}

func TestRunIdempotencyKeyIsStableAcrossAttempts(t *testing.T) {
	readKey := func(attempt int) string {
		s := baseSpec(t, fakecmd(t), "env-dump")
		s.Ctx.Attempt = attempt
		s.Ctx.Step = "load"
		s.Stdout = &bytes.Buffer{}
		res, err := runBounded(t, time.Minute, context.Background(), s)
		if err != nil || res.Outcome != Succeeded {
			t.Fatalf("attempt %d: outcome %v err %v", attempt, res.Outcome, err)
		}
		var env map[string]string
		if err := json.Unmarshal([]byte(strings.TrimSpace(s.Stdout.(*bytes.Buffer).String())), &env); err != nil {
			t.Fatalf("decode env: %v", err)
		}
		return env["PACEQ_IDEMPOTENCY_KEY"]
	}
	if readKey(1) == "" || readKey(1) != readKey(2) {
		t.Error("PACEQ_IDEMPOTENCY_KEY changed between attempts of the same step")
	}
}

func TestRunStdinIsNeverInherited(t *testing.T) {
	// cat copies stdin; given the runner's closed stdin it sees EOF at once
	// and exits 0. An inherited pipe would leave it blocked and the bounded
	// run would fail the test.
	s := baseSpec(t, "/bin/cat")
	res, err := runBounded(t, 15*time.Second, context.Background(), s)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != Succeeded {
		t.Errorf("outcome = %v, want Succeeded: cat must see EOF immediately", res.Outcome)
	}
}

func TestRunWorkdirIsApplied(t *testing.T) {
	dir := t.TempDir()
	s := baseSpec(t, "/bin/pwd")
	s.Workdir = dir
	buf := &bytes.Buffer{}
	s.Stdout = buf
	res, err := runBounded(t, time.Minute, context.Background(), s)
	if err != nil || res.Outcome != Succeeded {
		t.Fatalf("outcome = %v err = %v", res.Outcome, err)
	}
	if got := strings.TrimSpace(buf.String()); got != dir {
		t.Errorf("pwd = %q, want %q", got, dir)
	}
}

func TestRunWithShellOptInRunsThroughSh(t *testing.T) {
	s := baseSpec(t, "echo shell-$((40+2))")
	s.Shell = true
	buf := &bytes.Buffer{}
	s.Stdout = buf
	res, err := runBounded(t, time.Minute, context.Background(), s)
	if err != nil || res.Outcome != Succeeded {
		t.Fatalf("outcome = %v err = %v", res.Outcome, err)
	}
	if got := strings.TrimSpace(buf.String()); got != "shell-42" {
		t.Errorf("stdout = %q, want shell-42", got)
	}
}

func TestRunCoreLimitIsZeroInTheChild(t *testing.T) {
	s := baseSpec(t, fakecmd(t), "rlimits")
	buf := &bytes.Buffer{}
	s.Stdout = buf
	res, err := runBounded(t, time.Minute, context.Background(), s)
	if err != nil || res.Outcome != Succeeded {
		t.Fatalf("outcome = %v err = %v", res.Outcome, err)
	}
	var rl struct {
		CoreCur uint64 `json:"core_cur"`
		CoreMax uint64 `json:"core_max"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rl); err != nil {
		t.Fatalf("decode rlimits: %v", err)
	}
	if rl.CoreCur != 0 {
		t.Errorf("RLIMIT_CORE cur = %d, want 0: core dumps carry secrets", rl.CoreCur)
	}
}

func TestRunCreatesTheOutputFileForTheJob(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "artifacts.ndjson")
	s := baseSpec(t, fakecmd(t), "write-output")
	s.OutputPath = out
	res, err := runBounded(t, time.Minute, context.Background(), s)
	if err != nil || res.Outcome != Succeeded {
		t.Fatalf("outcome = %v err = %v", res.Outcome, err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("output file: %v", err)
	}
	if n := bytes.Count(data, []byte("\n")); n != 2 {
		t.Errorf("output file has %d lines, want 2:\n%s", n, data)
	}
	info, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("output file mode = %o, want 600", perm)
	}
}

func TestRunSpewCompletesWithoutBufferingTheWorld(t *testing.T) {
	s := baseSpec(t, fakecmd(t), "spew", "64")
	s.Stdout = io.Discard
	start := time.Now()
	res, err := runBounded(t, 2*time.Minute, context.Background(), s)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != Succeeded {
		t.Errorf("outcome = %v, want Succeeded", res.Outcome)
	}
	if time.Since(start) > 90*time.Second {
		t.Errorf("64 MiB took %s, the pipe path is broken", time.Since(start))
	}
}

func TestRunThousandTimesLeakNeitherDescriptorsNorGoroutines(t *testing.T) {
	fdCount := func(t *testing.T) int {
		t.Helper()
		if runtime.GOOS != "linux" {
			t.Skip("needs /proc/self/fd, which " + runtime.GOOS + " does not have")
		}
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatalf("read /proc/self/fd: %v", err)
		}
		return len(entries)
	}

	spec := func() Spec {
		s := baseSpec(t, "/bin/true")
		s.Timeout = 30 * time.Second
		return s
	}

	// Warm up every lazy path first, so the before sample cannot be compared
	// against a cold process.
	for i := 0; i < 5; i++ {
		if _, err := Run(context.Background(), spec()); err != nil {
			t.Fatalf("warmup run: %v", err)
		}
	}
	fdsBefore, gorsBefore := fdCount(t), runtime.NumGoroutine()

	for i := 0; i < 1000; i++ {
		res, err := Run(context.Background(), spec())
		if err != nil || res.Outcome != Succeeded {
			t.Fatalf("run %d: outcome %v err %v", i, res.Outcome, err)
		}
	}

	fdsAfter, gorsAfter := fdCount(t), runtime.NumGoroutine()
	if fdsAfter-fdsBefore > 5 {
		t.Errorf("file descriptors grew from %d to %d across 1000 runs", fdsBefore, fdsAfter)
	}
	// A finished child can still be reaped on the runtime's own schedule, so
	// the goroutine count is sampled with a short settle loop instead of one
	// immediate read: a real leak stays above the baseline for every sample,
	// while normal reaping converges to it.
	gors := gorsAfter
	for i := 0; i < 50 && gors != gorsBefore; i++ {
		time.Sleep(2 * time.Millisecond)
		gors = runtime.NumGoroutine()
	}
	if gors != gorsBefore {
		t.Errorf("goroutines went from %d to %d: Run returned while something it owned was still alive", gorsBefore, gors)
	}
}

func TestRunRefusesContractViolationsBeforeStartingAnything(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Spec)
	}{
		{"zero timeout", func(s *Spec) { s.Timeout = 0 }},
		{"negative timeout", func(s *Spec) { s.Timeout = -time.Second }},
		{"timeout over the system cap", func(s *Spec) { s.Timeout = 2 * time.Hour }},
		{"empty argv", func(s *Spec) { s.Argv = nil }},
		{"relative bare argv0", func(s *Spec) { s.Argv = []string{"ls"} }},
		{"reserved env key from job", func(s *Spec) { s.Env = map[string]string{"PACEQ_RUN_ID": "forged"} }},
		{"reserved env key from inherit", func(s *Spec) { s.InheritEnv = []string{"PACEQ_STEP"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := baseSpec(t, "/bin/true")
			tc.mut(&s)
			res, err := Run(context.Background(), s)
			if err == nil {
				t.Fatalf("Run accepted a broken spec, result %+v", res)
			}
			if !errors.Is(err, ErrInvalidSpec) {
				t.Errorf("err = %v, want it to wrap ErrInvalidSpec", err)
			}
		})
	}
}

func TestRunDefaultClockAndGraceApplyWhenUnset(t *testing.T) {
	s := baseSpec(t, "/bin/true")
	s.Clock = nil
	s.KillGrace = 0
	res, err := runBounded(t, time.Minute, context.Background(), s)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Outcome != Succeeded {
		t.Fatalf("outcome = %v", res.Outcome)
	}
	now := clock.System().Now()
	started := time.UnixMilli(res.StartedAt)
	if d := now.Sub(started); d < 0 || d > time.Minute {
		t.Errorf("StartedAt = %s is not a recent wall reading (%s away)", started, d)
	}
}
