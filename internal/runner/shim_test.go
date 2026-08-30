package runner

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/spool"
)

// The shim's tests. In-process ShimMain calls cover the outcome mapping and
// the spool write; subprocess rows cover the parts that only exist across a
// process boundary — the watchdog's EOF and the daemon side's read of what
// the shim left behind.

func shimConfig(argv []string) ShimConfig {
	return ShimConfig{
		RunID:      "01JQ9F0R7K3M5N7P9R1T3V5X7Z",
		Step:       "dump",
		Attempt:    2,
		ClaimEpoch: 42,
		Timeout:    10 * time.Second,
		KillGrace:  200 * time.Millisecond,
		WatchFD:    -1,
		BaseFD:     -1,
		Argv:       argv,
	}
}

func shimResult(t *testing.T, cfg ShimConfig, dir string) spool.Result {
	t.Helper()
	res, err := spool.ReadResult(filepath.Join(dir, spool.FileName(cfg.RunID, cfg.Step, cfg.Attempt)))
	if err != nil {
		t.Fatalf("read the result the shim wrote: %v", err)
	}
	return res
}

func TestShimMainExitCodePassesThroughAndSpools(t *testing.T) {
	dir := t.TempDir()
	cfg := shimConfig([]string{"/bin/sh", "-c", "exit 3"})
	cfg.SpoolDir = dir

	if code := ShimMain(t.Context(), cfg); code != 3 {
		t.Fatalf("exit code = %d, want the child's 3", code)
	}
	res := shimResult(t, cfg, dir)
	if res.Outcome != spool.OutcomeFailed || res.ExitCode != 3 {
		t.Fatalf("outcome = %s exit = %d, want failed/3", res.Outcome, res.ExitCode)
	}
	if res.KilledBy != "" {
		t.Fatalf("killed_by = %q for a plain exit", res.KilledBy)
	}
	if res.ClaimEpoch != 42 || res.Attempt != 2 || res.Step != "dump" {
		t.Fatalf("the identity did not reach the file: %+v", res)
	}
}

func TestShimMainExitZeroIsSucceeded(t *testing.T) {
	dir := t.TempDir()
	cfg := shimConfig([]string{"/bin/sh", "-c", "true"})
	cfg.SpoolDir = dir

	if code := ShimMain(t.Context(), cfg); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if res := shimResult(t, cfg, dir); res.Outcome != spool.OutcomeSucceeded {
		t.Fatalf("outcome = %s, want succeeded", res.Outcome)
	}
}

func TestShimMainSpawnFailureSpools127(t *testing.T) {
	dir := t.TempDir()
	cfg := shimConfig([]string{"/nonexistent/binary-9d2f"})
	cfg.SpoolDir = dir

	if code := ShimMain(t.Context(), cfg); code != 127 {
		t.Fatalf("exit code = %d, want 127", code)
	}
	if res := shimResult(t, cfg, dir); res.Outcome != spool.OutcomeSpawnFailed {
		t.Fatalf("outcome = %s, want spawn_failed: a refused spawn is a known outcome", res.Outcome)
	}
}

func TestShimMainRefusalsSpoolBeforeAnyProcess(t *testing.T) {
	dir := t.TempDir()

	cfg := shimConfig(nil)
	cfg.SpoolDir = dir
	if code := ShimMain(t.Context(), cfg); code != 127 {
		t.Fatalf("empty argv: exit = %d, want 127", code)
	}

	cfg = shimConfig([]string{"sh", "-c", "true"})
	cfg.SpoolDir = dir
	if code := ShimMain(t.Context(), cfg); code != 127 {
		t.Fatalf("relative argv: exit = %d, want 127", code)
	}

	cfg = shimConfig([]string{"/bin/sh", "-c", "true"})
	cfg.SpoolDir = dir
	cfg.Timeout = 0
	if code := ShimMain(t.Context(), cfg); code != 127 {
		t.Fatalf("no timeout: exit = %d, want 127", code)
	}
	shimResult(t, cfg, dir) // even a refusal is a known outcome on file
}

func TestShimMainTimeoutKillsAndSpoolsKilledBy(t *testing.T) {
	dir := t.TempDir()
	cfg := shimConfig([]string{"/bin/sh", "-c", "sleep 30"})
	cfg.SpoolDir = dir
	cfg.Timeout = 150 * time.Millisecond

	start := time.Now()
	if code := ShimMain(t.Context(), cfg); code == 0 {
		t.Fatal("a timed-out child cannot report success")
	}
	if took := time.Since(start); took > 15*time.Second {
		t.Fatalf("the timeout path took %s; the child was not killed", took)
	}
	res := shimResult(t, cfg, dir)
	if res.Outcome != spool.OutcomeTimedOut || res.KilledBy != "timeout" {
		t.Fatalf("outcome = %s killed_by = %q, want timed_out/timeout", res.Outcome, res.KilledBy)
	}
}

// TestShimIgnoreTermChild is the stubborn child other tests spawn: it ignores
// SIGTERM exactly like a data-warehouse loader might, then sleeps until
// something insists.
func TestShimIgnoreTermChild(t *testing.T) {
	if os.Getenv("PACEQ_TEST_GRANDCHILD") != "1" {
		t.Skip("only runs as a spawned child")
	}
	_ = syscall.Setpgid(0, 0)
	signalIgnoreTerm()
	time.Sleep(5 * time.Minute)
}

// TestShimExecChild is the shim subprocess of the watchdog test: it runs
// ShimMain for real, with fd 3 as its watchdog read end, around a child that
// ignores SIGTERM.
func TestShimExecChild(t *testing.T) {
	if os.Getenv("PACEQ_TEST_SHIM_CHILD") != "1" {
		t.Skip("only runs as a spawned child")
	}
	cfg := shimConfig([]string{
		os.Args[0], "-test.run=TestShimIgnoreTermChild", "-test.count=1",
	})
	cfg.Env = append(os.Environ(), "PACEQ_TEST_GRANDCHILD=1")
	cfg.SpoolDir = os.Getenv("PACEQ_TEST_SHIM_SPOOL")
	cfg.Timeout = time.Minute
	cfg.KillGrace = 500 * time.Millisecond
	cfg.WatchFD = 3
	cfg.BaseFD = 4

	code := ShimMain(t.Context(), cfg)
	os.Exit(code)
}

// TestShimMainWatchdogEOFKillsTheWholeGroup: the daemon's death is a pipe
// closing, and a child that ignores SIGTERM still dies within the grace,
// with the result file saying so.
func TestShimMainWatchdogEOFKillsTheWholeGroup(t *testing.T) {
	spoolDir := t.TempDir()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// The child names fd 3 and fd 4, so both have to be here. ExtraFiles[i]
	// lands on fd 3+i, and an fd the child names but the parent never passed
	// is not an error anywhere: it is whatever this process had open there,
	// and the shim writes its baseline straight into it. Under `git push` that
	// descriptor was git's pipe to remote-curl, and one JSON line into it
	// killed the push with "remote-curl: unknown command" long after the gate
	// had gone green.
	baseR, baseW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer baseR.Close()

	shim := exec.Command(os.Args[0], "-test.run=TestShimExecChild", "-test.count=1")
	shim.Env = append(os.Environ(),
		"PACEQ_TEST_SHIM_CHILD=1",
		"PACEQ_TEST_SHIM_SPOOL="+spoolDir,
	)
	shim.ExtraFiles = []*os.File{r, baseW} // fd 3 is the watchdog, fd 4 the baseline

	// Reading the baseline is what keeps the two descriptor numbers honest: a
	// child that writes it somewhere else leaves this read empty.
	baseline := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(baseR).ReadString('\n')
		baseline <- line
	}()
	shimStdout := &strings.Builder{}
	shim.Stderr = stderrOf(shimStdout)

	runErr := make(chan error, 1)
	go func() {
		err := shim.Run()
		// The parent's copy of the write end has to go, or the reader above
		// never sees EOF when the child dies without writing.
		_ = baseW.Close()
		runErr <- err
	}()

	// The watchdog write end lives in this process; closing it is exactly
	// what the kernel does when a daemon dies, whatever the cause.
	waitForMarkerProc(t, "PACEQ_TEST_GRANDCHILD=1")
	start := time.Now()
	if err := w.Close(); err != nil {
		t.Fatalf("close the watchdog: %v", err)
	}

	select {
	case err := <-runErr:
		// A nonzero exit is the shim doing its job: the child died to
		// SIGKILL and the shim passes 128+9 through. The assertion that
		// matters is the spool file below.
		_ = err
	case <-time.After(30 * time.Second):
		t.Fatal("the shim did not notice the EOF")
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Fatalf("EOF to cleanup took %s; the group survived its grace", took)
	}

	// The baseline has to have come down fd 4. An empty read means the child
	// wrote it to a descriptor this test never handed it, which is a write
	// into whatever the parent process had open there.
	select {
	case line := <-baseline:
		if !strings.Contains(line, "\"pid\"") || !strings.Contains(line, "\"start_ticks\"") {
			t.Errorf("baseline line = %q, want the child's pid and start ticks on fd 4", line)
		}
	case <-time.After(10 * time.Second):
		t.Error("the baseline never arrived on fd 4")
	}

	cfg := shimConfig(nil) // only the identity matters here
	res := shimResult(t, cfg, spoolDir)
	if res.KilledBy != "daemon_gone" {
		t.Fatalf("killed_by = %q, want daemon_gone", res.KilledBy)
	}
	waitForMarkerProcGone(t, "PACEQ_TEST_GRANDCHILD=1")
}

func TestVerifiedGroupKillRefusesAMismatchedBaseline(t *testing.T) {
	var mu sync.Mutex
	var sent []int
	restore := captureGroupKill(func(pgid int, sig syscall.Signal) error {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, pgid)
		return nil
	})
	defer restore()

	if err := VerifiedGroupKill(os.Getpid(), 1, 10*time.Millisecond, nil); err == nil {
		t.Fatal("a wrong start ticks value was accepted for a kill")
	}
	if err := VerifiedGroupKill(os.Getpid(), 0, 10*time.Millisecond, nil); err == nil {
		t.Fatal("a missing baseline was accepted for a kill")
	}
	if err := VerifiedGroupKill(999999, 1, 10*time.Millisecond, nil); err == nil {
		t.Fatal("a gone pid was accepted for a kill")
	}
	if err := VerifiedGroupKill(0, 1, 10*time.Millisecond, nil); err == nil {
		t.Fatal("no group was named")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 0 {
		t.Fatalf("signals went out on refusals: %v", sent)
	}
}

func TestVerifiedGroupKillKillsAMatchingGroup(t *testing.T) {
	// A real group: this child leads its own group, so the verified kill
	// has a genuine target whose ticks must match to fire at all.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	cmd := exec.Command(os.Args[0], "-test.run=TestShimSleeperChild", "-test.count=1")
	cmd.Env = append(os.Environ(), "PACEQ_TEST_SLEEPER=1")
	cmd.ExtraFiles = []*os.File{w}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	// The child sets its own pgid; the pid is the group.
	pid := cmd.Process.Pid
	waitForMarkerProc(t, "PACEQ_TEST_SLEEPER=1")

	if err := VerifiedGroupKill(pid, -1, 10*time.Millisecond, nil); err == nil {
		t.Fatal("a fabricated baseline was accepted")
	}
	_ = r.Close()
}

// TestShimSleeperChild leads its own process group and blocks, so the
// verified-kill test has a real target with a real start-ticks identity.
// A plain sleep, not select{}: a lone blocking select trips the runtime's
// deadlock detector and the child would die before the test sees it.
func TestShimSleeperChild(t *testing.T) {
	if os.Getenv("PACEQ_TEST_SLEEPER") != "1" {
		t.Skip("only runs as a spawned child")
	}
	_ = syscall.Setpgid(0, 0)
	time.Sleep(5 * time.Minute)
}

// --- the daemon side ---

var (
	shimMu   sync.Mutex
	shimPath string
)

// shimFixture builds testdata/shimcmd once per run of the package. The
// directory is deliberately not a t.TempDir: a once-built fixture must
// outlive whichever row built it, or the cached path dies with the first
// test's cleanup.
func shimFixture(t *testing.T) string {
	t.Helper()
	shimMu.Lock()
	defer shimMu.Unlock()
	if shimPath != "" {
		if _, err := os.Stat(shimPath); err == nil {
			return shimPath
		}
	}
	durable, err := os.MkdirTemp("", "paceq-shim-fixture")
	if err != nil {
		t.Fatalf("tempdir for the shim fixture: %v", err)
	}
	path := filepath.Join(durable, "shimcmd-fixture")
	build := exec.Command("go", "build", "-o", path, "./testdata/shimcmd")
	build.Dir = moduleRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the shim fixture: %v\n%s", err, out)
	}
	shimPath = path
	return shimPath
}

// TestSpawnViaShimRoundTrip drives the daemon side against a real shim
// subprocess: the baseline arrives over the pipe, the spool file becomes the
// returned result, and the file stays until the verdict commits.
func TestSpawnViaShimRoundTrip(t *testing.T) {
	dir := t.TempDir()
	spoolDir := filepath.Join(dir, "spool", "attempts")

	var mu sync.Mutex
	baselinePID := 0
	spec := Spec{
		Argv:      []string{"/bin/sh", "-c", "exit 5"},
		Timeout:   10 * time.Second,
		KillGrace: 200 * time.Millisecond,
		Ctx: RunContext{
			RunID:   "01JQ9F0R7K3M5N7P9R1T3V5X7Z",
			Step:    "dump",
			Attempt: 2,
		},
		OnStart: func(pid int) {
			mu.Lock()
			defer mu.Unlock()
			baselinePID = pid
		},
	}

	res, err := SpawnViaShim(t.Context(), spec, ShimTarget{
		Executable: shimFixture(t),
		SpoolDir:   spoolDir,
		ClaimEpoch: 42,
	})
	if err != nil {
		t.Fatalf("spawn via shim: %v", err)
	}
	if res.Outcome != Failed || res.ExitCode != 5 {
		t.Fatalf("outcome = %v exit = %d, want Failed/5", res.Outcome, res.ExitCode)
	}
	mu.Lock()
	if baselinePID <= 0 {
		mu.Unlock()
		t.Fatal("the baseline pipe never named the child; the orphan sweep would be blind")
	}
	mu.Unlock()
	// The file stays until the verdict commits — the crash window's whole
	// point.
	if _, err := os.Stat(filepath.Join(spoolDir,
		spool.FileName(spec.Ctx.RunID, spec.Ctx.Step, spec.Ctx.Attempt))); err != nil {
		t.Fatalf("the result file was removed before the verdict: %v", err)
	}
}

// TestSpawnViaShimWithoutASpoolFallsBack: a shim that could not write its
// result still leaves the daemon able to say what it saw.
func TestSpawnViaShimWithoutASpoolFallsBack(t *testing.T) {
	dir := t.TempDir()
	spoolDir := filepath.Join(dir, "spool", "attempts")

	spec := Spec{
		Argv:       []string{"/bin/sh", "-c", "exit 7"},
		Timeout:    10 * time.Second,
		KillGrace:  200 * time.Millisecond,
		InheritEnv: []string{"SHIMCMD_NO_SPOOL"},
		Ctx: RunContext{
			RunID:   "01JQ9F0R7K3M5N7P9R1T3V5X7Z",
			Step:    "dump",
			Attempt: 2,
		},
	}
	t.Setenv("SHIMCMD_NO_SPOOL", "1")

	res, err := SpawnViaShim(t.Context(), spec, ShimTarget{
		Executable: shimFixture(t),
		SpoolDir:   spoolDir,
		ClaimEpoch: 42,
	})
	if err != nil {
		t.Fatalf("spawn via shim: %v", err)
	}
	if res.Outcome != Failed || res.ExitCode != 7 {
		t.Fatalf("outcome = %v exit = %d want Failed/7 from the wait status, reason_data %s err %v", res.Outcome, res.ExitCode, res.ReasonData, res.Err)
	}
	if res.ReasonData["spool"] != "missing" {
		t.Fatalf("the fallback must say the spool was missing, got %v", res.ReasonData)
	}
}

func TestSpawnViaShimRefusesAnUnusableTarget(t *testing.T) {
	base := Spec{Argv: []string{"/bin/sh", "-c", "true"}, Timeout: time.Second}
	if _, err := SpawnViaShim(t.Context(), base, ShimTarget{SpoolDir: t.TempDir()}); err == nil {
		t.Fatal("no executable was refused")
	}
	if _, err := SpawnViaShim(t.Context(), base, ShimTarget{Executable: os.Args[0]}); err == nil {
		t.Fatal("no spool directory was refused")
	}
}

// --- helpers ---

func signalIgnoreTerm() {
	// signal.Ignore keeps the stub child alive through the whole grace; a
	// plain Notify-and-forget is enough, nothing here reads the channel.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("find the working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the shim tests")
		}
		dir = parent
	}
}

// waitForMarkerProc blocks until some live process carries marker in its
// environment, then returns. It is how a test knows its spawned child chain
// reached the stage under test.
func waitForMarkerProc(t *testing.T, marker string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if markerProcAlive(marker) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a process carrying %s never appeared", marker)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForMarkerProcGone is the other direction, and the assertion half of the
// watchdog test: nothing carrying the marker survives.
func waitForMarkerProcGone(t *testing.T, marker string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if !markerProcAlive(marker) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a process carrying %s is still alive", marker)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func markerProcAlive(marker string) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	for _, e := range entries {
		if _, conv := strconv.Atoi(e.Name()); conv != nil {
			continue
		}
		raw, err := os.ReadFile("/proc/" + e.Name() + "/environ")
		if err == nil && strings.Contains(string(raw), marker+"\x00") {
			return true
		}
	}
	return false
}

func stderrOf(b *strings.Builder) *strings.Builder { return b }

// Why an attempt was signalled is the sender's knowledge, not the
// receiver's (#204). The daemon side leaves it beside the result before it
// kills, the shim stamps it into the result, and the result is what both the
// live verdict and recovery read.
func TestSpawnViaShimRecordsWhyTheAttemptWasSignalled(t *testing.T) {
	for _, tc := range []struct {
		name     string
		argv     []string
		cancel   bool
		killedBy string
		want     bool
	}{
		{
			name:     "the daemon answering a cancellation",
			argv:     []string{"/bin/sh", "-c", "sleep 60"},
			cancel:   true,
			killedBy: spool.KilledByCancel,
			want:     true,
		},
		{
			name:     "a signal from outside",
			argv:     []string{"/bin/sh", "-c", "kill -TERM $$"},
			cancel:   false,
			killedBy: "",
			want:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spoolDir := filepath.Join(t.TempDir(), "spool", "attempts")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			spec := Spec{
				Argv:    tc.argv,
				Timeout: 30 * time.Second,
				// Room for the shim to write its result before
				// cmd.WaitDelay gives up on it.
				KillGrace: 5 * time.Second,
				Ctx: RunContext{
					RunID:   "01JQ9F0R7K3M5N7P9R1T3V5X7Z",
					Step:    "dump",
					Attempt: 2,
				},
			}
			if tc.cancel {
				spec.OnStart = func(int) { cancel() }
			}

			res, err := SpawnViaShim(ctx, spec, ShimTarget{
				Executable: shimFixture(t),
				SpoolDir:   spoolDir,
				ClaimEpoch: 42,
			})
			if err != nil {
				t.Fatalf("spawn via shim: %v", err)
			}
			if res.Outcome != Signalled {
				t.Fatalf("outcome = %v, want Signalled; reason_data %v", res.Outcome, res.ReasonData)
			}
			file := shimResult(t, ShimConfig{RunID: spec.Ctx.RunID, Step: spec.Ctx.Step, Attempt: spec.Ctx.Attempt}, spoolDir)
			if file.KilledBy != tc.killedBy {
				t.Fatalf("the file says killed_by %q, want %q", file.KilledBy, tc.killedBy)
			}
			if got := res.ReasonData["cancelled"] == true; got != tc.want {
				t.Fatalf("the returned verdict reads cancelled = %v, want %v (%v)", got, tc.want, res.ReasonData)
			}
			// The mark is a note to a process that has exited.
			if spool.CancelMarked(spoolDir, spec.Ctx.RunID, spec.Ctx.Step, spec.Ctx.Attempt) {
				t.Fatal("the cancel mark outlived the shim that read it")
			}
		})
	}
}
