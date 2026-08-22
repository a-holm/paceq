package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// The fixtures are built once per run of the package and live outside any
// subtest's tempdir: a subtest's tempdir is deleted when the subtest ends,
// which would take a once-built binary away from the rows after it.

var (
	paceqOnce  sync.Once
	paceqPath  string
	fakeOnce   sync.Once
	fakePath   string
	fakeErr    error
	fakeErrMsg string
)

func durableTempDir(t *testing.T, pattern string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	return dir
}

// moduleRoot walks up from the working directory until it finds go.mod.
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
			t.Fatal("no go.mod above the harness")
		}
		dir = parent
	}
}

// paceqBinary builds the real daemon once and returns its path.
func paceqBinary(t *testing.T) string {
	t.Helper()
	paceqOnce.Do(func() {
		path := filepath.Join(durableTempDir(t, "paceq-serve-bin"), "paceq")
		build := exec.Command("go", "build", "-o", path, "./cmd/paceq")
		build.Dir = moduleRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build paceq: %v\n%s", err, out)
		}
		paceqPath = path
	})
	return paceqPath
}

// stepCommand builds the runner's own test command once. Its sleep and
// ignore-term modes carry everything these rows need: one that dies on
// SIGTERM like a well behaved job, one that ignores it so only SIGKILL
// through the group can end it.
func stepCommand(t *testing.T) string {
	t.Helper()
	fakeOnce.Do(func() {
		path := filepath.Join(durableTempDir(t, "paceq-serve-fakecmd"), "fakecmd")
		build := exec.Command("go", "build", "-o", path, "./internal/runner/testdata/fakecmd")
		build.Dir = moduleRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			fakeErr = err
			fakeErrMsg = string(out)
			return
		}
		fakePath = path
	})
	if fakeErr != nil {
		t.Fatalf("build fakecmd: %v\n%s", fakeErr, fakeErrMsg)
	}
	return fakePath
}

// workspace is one project directory with an applied state directory.
type workspace struct {
	Dir      string // the project directory; serve runs with this as cwd
	StateDir string // <Dir>/state: lock file, database, logs
	DBPath   string // <StateDir>/state.db
}

func newWorkspace(t *testing.T) *workspace {
	t.Helper()
	dir := t.TempDir()
	ws := &workspace{
		Dir: dir,
		// The CLI resolves the state directory to <project>/.paceq; the
		// workspace must use that name or serve will look somewhere else.
		StateDir: filepath.Join(dir, ".paceq"),
		DBPath:   filepath.Join(dir, ".paceq", store.DatabaseFileName),
	}
	if err := os.MkdirAll(ws.StateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	st := openStore(t, ws)
	closeStore(t, st)
	// A plain store.Open creates the database without the mode rule
	// OpenState enforces; tighten it so the daemon accepts what we seeded.
	if err := os.Chmod(ws.DBPath, 0o600); err != nil {
		t.Fatalf("tighten the database mode: %v", err)
	}
	return ws
}

func openStore(t *testing.T, ws *workspace) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), ws.DBPath, store.Options{})
	if err != nil {
		t.Fatalf("open the store at %s: %v", ws.DBPath, err)
	}
	return s
}

func closeStore(t *testing.T, s *store.Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Errorf("close the store: %v", err)
	}
}

// seedQueuedRun applies a one step job whose command is the given fakecmd
// invocation, then materialises one queued manual run for it. It returns the
// run id, which is also the marker every child process carries in its
// environment (PACEQ_RUN_ID).
func seedQueuedRun(t *testing.T, ws *workspace, cmd string, args ...string) string {
	t.Helper()
	ctx := context.Background()

	parts := make([]string, 0, len(args)+1)
	for _, a := range append([]string{cmd}, args...) {
		raw, err := json.Marshal(a)
		if err != nil {
			t.Fatalf("encode %q: %v", a, err)
		}
		parts = append(parts, string(raw))
	}
	spec := fmt.Sprintf(`{"schema":"paceq.job.v1","name":"drainjob","max_concurrent":1,`+
		`"steps":[{"name":"only","run":[%s],"shell":false}]}`,
		strings.Join(parts, ","))
	fingerprint := strconv.Itoa(os.Getpid()) + "-" + strings.Join(args, "-")

	st := openStore(t, ws)
	defer closeStore(t, st)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	version, _, err := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "drainjob", SpecHash: "sha256:drainjob-" + fingerprint,
		SpecJSON: spec,
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	run, err := st.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "drainjob",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "test",
		Steps:        []store.NewStep{{Name: "only"}},
	})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	return run.ID
}

// lockedBuffer is stderr output safe to read while the daemon writes it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) contains(needle string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Contains(b.buf.String(), needle)
}

// serveProc is one running paceq serve subprocess.
type serveProc struct {
	cmd    *exec.Cmd
	stderr *lockedBuffer
	exited chan int // carries the exit code
}

func startServe(t *testing.T, ws *workspace, extra ...string) *serveProc {
	t.Helper()
	p := &serveProc{
		cmd:    exec.Command(paceqBinary(t), append([]string{"serve"}, extra...)...),
		stderr: &lockedBuffer{},
		exited: make(chan int, 1),
	}
	p.cmd.Dir = ws.Dir
	p.cmd.Stderr = p.stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start paceq serve: %v", err)
	}
	go func() {
		err := p.cmd.Wait()
		code := 0
		if err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				code = ee.ExitCode()
			} else {
				code = -1
			}
		}
		p.exited <- code
	}()
	t.Cleanup(func() { _ = p.cmd.Process.Kill() })
	return p
}

// waitReady blocks until the daemon logged its ready line on stderr.
func (p *serveProc) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !p.stderr.contains(`"msg":"daemon ready"`) {
		if time.Now().After(deadline) {
			t.Fatalf("the daemon never became ready:\n%s", p.stderrSnapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (p *serveProc) stderrSnapshot() string {
	p.stderr.mu.Lock()
	defer p.stderr.mu.Unlock()
	return p.stderr.buf.String()
}

// signal delivers one stop signal to the daemon process.
func (p *serveProc) signal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("send %s: %v", sig, err)
	}
}

// waitExit blocks for the daemon to end and reports its exit code.
func (p *serveProc) waitExit(t *testing.T, within time.Duration) int {
	t.Helper()
	select {
	case code := <-p.exited:
		return code
	case <-time.After(within):
		t.Fatalf("the daemon did not exit within %s", within)
		return -1
	}
}

// waitForChildRunning polls until the seeded run's step is executing AND its
// process is alive on the machine: proof that the daemon claimed the run and
// that the process this row is going to stop really exists. The row state
// alone is not enough: it turns running a moment before the spawn lands, and
// a stop signal that arrives in that window would drain an empty pool.
func waitForChildRunning(t *testing.T, ws *workspace, p *serveProc, runID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		detail := readRun(t, ws, runID)
		rowRunning := len(detail.Steps) == 1 && detail.Steps[0].State == "running"
		if rowRunning && procCarriesRunID(runID) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the step's process never appeared; last read: %+v\ndaemon stderr:\n%s",
				detail, p.stderrSnapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func openReadOnly(t *testing.T, ws *workspace) *store.Store {
	t.Helper()
	s, err := store.OpenReadOnly(context.Background(), ws.DBPath, store.Options{})
	if err != nil {
		t.Fatalf("open the state read-only: %v", err)
	}
	return s
}

func readRun(t *testing.T, ws *workspace, runID string) store.RunDetail {
	t.Helper()
	s := openReadOnly(t, ws)
	defer func() { _ = s.Close() }()
	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read run %s: %v", runID, err)
	}
	return detail
}

// procCarriesRunID scans /proc for any live process whose environment names
// the run id. The scan is exactly the check the acceptance list asks for,
// sharpened to one exact value so another test's processes elsewhere on the
// machine can never be mistaken for ours.
func procCarriesRunID(runID string) bool {
	marker := []byte("PACEQ_RUN_ID=" + runID)
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue // not a process directory
		}
		raw, err := os.ReadFile("/proc/" + entry.Name() + "/environ")
		if err != nil {
			continue // gone between listing and reading
		}
		if bytes.Contains(raw, marker) {
			return true
		}
	}
	return false
}

// requireNoOrphanFails if anything carrying the run id is still alive after a
// bounded wait: a clean stop may take a moment to be seen by /proc, but it
// may never fail to happen.
func requireNoOrphan(t *testing.T, runID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for procCarriesRunID(runID) {
		if time.Now().After(deadline) {
			t.Fatal("a process carrying the interrupted run is still alive after the stop")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
