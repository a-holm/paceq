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

// lockedBuffer is stderr output safe to read while the daemon writes it. Each
// write also wakes whoever waits for a line, so a waiter learns of a log line
// on the write that carries it instead of on its next poll. A stop test's
// window can be short, and a poll interval spent inside it is a flake.
type lockedBuffer struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	changed chan struct{} // closed and replaced by every write
}

func newLockedBuffer() *lockedBuffer {
	return &lockedBuffer{changed: make(chan struct{})}
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	close(b.changed)
	b.changed = make(chan struct{})
	return n, err
}

// containsOrWait reports whether the buffer already holds needle, and when it
// does not, hands back the channel the next write closes. Both are read under
// one lock, so no write can land between the look and the wait.
func (b *lockedBuffer) containsOrWait(needle string) (bool, <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if strings.Contains(b.buf.String(), needle) {
		return true, nil
	}
	return false, b.changed
}

// drainStartMsg is the daemon's own announcement that phase two has begun:
// the intake is shut, the executors' context is cancelled, and the process
// now waits for the steps in flight. Everything a stop signal can convert
// into a hard stop happens after this line.
const drainStartMsg = "draining running work"

// milestones are the daemon's own stop sequence in the order it logs them.
// A failure message quotes the last one seen, so a reader is told what the
// daemon was doing rather than only that a system call failed.
var milestones = []struct{ msg, state string }{
	{"daemon ready", "serving"},
	{"intake closed", "the intake is closed, the drain has not started"},
	{drainStartMsg, "draining the work in flight"},
	{"second stop signal: killing every process group now", "answering a second stop signal"},
	{"daemon stopped cleanly", "finished its clean stop"},
}

// serveProc is one running paceq serve subprocess.
type serveProc struct {
	cmd    *exec.Cmd
	stderr *lockedBuffer
	exited chan int      // carries the exit code
	gone   chan struct{} // closed once Wait returned and stderr is complete

	mu   sync.Mutex
	done bool
	code int
}

func startServe(t *testing.T, ws *workspace, extra ...string) *serveProc {
	t.Helper()
	p := &serveProc{
		cmd:    exec.Command(paceqBinary(t), append([]string{"serve"}, extra...)...),
		stderr: newLockedBuffer(),
		exited: make(chan int, 1),
		gone:   make(chan struct{}),
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
		// Wait has returned, so the stderr copy is complete: anything the
		// daemon logged is readable before any reader learns it is gone.
		p.mu.Lock()
		p.done, p.code = true, code
		p.mu.Unlock()
		close(p.gone)
		p.exited <- code
	}()
	t.Cleanup(func() { _ = p.cmd.Process.Kill() })
	return p
}

// waitReady blocks until the daemon logged its ready line on stderr. The line
// is written inside Serve, after main installed the process signal context and
// after serve registered the hard stop channel, so a daemon that is ready has
// its handlers in place: no stop signal sent after this call can be missed.
func (p *serveProc) waitReady(t *testing.T) {
	t.Helper()
	p.waitForLog(t, "daemon ready", "the daemon becoming ready", 15*time.Second)
}

// waitForDrainStart blocks until the daemon says its drain has begun. This is
// the state a second stop signal converts into a hard stop; before it there is
// nothing to convert, and after the drain ends there is no daemon left.
func (p *serveProc) waitForDrainStart(t *testing.T) {
	t.Helper()
	p.waitForLog(t, drainStartMsg, "the drain starting", 20*time.Second)
}

// waitForLog blocks until the daemon logs one slog message, waking on the
// write that carries it. It gives up as soon as the daemon has exited without
// ever logging it, because a dead process writes nothing more, and it names
// what was not observed either way.
func (p *serveProc) waitForLog(t *testing.T, msg, what string, within time.Duration) {
	t.Helper()
	needle := `"msg":"` + msg + `"`
	timeout := time.NewTimer(within)
	defer timeout.Stop()
	for {
		found, changed := p.stderr.containsOrWait(needle)
		if found {
			return
		}
		select {
		case <-changed: // the daemon wrote something; look again
		case <-p.gone:
			// Wait has returned, so every byte the daemon wrote is in
			// the buffer already: one last look settles it.
			if found, _ := p.stderr.containsOrWait(needle); found {
				return
			}
			t.Fatalf("%s was never observed: the daemon exited first, %s\ndaemon stderr:\n%s",
				what, p.state(), p.stderrSnapshot())
		case <-timeout.C:
			t.Fatalf("%s was never observed within %s; the daemon is %s\ndaemon stderr:\n%s",
				what, within, p.state(), p.stderrSnapshot())
		}
	}
}

// state describes the daemon the way a failure message needs it: gone and with
// which code, or alive and at which point of its stop sequence.
func (p *serveProc) state() string {
	p.mu.Lock()
	done, code := p.done, p.code
	p.mu.Unlock()
	if done {
		return fmt.Sprintf("already gone, exit code %d, last logged: %s", code, p.lastMilestone())
	}
	return "still running, last logged: " + p.lastMilestone()
}

// lastMilestone names the furthest point of the stop sequence the daemon has
// reached on stderr.
func (p *serveProc) lastMilestone() string {
	out := p.stderrSnapshot()
	last := "nothing yet"
	for _, m := range milestones {
		if strings.Contains(out, `"msg":"`+m.msg+`"`) {
			last = m.state
		}
	}
	return last
}

func (p *serveProc) stderrSnapshot() string {
	p.stderr.mu.Lock()
	defer p.stderr.mu.Unlock()
	return p.stderr.buf.String()
}

// signal delivers one stop signal to the daemon process. The ordinal is the
// caller's name for this request ("first", "second"): a stop test's whole
// claim rests on which request failed, so the message must say so, and it must
// say what the daemon was doing when the signal was refused.
func (p *serveProc) signal(t *testing.T, ordinal string, sig syscall.Signal) {
	t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("the %s stop request (%s) never reached the daemon: %v\nthe daemon is %s\ndaemon stderr:\n%s",
			ordinal, sig, err, p.state(), p.stderrSnapshot())
	}
}

// waitExit blocks for the daemon to end and reports its exit code. A daemon
// that outlives the budget is reported with the point of the stop sequence it
// reached, because "it did not exit" alone never says which promise broke.
func (p *serveProc) waitExit(t *testing.T, within time.Duration) int {
	t.Helper()
	select {
	case code := <-p.exited:
		return code
	case <-time.After(within):
		t.Fatalf("the daemon did not exit within %s; it is %s\ndaemon stderr:\n%s",
			within, p.state(), p.stderrSnapshot())
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
		// A poll interval, not a gate: the loop's exit is the observed state.
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
		// A poll interval, not a gate: the loop's exit is /proc going quiet.
		time.Sleep(25 * time.Millisecond)
	}
}
