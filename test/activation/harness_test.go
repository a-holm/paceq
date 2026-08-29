package activation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// The waits. They are budgets, not expectations: the sensor is due the instant
// apply writes it and the schedule fires on the second, so both halves land in
// the first wakes after the daemon reports ready. The budget is wide enough
// that a loaded machine cannot fail a correct daemon, and finite so an
// unwired one is a failure rather than a hang.
const (
	readyWait      = 30 * time.Second
	activationWait = 60 * time.Second
	stopWait       = 30 * time.Second
	pollEvery      = 100 * time.Millisecond
)

// The fixtures are built once per run of the package and live outside any
// subtest's tempdir: a subtest's tempdir is deleted when the subtest ends,
// which would take a once-built binary away from the rows after it.
var (
	paceqOnce  sync.Once
	paceqPath  string
	paceqErr   error
	sensorOnce sync.Once
	sensorPath string
	sensorErr  error
)

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

// durableTempDir makes a directory that lives for the whole test binary run.
func durableTempDir(t *testing.T, pattern string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	return dir
}

// build compiles one package of this module into its own directory. The
// compiler's own words travel in the error, so the row that builds second sees
// the failure as fully as the row that built first.
func build(t *testing.T, pattern, pkg string) (string, error) {
	t.Helper()
	path := filepath.Join(durableTempDir(t, pattern), filepath.Base(pkg))
	cmd := exec.Command("go", "build", "-buildvcs=false", "-o", path, pkg)
	cmd.Dir = moduleRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("%w\n%s", err, out)
	}
	return path, nil
}

// paceqBinary builds the real binary once. Both halves of the proof run
// through it: apply is a subcommand of the same program the daemon is.
func paceqBinary(t *testing.T) string {
	t.Helper()
	const pkg = "./cmd/paceq"
	paceqOnce.Do(func() {
		paceqPath, paceqErr = build(t, "paceq-activation-bin", pkg)
	})
	if paceqErr != nil {
		t.Fatalf("build %s: %v", pkg, paceqErr)
	}
	return paceqPath
}

// sensorFixture builds the sensor the applied job names. It lives under
// testdata so no ordinary build of the module carries it.
func sensorFixture(t *testing.T) string {
	t.Helper()
	const pkg = "./test/activation/testdata/activationsensor"
	sensorOnce.Do(func() {
		sensorPath, sensorErr = build(t, "paceq-activation-sensor", pkg)
	})
	if sensorErr != nil {
		t.Fatalf("build %s: %v", pkg, sensorErr)
	}
	return sensorPath
}

// workspace is one project directory, created the way an operator creates one.
type workspace struct {
	Dir      string // the project directory; every command runs with this as cwd
	StateDir string // <Dir>/.paceq: lock file, database, logs
	DBPath   string // <StateDir>/state.db
}

// newWorkspace runs `paceq init`, which is the only supported way to make a
// state directory: it migrates the database and sets the modes the daemon
// refuses to start without. The example job it writes alongside is never
// applied here, so it materialises nothing.
func newWorkspace(t *testing.T) *workspace {
	t.Helper()
	dir := t.TempDir()
	ws := &workspace{
		Dir:      dir,
		StateDir: filepath.Join(dir, ".paceq"),
		DBPath:   filepath.Join(dir, ".paceq", store.DatabaseFileName),
	}
	paceq(t, ws, "init")
	return ws
}

// paceq runs one paceq subcommand to completion in the workspace.
func paceq(t *testing.T, ws *workspace, args ...string) string {
	t.Helper()
	cmd := exec.Command(paceqBinary(t), args...)
	cmd.Dir = ws.Dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("paceq %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// writeJob puts one job file in the project's jobs directory and returns the
// path apply is given.
func writeJob(t *testing.T, ws *workspace, name, body string) string {
	t.Helper()
	rel := filepath.Join("jobs", name)
	if err := os.WriteFile(filepath.Join(ws.Dir, rel), []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
	return rel
}

// applyJob loads one job file. It runs before any daemon exists, because the
// daemon holds the state directory's lock and every writing command is refused
// while it does.
func applyJob(t *testing.T, ws *workspace, path string) {
	t.Helper()
	out := paceq(t, ws, "apply", path)
	t.Logf("paceq apply %s:\n%s", path, out)
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

func (b *lockedBuffer) snapshot() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// serveProc is one running paceq serve subprocess.
type serveProc struct {
	cmd     *exec.Cmd
	stderr  *lockedBuffer
	exited  chan int
	stopped bool
}

// startServe starts the daemon and registers its stop, so no row can leave a
// daemon or a step process behind for the next one.
func startServe(t *testing.T, ws *workspace) *serveProc {
	t.Helper()
	p := &serveProc{
		cmd:    exec.Command(paceqBinary(t), "serve"),
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
	t.Cleanup(func() { p.stop(t) })
	return p
}

// waitReady blocks until the daemon logged its ready line on stderr.
func (p *serveProc) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(readyWait)
	for !strings.Contains(p.stderr.snapshot(), `"msg":"daemon ready"`) {
		if time.Now().After(deadline) {
			t.Fatalf("the daemon never became ready within %s:\n%s", readyWait, p.stderr.snapshot())
		}
		select {
		case code := <-p.exited:
			p.stopped = true
			t.Fatalf("the daemon exited %d before it was ready:\n%s", code, p.stderr.snapshot())
		default:
		}
		time.Sleep(pollEvery)
	}
}

// stop ends the daemon the way an operator does, and insists if the drain
// takes longer than the budget.
func (p *serveProc) stop(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Logf("send SIGTERM: %v", err)
	}
	select {
	case <-p.exited:
	case <-time.After(stopWait):
		_ = p.cmd.Process.Kill()
		<-p.exited
		t.Errorf("the daemon did not stop within %s:\n%s", stopWait, p.stderr.snapshot())
	}
}

// waitFor polls until the condition holds, and fails naming what never
// happened together with everything the daemon said while it did not.
func waitFor(t *testing.T, p *serveProc, what string, cond func() bool) {
	t.Helper()
	started := time.Now()
	deadline := started.Add(activationWait)
	for {
		if cond() {
			t.Logf("%s took %s", what, time.Since(started).Round(time.Millisecond))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s and it never happened\ndaemon stderr:\n%s",
				activationWait, what, p.stderr.snapshot())
		}
		time.Sleep(pollEvery)
	}
}

// openReadOnly opens the state without taking the write lock the daemon holds.
func openReadOnly(t *testing.T, ws *workspace) *store.Store {
	t.Helper()
	s, err := store.OpenReadOnly(context.Background(), ws.DBPath, store.Options{})
	if err != nil {
		t.Fatalf("open the state read-only: %v", err)
	}
	return s
}

// readSensor reads one sensor row, drift columns included.
func readSensor(t *testing.T, ws *workspace, name string) store.SensorSummary {
	t.Helper()
	s := openReadOnly(t, ws)
	defer func() { _ = s.Close() }()
	row, err := s.GetSensor(context.Background(), name)
	if err != nil {
		t.Fatalf("read sensor %s: %v", name, err)
	}
	return row
}

// readSensorTicks reads the ticks recorded against one sensor, newest first.
func readSensorTicks(t *testing.T, ws *workspace, name string) []store.SensorTickView {
	t.Helper()
	s := openReadOnly(t, ws)
	defer func() { _ = s.Close() }()
	ticks, err := s.SensorTicks(context.Background(), name, 20)
	if err != nil {
		t.Fatalf("read the ticks of sensor %s: %v", name, err)
	}
	return ticks
}

// readSchedules reads every schedule row in the database.
func readSchedules(t *testing.T, ws *workspace) []store.ScheduleRow {
	t.Helper()
	s := openReadOnly(t, ws)
	defer func() { _ = s.Close() }()
	rows, err := s.ListAllSchedules(context.Background())
	if err != nil {
		t.Fatalf("read the schedules: %v", err)
	}
	return rows
}

// readScheduleTicks reads the ticks recorded against one schedule.
func readScheduleTicks(t *testing.T, ws *workspace, job, name string) []store.TickView {
	t.Helper()
	s := openReadOnly(t, ws)
	defer func() { _ = s.Close() }()
	ticks, err := s.ScheduleTicks(context.Background(), job, name)
	if err != nil {
		t.Fatalf("read the ticks of schedule %s/%s: %v", job, name, err)
	}
	return ticks
}

// readRuns reads the runs of one job, newest first.
func readRuns(t *testing.T, ws *workspace, job string) []store.JobRunSummary {
	t.Helper()
	s := openReadOnly(t, ws)
	defer func() { _ = s.Close() }()
	runs, err := s.JobLastRuns(context.Background(), job)
	if err != nil {
		t.Fatalf("read the runs of job %s: %v", job, err)
	}
	return runs
}

// quote renders a nullable text column for a failure message.
func quote(v *string) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%q", *v)
}
