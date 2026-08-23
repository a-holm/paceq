package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/api"
	"github.com/a-holm/paceq/internal/store"
)

// The live-daemon half of dual mode: a real paceq serve subprocess holds the
// state, its socket answers, and the CLI writes through it. These tests are
// the actor='api' proof and the --socket none story of M2-08.
//
// The binary is built once per run of the package; each test brings its own
// workspace and its own daemon, and waits on readiness by polling the
// daemon's own health surface, never by sleeping a fixed amount.

var (
	liveOnce  sync.Once
	liveBin   string
	liveErr   error
	liveWhere string
)

func liveBinary(t *testing.T) string {
	t.Helper()
	liveOnce.Do(func() {
		dir, err := os.MkdirTemp("", "paceq-64-bin")
		if err != nil {
			liveErr = err
			return
		}
		liveWhere = dir
		bin := filepath.Join(dir, "paceq")
		build := exec.Command("go", "build", "-buildvcs=false", "-o", bin, "./cmd/paceq")
		build.Dir = moduleRootForTests(t)
		if out, err := build.CombinedOutput(); err != nil {
			liveErr = fmt.Errorf("build paceq: %v\n%s", err, out)
			return
		}
		liveBin = bin
	})
	if liveErr != nil {
		t.Fatalf("%v", liveErr)
	}
	return liveBin
}

// liveDaemon is one running paceq serve subprocess with its socket on.
type liveDaemon struct {
	proc   *exec.Cmd
	sock   string
	stderr *bytes.Buffer
	mu     sync.Mutex
	exited chan int
}

func startLiveDaemon(t *testing.T, dir string) *liveDaemon {
	t.Helper()
	bin := liveBinary(t)
	sock := filepath.Join(t.TempDir(), "d.sock")

	cmd := exec.Command(bin, "serve", "--socket", sock)
	cmd.Dir = dir
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	d := &liveDaemon{proc: cmd, sock: sock, stderr: stderr, exited: make(chan int, 1)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start serve: %v", err)
	}
	go func() {
		err := cmd.Wait()
		code := 0
		if err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				code = ee.ExitCode()
			} else {
				code = -1
			}
		}
		d.exited <- code
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	// Readiness is the daemon answering its own health surface.
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := apiProbeForTest(sock)
		if err == nil {
			return d
		}
		select {
		case code := <-d.exited:
			d.mu.Lock()
			out := d.stderr.String()
			d.mu.Unlock()
			t.Fatalf("the daemon exited %d before it was ready:\n%s", code, out)
		default:
		}
		if time.Now().After(deadline) {
			d.mu.Lock()
			out := d.stderr.String()
			d.mu.Unlock()
			t.Fatalf("the daemon never became ready:\n%s", out)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// runAgainst runs the in-process command line with PACEQ_SOCKET pinned.
func runAgainst(t *testing.T, dir, sock string, args ...string) result {
	t.Helper()
	return runCLI(t, dir, map[string]string{"PACEQ_SOCKET": sock}, args...)
}

// applyJob applies one job file through the direct path while the daemon is
// down, the way any project is set up.
func applyJob(t *testing.T, dir, body string) {
	t.Helper()
	jobs := filepath.Join(dir, "jobs")
	if err := os.MkdirAll(jobs, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobs, "job.yaml"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	res := runCLI(t, dir, nil, "apply")
	if res.code != ExitOK {
		t.Fatalf("apply exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
}

const quickJob = `name: quick
description: One step that cannot fail.
steps:
  - name: only
    run: ["/bin/true"]
`

const failingWireJob = `name: failing
description: One step that always fails.
steps:
  - name: die
    run: ["/bin/false"]
`

// TestRunThroughTheSocketRecordsTheAPIClientAsActor is criterion 3: a write
// with the daemon up goes over the socket, proven by run_events.actor.
func TestRunThroughTheSocketRecordsTheAPIClientAsActor(t *testing.T) {
	dir := newProjectWithState(t)
	applyJob(t, dir, quickJob)
	d := startLiveDaemon(t, dir)

	res := runAgainst(t, dir, d.sock, "run", "quick")
	if res.code != ExitOK {
		t.Fatalf("run exited %d\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}

	st := openReadOnlyProject(t, dir)
	defer func() { _ = st.Close() }()
	rows, err := st.ListRuns(context.Background(), store.RunFilter{})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read back %d runs (err=%v), want 1", len(rows), err)
	}
	events, err := st.RunEvents(context.Background(), rows[0].ID)
	if err != nil || len(events) == 0 {
		t.Fatalf("no events for %s (err=%v)", rows[0].ID, err)
	}
	if events[0].Kind != "run.queued" || events[0].Actor != "api" {
		t.Fatalf("first event is (%s, actor %s), want (run.queued, api)",
			events[0].Kind, events[0].Actor)
	}
}

// The same write with the daemon down records actor cli: criterion 4.
func TestRunWithTheDaemonDownRecordsTheCLIClientAsActor(t *testing.T) {
	dir := newProjectWithState(t)
	applyJob(t, dir, quickJob)
	silent := filepath.Join(t.TempDir(), "nobody.sock")

	res := runAgainst(t, dir, silent, "run", "quick")
	if res.code != ExitOK {
		t.Fatalf("run exited %d\nstderr:\n%s", res.code, res.stderr)
	}

	st := openReadOnlyProject(t, dir)
	defer func() { _ = st.Close() }()
	rows, err := st.ListRuns(context.Background(), store.RunFilter{})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read back %d runs (err=%v), want 1", len(rows), err)
	}
	events, _ := st.RunEvents(context.Background(), rows[0].ID)
	if len(events) == 0 || !strings.HasPrefix(events[0].Actor, "cli:") {
		t.Fatalf("actor is %v, want cli:<uid>", events)
	}
}

// A failed run waited for over the socket still exits 5: the mapping of
// --wait outcomes does not depend on who executed the work.
func TestASocketRunThatFailsStillExitsFive(t *testing.T) {
	dir := newProjectWithState(t)
	applyJob(t, dir, failingWireJob)
	d := startLiveDaemon(t, dir)

	res := runAgainst(t, dir, d.sock, "run", "failing")
	if res.code != ExitRunFailed {
		t.Fatalf("run exited %d, want 5\nstdout:\n%s\nstderr:\n%s",
			res.code, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stderr, RUN_FAILED_STEP_TEXT) {
		t.Errorf("the failure does not name the step's reason:\n%s", res.stderr)
	}
}

// RUN_FAILED_STEP_TEXT is the phrase the local path prints for this reason.
const RUN_FAILED_STEP_TEXT = `step "die" failed`

// Criterion 5: --socket none forces direct even while a daemon runs, and the
// held lock is then an immediate exit 6.
func TestSocketNoneWhileADaemonRunsExitsBusyAtOnce(t *testing.T) {
	dir := newProjectWithState(t)
	applyJob(t, dir, quickJob)
	startLiveDaemon(t, dir)

	began := time.Now()
	res := runCLI(t, dir, map[string]string{"PACEQ_SOCKET": "none"}, "run", "quick")
	took := time.Since(began)
	if res.code != ExitBusy {
		t.Fatalf("--socket none against a running daemon exited %d, want 6\nstderr:\n%s",
			res.code, res.stderr)
	}
	if took > 2*time.Second {
		t.Errorf("the refusal took %s; forcing direct must not wait on anything", took)
	}
}

// Criterion 3 for the other verb: cancel through the socket names the api as
// the requester.
func TestCancelThroughTheSocketNamesTheAPIAsRequester(t *testing.T) {
	dir := newProjectWithState(t)
	applyJob(t, dir, quickJob)
	d := startLiveDaemon(t, dir)

	created := runAgainst(t, dir, d.sock, "run", "quick", "-o", "json")
	if created.code != ExitOK {
		t.Fatalf("seed run exited %d:\n%s", created.code, created.stderr)
	}
	var envelope struct {
		Run struct {
			ID string `json:"id"`
		} `json:"run"`
	}
	if err := json.Unmarshal([]byte(created.stdout), &envelope); err != nil {
		t.Fatalf("decode the seed record: %v\n%s", err, created.stdout)
	}

	cancelled := runAgainst(t, dir, d.sock, "runs", "cancel", envelope.Run.ID, "--reason", "checked")
	if cancelled.code != ExitOK {
		t.Fatalf("cancel exited %d\nstderr:\n%s", cancelled.code, cancelled.stderr)
	}

	st := openReadOnlyProject(t, dir)
	defer func() { _ = st.Close() }()
	requested, by, err := st.CancelRequested(context.Background(), envelope.Run.ID)
	if err != nil || !requested || by != "api" {
		t.Fatalf("cancellation not on record for api (requested=%v by=%q err=%v)", requested, by, err)
	}
}

// Apply through the socket produces the same JSON report shape as direct.
func TestApplyThroughTheSocketMatchesTheDirectReportShape(t *testing.T) {
	dir := newProjectWithState(t)
	jobs := filepath.Join(dir, "jobs")
	if err := os.MkdirAll(jobs, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobs, "job.yaml"), []byte(quickJob), 0o600); err != nil {
		t.Fatal(err)
	}
	d := startLiveDaemon(t, dir)

	res := runAgainst(t, dir, d.sock, "apply", "-o", "json")
	if res.code != ExitOK {
		t.Fatalf("apply exited %d\nstderr:\n%s", res.code, res.stderr)
	}
	document := res.json(t)
	applied, ok := document["applied"].([]any)
	if !ok || len(applied) != 2 {
		// init's example job is in the same directory and applies too.
		t.Fatalf("the applied list is thin: %v", document)
	}
	var entry map[string]any
	for _, raw := range applied {
		if candidate, _ := raw.(map[string]any); candidate["job"] == "quick" {
			entry = candidate
		}
	}
	if entry == nil {
		t.Fatalf("the quick job is not in the applied list: %v", applied)
	}
	for _, field := range []string{"job", "file", "version", "spec_hash", "file_sha256"} {
		if _, has := entry[field]; !has {
			t.Errorf("the applied entry lacks %q: %v", field, entry)
		}
	}

	again := runAgainst(t, dir, d.sock, "apply", "-o", "json")
	if again.code != ExitOK {
		t.Fatalf("second apply exited %d", again.code)
	}
	repeat := again.json(t)
	if unchanged, ok := repeat["unchanged"].([]any); !ok || len(unchanged) != 2 {
		t.Fatalf("applying twice is not idempotent: %v", repeat)
	}
}

// openReadOnlyProject opens one project's state read-only for assertions.
func openReadOnlyProject(t *testing.T, dir string) *store.Store {
	t.Helper()
	st, err := store.OpenReadOnly(context.Background(),
		filepath.Join(dir, ".paceq", store.DatabaseFileName), store.Options{})
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	return st
}

// apiProbeForTest asks the daemon's health surface once.
func apiProbeForTest(sock string) error {
	return api.Probe(sock, "dev")
}

// moduleRootForTests walks up from the working directory until it finds the
// module, so a build of ./cmd/paceq runs against this checkout.
func moduleRootForTests(t *testing.T) string {
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
			t.Fatal("no go.mod above the tests")
		}
		dir = parent
	}
}
