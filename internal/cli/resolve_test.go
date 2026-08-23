package cli

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/api"
	"github.com/a-holm/paceq/internal/store"
)

// TestSocketResolutionFollowsTheDocumentedOrder walks the axis once:
// --socket flag, then PACEQ_SOCKET, then XDG_RUNTIME_DIR/paceq.sock, then
// the state directory as the last resort. none disables the socket from
// either place that can name it.
func TestSocketResolutionFollowsTheDocumentedOrder(t *testing.T) {
	cases := []struct {
		name     string
		flag     string
		environ  map[string]string
		wantPath string
		wantOff  bool
	}{
		{"nothing said falls to the state directory", "", nil, "/state/paceq.sock", false},
		{"the flag wins over everything", "/flag/s.sock", map[string]string{"PACEQ_SOCKET": "/env/e.sock"}, "/flag/s.sock", false},
		{"none on the flag forces direct even with an env path", "none", map[string]string{"PACEQ_SOCKET": "/env/e.sock"}, "", true},
		{"the env names the socket without a flag", "", map[string]string{"PACEQ_SOCKET": "/env/e.sock"}, "/env/e.sock", false},
		{"the env can also say none", "", map[string]string{"PACEQ_SOCKET": "none"}, "", true},
		{"XDG runtime is next", "", map[string]string{"XDG_RUNTIME_DIR": "/xdg"}, "/xdg/paceq.sock", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &globals{socket: tc.flag}
			env := Env{Getenv: lookup(tc.environ)}
			got := resolveSocket(g, env, "/state")
			if got.off != tc.wantOff || got.path != tc.wantPath {
				t.Fatalf("resolveSocket = %+v, want path %q off=%v", got, tc.wantPath, tc.wantOff)
			}
		})
	}
}

// A read command answers while the daemon is down, and says so: one stderr
// line for people, daemon_up false in the JSON document for scripts.
func TestReadsMarkADownDaemonOnStderrAndInTheEnvelope(t *testing.T) {
	dir := newProjectWithState(t)
	silent := filepath.Join(t.TempDir(), "silent.sock")

	cases := []struct {
		name       string
		args       []string
		wantSubstr string // a fragment the wrapped document must carry
	}{
		{"status wraps its array", []string{"status", "-o", "json"}, "jobs"},
		{"runs list wraps its array", []string{"runs", "list", "-o", "json"}, "runs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runCLI(t, dir, map[string]string{
				"PACEQ_SOCKET": silent,
				"PACEQ_OUTPUT": "json",
			}, tc.args...)
			if res.code != ExitOK {
				t.Fatalf("exit %d, want 0\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
			}
			document := res.json(t)
			if up, ok := document["daemon_up"].(bool); !ok || up {
				t.Fatalf("daemon_up is %v, want false; document: %v", document["daemon_up"], document)
			}
			if _, ok := document[tc.wantSubstr]; !ok {
				t.Fatalf("the document lost its %s payload: %v", tc.wantSubstr, document)
			}
			if !strings.Contains(res.stderr, "daemon down") {
				t.Errorf("stderr does not mark the daemon down:\n%s", res.stderr)
			}
		})
	}

	show := runCLIQueuedRun(t, dir, silent, "-o", "json")
	document := show.json(t)
	if up, ok := document["daemon_up"].(bool); !ok || up {
		t.Fatalf("runs show carries daemon_up %v, want false: %v", document["daemon_up"], document)
	}
	if !strings.Contains(show.stderr, "daemon down") {
		t.Errorf("runs show does not mark the daemon down:\n%s", show.stderr)
	}
}

// With no socket named at all the default path is probed like any other:
// the fact reported is whether a daemon lives in this project, not what the
// caller typed. A project without a running daemon therefore marks its
// reads, identically in both suite passes, which is what lets the goldens
// be shared.
func TestReadsWithTheDefaultPathMarkADownDaemon(t *testing.T) {
	dir := newProjectWithState(t)

	res := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"}, "status")
	if res.code != ExitOK {
		t.Fatalf("exit %d\nstderr:\n%s", res.code, res.stderr)
	}
	if !strings.Contains(res.stderr, "daemon down") {
		t.Errorf("no daemon runs here, yet the read stays quiet:\n%s", res.stderr)
	}
	if up, ok := res.json(t)["daemon_up"].(bool); !ok || up {
		t.Error("daemon_up is missing or true although the daemon is down")
	}

	// --socket none is the one setting that skips the probe: the caller
	// said there is no daemon to ask about.
	quiet := runCLI(t, dir, map[string]string{
		"PACEQ_OUTPUT": "json",
		"PACEQ_SOCKET": "none",
	}, "status")
	if strings.Contains(quiet.stderr, "daemon down") {
		t.Errorf("none was named, yet stderr marks the daemon down:\n%s", quiet.stderr)
	}
	var bare []any
	if err := json.Unmarshal([]byte(strings.TrimSpace(quiet.stdout)), &bare); err != nil {
		t.Fatalf("with none the listing is a bare JSON array: %v\n%s", err, quiet.stdout)
	}
}

// --socket none plus a held lock is the immediate exit 6 story: the user
// forbade the daemon, so there is nothing to retry.
func TestSocketNoneWithAHeldLockIsBusyAtOnce(t *testing.T) {
	dir := newProjectWithState(t)
	lock, err := store.AcquireStateLock(filepath.Join(dir, ".paceq"))
	if err != nil {
		t.Fatalf("hold the lock: %v", err)
	}
	defer func() { _ = lock.Release() }()

	res := runCLI(t, dir, map[string]string{
		"PACEQ_SOCKET": "none",
		"PACEQ_OUTPUT": "json",
	}, "runs", "cancel", "01AAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	if res.code != ExitBusy {
		t.Fatalf("--socket none with a held lock exited %d, want 6\nstderr:\n%s", res.code, res.stderr)
	}
	if !strings.Contains(res.stderr, "PQ5002") {
		t.Errorf("the refusal is not the state-lock one:\n%s", res.stderr)
	}
}

// The startup race closes with exactly one bounded retry phase: the writer
// finds the lock held, tries the socket again within the deadline, and the
// listener that appeared in between answers.
func TestAFlockFailureRetriesTheSocketOnce(t *testing.T) {
	dir := newProjectWithState(t)
	stateDir := filepath.Join(dir, ".paceq")
	sock := filepath.Join(t.TempDir(), "late.sock")

	lock, err := store.AcquireStateLock(stateDir)
	if err != nil {
		t.Fatalf("hold the lock first: %v", err)
	}
	defer func() { _ = lock.Release() }()

	// The daemon half-started: it owns the lock and its listener arrives
	// while the writer is inside the retry phase, and stays up.
	shutdown := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		stop, err := api.Serve(context.Background(), api.Config{
			Path: sock,
			Deps: api.Deps{Version: "test"},
			Log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if err != nil {
			t.Errorf("serve: %v", err)
			return
		}
		<-shutdown
		stop(context.Background())
	}()

	env := environFor(t, dir, sock)
	g := &globals{}
	plan := planWrite(context.Background(), env, g)
	close(shutdown)
	<-stopped

	if plan.err != nil || plan.client == nil {
		t.Fatalf("the write did not end up on the socket: plan=%+v err=%v", plan, plan.err)
	}
	defer func() { _ = plan.client.Close() }()
}

// environFor builds an Env the way runCLI does but returns it, because the
// resolution helpers take an Env directly.
func environFor(t *testing.T, dir, sock string) Env {
	t.Helper()
	return Env{
		Stdout: io.Discard,
		Stderr: io.Discard,
		Dir:    dir,
		Getenv: lookup(map[string]string{"PACEQ_SOCKET": sock}),
	}
}

// newProjectWithState gives every test one initialised project.
func newProjectWithState(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	res := runCLI(t, dir, nil, "init")
	if res.code != ExitOK {
		t.Fatalf("init exited %d:\n%s", res.code, res.stderr)
	}
	return dir
}

// runCLIQueuedRun creates one real queued run through the direct path and
// then runs `runs show` against the given socket setting, so the envelope of
// a single-run read is covered without a daemon.
func runCLIQueuedRun(t *testing.T, dir, sock string, extra ...string) result {
	t.Helper()
	st, err := store.OpenState(context.Background(), filepath.Join(dir, ".paceq"), store.Options{})
	if err != nil {
		t.Fatalf("open the state: %v", err)
	}
	defer func() { _ = st.Close() }()
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	version, _, err := st.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "queued",
		SpecHash: "sha256:test-queued",
		SpecJSON: `{"schema":"paceq.job.v1","name":"queued","steps":[{"name":"only","run":["/bin/true"],"shell":false}]}`,
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	run, err := st.CreateRunWithSteps(context.Background(), store.NewRun{
		JobName:      "queued",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "test",
		Steps:        []store.NewStep{{Name: "only"}},
	})
	if err != nil {
		t.Fatalf("seed the run: %v", err)
	}
	args := append([]string{"runs", "show", run.ID}, extra...)
	return runCLI(t, dir, map[string]string{"PACEQ_SOCKET": sock}, args...)
}
