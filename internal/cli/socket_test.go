package cli

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// plantSocket leaves a listening unix socket at path, at the mode a test wants
// to see refused or accepted, and closes it when the test ends.
func plantSocket(t *testing.T, path string, mode os.FileMode) {
	t.Helper()

	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s to %#o: %v", path, mode, err)
	}
}

// TestSocketVerdictRefusesEveryFileTheModelForbids walks the file half of the
// check once. The caller's uid is a parameter rather than this process's, so a
// socket owned by somebody else can be judged without a second account.
func TestSocketVerdictRefusesEveryFileTheModelForbids(t *testing.T) {
	dir := t.TempDir()
	mine := os.Geteuid()

	good := filepath.Join(dir, "good.sock")
	plantSocket(t, good, 0o600)

	group := filepath.Join(dir, "group.sock")
	plantSocket(t, group, 0o660)

	world := filepath.Join(dir, "world.sock")
	plantSocket(t, world, 0o777)

	regular := filepath.Join(dir, "regular.sock")
	if err := os.WriteFile(regular, []byte("not a socket"), 0o600); err != nil {
		t.Fatalf("write %s: %v", regular, err)
	}

	cases := []struct {
		name   string
		path   string
		euid   int
		refuse string // a fragment the refusal must carry, empty when it must pass
	}{
		{"the daemon's own socket is accepted", good, mine, ""},
		{"a group readable socket is still ours", group, mine, ""},
		{"world write is refused whoever owns it", world, mine, "every account"},
		{"a regular file at the socket path is refused", regular, mine, "not a socket"},
		{"another account's socket is refused", good, mine + 1, "owns it"},
		{"root may talk to another account's daemon", good, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info, err := os.Stat(tc.path)
			if err != nil {
				t.Fatalf("stat %s: %v", tc.path, err)
			}
			got := socketVerdict(tc.path, info, tc.euid)
			if tc.refuse == "" {
				if got != nil {
					t.Fatalf("socketVerdict refused a socket it must accept: %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("socketVerdict accepted a socket it must refuse")
			}
			var refused *untrustedSocket
			if !errors.As(got, &refused) {
				t.Fatalf("the refusal is not an *untrustedSocket: %T %v", got, got)
			}
			if !strings.Contains(got.Error(), tc.path) {
				t.Errorf("the refusal does not name the path: %v", got)
			}
			if !strings.Contains(got.Error(), tc.refuse) {
				t.Errorf("the refusal does not say %q: %v", tc.refuse, got)
			}
		})
	}
}

// TestPeerVerdictJudgesWhoIsActuallyListening covers the half of the check
// that runs on the open connection. "cannot tell" must never refuse: away from
// Linux there is no SO_PEERCRED, and a refusal there would be a guess.
func TestPeerVerdictJudgesWhoIsActuallyListening(t *testing.T) {
	cases := []struct {
		name    string
		uid     int
		known   bool
		euid    int
		refused bool
	}{
		{"our own daemon answers", 1000, true, 1000, false},
		{"another account answers", 1001, true, 1000, true},
		{"root may talk to anybody's daemon", 1001, true, 0, false},
		{"a platform that cannot tell does not guess", 0, false, 1000, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := peerVerdict("/run/paceq/paceq.sock", tc.uid, tc.known, tc.euid)
			if tc.refused != (err != nil) {
				t.Fatalf("peerVerdict = %v, want refused=%v", err, tc.refused)
			}
		})
	}
}

// TestSocketResolutionFollowsTheDocumentedOrder walks the axis once: the
// --socket flag, then PACEQ_SOCKET, then $XDG_RUNTIME_DIR/paceq.sock, then the
// state directory as the last resort. none disables the socket from either
// place that can name it.
func TestSocketResolutionFollowsTheDocumentedOrder(t *testing.T) {
	cases := []struct {
		name     string
		flag     string
		environ  map[string]string
		wantPath string
		wantOff  bool
	}{
		{"nothing said falls to the state directory", "", nil, "/state/paceq.sock", false},
		{"the flag wins over everything", "/flag/s.sock", map[string]string{"PACEQ_SOCKET": "/env/e.sock", "XDG_RUNTIME_DIR": "/xdg"}, "/flag/s.sock", false},
		{"none on the flag forces direct even with an env path", "none", map[string]string{"PACEQ_SOCKET": "/env/e.sock"}, "", true},
		{"the env names the socket without a flag", "", map[string]string{"PACEQ_SOCKET": "/env/e.sock", "XDG_RUNTIME_DIR": "/xdg"}, "/env/e.sock", false},
		{"the env can also say none", "", map[string]string{"PACEQ_SOCKET": "none"}, "", true},
		{"the runtime directory is next", "", map[string]string{"XDG_RUNTIME_DIR": "/xdg"}, "/xdg/paceq.sock", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveSocket(&globals{socket: tc.flag}, testEnv("", tc.environ), "/state")
			if got.off != tc.wantOff || got.path != tc.wantPath {
				t.Fatalf("resolveSocket = %+v, want path %q off=%v", got, tc.wantPath, tc.wantOff)
			}
		})
	}
}

// TestDaemonSocketIsQuietWhenNothingIsThere: an absent socket is the other half
// of the dual-mode design, not a failure. It must never become a refusal, or
// every command on a machine without a daemon would stop working.
func TestDaemonSocketIsQuietWhenNothingIsThere(t *testing.T) {
	path, err := daemonSocket(testEnv(t.TempDir(), nil), &globals{})
	if err != nil {
		t.Fatalf("an empty state directory refused the command: %v", err)
	}
	if path != "" {
		t.Fatalf("daemonSocket found %q where nothing is listening", path)
	}
}

// TestDaemonSocketAcceptsTheDaemonsOwnSocket is the guard against a check that
// is satisfied by refusing everything. It goes through the last resort of the
// resolution order, which is where a project's own daemon puts its socket.
func TestDaemonSocketAcceptsTheDaemonsOwnSocket(t *testing.T) {
	dir := newStateDirWithSocket(t, 0o600)

	path, err := daemonSocket(testEnv(dir, nil), &globals{})
	if err != nil {
		t.Fatalf("the daemon's own socket was refused: %v", err)
	}
	if path == "" {
		t.Fatal("daemonSocket ignored a socket it should have accepted")
	}
}

// TestDaemonSocketRefusesAPlantedSocket: a world writable socket stops the
// command with the path and the mode in the message, and the exit code a
// script branches on is the validation one.
func TestDaemonSocketRefusesAPlantedSocket(t *testing.T) {
	dir := newStateDirWithSocket(t, 0o777)
	socketPath := filepath.Join(dir, stateDirName, socketName)

	path, err := daemonSocket(testEnv(dir, nil), &globals{})
	if err == nil {
		t.Fatalf("a world writable socket was accepted as %q", path)
	}
	if !strings.Contains(err.Error(), socketPath) {
		t.Errorf("the refusal does not name the path: %v", err)
	}
	if !strings.Contains(err.Error(), "0777") {
		t.Errorf("the refusal does not name the mode: %v", err)
	}
	if code := socketRefusedError(err).code; code != ExitValidation {
		t.Errorf("a refused socket exits %d, want %d", code, ExitValidation)
	}
}

// TestARefusedSocketDoesNotFallBackToTheDirectPath is the whole point of the
// refusal: the command has to stop, because a silent change of transport is
// how an attempt to answer for the daemon goes unnoticed.
func TestARefusedSocketDoesNotFallBackToTheDirectPath(t *testing.T) {
	dir := newStateDirWithSocket(t, 0o777)

	res := runCLI(t, dir, nil, "notifications", "retry", "1")
	if res.code != ExitValidation {
		t.Fatalf("exit %d, want %d\nstdout:\n%s\nstderr:\n%s",
			res.code, ExitValidation, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stderr, "refusing the daemon socket") {
		t.Errorf("the user is not told why the command stopped:\n%s", res.stderr)
	}
}

// TestEveryUnixDialGoesThroughTheCheckedHelper is the guard that keeps the
// ownership check from being lost the next time the socket layer is
// reorganised, which is exactly how it was lost before (#185). socket.go owns
// the only dial; anything else that reaches a unix socket by hand skips the
// peer check without anybody noticing.
func TestEveryUnixDialGoesThroughTheCheckedHelper(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") || name == "socket.go" {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for number, line := range strings.Split(string(body), "\n") {
			if strings.Contains(line, `"unix"`) && strings.Contains(line, "Dial") {
				t.Errorf("%s:%d dials a unix socket outside socket.go, so the "+
					"peer check does not run:\n\t%s", name, number+1, strings.TrimSpace(line))
			}
		}
	}
}

// newStateDirWithSocket gives back a project directory whose state directory
// holds one listening socket at the mode the test wants judged.
func newStateDirWithSocket(t *testing.T, mode os.FileMode) string {
	t.Helper()

	dir := t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	plantSocket(t, filepath.Join(stateDir, socketName), mode)
	return dir
}

// testEnv is an Env holding only what a test puts in it, so a socket path set
// on the developer's machine cannot change what resolution answers.
func testEnv(dir string, environ map[string]string) Env {
	return Env{Stdout: io.Discard, Stderr: io.Discard, Dir: dir, Getenv: lookup(environ)}
}
