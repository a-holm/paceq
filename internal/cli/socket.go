package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// The daemon socket seen from the client side: which file a command dials,
// and whether that file may be trusted once it is found.
//
// paceq authorises on the socket by file permissions alone (SECURITY.md TG2).
// That model only holds while the client checks the file too, because whoever
// creates the path first is who the CLI ends up talking to. Every dial in this
// package therefore goes through dialSocket, and every command that looks for
// a daemon goes through daemonSocket. socket_test.go fails the build if a
// second dial appears elsewhere.

// socketName is the file name of the daemon's socket inside its directory.
const socketName = "paceq.sock"

// socketOff is the word that turns the socket off wherever a socket can be
// named. It disables the dial and the liveness probe both: the caller said
// there is no daemon to ask about, so nothing asks.
const socketOff = "none"

// socketSetting is what resolution concluded: where the socket is, or that the
// caller turned it off.
type socketSetting struct {
	// path is the socket to dial. Empty when off, and empty when nothing
	// named one and there is no state directory to hold one.
	path string

	// off means socketOff was named. Writes then go to the state directory
	// even while a daemon runs, because two writers are forbidden either
	// way, and reads do not ask whether anybody is there.
	off bool
}

// resolveSocket applies the documented order: --socket, then PACEQ_SOCKET,
// then $XDG_RUNTIME_DIR/paceq.sock, then the state directory as the last
// resort (03 section 3.6).
//
// It touches nothing on disk. Which socket a command means and whether that
// socket may be trusted are two questions, and answering them in one place is
// how one of them ends up skipped.
func resolveSocket(g *globals, env Env, stateDir string) socketSetting {
	switch {
	case g.socket == socketOff:
		return socketSetting{off: true}
	case g.socket != "":
		return socketSetting{path: g.socket}
	}
	switch named := env.Getenv("PACEQ_SOCKET"); {
	case named == socketOff:
		return socketSetting{off: true}
	case named != "":
		return socketSetting{path: named}
	}
	if runtimeDir := env.Getenv("XDG_RUNTIME_DIR"); runtimeDir != "" {
		return socketSetting{path: filepath.Join(runtimeDir, socketName)}
	}
	if stateDir == "" {
		return socketSetting{}
	}
	return socketSetting{path: filepath.Join(stateDir, socketName)}
}

// untrustedSocket is a socket file paceq will not talk to: the wrong owner, a
// mode that invites the whole machine, or a path that is not a socket at all.
// It is loud on purpose. Falling back to the direct path would turn somebody
// else's socket into a silent change of transport, the command would still
// appear to work, and the next one would walk into the same file.
type untrustedSocket struct {
	path string
	why  string
}

func (e *untrustedSocket) Error() string {
	return fmt.Sprintf("refusing the daemon socket at %s: %s", e.path, e.why)
}

// socketRefusal is a write the daemon refused on its side. The code is the
// daemon's error class ("not_found", "invalid_state"); the message is its
// own words for what went wrong.
type socketRefusal struct {
	code    string
	message string
}

func (e *socketRefusal) Error() string { return e.message }

// daemonSocket answers which socket a command may dial. An empty path with no
// error means no daemon serves this project and the caller writes directly,
// which is the other half of the dual-mode design rather than a failure. An
// error means a socket is there that paceq refuses to trust.
func daemonSocket(env Env, g *globals) (string, error) {
	setting := resolveSocket(g, env, g.stateDirOrEmpty(env))
	if setting.off || setting.path == "" {
		return "", nil
	}
	present, err := checkSocketFile(setting.path)
	if err != nil || !present {
		return "", err
	}
	return setting.path, nil
}

// checkSocketFile decides what the file at path is before anything connects
// to it. Nothing there is not a failure, so it answers false with no error;
// anything there that fails the two rules is an *untrustedSocket.
//
// The answer is about the file only. It is made before the dial and can be
// overtaken by whoever may write the containing directory, which is why
// dialSocket asks the kernel again once the connection stands.
func checkSocketFile(path string) (bool, error) {
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, &untrustedSocket{path: path, why: fmt.Sprintf("it cannot be read: %v", err)}
	}
	return true, socketVerdict(path, info, os.Geteuid())
}

// socketVerdict is the trust decision on one file, with the filesystem and the
// caller's identity passed in. Root is exempt from the ownership rule: it
// legitimately administers another user's daemon, and it owns everything on
// the machine anyway, so the check could not tell the two cases apart.
func socketVerdict(path string, info fs.FileInfo, euid int) error {
	if info.Mode()&fs.ModeSocket == 0 {
		return &untrustedSocket{
			path: path,
			why:  fmt.Sprintf("it is not a socket: mode %s", info.Mode()),
		}
	}
	if perm := info.Mode().Perm(); perm&0o002 != 0 {
		return &untrustedSocket{
			path: path,
			why:  fmt.Sprintf("mode %#o lets every account on this machine talk to the daemon", perm),
		}
	}
	owner, known := fileOwner(info)
	if known && owner != euid && euid != 0 {
		return &untrustedSocket{
			path: path,
			why:  fmt.Sprintf("uid %d owns it, and this command runs as uid %d", owner, euid),
		}
	}
	return nil
}

// dialSocket opens one connection to the daemon and asks the kernel who
// answered. The credentials it reads are the ones the listening process had
// when it called listen, so this half of the ownership check cannot be beaten
// by swapping the file between the stat and the connect.
func dialSocket(ctx context.Context, path string) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	uid, known := peerUID(conn)
	if err := peerVerdict(path, uid, known, os.Geteuid()); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// peerVerdict refuses a connection whose far end belongs to another user.
// known is false where the platform cannot say, and there the file check made
// before the dial is all paceq has.
func peerVerdict(path string, uid int, known bool, euid int) error {
	if !known || euid == 0 || uid == euid {
		return nil
	}
	return &untrustedSocket{
		path: path,
		why:  fmt.Sprintf("uid %d listens on it, and this command runs as uid %d", uid, euid),
	}
}

// socketClient is the only http.Client in this package that reaches a unix
// socket, so the peer check cannot be skipped by building one by hand.
func socketClient(path string, timeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialSocket(ctx, path)
			},
		},
		Timeout: timeout,
	}
}

// daemonResponds reports whether a daemon paceq trusts answers right now. Half
// a second is generous for a local unix socket. A dead socket file, a refused
// dial and a socket another user listens on all read as "down": a read has
// nothing to gain from a stranger's answer, and refusing to print history
// because somebody planted a file would hand them the outage for free.
func daemonResponds(socketPath string) bool {
	if socketPath == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := dialSocket(ctx, socketPath)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// daemonHoldsState refuses a direct write while a daemon holds this state
// directory and there is no socket to send the write to.
//
// The two questions are separate and used to be answered by the same stat:
// which socket to dial, and whether anybody is there. A daemon started without
// --socket, which is what the shipped unit does, has no socket file and still
// owns the state. Falling through to the direct path then hands the operator
// the flock's refusal, which names a lock file and a pid rather than the flag
// that would have let the write through (#215).
//
// It answers nil whenever it cannot tell. The flock is still what forbids two
// writers; this only decides which refusal the operator reads.
func daemonHoldsState(ctx context.Context, env Env, g *globals, what string) *Error {
	if resolveSocket(g, env, g.stateDirOrEmpty(env)).off {
		// The caller said there is no daemon to ask about, so nothing asks,
		// and the write goes to the state directory as documented.
		return nil
	}
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return nil
	}
	defer func() { _ = ro.Close() }()
	owner, running, err := ro.DaemonSession(ctx)
	if err != nil || !running {
		return nil
	}
	return &Error{
		code: ExitBusy,
		what: fmt.Sprintf("%s: a daemon holds this state (pid %d, paceq %s, started %s) "+
			"and exposes no socket to send the write to",
			what, owner.PID, owner.Version, owner.StartedAt.Format(time.RFC3339)),
		next: []string{
			"start the daemon with --socket, so the CLI reaches it instead of writing behind it",
			"or stop it first: systemctl stop paceq, or kill " + strconv.Itoa(owner.PID),
		},
	}
}

// noBody is the request of a route whose whole argument is its path. It has to
// be written down at the call site, and that is the point: a helper that sent
// an empty object whenever nothing was passed is what let every sensor
// argument disappear between the command line and the daemon (#207). A route
// that does carry arguments can no longer reach the wire without them unless
// somebody types this word into the diff.
type noBody struct{}

// sockPostJSON sends one POST with a JSON body over a unix socket and reports
// only whether the daemon accepted it. The body is marshalled rather than
// pasted together, so a value carrying a quote or a backslash arrives as
// itself.
func sockPostJSON(ctx context.Context, socketPath, path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix"+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return sendSocket(ctx, socketPath, req)
}

func sendSocket(ctx context.Context, socketPath string, req *http.Request) error {
	resp, err := socketClient(socketPath, 5*time.Second).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("daemon returned %d", resp.StatusCode)
	}
	return nil
}

// postForJSON POSTs one JSON body over the daemon's unix socket and decodes
// the JSON answer. A non 2xx answer is a refusal, not an outage: it comes
// back as a socketRefusal naming the daemon's error class, so the caller can
// render it exactly like the same refusal made in this process.
func postForJSON(ctx context.Context, socketPath, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix"+path,
		bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := socketClient(socketPath, 10*time.Second).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var doc struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&doc); err == nil && doc.Error != "" {
			return &socketRefusal{code: doc.Code, message: doc.Error}
		}
		return fmt.Errorf("the daemon answered %d for %s", resp.StatusCode, path)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// socketRefusedError is what the user sees when a command found a socket it
// will not talk to. Exit 4: nothing is broken, something has to be corrected
// before paceq will reach this state directory through a daemon.
func socketRefusedError(err error) *Error {
	return &Error{
		code: ExitValidation,
		what: err.Error(),
		err:  err,
		next: []string{
			"ls -l the path: paceq uses a socket only when it owns it and no other account may write to it",
			"remove it if a crash left it behind, then run the command again",
		},
	}
}

// stopOnRefusal separates the two ways a socket call can fail. An outage is
// something a command may write around, and nil says so. A refusal never is.
func stopOnRefusal(err error) *Error {
	var refused *untrustedSocket
	if errors.As(err, &refused) {
		return socketRefusedError(refused)
	}
	return nil
}
