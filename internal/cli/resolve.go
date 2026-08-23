package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/a-holm/paceq/internal/api"
	"github.com/a-holm/paceq/internal/store"
)

// This file is the dual-mode decision (M2-08): which transport a command
// uses, and what it says about a daemon that is not there.
//
// Two axes stay independent on purpose. Transport (socket versus direct) is
// about who writes the state. Rendering (text versus JSON, via -o,
// PACEQ_OUTPUT and the terminal) is about how the answer looks. Nothing in
// this file may let one imply the other: a socket write renders exactly like
// a direct one, and a daemon-down note reaches stderr in JSON mode too, so a
// jq pipeline keeps working while the human still hears the news.

// socketName is the file name inside the chosen directory. plans:docs/PLAN.md
// documents $RUNTIME_DIR/paceq.sock; the issue text's pulseq.sock spelling
// predates the rename.
const socketName = "paceq.sock"

// socketSetting is what resolution concluded about the unix socket.
type socketSetting struct {
	// path is where to dial or listen. Empty only when off.
	path string

	// off means "none" was named: writes go to the state directory even if
	// a daemon is running, because two writers are forbidden, and reads do
	// not bother asking whether anybody is there.
	off bool
}

// resolveSocket applies the documented order: --socket, then PACEQ_SOCKET,
// then $XDG_RUNTIME_DIR/paceq.sock, then the state directory as the last
// resort. The XDG fallback is provisional until M2-11 pins the systemd
// RuntimeDirectory contract; nothing else invents paths before then.
func resolveSocket(g *globals, env Env, stateDir string) socketSetting {
	if g.socket == "none" {
		return socketSetting{off: true}
	}
	if g.socket != "" {
		return socketSetting{path: g.socket}
	}
	if fromEnv := env.Getenv("PACEQ_SOCKET"); fromEnv != "" {
		if fromEnv == "none" {
			return socketSetting{off: true}
		}
		return socketSetting{path: fromEnv}
	}
	if xdg := env.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		return socketSetting{path: filepath.Join(xdg, socketName)}
	}
	return socketSetting{path: filepath.Join(stateDir, socketName)}
}

// writePlan is the outcome of writer resolution: exactly one of client
// (socket mode), st (direct mode, already holding the state lock through the
// store) or err (a refusal worth reporting) is set.
type writePlan struct {
	client *api.Client
	st     *store.Store
	err    error
}

// retryWindow bounds the single retry phase after a failed flock. A daemon
// that has just taken the lock needs a moment before its listener exists;
// half a second more than that is not a start race any more, it is a stuck
// daemon, and the busy refusal is the honest answer.
const retryWindow = 750 * time.Millisecond

// retryStep is the poll cadence inside the retry phase.
const retryStep = 15 * time.Millisecond

// retryTicks is how many ticker ticks the retry phase may spend. Ticks of a
// real ticker are wall time whatever the command's clock reads, so the
// window stays bounded on a frozen test clock too.
const retryTicks = int(retryWindow / retryStep)

// planWrite resolves how one writing command reaches the state, in the order
// M2-08 fixes: try the socket; fall back to the flock-guarded direct store;
// when the lock is held, retry the socket once within a bounded window
// before reporting busy. --socket none skips both dial attempts.
func planWrite(ctx context.Context, env Env, g *globals) writePlan {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return writePlan{err: err}
	}
	setting := resolveSocket(g, env, stateDir)
	clk := clkOf(env)

	if !setting.off {
		client, probeErr := dialUp(setting.path, env)
		if client != nil {
			return writePlan{client: client}
		}
		var refused *api.SocketRefused
		if errors.As(probeErr, &refused) {
			// Fail closed: a hostile socket file is not a reason to
			// quietly write somewhere else, it is something to report.
			return writePlan{err: validationError(probeErr.Error(), probeErr,
				"remove or repair the socket file, then run the command again",
				"--socket none  writes directly when you meant to skip the daemon")}
		}
		var wire *api.WireError
		if errors.As(probeErr, &wire) && wire.Status == 409 {
			// A daemon of another major is running. Writing around it
			// would be refused by its lock anyway; the actionable
			// sentence beats the bare PQ5002 here.
			return writePlan{err: wireFailure(env, wire)}
		}
	}

	st, err := store.OpenState(ctx, stateDir, store.Options{Clock: clk})
	if err == nil {
		return writePlan{st: st}
	}
	var locked *store.LockedError
	if !errors.As(err, &locked) {
		return writePlan{err: err}
	}

	// The lock was busy. A daemon may have started between our first dial
	// and this flock; one bounded retry closes that race. The bound counts
	// ticks of a real ticker rather than readings of the command's clock:
	// a frozen test clock must not turn the window into an endless spin,
	// and a wall-clock wait-for-the-world is what this phase measures, the
	// same way logs -f waits for new lines.
	ticker := clk.NewTicker(retryStep)
	defer ticker.Stop()
	if !setting.off {
		for range retryTicks {
			select {
			case <-ctx.Done():
				return writePlan{err: interruptedError(ctx.Err())}
			case <-ticker.C:
			}
			client, probeErr := dialUp(setting.path, env)
			if client != nil {
				return writePlan{client: client}
			}
			var refused *api.SocketRefused
			if errors.As(probeErr, &refused) {
				break // loud refusal beats waiting out the window
			}
		}
	}
	return writePlan{err: busyError(err)}
}

// dialUp probes one socket and hands back a live client. nil with nil error
// cannot happen; nil with ErrNoDaemon-shaped silence means nobody is home.
func dialUp(path string, env Env) (*api.Client, error) {
	if err := api.Probe(path, version); err != nil {
		return nil, err
	}
	return api.Dial(path, version)
}

// noteDaemon is the read side of liveness. It probes the resolved socket,
// prints the down marker for people, and answers whether the daemon is down
// so JSON documents can carry daemon_up beside their payload. The probe
// happens whatever path resolution landed on (only "none" skips it): the
// fact reported is whether a daemon lives in this project, not what flags
// the caller typed, which keeps rendering independent of transport.
func noteDaemon(setting socketSetting, env Env, out *ui) bool {
	if setting.off || setting.path == "" {
		return false
	}
	err := api.Probe(setting.path, version)
	if err == nil {
		return false
	}
	fmt.Fprintf(out.err, "paceq: daemon down\n")
	return true
}

// wrapDaemonUp returns the value for a document's daemon_up field, set only
// when a probe actually happened.
func daemonField(down bool, probed bool) *bool {
	if !probed {
		return nil
	}
	up := !down
	return &up
}

// wireFailure turns a refused call into a paceq error whose exit code comes
// from the status table and whose words come from the daemon.
func wireFailure(env Env, wire *api.WireError) *Error {
	what := wire.Message
	if what == "" {
		what = fmt.Sprintf("the daemon refused the request (http %d, %s)", wire.Status, wire.Code)
	}
	next := []string{}
	switch {
	case wire.Code == "version_mismatch":
		next = append(next, "--socket none  talks to the state directory directly instead")
	case wireExit(wire) == ExitBusy:
		next = append(next, "paceq doctor  reports who holds the state directory")
	default:
		next = append(next, "run the command again once the daemon is healthy")
	}
	return &Error{code: wireExit(wire), what: what, next: next}
}

// wireExit turns a failed socket call into the exit code the shell sees.
// The table is the whole contract: every HTTP status the daemon can send,
// plus the two transport failures, lands on exactly one documented code
// (03 section 7.2), and exitwire_test.go holds each row in place.
//
// A dial failure is deliberately absent: it is not an error here at all,
// because resolution falls back to direct mode before any of this runs.
func wireExit(err error) int {
	if err == nil {
		return ExitOK
	}
	var wire *api.WireError
	if errors.As(err, &wire) {
		return wireStatusExit(wire.Status, wire.Code)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ExitTimeout
	}
	return ExitInternal
}

// wireStatusExit is one row per HTTP answer.
func wireStatusExit(status int, code string) int {
	switch {
	case status == 200 || status == 201:
		return ExitOK
	case status == 400:
		return ExitUsage
	case status == 404:
		return ExitNotFound
	case status == 422:
		return ExitValidation
	case status == 409 && code == "version_mismatch":
		return ExitInternal
	case status == 409:
		return ExitBusy
	case status == 504:
		return ExitTimeout
	default:
		return ExitInternal
	}
}
