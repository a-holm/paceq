package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// Notifier is one delivery target. Send must be safe to call with a context
// that already carries the delivery timeout, and it must not invent retry
// policy of its own: the dispatcher owns attempts.
type Notifier interface {
	Send(ctx context.Context, msg OutboxMsg) error
}

// OutboxMsg is what a notifier receives. It is declared here (not in store)
// so this package stays leaf: no internal import may cross into it
// (internal/arch/deps_test.go pins "notify": {}).
type OutboxMsg struct {
	ID       int64
	Topic    string
	Subject  string
	Target   string
	Payload  string
	Attempts int

	// Suppressed and WindowOpenedAt ride only on throttled groups: an
	// opener message reports how many later events collapsed into it.
	Suppressed     int64
	WindowOpenedAt time.Time
}

// WirePayload merges the stored payload with the delivery facts. The stored
// JSON is never re-rendered or templated: new keys are appended to a decoded
// copy, nothing existing is rewritten (#29 AC: no templating in this path).
func WirePayload(msg OutboxMsg) string {
	if msg.Suppressed <= 0 && msg.WindowOpenedAt.IsZero() {
		return msg.Payload
	}
	var base map[string]any
	if err := json.Unmarshal([]byte(msg.Payload), &base); err != nil || base == nil {
		return msg.Payload // Not ours to fix: hand over exactly what was stored.
	}
	if msg.Suppressed > 0 {
		base["similar_suppressed"] = msg.Suppressed
	}
	if !msg.WindowOpenedAt.IsZero() {
		base["window_opened_at"] = msg.WindowOpenedAt.UnixMilli()
	}
	wire, err := json.Marshal(base)
	if err != nil {
		return msg.Payload
	}
	return string(wire)
}

// backoffLadder is the escalation ladder: 10s, 30s, 2m, 10m, 30m, then one
// hour forever. Index is the attempt number just failed (1-based), so the
// first retry waits 10s.
var backoffLadder = [...]time.Duration{
	10 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	30 * time.Minute,
	time.Hour,
}

// Backoff returns how long attempt n's failure waits before trying again.
// Attempts past the top of the ladder sit at the cap.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	idx := attempt - 1
	if idx >= len(backoffLadder) {
		idx = len(backoffLadder) - 1
	}
	return backoffLadder[idx]
}

// DefaultMaxAttempts is how many delivery tries one notification gets before
// failed_at seals it. Eight covers roughly ten hours of a down relay at the
// capped top of the ladder.
const DefaultMaxAttempts = 8

// ExecNotifier runs one command per delivery. The event goes in as JSON on
// stdin plus a handful of PULSEQ_* variables; the command's exit decides
// success. This is the only integration surface paceq carries for alerting:
// everything else is somebody else's script (10 section 3).
type ExecNotifier struct {
	// Name mirrors the configuration key, for error text only.
	Name string
	// Argv is the exact process invocation. argv[0] is absolute - the loader
	// refuses anything else, because an empty environment baseline has no
	// PATH to resolve bare names through.
	Argv []string
	// InheritEnv names the process environment variables passed through.
	// Everything else starts empty: deny by default, exactly like job steps
	// (08 section 3.2).
	InheritEnv []string

	// LookPath lets tests plant a stub without touching PATH itself; nil
	// means argv[0] goes to the OS unchanged (it must be absolute).
	LookPath func(string) (string, error)

	// Start allows a test to intercept execution entirely. Nil means real
	// subprocess semantics below.
	Start func(ctx context.Context, argv []string, env []string, stdin string) (wait func() error, cancel func(), err error)
}

// Send runs the command with the event on stdin. The child leads its own
// process group (the same rule as every step): when the deadline fires the
// whole group is killed, not just the leader that ignores signals.
func (e *ExecNotifier) Send(ctx context.Context, msg OutboxMsg) error {
	argv := e.Argv
	env := BaseEnv(e.InheritEnv, map[string]string{
		"PULSEQ_EVENT":   msg.Topic,
		"PULSEQ_SUBJECT": msg.Subject,
		"PULSEQ_TARGET":  msg.Target,
	})
	stdin := WirePayload(msg)

	if e.Start != nil { // test seam
		wait, cancel, err := e.Start(ctx, argv, env, stdin)
		defer cancel()
		if err != nil {
			return e.wrapSendError(wait(), ctx)
		}
		werr := wait()
		return e.wrapSendError(werr, ctx)
	}

	path := argv[0]
	if e.LookPath != nil {
		resolved, err := e.LookPath(path)
		if err != nil {
			return fmt.Errorf("notifier %q: %w", e.Name, err)
		}
		path = resolved
	}
	cmd := exec.CommandContext(ctx, path, argv[1:]...)
	cmd.Env = env
	cmd.Stdin = strings.NewReader(stdin)
	var stderr bytes.Buffer
	cmd.Stderr = &limitedBuffer{buf: &stderr, max: 4 << 10}
	setOwnProcessGroup(cmd)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("notifier %q: %w", e.Name, err)
	}
	stopEscalation := watchForTimeout(cmd, ctx)
	runErr := cmd.Wait()
	stopEscalation()
	return e.wrapSendError(runErr, ctx)
}

func (e *ExecNotifier) wrapSendError(runErr error, ctx context.Context) error {
	if runErr == nil && ctx.Err() == nil {
		return nil
	}
	if ctx.Err() != nil || errorsIsDeadline(runErr, ctx) {
		return fmt.Errorf("notifier %q: timed out, killed the whole process group", e.Name)
	}
	return fmt.Errorf("notifier %q: %w", e.Name, runErr)
}

// StderrNotifier writes the payload to the writer it was built with. It never
// fails and it never leaves the building: the development target (06
// section 8), useful long before any real notifier exists.
type StderrNotifier struct {
	Out io.Writer
}

func (s *StderrNotifier) Send(_ context.Context, msg OutboxMsg) error {
	fmt.Fprintf(s.Out, "paceq notify [%s] %s: %s\n", msg.Topic, msg.Subject, WirePayload(msg))
	return nil
}

// limitedBuffer caps what is kept from a chatty child. The FIRST bytes are
// the ones usually printed before a crash cascade, and 4 KiB is enough of an
// error message to act on.
type limitedBuffer struct {
	buf *bytes.Buffer
	max int64
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	if l.max <= 0 {
		return len(p), nil // Drop silently from here on.
	}
	take := p
	if int64(len(take)) > l.max {
		take = take[:l.max]
	}
	l.max -= int64(len(take))
	n, werr := l.buf.Write(take)
	if werr != nil {
		return n, werr
	}
	return len(p), nil // The rest was dropped on purpose; report success.
}

func (l *limitedBuffer) String() string { return l.buf.String() }

// errorsIsDeadline reports whether runErr is the context's own death wearing
// exec clothes.
func errorsIsDeadline(runErr error, ctx context.Context) bool {
	return runErr != nil && ctx.Err() != nil &&
		(containsAll(runErr.Error(), "context") || containsAll(runErr.Error(), "killed"))
}

func containsAll(haystack, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) &&
		strings.Contains(haystack, needle)
}
