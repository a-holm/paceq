package serve

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/daemon"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// TestOneStopSignalDrainsTheRunningWork is acceptance criterion 3 and 4 as a
// real process: a daemon executing a sleeping step is asked to stop once. It
// must end cleanly, quickly, with the work handed back untouched, with the
// session row closed as clean, and with no process anywhere on the machine
// still carrying this run's id in its environment.
func TestOneStopSignalDrainsTheRunningWork(t *testing.T) {
	ws := newWorkspace(t)
	runID := seedQueuedRun(t, ws, stepCommand(t), "sleep", "45s")

	p := startServe(t, ws)
	p.waitReady(t)
	waitForChildRunning(t, ws, p, runID)

	started := time.Now()
	p.signal(t, "only", syscall.SIGTERM)
	code := p.waitExit(t, 20*time.Second)
	took := time.Since(started)
	if code != 0 {
		t.Fatalf("the graceful stop exited %d, want 0\nstderr:\n%s", code, p.stderrSnapshot())
	}
	// The drain had no reason to take any real time: one short process
	// group to end and two rows to write. The bound sits far under the
	// default 30s drain timeout so only the clean path can pass, but far
	// above scheduling noise so a loaded machine cannot flake.
	if took > 15*time.Second {
		t.Errorf("the graceful stop took %s, which no clean drain explains", took)
	}
	t.Logf("graceful drain of a running step took %s", took)

	requireNoOrphan(t, runID)

	detail := readRun(t, ws, runID)
	if detail.Run.State != string(model.RunQueued) {
		t.Errorf("the interrupted run is %s, want queued\ndaemon stderr:\n%s",
			detail.Run.State, p.stderrSnapshot())
	}
	if detail.Run.DeferReason != model.DeferReasonAfterShutdown {
		t.Errorf("defer_reason is %q, want %q", detail.Run.DeferReason, model.DeferReasonAfterShutdown)
	}
	if len(detail.Steps) != 1 {
		t.Fatalf("%d steps came back, want 1", len(detail.Steps))
	}
	step := detail.Steps[0]
	if step.State != string(model.StepPending) {
		t.Errorf("the interrupted step is %s, want pending\ndaemon stderr:\n%s",
			step.State, p.stderrSnapshot())
	}
	// The executor's StartStep spent attempt 1; the handback restores it,
	// because a daemon restart is nobody's fault (05 section 3.2).
	if step.Attempt != 0 {
		t.Errorf("attempt is %d after the drain, want the pre-execution 0\n"+
			"daemon stderr:\n%s", step.Attempt, p.stderrSnapshot())
	}
	if step.ReasonCode != string(reason.RUNInterruptedShutdown) {
		t.Errorf("the step says %q, want %q\ndaemon stderr:\n%s",
			step.ReasonCode, reason.RUNInterruptedShutdown, p.stderrSnapshot())
	}

	s := openReadOnly(t, ws)
	defer func() { _ = s.Close() }()
	events, err := s.RunEvents(t.Context(), runID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var sawInterrupt, sawDrained bool
	for _, e := range events {
		if e.Kind == "step.interrupted" && e.ReasonCode == string(reason.RUNInterruptedShutdown) {
			sawInterrupt = true
		}
		if e.Kind == "run.drained" && e.FromState == "running" && e.ToState == "queued" {
			sawDrained = true
		}
	}
	if !sawInterrupt || !sawDrained {
		t.Fatalf("the shutdown events are incomplete: interrupt=%v drained=%v in %+v"+
			"\ndaemon stderr:\n%s", sawInterrupt, sawDrained, events, p.stderrSnapshot())
	}

	sess, ok, err := s.LatestSession(t.Context())
	if err != nil || !ok {
		t.Fatalf("no session row came back (ok=%v, err=%v)", ok, err)
	}
	if sess.StopReason != "clean" || sess.StoppedAt.IsZero() {
		t.Errorf("the session ended as %+v, want stopped_at set and stop_reason clean", sess)
	}

	walPath := ws.DBPath + "-wal"
	if info, err := os.Stat(walPath); err == nil && info.Size() > 8192 {
		t.Errorf("the wal holds %d bytes after the clean stop, want at most one page", info.Size())
	}
}

// TestASecondStopSignalInsistsWithSigkill is acceptance criterion 5 as a real
// process: while the drain of a step that ignores SIGTERM is running, a second
// stop signal ends the discussion. Every live process group gets SIGKILL and
// the daemon exits 130 at once, leaving the run exactly as it was for the
// next start to converge.
func TestASecondStopSignalInsistsWithSigkill(t *testing.T) {
	ws := newWorkspace(t)
	runID := seedQueuedRun(t, ws, stepCommand(t), "ignore-term", "60s")

	// A long drain makes the claim sharp: nothing except insistence can
	// finish a stop of an ignore-term step this fast.
	p := startServe(t, ws, "--drain-timeout", "45s")
	p.waitReady(t)
	waitForChildRunning(t, ws, p, runID)

	p.signal(t, "first", syscall.SIGTERM) // the graceful path begins
	// The second request only means something while the first stop is still
	// draining, so wait for the daemon to say the drain started rather than
	// for a duration. The step ignores the term half of the runner's
	// escalation, so from that line the drain still owes it the whole
	// grace: that is the window the insistence lands in.
	p.waitForDrainStart(t)

	insisted := time.Now()
	p.signal(t, "second", syscall.SIGTERM) // the operator insists
	// The budget outlasts the graceful path on purpose. A stop that drained
	// to its end instead of answering the insistence is worth reporting as
	// the wrong exit code rather than as a harness timeout.
	code := p.waitExit(t, 30*time.Second)
	took := time.Since(insisted)
	if code != daemon.ExitHardStop {
		t.Fatalf("the second stop request exited %d, want %d after %s\nstderr:\n%s",
			code, daemon.ExitHardStop, took, p.stderrSnapshot())
	}
	// Measured from the second request, because that is what the claim is
	// about. The graceful path cannot answer this fast from the same point:
	// it still owes this step the runner's term-to-kill escalation, and it
	// may take the whole drain timeout named on the command line.
	if took > 5*time.Second {
		t.Errorf("the hard stop took %s after the second request, too slow for insistence", took)
	}
	t.Logf("the hard stop answered the second request in %s", took)

	requireNoOrphan(t, runID)

	detail := readRun(t, ws, runID)
	if detail.Run.State != "running" {
		t.Errorf("the killed run was rewritten to %s; a hard stop invents no verdict", detail.Run.State)
	}
	if len(detail.Steps) == 1 && detail.Steps[0].State != "running" {
		t.Errorf("the killed step was rewritten to %s; a hard stop invents no verdict",
			detail.Steps[0].State)
	}

	s := openReadOnly(t, ws)
	defer func() { _ = s.Close() }()
	sess, ok, err := s.LatestSession(t.Context())
	if err != nil || !ok {
		t.Fatalf("no session row came back (ok=%v, err=%v)", ok, err)
	}
	if !sess.StoppedAt.IsZero() || sess.StopReason != "" {
		t.Errorf("the session row claims a stop it never made: %+v", sess)
	}
}

// TestServeRefusesToShareTheStateEndToEnd runs criterion 2 across a real
// process boundary: while one serve holds the state directory, a second
// binary is refused with exit 6 before it opens anything for writing.
func TestServeRefusesToShareTheStateEndToEnd(t *testing.T) {
	ws := newWorkspace(t)
	runID := seedQueuedRun(t, ws, stepCommand(t), "sleep", "30s")

	first := startServe(t, ws)
	first.waitReady(t)
	waitForChildRunning(t, ws, first, runID)

	refused := startServe(t, ws)
	code := refused.waitExit(t, 15*time.Second)
	if code != 6 {
		t.Fatalf("the second daemon exited %d, want 6 (busy)\nstderr:\n%s",
			code, refused.stderrSnapshot())
	}
	if out := refused.stderrSnapshot(); !strings.Contains(out, "already") {
		t.Errorf("the refusal says %q, want it to name the holder", out)
	}

	// The first daemon keeps serving: cancel it cleanly and let the row
	// end queued, which also cleans up the workspace.
	first.signal(t, "only", syscall.SIGTERM)
	if code := first.waitExit(t, 20*time.Second); code != 0 {
		t.Fatalf("the first daemon exited %d after its own stop, want 0", code)
	}
	requireNoOrphan(t, runID)
}
