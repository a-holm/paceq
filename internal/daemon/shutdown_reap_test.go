package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reconcile"
)

// fakeMachine is the /proc the drain reads in these rows: a set of live
// processes, each removable by a signal, and a record of what was sent to
// which group.
type fakeMachine struct {
	mu     sync.Mutex
	live   []reconcile.Process
	sent   []string
	diesOn syscall.Signal // the signal that removes a process; 0 means none
}

func newFakeMachine(diesOn syscall.Signal, procs ...reconcile.Process) *fakeMachine {
	return &fakeMachine{live: procs, diesOn: diesOn}
}

func (m *fakeMachine) scan() ([]reconcile.Process, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]reconcile.Process, len(m.live))
	copy(out, m.live)
	return out, nil
}

func (m *fakeMachine) signal(pgid int, sig syscall.Signal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, sigKey(pgid, sig))
	if m.diesOn != 0 && sig == m.diesOn {
		kept := m.live[:0]
		for _, p := range m.live {
			if p.PGID != pgid {
				kept = append(kept, p)
			}
		}
		m.live = kept
	}
	return nil
}

func (m *fakeMachine) delivered() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.sent...)
}

func sigKey(pgid int, sig syscall.Signal) string {
	name := sig.String()
	switch sig {
	case syscall.SIGTERM:
		name = "TERM"
	case syscall.SIGKILL:
		name = "KILL"
	}
	return fmt.Sprintf("%s->%d", name, pgid)
}

// installMachine points both drain seams at one fake and restores them.
func installMachine(t *testing.T, m *fakeMachine) {
	t.Helper()
	scan, sig := scanRunProcesses, signalGroup
	scanRunProcesses = m.scan
	signalGroup = m.signal
	t.Cleanup(func() { scanRunProcesses, signalGroup = scan, sig })
}

// reapShutdown builds a shutdown whose every phase but the drain is already
// finished, so a row only has to say what the machine looked like.
func reapShutdown(clk clock.Clock, logger *slog.Logger, runs []string, closed *bool) *shutdown {
	return &shutdown{
		cfg:          Config{KillGrace: 2 * time.Second},
		clk:          clk,
		log:          logger,
		statuses:     newStatuses(clk.Now),
		stopIntake:   func() {},
		stopExec:     func() {},
		execDrained:  closedChan(),
		loopsDrained: closedChan(),
		runsInFlight: func() []string { return runs },
		closeSession: func(context.Context) error {
			*closed = true
			return nil
		},
	}
}

// TestTheDrainEndsAProcessGroupItsExecutorLeftBehind: the executor returned,
// the group did not. The drain finds it, asks it to go, and only then calls
// the stop clean.
func TestTheDrainEndsAProcessGroupItsExecutorLeftBehind(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec, logger := newRecLog()
		m := newFakeMachine(syscall.SIGTERM, reconcile.Process{PID: 4242, PGID: 4242, RunID: "run-a"})
		installMachine(t, m)

		sessionClosed := false
		sd := reapShutdown(clock.System(), logger, []string{"run-a"}, &sessionClosed)

		if err := sd.run(context.Canceled); err != nil {
			t.Fatalf("a stop that reaped its leftover group reported %v, want clean", err)
		}
		synctest.Wait()

		if got := m.delivered(); len(got) != 1 || got[0] != "TERM->4242" {
			t.Errorf("the drain delivered %v, want one TERM to the group", got)
		}
		if warns := rec.named("a step's process group outlived its executor; the drain is ending it"); len(warns) != 1 {
			t.Errorf("the escalation was not reported: %v", rec.records)
		}
		if lines := rec.named("daemon stopped cleanly"); len(lines) != 1 {
			t.Errorf("a stop that reaped everything did not report itself clean: %v", rec.records)
		}
		if !sessionClosed {
			t.Error("the session row was not closed after a stop that reaped everything")
		}
	})
}

// TestTheDrainRefusesToReportACleanStopOverASurvivor is the honesty rule. A
// group that answers neither signal is what an operator has to be told about,
// because being told the stop worked is what stops the looking.
func TestTheDrainRefusesToReportACleanStopOverASurvivor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec, logger := newRecLog()
		m := newFakeMachine(0, reconcile.Process{PID: 99, PGID: 99, RunID: "run-a"})
		installMachine(t, m)

		sessionClosed := false
		sd := reapShutdown(clock.System(), logger, []string{"run-a"}, &sessionClosed)

		err := sd.run(context.Canceled)
		synctest.Wait()

		if !errors.Is(err, ErrStepProcessSurvived) {
			t.Fatalf("the stop reported %v, want %v", err, ErrStepProcessSurvived)
		}
		if lines := rec.named("daemon stopped cleanly"); len(lines) != 0 {
			t.Error("the daemon called a stop clean over a surviving job process")
		}
		errs := rec.named("the stop left a job process running; this daemon did not stop cleanly")
		if len(errs) != 1 {
			t.Fatalf("the survivor was not reported: %v", rec.records)
		}
		if got, _ := errs[0]["processes"].(string); got != "pid 99 (pgid 99, run run-a)" {
			t.Errorf("the report names %q, want the surviving pid, group and run", got)
		}
		if sessionClosed {
			t.Error("the session row was closed as clean although a job process survived")
		}
		got := m.delivered()
		if len(got) != 2 || got[0] != "TERM->99" || got[1] != "KILL->99" {
			t.Errorf("the drain delivered %v, want a TERM then a KILL to the group", got)
		}
	})
}

// TestTheDrainOnlyTouchesTheRunsItAnswersFor: another installation's job
// process, and any process of a run this daemon was not driving, is nobody's
// to signal here.
func TestTheDrainOnlyTouchesTheRunsItAnswersFor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, logger := newRecLog()
		m := newFakeMachine(0, reconcile.Process{PID: 77, PGID: 77, RunID: "someone-elses-run"})
		installMachine(t, m)

		sessionClosed := false
		sd := reapShutdown(clock.System(), logger, []string{"run-a"}, &sessionClosed)

		if err := sd.run(context.Canceled); err != nil {
			t.Fatalf("a stop with nothing of its own left reported %v, want clean", err)
		}
		synctest.Wait()

		if got := m.delivered(); len(got) != 0 {
			t.Errorf("the drain signalled %v; nothing of this daemon's was running", got)
		}
		if !sessionClosed {
			t.Error("the session row was not closed although nothing of ours survived")
		}
	})
}

// TestTheDrainCensusNamesTheRunsInFlight: the pool's book is read, and it is
// read as a snapshot the drain can keep after the executors have let go.
func TestTheDrainCensusNamesTheRunsInFlight(t *testing.T) {
	p := newExecutorPool(nil, nil, nil, 2)
	p.driving = map[string]bool{"run-a": true, "run-b": true}

	got := p.inFlight()
	if len(got) != 2 {
		t.Fatalf("the census named %v, want both driven runs", got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		seen[id] = true
	}
	if !seen["run-a"] || !seen["run-b"] {
		t.Errorf("the census named %v, want run-a and run-b", got)
	}

	// The snapshot is a copy: the pool's own book empties as executors
	// return, and the drain has to keep answering for what it read.
	p.driving = map[string]bool{}
	if len(got) != 2 {
		t.Errorf("the census changed to %v when the pool's book emptied", got)
	}
}
