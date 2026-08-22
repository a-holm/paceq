package runner

import (
	"slices"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// fakeKiller records signals instead of delivering them. It is the seam the
// plan calls a fake Killer: the signal layer's decisions get tested without a
// process, and the real group kill is exercised by every integration test.
type fakeKiller struct {
	mu   sync.Mutex
	sigs []syscall.Signal
	pgid int
}

func (f *fakeKiller) killGroup(pgid int, sig syscall.Signal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pgid = pgid
	if sig == 0 { // probe
		return syscall.ESRCH
	}
	f.sigs = append(f.sigs, sig)
	return nil
}

func (f *fakeKiller) sent() []syscall.Signal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sigs)
}

func (f *fakeKiller) lastPgid() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pgid
}

const escalationTestGroup = 4321

func TestEscalationSendsTermThenKillAfterGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k := &fakeKiller{}
		e := newEscalation(k.killGroup, 5*time.Second, clock.System())
		defer e.stop()
		e.setGroup(escalationTestGroup)

		e.fire()

		synctest.Wait()
		got := k.sent()
		if len(got) != 1 || got[0] != syscall.SIGTERM {
			t.Fatalf("after fire: %v sent, want exactly one SIGTERM", got)
		}
		if k.lastPgid() != -escalationTestGroup {
			t.Fatalf("targeted %d, want %d: kills must address the group", k.lastPgid(), -escalationTestGroup)
		}

		time.Sleep(4 * time.Second)
		synctest.Wait()
		if got := k.sent(); len(got) != 1 {
			t.Errorf("before grace: %v sent, want still only SIGTERM", got)
		}

		time.Sleep(2 * time.Second)
		synctest.Wait()
		got = k.sent()
		if len(got) != 2 || got[1] != syscall.SIGKILL {
			t.Fatalf("after grace: %v sent, want SIGTERM then SIGKILL", got)
		}
	})
}

func TestEscalationFiresOnlyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k := &fakeKiller{}
		e := newEscalation(k.killGroup, time.Second, clock.System())
		defer e.stop()
		e.setGroup(escalationTestGroup)

		e.fire()
		e.fire() // a deadline and a cancelled parent may race; one sequence only
		synctest.Wait()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := k.sent(); !slices.Equal(got, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}) {
			t.Fatalf("sent %v after two fires, want exactly TERM then KILL", got)
		}
	})
}

func TestEscalationStopsWhenDisarmed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k := &fakeKiller{}
		e := newEscalation(k.killGroup, time.Second, clock.System())
		e.setGroup(escalationTestGroup)

		e.fire()
		synctest.Wait()
		e.stop() // the process died on its own before grace ended

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := k.sent(); !slices.Equal(got, []syscall.Signal{syscall.SIGTERM}) {
			t.Fatalf("sent %v after disarm, want only the first SIGTERM", got)
		}
	})
}

func TestGroupKillTargetsTheNegativePgid(t *testing.T) {
	var captured int
	var sig syscall.Signal
	restore := captureGroupKill(func(pgid int, s syscall.Signal) error {
		captured, sig = pgid, s
		return nil
	})
	defer restore()

	const pid = 4711
	if err := terminateGroup(pid, syscall.SIGTERM); err != nil {
		t.Fatalf("terminateGroup: %v", err)
	}
	if captured != -pid {
		t.Errorf("signalled %d, want %d: the whole group must be addressed, not the pid", captured, -pid)
	}
	if sig != syscall.SIGTERM {
		t.Errorf("signal = %v, want SIGTERM", sig)
	}
}
