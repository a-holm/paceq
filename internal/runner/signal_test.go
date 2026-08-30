package runner

import (
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/procfs"
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

// TestEveryGroupKillTargetsTheNegativePgid holds the invariant on the paths
// that run: signalling the bare pid would leave grandchildren running as
// orphans holding files and ports, which is exactly the leak Setpgid exists to
// prevent. Both deliveries of the escalation and both of the verified kill go
// through the package's own killer seam, so a bare pgid at any of the four
// sites fails here.
func TestEveryGroupKillTargetsTheNegativePgid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var targets []int
		restore := captureGroupKill(func(pgid int, sig syscall.Signal) error {
			mu.Lock()
			defer mu.Unlock()
			targets = append(targets, pgid)
			return nil
		})
		defer restore()
		delivered := func() []int {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(targets)
		}

		// NewEscalator carries no killer of its own, so its SIGTERM and its
		// SIGKILL take the same seam Run's do.
		x := NewEscalator(5*time.Second, clock.System())
		x.SetGroup(escalationTestGroup)
		if err := x.Fire(); err != nil {
			t.Fatalf("Fire: %v", err)
		}
		time.Sleep(6 * time.Second)
		synctest.Wait()
		x.Stop()

		want := []int{-escalationTestGroup, -escalationTestGroup}
		if got := delivered(); !slices.Equal(got, want) {
			t.Fatalf("the escalation addressed %v, want %v: the whole group must be signalled, not the pid", got, want)
		}

		// The verified kill is the shim's route to the same groups. It
		// refuses unless the group's recorded start ticks still match, so the
		// target has to be a live process this reader can measure.
		pid := os.Getpid()
		ticks, ok := procfs.ProcStartTicks(pid)
		if !ok {
			return // no start-ticks reader here, so VerifiedGroupKill delivers nothing
		}
		if err := VerifiedGroupKill(pid, ticks, 30*time.Millisecond, nil); err != nil {
			t.Fatalf("VerifiedGroupKill: %v", err)
		}
		want = append(want, -pid, -pid)
		if got := delivered(); !slices.Equal(got, want) {
			t.Fatalf("the verified kill addressed %v, want %v: the whole group must be signalled, not the pid", got, want)
		}
	})
}
