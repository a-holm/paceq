//go:build unix

package runner

import (
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
)

// The hard stop reaches every group the daemon started, at once, without
// knowing anything about runs or engines. These tests prove the registry
// delivers to exactly the groups registered and to none after release.

func TestKillAllSignalsEveryRegisteredGroup(t *testing.T) {
	var mu sync.Mutex
	signalled := map[int]syscall.Signal{}
	restore := captureGroupKill(func(pgid int, sig syscall.Signal) error {
		mu.Lock()
		defer mu.Unlock()
		signalled[pgid] = sig
		return nil
	})
	defer restore()

	releaseA := registerGroup(4100)
	releaseB := registerGroup(5200)
	defer releaseA()
	defer releaseB()

	KillAllProcessGroups(syscall.SIGKILL)

	mu.Lock()
	defer mu.Unlock()
	// The killer seam takes the negative group id, exactly as every other
	// delivery in this package does.
	if got := signalled[-4100]; got != syscall.SIGKILL {
		t.Errorf("group 4100 got %v, want SIGKILL", got)
	}
	if got := signalled[-5200]; got != syscall.SIGKILL {
		t.Errorf("group 5200 got %v, want SIGKILL", got)
	}
}

func TestReleasedGroupIsNotSignalled(t *testing.T) {
	var count atomic.Int32
	restore := captureGroupKill(func(pgid int, sig syscall.Signal) error {
		count.Add(1)
		return nil
	})
	defer restore()

	release := registerGroup(6300)
	release()

	KillAllProcessGroups(syscall.SIGKILL)

	if n := count.Load(); n != 0 {
		t.Errorf("%d groups were signalled after every registration was released, want 0", n)
	}
}

func TestRegisterGroupIsSafeForConcurrentUse(t *testing.T) {
	var count atomic.Int32
	restore := captureGroupKill(func(pgid int, sig syscall.Signal) error {
		count.Add(1)
		return nil
	})
	defer restore()

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pgid := 7000 + i
			release := registerGroup(pgid)
			KillAllProcessGroups(syscall.SIGTERM)
			release()
		}()
	}
	wg.Wait()

	// Only the deliveries of this last sweep count: everything before raced
	// with groups other goroutines still held.
	count.Store(0)
	KillAllProcessGroups(syscall.SIGTERM)
	if n := count.Load(); n != 0 {
		t.Errorf("%d groups survived their own release, want 0", n)
	}
}
