package leases

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// countedStore forwards to a real store and counts the renewal attempts, so a
// test can prove how many follower ticks a handover took. allowRelease off is
// the crash simulation: the loop's shutdown can no longer delete the row,
// which is exactly what happens when a process dies without running its
// shutdown sequence.
type countedStore struct {
	inner        Store
	pulse        *pulse
	acquires     atomic.Int64
	allowRelease atomic.Bool
}

func newCounted(inner *store.Store, p *pulse) *countedStore {
	c := &countedStore{inner: inner, pulse: p}
	c.allowRelease.Store(true)
	return c
}

func (c *countedStore) AcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (store.LeaseGrant, bool, error) {
	c.acquires.Add(1)
	g, ok, err := c.inner.AcquireOrRenew(ctx, name, holder, ttl)
	c.pulse.ping()
	return g, ok, err
}

func (c *countedStore) ReleaseLease(ctx context.Context, name, holder string) (bool, error) {
	if !c.allowRelease.Load() {
		return false, nil
	}
	return c.inner.ReleaseLease(ctx, name, holder)
}

func (c *countedStore) AppendLeaseEvent(ctx context.Context, e store.LeaseEvent) error {
	return c.inner.AppendLeaseEvent(ctx, e)
}

func openPeerPort(t *testing.T, dbPath string, p *pulse) *countedStore {
	t.Helper()
	s, err := store.Open(context.Background(), dbPath, store.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return newCounted(s, p)
}

// stint reports one leadership beginning: the body announces its epoch and
// stays alive until cancelled.
type stint struct {
	pulse  *pulse
	epochs chan int64
	live   atomic.Bool
}

func newStint(p *pulse) *stint { return &stint{pulse: p, epochs: make(chan int64, 8)} }

func (s *stint) body(ctx context.Context, epoch int64) error {
	s.epochs <- epoch
	s.live.Store(true)
	s.pulse.ping()
	defer func() {
		s.live.Store(false)
		s.pulse.ping()
	}()
	<-ctx.Done()
	return ctx.Err()
}

// peer is one instance in the two process thought experiment: its own store
// handle, its own holder id, its own loop.
type peer struct {
	holder string
	port   *countedStore
	stint  *stint
	cancel context.CancelFunc
	done   chan error
}

func newPeer(t *testing.T, holder, dbPath string, p *pulse) *peer {
	return &peer{holder: holder, port: openPeerPort(t, dbPath, p), stint: newStint(p)}
}

func (p *peer) start(holder string) {
	var ctx context.Context
	ctx, p.cancel = context.WithCancel(context.Background())
	p.done = make(chan error, 1)
	p.holder = holder
	go func() { p.done <- RunAsLeader(ctx, p.port, peerOptions(p.holder), p.stint.body) }()
}

const (
	integrationTTL   = 30 * time.Second
	integrationRenew = 10 * time.Second
)

func peerOptions(holder string) Options {
	return Options{
		Name:   "scheduler",
		Holder: holder,
		TTL:    integrationTTL,
		Renew:  integrationRenew,
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestTwoInstancesOnOneDatabaseElectExactlyOneLeader drives three loop lives
// against two real stores on one database file, which is the situation flock
// cannot cover: two statedirs, one database. One virtual timeline walks the
// whole life of a role lease:
//
//  1. Election: exactly one of two starters leads, still alone after several
//     full renewal cycles.
//  2. Clean handover: the leader stops gracefully, deletes its own row, and
//     the follower leads within about one renew interval on a fresh epoch 1.
//  3. Crash: the current leader dies without releasing anything, so the row
//     stays behind, live until its expiry.
//  4. Overlapping restart: the crashed role comes back under the same name,
//     follows past the ttl, and takes over with the fencing token bumped by
//     exactly one.
//
// The real pulseq serve processes arrive with M2-01; what is proven here is
// everything beneath them.
func TestTwoInstancesOnOneDatabaseElectExactlyOneLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPulse()
		dbPath := filepath.Join(t.TempDir(), "state.db")
		a := newPeer(t, "holder-a", dbPath, p)
		b := newPeer(t, "holder-b", dbPath, p)
		a.start("holder-a")
		b.start("holder-b")

		// Phase 1: election. Exactly one flag settles true and stays true
		// through five full renewal cycles; both true at once would be the
		// double decision this issue exists to prevent.
		p.wait(t, "one leader elected", func() bool {
			liveA, liveB := a.stint.live.Load(), b.stint.live.Load()
			return (liveA || liveB) && !(liveA && liveB)
		})
		time.Sleep(5 * integrationRenew)
		liveA, liveB := a.stint.live.Load(), b.stint.live.Load()
		if (liveA && liveB) || (!liveA && !liveB) {
			t.Fatalf("leadership drifted after five renewal cycles: a=%v b=%v", liveA, liveB)
		}
		leader, follower := a, b
		if liveB {
			leader, follower = b, a
		}
		if e := <-leader.stint.epochs; e != 1 {
			t.Errorf("the first leader took epoch %d, want 1", e)
		}

		// Phase 2: clean handover. The leader releases its own row on the way
		// out and the follower claims the empty lease within about one renew
		// interval, on a fresh row whose history starts over at one.
		beforeClean := follower.port.acquires.Load()
		leader.cancel()
		join(t, leader.done)
		p.wait(t, "the successor taking over", func() bool { return follower.stint.live.Load() })
		if e := <-follower.stint.epochs; e != 1 {
			t.Errorf("the successor took epoch %d after a clean release, want 1: "+
				"the row was deleted, history starts over", e)
		}
		if took := follower.port.acquires.Load() - beforeClean; took > 3 {
			t.Errorf("the clean handover took %d follower ticks, a released row must "+
				"be claimable within about one renew interval", took)
		}

		// Phase 3: crash. The current leader dies without releasing anything,
		// so the row stays behind, live until its expiry. The kill lands just
		// after a confirmed renewal, which pins the arithmetic: the row is
		// good for the full ttl from here, so the next two ticks must be
		// refused and only the third can take over.
		crashed := follower
		confirmedJustBefore := crashed.port.acquires.Load()
		p.wait(t, "one more confirmed renewal before the kill", func() bool {
			return crashed.port.acquires.Load() > confirmedJustBefore
		})
		crashed.port.allowRelease.Store(false)
		crashed.cancel()
		join(t, crashed.done)

		// Phase 4: overlapping restart. The role comes back under a new
		// process identity, as a real restart mints a fresh holder id. It
		// finds the live foreign row left by the dead holder, follows past
		// the ttl, and takes over with the fencing token bumped by exactly
		// one.
		crashed.start(crashed.holder + "-restarted")
		p.wait(t, "the takeover after the ttl", func() bool { return crashed.stint.live.Load() })
		if e := <-crashed.stint.epochs; e != 2 {
			t.Errorf("the takeover produced epoch %d, want exactly 1+1", e)
		}
		if waited := crashed.port.acquires.Load() - confirmedJustBefore; waited < 3 {
			t.Errorf("the restarted holder needed only %d renewal attempts to take "+
				"over, a live row of a holder that died right after renewing must "+
				"hold out for the ttl (two refusals plus the taking attempt)", waited)
		}

		// Wind the surviving loop down cleanly so the bubble can close.
		crashed.cancel()
		join(t, crashed.done)
	})
}

// join waits for one loop goroutine to exit and checks how it exited.
func join(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the loop returned %v, want context.Canceled", err)
		}
	case <-time.After(parkDeadline):
		t.Fatal("the loop did not exit within the join budget")
	}
}
