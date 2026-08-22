package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// leaseOrigin is the wall clock reading the lease tests start from. A fixed
// instant keeps every expected timestamp an offset rather than a moving target.
var leaseOrigin = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

// leaseTTL is the time to live every test in this file hands to the store
// methods. The loop in internal/leases owns the production value; these tests
// care about the arithmetic, not the policy.
const leaseTTL = 15 * time.Second

// leaseStore opens a migrated store on a fake clock. Lease time is computed
// inside the store from this clock, never handed in by a caller.
func leaseStore(t *testing.T, clk *clock.Fake) *Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(context.Background(), path, Options{Clock: clk})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store at %q: %v", path, err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func TestFirstAcquireGrantsEpochOne(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)

	got, ok, err := s.AcquireOrRenew(context.Background(), "scheduler", "holder-a", leaseTTL)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !ok {
		t.Fatal("first acquire refused on an empty table")
	}
	if got.Epoch != 1 {
		t.Errorf("first grant has epoch %d, want 1", got.Epoch)
	}
	if got.Name != "scheduler" || got.Holder != "holder-a" {
		t.Errorf("grant names the wrong lease: %+v", got)
	}
	wantExpiry := leaseOrigin.Add(leaseTTL)
	if !got.ExpiresAt.Equal(wantExpiry) {
		t.Errorf("grant expires at %s, want %s", got.ExpiresAt, wantExpiry)
	}
	if !got.AcquiredAt.Equal(leaseOrigin) {
		t.Errorf("grant acquired at %s, want %s", got.AcquiredAt, leaseOrigin)
	}
}

func TestRenewalKeepsEpochAndAcquiredAt(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)

	first, ok, err := s.AcquireOrRenew(context.Background(), "scheduler", "holder-a", leaseTTL)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}

	clk.Advance(5 * time.Second)
	renewed, ok, err := s.AcquireOrRenew(context.Background(), "scheduler", "holder-a", leaseTTL)
	if err != nil || !ok {
		t.Fatalf("renew: ok=%v err=%v", ok, err)
	}
	if renewed.Epoch != first.Epoch {
		t.Errorf("renewal moved epoch from %d to %d, renewal must never bump the fencing token",
			first.Epoch, renewed.Epoch)
	}
	if !renewed.AcquiredAt.Equal(first.AcquiredAt) {
		t.Errorf("renewal moved acquired_at from %s to %s, renewal must keep the original acquisition",
			first.AcquiredAt, renewed.AcquiredAt)
	}
	wantExpiry := leaseOrigin.Add(5 * time.Second).Add(leaseTTL)
	if !renewed.ExpiresAt.Equal(wantExpiry) {
		t.Errorf("renewal set expires_at to %s, want %s", renewed.ExpiresAt, wantExpiry)
	}
}

func TestLiveLeaseRefusesAnotherHolder(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)

	if _, ok, err := s.AcquireOrRenew(context.Background(), "scheduler", "holder-a", leaseTTL); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}

	clk.Advance(5 * time.Second)
	got, ok, err := s.AcquireOrRenew(context.Background(), "scheduler", "holder-b", leaseTTL)
	if err != nil {
		t.Fatalf("acquire from holder-b: %v", err)
	}
	if ok {
		t.Errorf("holder-b took a live lease: %+v", got)
	}
	if got.Holder != "" || got.Epoch != 0 {
		t.Errorf("a refused attempt returned a grant: %+v", got)
	}

	held, ok, err := s.LeaseHolder(context.Background(), "scheduler")
	if err != nil || !ok {
		t.Fatalf("read holder: ok=%v err=%v", ok, err)
	}
	if held.Holder != "holder-a" {
		t.Errorf("lease still held by %q, want holder-a", held.Holder)
	}
}

func TestTakeoverAfterExpiryBumpsEpochByExactlyOne(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)

	first, ok, err := s.AcquireOrRenew(context.Background(), "reaper", "holder-a", leaseTTL)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}

	clk.Advance(leaseTTL + time.Second)
	got, ok, err := s.AcquireOrRenew(context.Background(), "reaper", "holder-b", leaseTTL)
	if err != nil || !ok {
		t.Fatalf("take over an expired lease: ok=%v err=%v", ok, err)
	}
	if got.Epoch != first.Epoch+1 {
		t.Errorf("takeover produced epoch %d, want exactly %d+1", got.Epoch, first.Epoch)
	}
	if got.Holder != "holder-b" {
		t.Errorf("takeover left holder %q, want holder-b", got.Holder)
	}
	if !got.AcquiredAt.Equal(leaseOrigin.Add(leaseTTL + time.Second)) {
		t.Errorf("takeover kept acquired_at %s, a takeover is a new acquisition", got.AcquiredAt)
	}
}

func TestTakeoverOpensExactlyAtExpiry(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)

	held, ok, err := s.AcquireOrRenew(context.Background(), "scheduler", "holder-a", leaseTTL)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}

	before := held.ExpiresAt.Add(-1 * time.Millisecond)
	clk.Set(before)
	if _, ok, _ := s.AcquireOrRenew(context.Background(), "scheduler", "holder-b", leaseTTL); ok {
		t.Fatal("holder-b took the lease one millisecond before expiry")
	}

	clk.Set(held.ExpiresAt)
	if _, ok, _ := s.AcquireOrRenew(context.Background(), "scheduler", "holder-b", leaseTTL); !ok {
		t.Fatal("holder-b refused at the exact expiry instant, an expired lease must be free")
	}
}

func TestReleaseDeletesTheOwnRowOnly(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)

	if _, ok, _ := s.AcquireOrRenew(context.Background(), "scheduler", "holder-a", leaseTTL); !ok {
		t.Fatal("holder-a did not get the empty lease")
	}

	// A stale holder must not be able to delete someone else's live row.
	released, err := s.ReleaseLease(context.Background(), "scheduler", "holder-stale")
	if err != nil {
		t.Fatalf("release from a stale holder: %v", err)
	}
	if released {
		t.Error("release reported success for a holder that owns nothing")
	}
	if _, ok, _ := s.LeaseHolder(context.Background(), "scheduler"); !ok {
		t.Fatal("the live row disappeared, a stale release deleted it")
	}

	released, err = s.ReleaseLease(context.Background(), "scheduler", "holder-a")
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !released {
		t.Error("release reported failure for the holder that owns the row")
	}
	if _, ok, _ := s.LeaseHolder(context.Background(), "scheduler"); ok {
		t.Error("the lease row survived its own release")
	}
}

func TestCleanReleaseLetsTheNextAcquireStartFresh(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)

	if _, ok, _ := s.AcquireOrRenew(context.Background(), "scheduler", "holder-a", leaseTTL); !ok {
		t.Fatal("holder-a did not get the empty lease")
	}
	if _, err := s.ReleaseLease(context.Background(), "scheduler", "holder-a"); err != nil {
		t.Fatalf("release: %v", err)
	}

	got, ok, err := s.AcquireOrRenew(context.Background(), "scheduler", "holder-b", leaseTTL)
	if err != nil || !ok {
		t.Fatalf("acquire after a clean release: ok=%v err=%v", ok, err)
	}
	if got.Epoch != 1 {
		t.Errorf("acquire after a clean release took epoch %d, want 1: the row was deleted, history starts over",
			got.Epoch)
	}
}

func TestRolesHoldTheirOwnLeasesSideBySide(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)

	for _, role := range []string{"scheduler", "executor", "reaper"} {
		got, ok, err := s.AcquireOrRenew(context.Background(), role, "holder-a", leaseTTL)
		if err != nil || !ok {
			t.Fatalf("%s acquire: ok=%v err=%v", role, ok, err)
		}
		if got.Epoch != 1 {
			t.Errorf("%s grant has epoch %d, want 1", role, got.Epoch)
		}
	}

	// One role's expiry must not free another role's lease.
	clk.Advance(leaseTTL + time.Second)
	for _, role := range []string{"scheduler", "executor", "reaper"} {
		if _, ok, _ := s.LeaseHolder(context.Background(), role); !ok {
			t.Errorf("%s lost its own row when nothing touched it", role)
		}
	}
}

func TestLeaseHolderOfAnUnknownNameReportsNone(t *testing.T) {
	s := leaseStore(t, clock.NewFake(leaseOrigin))

	if _, ok, err := s.LeaseHolder(context.Background(), "sensor"); err != nil || ok {
		t.Fatalf("unknown lease read back ok=%v err=%v", ok, err)
	}
}
