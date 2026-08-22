package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
)

// The lease event trail. Every moment a role lease changes hands gets one row
// with a code from the closed catalogue, so the story of who led and when
// survives in the database even though the leases table itself only ever shows
// the present.

func TestLeaseEventsRoundTripNewestFirst(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)
	ctx := context.Background()

	moments := []struct {
		offset time.Duration
		code   reason.Code
		epoch  int64
	}{
		{0, reason.LEASEAcquired, 1},
		{10 * time.Second, reason.LEASELost, 1},
		{20 * time.Second, reason.LEASETakenOver, 2},
	}
	for _, m := range moments {
		err := s.AppendLeaseEvent(ctx, LeaseEvent{
			At:     leaseOrigin.Add(m.offset),
			Lease:  "scheduler",
			Holder: "holder-a",
			Epoch:  m.epoch,
			Code:   m.code,
		})
		if err != nil {
			t.Fatalf("append %s event: %v", m.code, err)
		}
	}

	got, err := s.LeaseEvents(ctx, "scheduler", 50)
	if err != nil {
		t.Fatalf("read events back: %v", err)
	}
	if len(got) != len(moments) {
		t.Fatalf("read %d events back, want %d", len(got), len(moments))
	}
	for i, want := range moments {
		e := got[len(got)-1-i] // newest first, so the walk starts at the last write
		if e.Code != want.code {
			t.Errorf("event %d carries %s, want %s", i, e.Code, want.code)
		}
		if e.Epoch != want.epoch {
			t.Errorf("event %d carries epoch %d, want %d", i, e.Epoch, want.epoch)
		}
		if !e.At.Equal(leaseOrigin.Add(want.offset)) {
			t.Errorf("event %d happened at %s, want %s", i, e.At, leaseOrigin.Add(want.offset))
		}
		if e.Lease != "scheduler" || e.Holder != "holder-a" {
			t.Errorf("event %d names the wrong lease or holder: %+v", i, e)
		}
	}
}

func TestLeaseEventsLimitCapsTheRead(t *testing.T) {
	s := leaseStore(t, clock.NewFake(leaseOrigin))
	ctx := context.Background()

	for i := range 5 {
		err := s.AppendLeaseEvent(ctx, LeaseEvent{
			At:     leaseOrigin.Add(time.Duration(i) * time.Second),
			Lease:  "reaper",
			Holder: "holder-a",
			Epoch:  int64(i + 1),
			Code:   reason.LEASEAcquired,
		})
		if err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}

	got, err := s.LeaseEvents(ctx, "reaper", 2)
	if err != nil {
		t.Fatalf("read events back: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("limit 2 returned %d events, want exactly the 2 newest", len(got))
	}
	if got[0].Epoch != 5 || got[1].Epoch != 4 {
		t.Errorf("the capped read returned epochs %d and %d, want the two newest (5 then 4)", got[0].Epoch, got[1].Epoch)
	}
}

func TestLeaseEventsOfAnUnknownLeaseAreEmpty(t *testing.T) {
	s := leaseStore(t, clock.NewFake(leaseOrigin))

	got, err := s.LeaseEvents(context.Background(), "sensor", 50)
	if err != nil {
		t.Fatalf("read events for a lease nobody wrote about: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("an unknown lease returned %d events, want none", len(got))
	}
}

// TestLeaseEventsAreSeparatePerRole pins that the trail is keyed by lease
// name: the scheduler's story must not leak into the reaper's read.
func TestLeaseEventsAreSeparatePerRole(t *testing.T) {
	s := leaseStore(t, clock.NewFake(leaseOrigin))
	ctx := context.Background()

	for _, name := range []string{"scheduler", "reaper"} {
		if err := s.AppendLeaseEvent(ctx, LeaseEvent{
			At:     leaseOrigin,
			Lease:  name,
			Holder: "holder-a",
			Epoch:  1,
			Code:   reason.LEASEAcquired,
		}); err != nil {
			t.Fatalf("append event for %s: %v", name, err)
		}
	}

	got, err := s.LeaseEvents(ctx, "scheduler", 50)
	if err != nil {
		t.Fatalf("read scheduler events: %v", err)
	}
	if len(got) != 1 || got[0].Lease != "scheduler" {
		t.Errorf("the scheduler read %d events, want exactly its own one: %+v", len(got), got)
	}
}
