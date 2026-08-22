package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The admission statement under contention. The issue's test plan asks for
// many holders racing on one name with the invariant that at most one of them
// ever holds a live lease and the fencing token never moves backwards. Both
// phases here are bounded by counts, not wall clocks, so nothing in this file
// can flake on a loaded machine: scheduling decides who wins a round, never
// whether the invariants hold.

// TestThirtyTwoHoldersRaceForOneLiveLease fires thirty two holders at a free
// lease at the same instant. Exactly one of them can ever see ok=true while
// the lease lives, every answer must come back clean, and the winner's grants
// are all renewals of the same epoch one row.
func TestThirtyTwoHoldersRaceForOneLiveLease(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)
	ctx := context.Background()

	const holders = 32
	const attemptsEach = 40

	type outcome struct {
		holder string
		ok     bool
		epoch  int64
	}
	var (
		mu       sync.Mutex
		grants   []outcome
		errCount int
	)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for h := range holders {
		holder := fmt.Sprintf("holder-%02d", h)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range attemptsEach {
				got, ok, err := s.AcquireOrRenew(ctx, "scheduler", holder, leaseTTL)
				mu.Lock()
				if err != nil {
					errCount++
				} else if ok {
					grants = append(grants, outcome{holder, true, got.Epoch})
				}
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if errCount != 0 {
		t.Errorf("%d attempts errored under contention, want zero leaked back to callers", errCount)
	}
	if len(grants) == 0 {
		t.Fatal("nobody won a free lease")
	}
	winner := grants[0].holder
	for _, g := range grants {
		if g.holder != winner {
			t.Errorf("a second holder (%s) held the live lease; only %s may have it", g.holder, winner)
		}
		if g.epoch != 1 {
			t.Errorf("a grant inside one live lease carried epoch %d, want 1", g.epoch)
		}
	}
	held, ok, err := s.LeaseHolder(ctx, "scheduler")
	if err != nil || !ok {
		t.Fatalf("read the settled holder: ok=%v err=%v", ok, err)
	}
	if held.Holder != winner || held.Epoch != 1 {
		t.Errorf("the row names %s epoch %d, want the only winner %s at epoch 1", held.Holder, held.Epoch, winner)
	}
}

// TestTakeoverRoundsBumpEpochMonotonically runs twelve rounds of thirty two
// holders against one name, letting the lease expire between rounds. Each
// round elects exactly one new holder and the fencing token climbs by exactly
// one per handover, in every observer's order.
func TestTakeoverRoundsBumpEpochMonotonically(t *testing.T) {
	clk := clock.NewFake(leaseOrigin)
	s := leaseStore(t, clk)
	ctx := context.Background()

	const holders = 32
	const rounds = 12

	var (
		mu   sync.Mutex
		seq  int
		errs []error
	)
	// observed pairs (order index, epoch) across everything any goroutine saw.
	var observed []struct {
		order int
		epoch int64
	}

	record := func(holder string, epoch int64, won bool, err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", holder, err))
			return
		}
		if won {
			seq++
			observed = append(observed, struct {
				order int
				epoch int64
			}{seq, epoch})
		}
	}

	for round := range rounds {
		var wg sync.WaitGroup
		start := make(chan struct{})
		for h := range holders {
			holder := fmt.Sprintf("holder-%02d-of-round-%02d", h, round)
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				got, ok, err := s.AcquireOrRenew(ctx, "scheduler", holder, leaseTTL)
				record(holder, got.Epoch, ok, err)
			}()
		}
		close(start)
		wg.Wait()

		held, ok, err := s.LeaseHolder(ctx, "scheduler")
		if err != nil || !ok {
			t.Fatalf("round %d: read the settled holder: ok=%v err=%v", round, ok, err)
		}
		wantEpoch := int64(round + 1)
		if held.Epoch != wantEpoch {
			t.Fatalf("round %d settled at epoch %d, want exactly %d", round, held.Epoch, wantEpoch)
		}
		// Expire the winner before the next round storms in.
		clk.Advance(leaseTTL + time.Second)
	}

	if len(errs) != 0 {
		t.Fatalf("%d contested attempts errored, first: %v", len(errs), errs[0])
	}
	for i := 1; i < len(observed); i++ {
		if observed[i].epoch < observed[i-1].epoch {
			t.Fatalf("observation %d saw epoch %d after observation %d saw %d: "+
				"the fencing token went backwards for somebody",
				observed[i].order, observed[i].epoch, observed[i-1].order, observed[i-1].epoch)
		}
	}
}
