package daemon

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/store"
)

// The overlap proofs. The dispatcher hands out run ids asynchronously: submit
// starts a goroutine and returns, and the queue read that named the run may
// repeat before the claim lands. Two executors for one run id in one process
// are the storm behind the chaos review's evidence - lease_epoch churning on a
// single pid, two processes spawning one step, an effect key executed past the
// crash bound - because the loser's handback finds the winner's live claim in
// the keeper's map and drains it: owner and epoch match, so the store cannot
// refuse. These two tests pin the two halves shut.

// gateEngine lets a test hold an executor inside ExecuteRun, the way a slow
// claim under store contention holds one for whole ticks. Releases may be
// repeated; every call lets go of whatever executions are waiting.
type gateEngine struct {
	mu      sync.Mutex
	waiting []chan struct{}
	entered chan string
}

func (g *gateEngine) ExecuteRun(_ context.Context, runID string) (string, error) {
	g.entered <- runID
	g.mu.Lock()
	ch := make(chan struct{})
	g.waiting = append(g.waiting, ch)
	g.mu.Unlock()
	<-ch
	return "succeeded", nil
}

func (g *gateEngine) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, ch := range g.waiting {
		close(ch)
	}
	g.waiting = nil
}

// TestASubmissionNeverOverlapsItsOwnExecution pins the pool half: while a run
// is being driven, handing the same id to submit again must not start a second
// executor. The dispatcher treats an accepted submission as done talking, so
// the duplicate answer is yes; the promise is that only one execution exists.
func TestASubmissionNeverOverlapsItsOwnExecution(t *testing.T) {
	gate := &gateEngine{
		entered: make(chan string, 4),
	}
	drainer := &stubEngine{}
	// Four slots on purpose: the duplicate must be refused by the pool's own
	// bookkeeping, not by the accident of a full pool, which would hide the
	// bug on any machine with more than one worker.
	p := &executorPool{
		eng:   gate,
		drain: drainer,
		clk:   clock.System(),
		log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		slots: make(chan struct{}, 4),
	}
	ctx := context.Background()

	if !p.submit(ctx, "run-1") {
		t.Fatalf("the first submit was refused on an empty pool")
	}
	select {
	case id := <-gate.entered:
		if id != "run-1" {
			t.Fatalf("the executor started for %q, want run-1", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the first submission never reached the engine")
	}

	if !p.submit(ctx, "run-1") {
		t.Fatalf("a duplicate submission answered no, which means pool full to the dispatcher")
	}

	// One execution is the whole contract. Anything arriving here is the
	// second executor the storm needs, so the wait has to stay short: the
	// bug shows up at once or never.
	select {
	case id := <-gate.entered:
		t.Fatalf("a second executor started for %q while the first still ran", id)
	case <-time.After(200 * time.Millisecond):
	}

	gate.release()
	select {
	case <-p.drained():
	case <-time.After(5 * time.Second):
		t.Fatalf("the pool never drained after the release")
	}

	// The finished execution must leave the pool's bookkeeping: the id is
	// drivable again, and no entry lingers to grow with every run.
	if !p.submit(ctx, "run-1") {
		t.Fatalf("a fresh submit was refused after the first executor left")
	}
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("the pool still believed run-1 was being driven")
	}
	gate.release()
	select {
	case <-p.drained():
	case <-time.After(5 * time.Second):
		t.Fatalf("the pool never drained the second execution")
	}
}

// TestAClaimLoserHandsNothingBack pins the handback half against the real
// store: an executor whose claim failed never owned the row, so it must leave
// the drain path untouched even though the keeper's map carries another live
// attempt's token for the same run - which is exactly what the map holds while
// the winner is mid-step. This is the transaction the storm ran over and over.
func TestAClaimLoserHandsNothingBack(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	st := openServeStore(t, clk)
	runID := seedClaimedRunningRun(t, st)

	// The loser's engine reports what ClaimRun answers when the winner took
	// the row first. The drainer stands where the shared keeper sits: asked
	// about the run, it truthfully names the live winner's token, and it
	// forwards any drain straight into the real store.
	drainer := &stubEngine{ref: store.LeaseRef{Owner: "serve:test", Epoch: 1}, held: true, st: st}
	loser := &stubEngine{err: fmt.Errorf("execute run %s: %w", runID, store.ErrNotClaimable)}
	p := newPoolFor(t, loser, drainer)

	p.execute(context.Background(), runID)

	if drainer.drains != 0 {
		t.Errorf("the claim loser drained %d times: an executor that never owned the row owes no handback", drainer.drains)
	}
	detail, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.Run.State != string(model.RunRunning) || detail.Run.LeaseEpoch != 1 {
		t.Errorf("the live winner's row moved to %s epoch %d: the loser fenced it mid-step",
			detail.Run.State, detail.Run.LeaseEpoch)
	}
}
