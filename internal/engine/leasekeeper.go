package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The run-lease keeper. Every claim an executor of this process makes is
// registered here with its fencing token, and one goroutine renews all of
// them together: a single transaction per tick no matter how many runs the
// daemon holds, so staying alive never competes with the workload for write
// budget (02 section 5.2).
//
// The renewal goroutine owns nothing but its own context. It never waits for
// work, and work never waits for it; the two meet only in the store (11 R3).
// When an answer comes back without a run this process believes it holds, or
// with a token that has moved on, the lease is lost and the worker driving
// that run is told at once: its process group dies and its result will be
// discarded, because the row belongs to someone else now.

// heldRun is one claim as the keeper sees it. The entries outlive their
// executor on purpose: between an executor returning and the drain handing
// the row back, the claim is still this process's to answer for, and it is
// still this process's to keep alive.
type heldRun struct {
	epoch int64

	cancelOnce sync.Once
	lostOnce   sync.Once
	cancel     chan struct{}
	lost       chan struct{}
}

// signalCancel and signalLost close their channel exactly once. Both the
// renewal loop and the executor's own exit can reach for the same handle at
// any moment, so the once values, not a lock held somewhere else, make the
// double close impossible.
func (h *heldRun) signalCancel() { h.cancelOnce.Do(func() { close(h.cancel) }) }
func (h *heldRun) signalLost()   { h.lostOnce.Do(func() { close(h.lost) }) }

// hold registers a claim and returns the handle plus the release call. The
// handle's channels close when the renewal loop hears of a cancellation or a
// loss, or when the executor itself stops; release keeps the entry in the map
// so a handback can still find the token after the executor is gone.
func (e *Engine) hold(runID string, epoch int64) (*heldRun, func()) {
	h := &heldRun{
		epoch:  epoch,
		cancel: make(chan struct{}),
		lost:   make(chan struct{}),
	}
	e.mu.Lock()
	if e.held == nil {
		e.held = make(map[string]*heldRun)
	}
	e.held[runID] = h
	e.mu.Unlock()
	return h, func() { e.release(h) }
}

// release ends the executor's side of the claim: nothing selects on the
// signals any more. The entry stays until the drain hands the run back, or a
// renewal tick finds the token gone, whichever happens first.
func (e *Engine) release(h *heldRun) {
	h.signalCancel()
	h.signalLost()
}

// forget drops one claim entry, but only if it is still the one named by the
// caller's token. A newer claim for the same run must never be deleted by an
// older handback arriving late.
func (e *Engine) forget(runID string, epoch int64) {
	e.mu.Lock()
	if e.held[runID] != nil && e.held[runID].epoch == epoch {
		delete(e.held, runID)
	}
	e.mu.Unlock()
}

// drop removes an entry the renewal loop has answered for the last time.
func (e *Engine) drop(runID string, h *heldRun) {
	e.mu.Lock()
	if e.held[runID] == h {
		delete(e.held, runID)
	}
	e.mu.Unlock()
}

// snapshot copies the held claims out from under the lock.
func (e *Engine) snapshot() map[string]*heldRun {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]*heldRun, len(e.held))
	for id, h := range e.held {
		out[id] = h
	}
	return out
}

// renewOnce answers one question for every claim at once: are we still who
// the database thinks we are? The answer carries cancellations back too, so
// a request needs no channel of its own to reach the killer (11 section 4.3).
func (e *Engine) renewOnce(ctx context.Context) error {
	held := e.snapshot()
	if len(held) == 0 {
		return nil
	}
	answers, err := e.Store.RenewRunLeases(ctx, e.Owner, e.ttl())
	if err != nil {
		// One failed renewal says nothing about ownership; the ttl tolerates
		// missed ticks, and the next one tries again. Staying quiet about the
		// failure itself would rot, so it goes to the log.
		logWarn("the lease renewal did not land", "owner", e.Owner, "err", err.Error())
		return nil
	}
	live := make(map[string]int64, len(answers))
	for _, a := range answers {
		live[a.ID] = a.LeaseEpoch
		if !a.CancelRequestedAt.IsZero() {
			if h, ok := held[a.ID]; ok {
				h.signalCancel()
			}
		}
	}
	for id, h := range held {
		ep, ok := live[id]
		if !ok || ep != h.epoch {
			// Either the run left us outright, or it came back wearing a new
			// token after a reap. Either way we are not the owner any more.
			// The entry leaves the map with the ownership: an executor still
			// running hears it through its handle, and one that already
			// returned has nothing left to be told.
			logWarn("lost a run lease - self-fencing", "run", id,
				"epoch", h.epoch, "seen", ep)
			h.signalLost()
			e.drop(id, h)
		}
	}
	return nil
}

// RunLeaseRenewals keeps every claim this process holds alive until the
// context ends. The daemon runs it beside its executors; a foreground run
// command runs it around ExecuteRun. Errors from individual ticks never end
// the loop: losing one renewal is survivable by design, and ending the loop
// would guarantee losing all of them.
func (e *Engine) RunLeaseRenewals(ctx context.Context) error {
	ticker := e.Clock.NewTicker(e.renewEvery())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := e.renewOnce(ctx); err != nil {
				return err
			}
		}
	}
}

// discardResult records that this attempt's verdict was thrown away because
// the lease moved on. The event is history, not state: a writer that cannot
// touch the run may still say why it went quiet.
func (e *Engine) discardResult(ctx context.Context, runID string, ref store.LeaseRef, why string) error {
	err := e.Store.AppendRunEvent(ctx, store.RunEvent{
		RunID:      runID,
		Kind:       "run.result_discarded",
		Actor:      ref.Owner,
		DetailJSON: fmt.Sprintf(`{"lease_epoch":%d,"why":%q}`, ref.Epoch, why),
	})
	if err != nil {
		return fmt.Errorf("discard the result of run %s: %w", runID, err)
	}
	return nil
}

func logWarn(msg string, args ...any) {
	slog.Warn(msg, args...)
}

// HeldLease reports the fencing token this process believes it holds for a
// run. The belief may be stale; every caller hands it straight back to the
// store, which is where it gets checked.
func (e *Engine) HeldLease(runID string) (store.LeaseRef, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, ok := e.held[runID]
	if !ok {
		return store.LeaseRef{}, false
	}
	return store.LeaseRef{Owner: e.Owner, Epoch: h.epoch}, true
}

// DrainRun hands one claimed run back at a clean stop. It is the store's
// transactional handback with this process's own token attached. Whatever the
// row answers, this process is done answering for the run once the call
// returns: a handed-back run belongs to the queue, and a refused handback was
// never ours to give. The entry only leaves when the store call itself
// landed, so a transient error keeps the claim alive for the next try.
func (e *Engine) DrainRun(ctx context.Context, runID string, ref store.LeaseRef, code reason.Code) (bool, error) {
	handed, err := e.Store.DrainRun(ctx, runID, ref, code)
	if err == nil {
		e.forget(runID, ref.Epoch)
	}
	return handed, err
}
