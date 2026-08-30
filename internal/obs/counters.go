package obs

import (
	"sort"
	"sync"

	"github.com/a-holm/paceq/internal/store"
)

// Counters is the in-memory half of the two-source rule (06 section 6.1):
// cumulative event counts that only ever move when an event commits. A
// restart resets them, which is acceptable and expected - Prometheus reads
// counter resets out of its own rate arithmetic - while everything stateful
// stays out of here and is read fresh from the database at scrape time.
//
// The zero value is ready to use, and one instance is shared by the store
// (as its store.MetricsHook) and by the Collector, so a scrape can never see
// a torn pair of maps.
type Counters struct {
	mu sync.Mutex
	// ticks counts decided evaluations per
	// (instigator, name, status, reason_code). The map is the label set:
	// only combinations that actually happened exist, which keeps the
	// series count bounded by what the configuration produced, not by
	// what it could produce.
	ticks map[tickKey]uint64
	// leaseReclaims counts runs whose leases the reaper took from dead
	// holders.
	leaseReclaims uint64
	// notificationsGaveUp counts notifications given up on for good, one
	// per decision. The outbox row cannot answer this: a retry clears
	// failed_at and a delivered row is pruned, so a count over rows walks
	// backwards under ordinary operator work.
	notificationsGaveUp uint64
}

// tickKey is one pulseq_tick_total series identity.
type tickKey struct {
	Kind   string
	Name   string
	Status string
	Reason string
}

// Compile-time proof that Counters is what the store calls back into.
var _ store.MetricsHook = (*Counters)(nil)

// NewCounters returns an empty counter set.
func NewCounters() *Counters {
	return &Counters{ticks: make(map[tickKey]uint64)}
}

// ObserveTick implements store.MetricsHook. It runs after the tick's
// transaction has committed, on a loop goroutine, so it stays cheap: one map
// increment under a mutex.
func (c *Counters) ObserveTick(kind, name, outcome, reasonCode string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ticks == nil {
		c.ticks = make(map[tickKey]uint64)
	}
	c.ticks[tickKey{Kind: kind, Name: name, Status: outcome, Reason: reasonCode}]++
}

// ObserveLeaseReclaims implements store.MetricsHook.
func (c *Counters) ObserveLeaseReclaims(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaseReclaims += uint64(n)
}

// ObserveNotificationGaveUp implements store.MetricsHook.
func (c *Counters) ObserveNotificationGaveUp() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notificationsGaveUp++
}

// snapshotTicks copies the tick map so a scrape renders stable bytes even if
// an event commits mid-render.
func (c *Counters) snapshotTicks() map[tickKey]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[tickKey]uint64, len(c.ticks))
	for k, v := range c.ticks {
		out[k] = v
	}
	return out
}

// snapshotReclaims reads the reclaim total under the same lock.
func (c *Counters) snapshotReclaims() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseReclaims
}

// snapshotGaveUp reads the permanent-failure total under the same lock.
func (c *Counters) snapshotGaveUp() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.notificationsGaveUp
}

// writeSortedTicks renders every tick series in one deterministic order:
// sorted by the full label set, so the same history always produces the same
// bytes whatever map iteration order says. The reason_code label is written
// even when empty, matching the exposition example in the issue: leaving it
// out would make "skipped with no reason" and "triggered" collide into one
// series identity.
func writeSortedTicks(w *Writer, ticks map[tickKey]uint64) {
	type row struct {
		key   tickKey
		count uint64
	}
	rows := make([]row, 0, len(ticks))
	for k, v := range ticks {
		rows = append(rows, row{k, v})
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i].key, rows[j].key
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Status != b.Status {
			return a.Status < b.Status
		}
		return a.Reason < b.Reason
	})
	for _, r := range rows {
		w.Metric("pulseq_tick_total", []L{
			Label("instigator", r.key.Kind),
			Label("name", r.key.Name),
			Label("status", r.key.Status),
			Label("reason_code", r.key.Reason),
		}, float64(r.count))
	}
}
