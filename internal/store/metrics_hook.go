package store

import (
	"errors"
)

// MetricsHook receives the event-time observations the in-memory side of
// /metrics is built from (#40): tick outcomes as their transactions commit,
// and lease reclaims as the reaper decides them. The interface lives here so
// the store stays free of any dependency on a metrics package; the
// implementation lives in the consumer. Nil means nobody is listening, which
// is the normal state of every process that never serves /metrics.
//
// The hook is called after the transaction has committed, never inside it:
// a counter must not be able to record something the database refused.
type MetricsHook interface {
	// ObserveTick records one decided evaluation. kind is "schedule" or
	// "sensor", name is the schedule or sensor name without its job
	// prefix, outcome is the status that landed in ticks.outcome, and
	// reasonCode is the closed vocabulary that explains a tick which
	// produced no run ("" for one that did).
	ObserveTick(kind, name, outcome, reasonCode string)

	// ObserveLeaseReclaims records n runs whose leases the reaper took
	// away from a dead holder.
	ObserveLeaseReclaims(n int)
}

const (
	// busyCode is plain SQLITE_BUSY and busySnapshotCode (tx.go) its
	// snapshot variant. Both say the write lock was contended, which is
	// what pulseq_db_busy_total counts.
	busyCode = 5
)

// isBusy reports whether err is one of the two SQLITE_BUSY outcomes. Plain
// BUSY surfaces only past busy_timeout, SNAPSHOT only from the lock upgrade
// the retry loop owns; both are contention facts worth exposing.
func isBusy(err error) bool {
	var coded interface{ Code() int }
	if !errors.As(err, &coded) {
		return false
	}
	return coded.Code() == busyCode || coded.Code() == busySnapshotCode
}

// observeTick forwards one committed evaluation to the hook, when there is
// one. The nil check lives here rather than at every call site.
func (s *Store) observeTick(kind, name, outcome, reasonCode string) {
	if s.metrics == nil {
		return
	}
	s.metrics.ObserveTick(kind, name, outcome, reasonCode)
}

// observeLeaseReclaims forwards one reaper sweep's reclaim count.
func (s *Store) observeLeaseReclaims(n int) {
	if s.metrics == nil || n <= 0 {
		return
	}
	s.metrics.ObserveLeaseReclaims(n)
}

// TakeWriteWaitMax returns the wall time of the slowest write transaction
// since the previous take, in seconds, and resets it to zero. Scrape-side
// semantics: a max that only ever ratchets up would go stale the first time
// the database had a genuinely bad second and then never report an ordinary
// one again. The swap is atomic because writers and the scraper share it.
func (s *Store) TakeWriteWaitMax() float64 {
	return float64(s.writeWaitMaxNanos.Swap(0)) / 1e9
}

// BusyTotal returns how many SQLITE_BUSY outcomes the write pool has seen
// since the store was opened. A monotone load, never reset: Prometheus owns
// the rate arithmetic.
func (s *Store) BusyTotal() uint64 {
	return s.busyTotal.Load()
}
