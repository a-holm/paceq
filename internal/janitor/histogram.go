package janitor

import (
	"sort"
	"sync"
	"time"
)

// lockHold is the histogram of write-transaction durations the retention
// batches produce. It is the evidence behind the acceptance criterion: the
// janitor never holds the write lock longer than a few tens of
// milliseconds, measured batch by batch rather than assumed.
//
// The ring keeps the most recent 4096 samples - about twenty full passes of
// a two-hundred-thousand-run database - so a long-lived daemon cannot grow
// this without bound.
type lockHold struct {
	mu      sync.Mutex
	samples []time.Duration
	next    int
	filled  bool
}

const lockHoldCapacity = 4096

func (h *lockHold) record(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.samples == nil {
		h.samples = make([]time.Duration, lockHoldCapacity)
	}
	h.samples[h.next] = d
	h.next++
	if h.next == lockHoldCapacity {
		h.next = 0
		h.filled = true
	}
}

// HoldSnapshot is one reading of the histogram.
type HoldSnapshot struct {
	Samples int
	Max     time.Duration
	P50     time.Duration
	P99     time.Duration
}

// Under proves a bound against the recorded sample set. An empty histogram
// proves nothing, so it fails closed.
func (s HoldSnapshot) Under(limit time.Duration) bool {
	return s.Samples > 0 && s.Max <= limit && s.P99 <= limit && s.P50 <= limit
}

// Hold reads the histogram out.
func (j *Janitor) Hold() HoldSnapshot {
	j.hist.mu.Lock()
	defer j.hist.mu.Unlock()

	n := len(j.hist.samples)
	if !j.hist.filled {
		n = j.hist.next
	}
	if n == 0 {
		return HoldSnapshot{}
	}
	sorted := make([]time.Duration, n)
	if j.hist.filled {
		copy(sorted, j.hist.samples[j.hist.next:])
		copy(sorted[len(j.hist.samples[j.hist.next:]):], j.hist.samples[:j.hist.next])
	} else {
		copy(sorted, j.hist.samples[:n])
	}
	sort.Slice(sorted, func(i, k int) bool { return sorted[i] < sorted[k] })
	pick := func(p float64) time.Duration {
		idx := int(p * float64(len(sorted)-1))
		return sorted[idx]
	}
	return HoldSnapshot{
		Samples: n,
		Max:     sorted[n-1],
		P50:     pick(0.50),
		P99:     pick(0.99),
	}
}
