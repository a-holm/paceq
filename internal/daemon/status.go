package daemon

import (
	"sort"
	"sync"
	"time"
)

// statuses is what the health endpoints read: one row per loop, with its tick
// count and the wall time of its last wake. It is memory only. A health check
// that touched the database would turn a locked database into a restart loop,
// which is exactly backwards (06 section 7.1).
//
// This surface is deliberately small. M2-08 puts a protocol on top of it; the
// facts it needs are collected here from the first loop that starts.
type statuses struct {
	mu  sync.Mutex
	m   map[string]*loopStat
	now func() time.Time
}

type loopStat struct {
	ticks    int64
	lastTick time.Time
}

// LoopStatus is one loop's line in a snapshot.
type LoopStatus struct {
	Name     string
	Ticks    int64
	LastTick time.Time
}

func newStatuses(now func() time.Time) *statuses {
	return &statuses{m: make(map[string]*loopStat), now: now}
}

// mark records one wake of a loop. Loops call it every time they look at the
// world, whether the ticker or the bus woke them.
func (s *statuses) mark(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.m[name]
	if !ok {
		st = &loopStat{}
		s.m[name] = st
	}
	st.ticks++
	st.lastTick = s.now()
}

// snapshot returns every known loop ordered by name, so a caller rendering it
// never depends on map order.
func (s *statuses) snapshot() []LoopStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LoopStatus, 0, len(s.m))
	for name, st := range s.m {
		out = append(out, LoopStatus{Name: name, Ticks: st.ticks, LastTick: st.lastTick})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
