package cronx

import (
	"time"
)

// Interval schedules are pure UTC arithmetic: a grid of points spaced d
// apart, anchored to the Unix epoch. Anchoring to anything computed at call
// time (a daemon start, a from value) would make fire times depend on
// uptime, which breaks determinism guarantee G9: two daemons with different
// uptimes must derive identical occurrence sets from the same schedule.
//
// The math runs in whole seconds. Parse rejects intervals below one second,
// so nothing is lost, and seconds keep every year the time package can
// represent reachable without nanosecond overflow.

func intervalStep(d time.Duration) int64 {
	return int64(d / time.Second)
}

// intervalNext returns the first grid point of d strictly after from.
func intervalNext(d time.Duration, from time.Time) time.Time {
	step := intervalStep(d)
	k := floorDiv(from.Unix(), step) + 1
	at := gridPoint(k, step)
	for !at.After(from) { // unreachable by construction; a cheap belt for readers
		k++
		at = gridPoint(k, step)
	}
	return at
}

// intervalPrev returns the last grid point of d strictly before from.
func intervalPrev(d time.Duration, from time.Time) time.Time {
	step := intervalStep(d)
	k := ceilDiv(from.Unix(), step) - 1
	at := gridPoint(k, step)
	for !at.Before(from) {
		k--
		at = gridPoint(k, step)
	}
	return at
}

// intervalBetween returns every grid point in (from, to], computed straight
// from the closed form rather than by stepping.
func (s Schedule) intervalBetween(from, to time.Time, tz *time.Location) []Occurrence {
	step := intervalStep(s.Interval)
	lo := floorDiv(from.Unix(), step) + 1
	hi := floorDiv(to.Unix(), step)

	out := make([]Occurrence, 0, maxInt(0, hi-lo+1))
	for k := lo; k <= hi; k++ {
		out = append(out, s.intervalOccurrence(gridPoint(k, step), tz))
	}
	return out
}

func (s Schedule) intervalOccurrence(at time.Time, tz *time.Location) Occurrence {
	u := at.UTC().Round(0)
	return Occurrence{At: u, LocalWall: formatLocalWall(u, tz)}
}

func gridPoint(k, step int64) time.Time {
	return time.Unix(k*step, 0).UTC()
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

func ceilDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a > 0 {
		q++
	}
	return q
}

func maxInt(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
