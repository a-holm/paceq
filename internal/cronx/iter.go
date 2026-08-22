package cronx

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrNoOccurrence reports that an expression never matches within the search
// horizon, such as "0 0 30 2 *" (February 30th). errors.Is works through the
// wrapped context.
var ErrNoOccurrence = errors.New("no occurrence within the four year search horizon")

const (
	// maxHorizon bounds how far one Next or Prev search may reach from its
	// starting point. Four long years cover every real schedule; an expression
	// with no hit inside it has a day/month combination that never exists
	// (T12 in the security review). Between walks the tuples directly and is
	// bounded by its own window end instead.
	maxHorizon = 4 * 366 * 24 * time.Hour

	// maxIterations is the hard cap on candidate wall clock tuples examined
	// for one answer. It is the backstop under maxHorizon against any
	// expression that would otherwise spin.
	maxIterations = 100_000
)

// localWallLayout renders Occurrence.LocalWall: local wall clock with offset.
const localWallLayout = "2006-01-02 15:04:05 -07:00"

// formatLocalWall renders at in tz as "2006-01-02 15:04:05 -07:00".
//
// Hand rolled because LocalWall is built once per occurrence and a year long
// Between spends a third of its time inside time.Time.Format. The offset
// rendering replicates Format exactly: sub minute parts truncate away and a
// fully truncated negative offset keeps the plus sign (-59 seconds prints
// "+00:00", not "-00:00"). formatLocalWallParityTest pins every branch of
// this against Format itself.
func formatLocalWall(at time.Time, tz *time.Location) string {
	l := at.In(tz)
	y, mo, d := l.Date()
	if y < 1 || y > 9999 {
		return l.Format(localWallLayout) // outside the guarded era: defer
	}
	h, mi, s := l.Clock()
	_, off := l.Zone()

	var b [26]byte
	b[0] = byte('0' + y/1000%10)
	b[1] = byte('0' + y/100%10)
	b[2] = byte('0' + y/10%10)
	b[3] = byte('0' + y%10)
	b[4] = '-'
	putTwo(b[5:7], int(mo))
	b[7] = '-'
	putTwo(b[8:10], d)
	b[10] = ' '
	putTwo(b[11:13], h)
	b[13] = ':'
	putTwo(b[14:16], mi)
	b[16] = ':'
	putTwo(b[17:19], s)
	b[19] = ' '

	neg := off < 0
	a := off
	if neg {
		a = -off
	}
	hh := a / 3600
	mm := (a % 3600) / 60
	if neg && (hh != 0 || mm != 0) {
		b[20] = '-'
	} else {
		b[20] = '+'
	}
	putTwo(b[21:23], hh)
	b[23] = ':'
	putTwo(b[24:26], mm)
	return string(b[:])
}

func putTwo(b []byte, v int) {
	b[0] = byte('0' + v/10%10)
	b[1] = byte('0' + v%10)
}

// wallPoint is a minute aligned local wall clock tuple inside some zone.
type wallPoint struct {
	y  int
	mo time.Month
	d  int
	h  int
	mi int
}

func lastDay(y int, mo time.Month) int {
	return time.Date(y, mo+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func weekday(y int, mo time.Month, d int) int {
	return int(time.Date(y, mo, d, 12, 0, 0, 0, time.UTC).Weekday())
}

// carryFromHour rolls hour overflow into the day.
func (w *wallPoint) carryFromHour() {
	if w.h <= 23 {
		return
	}
	w.h = 0
	w.d++
	w.carryDay()
}

// carryDay rolls day overflow into the next month, and month overflow into
// the next year, until the tuple names a real date.
func (w *wallPoint) carryDay() {
	for {
		if w.mo > 12 {
			w.mo = 1
			w.y++
		}
		if w.d <= lastDay(w.y, w.mo) {
			return
		}
		w.d = 1
		w.mo++
	}
}

func (w *wallPoint) borrowToHour() {
	if w.h >= 0 {
		return
	}
	w.h = 23
	w.d--
	w.borrowDay()
}

func (w *wallPoint) borrowDay() {
	for {
		if w.mo < 1 {
			w.mo = 12
			w.y--
		}
		if w.y < 1 {
			return
		}
		if w.d >= 1 {
			return
		}
		w.mo--
		if w.mo < 1 {
			w.mo = 12
			w.y--
		}
		w.d = lastDay(w.y, w.mo)
	}
}

// Next returns the first occurrence strictly after from, applying p at any
// daylight saving edge between the two. from may carry any location; the
// returned Occurrence.At is always UTC.
//
// For interval schedules the DST policy plays no part: intervals are pure UTC
// arithmetic on a grid anchored to the Unix epoch.
func (s Schedule) Next(from time.Time, tz *time.Location, p Policy) (Occurrence, error) {
	if err := checkArgs(tz, p); err != nil {
		return Occurrence{}, err
	}
	if s.Kind == KindInterval {
		return s.intervalOccurrence(intervalNext(s.Interval, from), tz), nil
	}
	return s.cronNext(from, tz, p)
}

// Prev returns the last occurrence strictly before from, mirroring Next: the
// same policy decides which instance of a doubled slot is real, and skipped
// instances are still occurrences, so Prev lands on them when they were the
// latest event on the timeline.
func (s Schedule) Prev(from time.Time, tz *time.Location, p Policy) (Occurrence, error) {
	if err := checkArgs(tz, p); err != nil {
		return Occurrence{}, err
	}
	if s.Kind == KindInterval {
		return s.intervalOccurrence(intervalPrev(s.Interval, from), tz), nil
	}
	return s.cronPrev(from, tz, p)
}

// Between returns every occurrence in the half open window (from, to],
// ordered by the schedule's local timetable, with Skipped occurrences in
// their place. In UTC terms instants rise throughout except across a spring
// forward seam, where skipped slots land between the real slots whose
// instants they share; see occKey.
//
// Between(t, t) is empty by definition of half open, and so is any window
// where to does not lie after from.
//
// When the iteration guard trips mid window (an expression whose matches stop
// before to), the occurrences found so far come back together with
// ErrNoOccurrence so a caller can tell a truncated answer from an empty one.
//
// One window costs ONE compile of the expression and one classification pass
// per matched slot; chaining Next would re-parse per fire and miss the backfill
// budget by an order of magnitude.
func (s Schedule) Between(from, to time.Time, tz *time.Location, p Policy) ([]Occurrence, error) {
	if err := checkArgs(tz, p); err != nil {
		return nil, err
	}
	if !to.After(from) {
		return []Occurrence{}, nil
	}

	if s.Kind == KindInterval {
		return s.intervalBetween(from, to, tz), nil
	}

	c, err := compile(s)
	if err != nil {
		return nil, err
	}

	uFrom := from.UTC().Round(0)
	uTo := to.UTC().Round(0)
	kFrom := occKey{pos: wallOrdinal(wallOf(uFrom, tz)), at: uFrom}
	// A tuple can only materialise instants from 14 hours BEFORE its wall
	// clock position onwards (the largest negative UTC offset any zone uses),
	// so tuples further than 16 wall hours past the end wall can never land
	// inside the window, whatever the zone.
	stopPos := wallOrdinal(wallOf(uTo, tz)) + 16*60

	out := []Occurrence{}
	var ebuf [2]emission
	start := wallPointOf(kFrom.pos)
	cur, ok := c.firstAtOrAfter(start)
	if !ok {
		return out, noOcc(s)
	}
	for {
		if cur.y > start.y+6 || cur.y > 9999 {
			return out, noOcc(s)
		}
		pos := wallOrdinal(cur)
		if pos > stopPos {
			return out, nil
		}
		if c.matches(cur) {
			for _, e := range classifyEmissions(cur, tz, p, &ebuf) {
				k := keyOf(cur, e.at)
				if kFrom.less(k) && !uTo.Before(k.at) {
					out = append(out, buildOccurrence(e, cur, tz))
				}
			}
		}
		nw, ok := c.nextAfter(cur)
		if !ok {
			// The expression ran out of matches inside the window: report
			// what was collected together with the truncation signal.
			return out, noOcc(s)
		}
		cur = nw
	}
}

func checkArgs(tz *time.Location, p Policy) error {
	if tz == nil {
		return fmt.Errorf("nil time zone: pass a location loaded with cronx.LoadZone")
	}
	return p.validate()
}

func noOcc(s Schedule) error {
	return fmt.Errorf("%w for expression %q: no matching date inside four years, check the day of month and month combination", ErrNoOccurrence, s.Expr)
}

// occKey totally orders emissions: PRIMARY the wall clock position of the
// tuple (the local timetable order), SECONDARY the instant for the two
// emissions one doubled slot produces. This order is what makes the sequence
// deterministic and split safe: chopping the window anywhere and joining the
// halves always reproduces the whole.
//
// In UTC terms instants rise with one exception: right after a spring
// forward gap, skipped slots (whose nonexistent wall times normalize into
// instants the post gap real slots also occupy) sit BETWEEN the real slots
// around them. Consumers key ticks on the pair of instant and skip reason,
// or dedupe on their side; the sequence itself stays deterministic.
type occKey struct {
	pos int64 // monotone encoding of the wall tuple
	at  time.Time
}

func wallOrdinal(w wallPoint) int64 {
	return (((int64(w.y)*16+int64(w.mo))*32+int64(w.d))*24+int64(w.h))*60 + int64(w.mi)
}

// wallPointOf decodes what wallOrdinal encoded. Only ordinals built by
// wallOrdinal or whole minute steps enter here, so the decode is exact.
func wallPointOf(ord int64) wallPoint {
	mi := ord % 60
	ord /= 60
	h := ord % 24
	ord /= 24
	d := ord % 32
	ord /= 32
	mo := ord % 16
	ord /= 16
	return wallPoint{y: int(ord), mo: time.Month(mo), d: int(d), h: int(h), mi: int(mi)}
}

// firstAtOrAfter returns the smallest matching wall clock tuple at or after w.
// The emission level filter in Between decides about strictness afterwards.
func (c *compiled) firstAtOrAfter(w wallPoint) (wallPoint, bool) {
	if c.matches(w) {
		return w, true
	}
	return c.nextAfter(w)
}

func keyOf(w wallPoint, at time.Time) occKey {
	return occKey{pos: wallOrdinal(w), at: at.UTC()}
}

func (k occKey) less(o occKey) bool {
	if k.pos != o.pos {
		return k.pos < o.pos
	}
	return k.at.Before(o.at)
}

// cronNext returns the emission with the smallest key strictly greater than
// the cursor's key. Wall position is the primary key, so the first qualifying
// emission found scanning tuples upward IS the answer: every later tuple has
// a larger position and cannot beat it.
func (s Schedule) cronNext(from time.Time, tz *time.Location, p Policy) (Occurrence, error) {
	c, err := compile(s)
	if err != nil {
		return Occurrence{}, err
	}
	u := from.UTC().Round(0)
	deadline := u.Add(maxHorizon)

	start := wallOf(u, tz)
	ku := occKey{pos: wallOrdinal(start), at: u}
	cur := start

	var ebuf [2]emission
	for i := 0; i < maxIterations; i++ {
		if cur.y > start.y+5 || cur.y > 9999 {
			return Occurrence{}, noOcc(s)
		}
		if c.matches(cur) {
			for _, e := range classifyEmissions(cur, tz, p, &ebuf) {
				if ku.less(keyOf(cur, e.at)) {
					occ := buildOccurrence(e, cur, tz)
					if occ.At.After(deadline) {
						return Occurrence{}, noOcc(s)
					}
					return occ, nil
				}
			}
		}
		nw, ok := c.nextAfter(cur)
		if !ok {
			return Occurrence{}, noOcc(s)
		}
		cur = nw
	}
	return Occurrence{}, noOcc(s)
}

// cronPrev mirrors cronNext: the largest key strictly below the cursor,
// scanning tuples downward.
func (s Schedule) cronPrev(from time.Time, tz *time.Location, p Policy) (Occurrence, error) {
	c, err := compile(s)
	if err != nil {
		return Occurrence{}, err
	}
	u := from.UTC().Round(0)
	deadline := u.Add(-maxHorizon)

	start := wallOf(u, tz)
	ku := occKey{pos: wallOrdinal(start), at: u}
	cur := start

	var ebuf [2]emission
	for i := 0; i < maxIterations; i++ {
		if cur.y < start.y-5 || cur.y < 1 {
			return Occurrence{}, noOcc(s)
		}
		if c.matches(cur) {
			emis := classifyEmissions(cur, tz, p, &ebuf)
			for j := len(emis) - 1; j >= 0; j-- {
				e := emis[j]
				if keyOf(cur, e.at).less(ku) {
					occ := buildOccurrence(e, cur, tz)
					if occ.At.Before(deadline) {
						return Occurrence{}, noOcc(s)
					}
					return occ, nil
				}
			}
		}
		pw, ok := c.prevBefore(cur)
		if !ok {
			return Occurrence{}, noOcc(s)
		}
		cur = pw
	}
	return Occurrence{}, noOcc(s)
}

func wallOf(u time.Time, tz *time.Location) wallPoint {
	l := u.In(tz)
	return wallPoint{y: l.Year(), mo: l.Month(), d: l.Day(), h: l.Hour(), mi: l.Minute()}
}

// emission is one concrete instant a policy decides to materialise for a
// matched slot, in chronological order within the slot.
type emission struct {
	at        time.Time
	skipped   bool
	reason    string
	requested bool // LocalWall shows the requested (missing) wall clock
}

// classifyEmissions implements the documented DST table for one matched wall
// clock tuple. Detection follows the standard construct-and-read-back trick:
// Go resolves a nonexistent wall time through the offset before the jump, so
// the normalized Hour differs from the requested one exactly when the slot
// fell into a gap, and its value IS the shift target. A duplicate is found by
// scanning plus and minus one hour, then thirty minutes, for a second instant
// rendering the same wall clock; Lord Howe shifts by thirty minutes, which is
// why the scan must not look at full hours only.
//
// The result goes into the caller's buffer so a long Between walk does not
// allocate a slice per matched slot.
func classifyEmissions(w wallPoint, tz *time.Location, p Policy, buf *[2]emission) []emission {
	t := time.Date(w.y, w.mo, w.d, w.h, w.mi, 0, 0, tz)

	if t.Hour() != w.h || t.Minute() != w.mi {
		// Spring forward: the wall time does not exist. t is the instant the
		// requested clock maps to after the jump.
		if p.SpringForward == Shift {
			buf[0] = emission{at: t}
			return buf[:1]
		}
		buf[0] = emission{at: t, skipped: true, reason: SkipReasonNonexistent, requested: true}
		return buf[:1]
	}

	for _, delta := range []time.Duration{time.Hour, 30 * time.Minute} {
		alt := t.Add(-delta)
		if alt.Hour() == w.h && alt.Minute() == w.mi {
			return fallBackEmissions(alt, t, p, buf)
		}
		alt = t.Add(delta)
		if alt.Hour() == w.h && alt.Minute() == w.mi {
			return fallBackEmissions(t, alt, p, buf)
		}
	}
	buf[0] = emission{at: t}
	return buf[:1]
}

func fallBackEmissions(first, second time.Time, p Policy, buf *[2]emission) []emission {
	if p.FallBack == Both {
		buf[0] = emission{at: first}
		buf[1] = emission{at: second}
		return buf[:2]
	}
	buf[0] = emission{at: first}
	buf[1] = emission{at: second, skipped: true, reason: SkipReasonDuplicate}
	return buf[:2]
}

// buildOccurrence turns an emission into the public shape. At leaves as UTC,
// always. LocalWall shows the actual wall clock of At, except for a skipped
// spring forward slot, where showing the shifted clock would hide the very
// thing being explained; there it shows the requested wall clock with the
// offset that applied just before the clocks jumped.
func buildOccurrence(e emission, w wallPoint, tz *time.Location) Occurrence {
	o := Occurrence{
		At:         e.at.UTC().Round(0),
		Skipped:    e.skipped,
		SkipReason: e.reason,
	}
	if e.requested {
		// Go built e.at by interpreting the requested wall clock with the
		// offset that applies BEFORE the jump. That offset falls straight
		// out of the difference between the wall clock read as UTC and the
		// instant itself.
		asUTC := time.Date(w.y, w.mo, w.d, w.h, w.mi, 0, 0, time.UTC)
		off := int(asUTC.Sub(e.at) / time.Second)
		o.LocalWall = renderRequestedWall(w, off)
	} else {
		o.LocalWall = formatLocalWall(e.at, tz)
	}
	return o
}

// renderRequestedWall shows the wall clock the user asked for together with
// the offset that applied just before the clocks jumped.
func renderRequestedWall(w wallPoint, off int) string {
	var b [26]byte
	b[0] = byte('0' + w.y/1000%10)
	b[1] = byte('0' + w.y/100%10)
	b[2] = byte('0' + w.y/10%10)
	b[3] = byte('0' + w.y%10)
	b[4] = '-'
	putTwo(b[5:7], int(w.mo))
	b[7] = '-'
	putTwo(b[8:10], w.d)
	b[10] = ' '
	putTwo(b[11:13], w.h)
	b[13] = ':'
	putTwo(b[14:16], w.mi)
	b[16] = ':'
	b[17], b[18] = '0', '0'
	b[19] = ' '

	neg := off < 0
	a := off
	if neg {
		a = -off
	}
	hh := a / 3600
	mm := (a % 3600) / 60
	if neg && (hh != 0 || mm != 0) {
		b[20] = '-'
	} else {
		b[20] = '+'
	}
	putTwo(b[21:23], hh)
	b[23] = ':'
	putTwo(b[24:26], mm)
	return string(b[:])
}

// matches reports whether the whole tuple satisfies every field set. Day of
// month and day of week follow the Vixie OR rule: when BOTH are restricted,
// either matching counts; otherwise both fields must match.
func (c *compiled) matches(w wallPoint) bool {
	if !containsInt(c.months, int(w.mo)) {
		return false
	}
	if !containsInt(c.hours, w.h) || !containsInt(c.minutes, w.mi) {
		return false
	}
	return c.dayMatches(w.y, w.mo, w.d)
}

// monthHas reports whether the month field admits mo.
func (c *compiled) monthHas(mo time.Month) bool {
	return containsInt(c.months, int(mo))
}

func (c *compiled) dayMatches(y int, mo time.Month, d int) bool {
	if d < 1 || d > lastDay(y, mo) {
		return false
	}
	if c.domWild && c.dowWild {
		return true
	}
	dm := !c.domWild && containsInt(c.doms, d)
	dw := !c.dowWild && containsInt(c.dows, weekday(y, mo, d))
	switch {
	case c.domWild:
		return dw
	case c.dowWild:
		return dm
	default:
		return dm || dw
	}
}

// nextAfter returns the smallest matching wall clock tuple strictly after w.
// Phase one tries a later minute inside w's own hour; phase two rolls into a
// fresh hour, day or month, where the finer fields always restart at their
// FIRST value. Field sets are jumped, not stepped, which is what makes
// impossible dates such as February 30th die after a handful of hops.
func (c *compiled) nextAfter(w wallPoint) (wallPoint, bool) {
	if c.monthHas(w.mo) && c.dayMatches(w.y, w.mo, w.d) && containsInt(c.hours, w.h) {
		if j := sort.SearchInts(c.minutes, w.mi+1); j < len(c.minutes) {
			n := w
			n.mi = c.minutes[j]
			return n, true
		}
	}

	n := w
	n.mi = 0
	n.h++
	n.carryFromHour()

	for k := 0; k < maxIterations; k++ {
		if n.y > w.y+6 || n.y > 9999 {
			return wallPoint{}, false // six years out, nothing relevant remains
		}
		if !containsInt(c.months, int(n.mo)) {
			n.d, n.h = 1, 0
			n.mo++
			n.carryDay()
			continue
		}
		nd, ok := c.nextDayIn(n.y, n.mo, n.d)
		if !ok {
			n.d, n.h = 1, 0
			n.mo++
			n.carryDay()
			continue
		}
		if nd != n.d {
			n.d = nd
			n.h = 0
		}
		if !containsInt(c.hours, n.h) {
			if i := sort.SearchInts(c.hours, n.h); i < len(c.hours) {
				n.h = c.hours[i]
			} else {
				n.d++
				n.h = 0
				n.carryDay()
				continue
			}
		}
		n.mi = c.minutes[0]
		return n, true
	}
	return wallPoint{}, false
}

// prevBefore mirrors nextAfter backward.
func (c *compiled) prevBefore(w wallPoint) (wallPoint, bool) {
	if c.monthHas(w.mo) && c.dayMatches(w.y, w.mo, w.d) && containsInt(c.hours, w.h) {
		if j := sort.SearchInts(c.minutes, w.mi) - 1; j >= 0 {
			n := w
			n.mi = c.minutes[j]
			return n, true
		}
	}

	n := w
	n.mi = 59
	n.h--
	n.borrowToHour()

	for k := 0; k < maxIterations; k++ {
		if n.y < w.y-6 || n.y < 1 {
			return wallPoint{}, false
		}
		if !containsInt(c.months, int(n.mo)) {
			n.retreatMonth()
			continue
		}
		pd, ok := c.prevDayIn(n.y, n.mo, n.d)
		if !ok {
			n.retreatMonth()
			continue
		}
		if pd != n.d {
			n.d = pd
			n.h = 23
		}
		if !containsInt(c.hours, n.h) {
			if i := sort.SearchInts(c.hours, n.h+1) - 1; i >= 0 {
				n.h = c.hours[i]
			} else {
				n.d--
				n.h = 23
				n.borrowDay()
				continue
			}
		}
		n.mi = c.minutes[len(c.minutes)-1]
		return n, true
	}
	return wallPoint{}, false
}

func (w *wallPoint) retreatMonth() {
	w.mo--
	if w.mo < 1 {
		w.mo = 12
		w.y--
	}
	w.d = lastDay(w.y, w.mo)
	w.h, w.mi = 23, 59
}

// nextDayIn returns the smallest day of this month, at or above d, whose day
// predicate holds. A wildcard day of month steps day by day for at most a
// week (weekday rule) before giving up for the month. A restricted day of
// month walks its own members ONLY when day of week is a wildcard; when both
// fields are restricted the Vixie OR rule makes the matching days a UNION of
// the two sets, so a plain day walk is the only honest search.
func (c *compiled) nextDayIn(y int, mo time.Month, d int) (int, bool) {
	last := lastDay(y, mo)
	switch {
	case c.domWild && c.dowWild:
		if d <= last {
			return d, true
		}
		return 0, false
	case c.domWild:
		for dd := d; dd <= last; dd++ {
			if c.dayMatches(y, mo, dd) {
				return dd, true
			}
		}
		return 0, false
	default:
		if !c.dowWild {
			for dd := d; dd <= last; dd++ {
				if c.dayMatches(y, mo, dd) {
					return dd, true
				}
			}
			return 0, false
		}
		dd := d
		for {
			i := sort.SearchInts(c.doms, dd)
			if i >= len(c.doms) || c.doms[i] > last {
				return 0, false
			}
			cand := c.doms[i]
			if c.dayMatches(y, mo, cand) {
				return cand, true
			}
			dd = cand + 1
		}
	}
}

func (c *compiled) prevDayIn(y int, mo time.Month, d int) (int, bool) {
	switch {
	case c.domWild && c.dowWild:
		if d >= 1 {
			return d, true
		}
		return 0, false
	case c.domWild:
		for dd := d; dd >= 1; dd-- {
			if c.dayMatches(y, mo, dd) {
				return dd, true
			}
		}
		return 0, false
	default:
		if !c.dowWild {
			// Both day fields restricted: mirror of the nextDayIn union walk.
			for dd := d; dd >= 1; dd-- {
				if c.dayMatches(y, mo, dd) {
					return dd, true
				}
			}
			return 0, false
		}
		dd := d
		for {
			i := sort.SearchInts(c.doms, dd+1) - 1
			if i < 0 {
				return 0, false
			}
			cand := c.doms[i]
			if cand < 1 {
				return 0, false
			}
			if c.dayMatches(y, mo, cand) {
				return cand, true
			}
			dd = cand - 1
		}
	}
}

func containsInt(set []int, v int) bool {
	i := sort.SearchInts(set, v)
	return i < len(set) && set[i] == v
}
