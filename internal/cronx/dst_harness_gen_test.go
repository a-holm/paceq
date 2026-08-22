package cronx

// The generators behind the differential and determinism suites. See
// dst_harness_test.go for the pinned properties: same seed, same sequence;
// five valid fields; at most one restricted day field; cuts that always tile
// the window in order.

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// randomCronExprs draws n cron expressions from the corpus shape. Every
// expression has five fields and stays inside the documented contract: no L,
// W or # extensions, day of week 0 to 6, and at most one of day of month and
// day of week restricted, because classic engines disagree on whether that
// combination unions or intersects. That disagreement is a contract question
// rather than a bug hunt, so the corpus never rolls it.
func randomCronExprs(rng *rand.Rand, n int) []string {
	out := make([]string, n)
	for i := range out {
		dom := "*"
		dow := "*"
		switch rng.IntN(3) {
		case 1:
			dom = randomRangeField(rng, 1, 31)
		case 2:
			dow = randomRangeField(rng, 0, 6)
		}
		out[i] = strings.Join([]string{
			randomRangeField(rng, 0, 59),
			randomRangeField(rng, 0, 23),
			dom,
			randomRangeField(rng, 1, 12),
			dow,
		}, " ")
	}
	return out
}

// randomRangeField returns *, */step, a single value, or a possibly stepped
// range, all inside low..high.
func randomRangeField(rng *rand.Rand, low, high int) string {
	span := high - low + 1
	switch rng.IntN(4) {
	case 0:
		return "*"
	case 1:
		return "*/" + strconv.Itoa(1+rng.IntN(span))
	case 2:
		return strconv.Itoa(low + rng.IntN(span))
	default:
		a := low + rng.IntN(span)
		b := low + rng.IntN(span)
		if a > b {
			a, b = b, a
		}
		body := strconv.Itoa(a) + "-" + strconv.Itoa(b)
		if rng.IntN(3) == 0 {
			body += "/" + strconv.Itoa(1+rng.IntN(b-a+1))
		}
		return body
	}
}

// partitionCuts returns up to maxCuts strictly increasing cut points strictly
// inside (from, to), so that the pieces (from, c1], (c1, c2], ..., (cn, to]
// tile the window exactly. Zero cuts means the window whole. The window must
// span at least two seconds.
func partitionCuts(rng *rand.Rand, from, to time.Time, maxCuts int) []time.Time {
	n := 0
	if maxCuts > 0 {
		n = rng.IntN(maxCuts + 1)
	}
	lo := from.Unix()
	hi := to.Unix()
	cuts := make([]time.Time, 0, n)
	seen := map[int64]bool{}
	for len(cuts) < n {
		pick := lo + 1 + rng.Int64N(hi-lo-1)
		if seen[pick] {
			continue
		}
		seen[pick] = true
		cuts = append(cuts, time.Unix(pick, 0).UTC())
	}
	for i := 1; i < len(cuts); i++ {
		for j := i; j > 0 && cuts[j].Before(cuts[j-1]); j-- {
			cuts[j], cuts[j-1] = cuts[j-1], cuts[j]
		}
	}
	return cuts
}
