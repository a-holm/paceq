package cronx

// Shared generators for the gold standard suites that compare our iterator
// against independent computations. They live beside the fixtures contract
// test and are usable by every future test in this package, which is why they
// are pinned by their own unit tests now instead of arriving inside a large
// driver later.
//
// Determinism rule: every generator takes its randomness explicitly. A fixed
// seed reproduces a run byte for byte, and no generator reads the wall clock
// or any other process level entropy source.

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRandomCronExprIsDeterministicBySeed(t *testing.T) {
	a := randomCronExprs(rand.New(rand.NewPCG(7, 11)), 200)
	b := randomCronExprs(rand.New(rand.NewPCG(7, 11)), 200)
	if len(a) != len(b) {
		t.Fatalf("same seed produced different counts: %d and %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("expr %d differs under the same seed: %q vs %q", i, a[i], b[i])
		}
	}
	c := randomCronExprs(rand.New(rand.NewPCG(8, 11)), 50)
	allSame := true
	for i := range c {
		if c[i] != a[i] {
			allSame = false
			break
		}
	}
	if allSame && len(c) > 1 {
		t.Fatalf("different seeds produced identical sequences, the generator is stuck")
	}
}

func TestRandomCronExprsAreFiveValidFields(t *testing.T) {
	exprs := randomCronExprs(rand.New(rand.NewPCG(42, 99)), 2000)
	if len(exprs) != 2000 {
		t.Fatalf("want 2000 expressions, got %d", len(exprs))
	}
	for i, expr := range exprs {
		fields := strings.Fields(expr)
		if len(fields) != 5 {
			t.Fatalf("expr %d %q has %d fields, want 5", i, expr, len(fields))
		}
		if err := checkField(fields[0], 0, 59); err != nil {
			t.Fatalf("expr %q minute: %v", expr, err)
		}
		if err := checkField(fields[1], 0, 23); err != nil {
			t.Fatalf("expr %q hour: %v", expr, err)
		}
		if err := checkField(fields[2], 1, 31); err != nil {
			t.Fatalf("expr %q day of month: %v", expr, err)
		}
		if err := checkField(fields[3], 1, 12); err != nil {
			t.Fatalf("expr %q month: %v", expr, err)
		}
		// Day of week uses 0 to 6 only: whether 7 means Sunday is exactly the
		// kind of field interpretation the differential test must not have to
		// guess about.
		if err := checkField(fields[4], 0, 6); err != nil {
			t.Fatalf("expr %q day of week: %v", expr, err)
		}
		// Restricted day of month and day of week never coexist: cron engines
		// disagree on whether that combination unions or intersects, so the
		// differential corpus keeps at most one of them restricted.
		if fields[2] != "*" && fields[4] != "*" {
			t.Fatalf("expr %q restricts both day fields, the corpus forbids that", expr)
		}
		if strings.ContainsAny(expr, "LW#") {
			t.Fatalf("expr %q uses an extension character outside the documented contract", expr)
		}
	}
}

func TestPartitionCutsTileTheWindow(t *testing.T) {
	rng := rand.New(rand.NewPCG(1234, 5))
	from := time.Date(2026, 3, 27, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC)

	first := partitionCuts(rand.New(rand.NewPCG(1234, 5)), from, to, 8)
	second := partitionCuts(rand.New(rand.NewPCG(1234, 5)), from, to, 8)
	if len(first) == 0 || len(first) != len(second) {
		t.Fatalf("same seed gave partitions of different length: %d then %d", len(first), len(second))
	}
	for i := range first {
		if !first[i].Equal(second[i]) {
			t.Fatalf("cut %d differs under the same seed: %s vs %s", i, first[i], second[i])
		}
	}

	prev := from
	for i, cut := range first {
		if !cut.After(prev) {
			t.Fatalf("cut %d %s does not advance past %s", i, cut, prev)
		}
		if !cut.Before(to) {
			t.Fatalf("cut %d %s is not strictly inside the window ending %s", i, cut, to)
		}
		prev = cut
	}

	rng = rand.New(rand.NewPCG(9, 9))
	for range 500 {
		cuts := partitionCuts(rng, from, to, 8)
		if len(cuts) < 0 || len(cuts) > 8 {
			t.Fatalf("%d cuts outside the 0 to 8 range", len(cuts))
		}
	}
}

// checkField validates one cron field against low..high, allowing *, lists,
// ranges and steps. This mirrors what the corpus may contain, not what the
// parser accepts; the parser is free to accept more.
func checkField(field string, low, high int) error {
	if field == "*" {
		return nil
	}
	for _, part := range strings.Split(field, ",") {
		body := part
		if before, after, found := strings.Cut(part, "/"); found {
			body = before
			step, err := strconv.Atoi(after)
			if err != nil || step <= 0 {
				return errInvalidStep
			}
		}
		if body == "*" {
			continue
		}
		lo, hi, isRange := strings.Cut(body, "-")
		start, err := strconv.Atoi(lo)
		if err != nil {
			return err
		}
		end := start
		if isRange {
			if end, err = strconv.Atoi(hi); err != nil {
				return err
			}
		}
		if start < low || start > high || end < low || end > high || end < start {
			return errOutOfRange
		}
	}
	return nil
}

var (
	errOutOfRange  = errString("value out of the field range")
	errInvalidStep = errString("step must be a positive number")
)

type errString string

func (e errString) Error() string { return string(e) }
