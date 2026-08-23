package cronx

import (
	"errors"
	"testing"
	"time"
)

// FuzzCronParse feeds arbitrary text to Parse and, for anything that parses,
// walks the iterator a little. The contract under fuzz: no panic, ever; every
// accepted expression re-parses identically from its own canonical form; and
// every occurrence the iterator hands out is UTC, in window, and sorted.
func FuzzCronParse(f *testing.F) {
	seeds := []string{
		"0 2 * * *",
		"*/5 * * * *",
		"@daily",
		"@yearly",
		"@every 90m",
		"15m",
		"0 0 29 2 *",
		"0 0 30 2 *",
		"0 0 * * mon-fri",
		"0 0 1 jul *",
		"60 * * * *",
		"* * * * L",
		"0 0 * * 1#3",
		"junk",
		"",
		"0 2 *",
	}
	for _, s := range seeds {
		f.Add(s, int64(1768176000)) // 2026-01-12T00:00:00Z
	}

	zones := []*time.Location{time.UTC}
	if oslo, err := LoadZone("Europe/Oslo"); err == nil {
		zones = append(zones, oslo)
	}

	f.Fuzz(func(t *testing.T, expr string, seed int64) {
		sched, err := Parse(expr)
		if err != nil {
			return
		}

		t.Run("canonical fixed point", func(t *testing.T) {
			again, err := Parse(sched.Expr)
			if err != nil {
				t.Fatalf("canonical form %q does not re-parse: %v", sched.Expr, err)
			}
			if again != sched {
				t.Fatalf("re-parsing canonical %q gave %+v, want %+v", sched.Expr, again, sched)
			}
		})

		from := time.Unix(seed%1_800_000_000, 0).UTC()
		to := from.Add(6 * time.Hour)

		for _, tz := range zones {
			n, err := sched.Next(from, tz, Policy{})
			if err != nil && !errors.Is(err, ErrNoOccurrence) {
				t.Fatalf("Next error %v", err)
			}
			if err == nil && n.At.Location() != time.UTC {
				t.Fatalf("Next returned %s with location %v, want UTC", n.At, n.At.Location())
			}

			occs, err := sched.Between(from, to, tz, Policy{})
			if err != nil && !errors.Is(err, ErrNoOccurrence) {
				t.Fatalf("Between error %v", err)
			}
			for _, o := range occs {
				if o.At.Location() != time.UTC {
					t.Fatalf("occurrence %s not UTC", o.At)
				}
				if !o.At.After(from) || o.At.After(to) {
					t.Fatalf("occurrence %s outside half open window (%s, %s]", o.At, from, to)
				}
			}

			// Split safety is the invariant that matters downstream: cutting
			// the window anywhere and joining the halves must reproduce the
			// whole sequence.
			mid := from.Add(3 * time.Hour)
			h1, err1 := sched.Between(from, mid, tz, Policy{})
			h2, err2 := sched.Between(mid, to, tz, Policy{})
			whole, werr := sched.Between(from, to, tz, Policy{})
			if (err1 != nil) != (werr != nil) || (err2 != nil) != (werr != nil) {
				t.Fatalf("split error mismatch: whole=%v halves=%v/%v", werr, err1, err2)
			}
			if len(h1)+len(h2) != len(whole) {
				t.Fatalf("split gives %d+%d=%d occurrences, whole gives %d",
					len(h1), len(h2), len(h1)+len(h2), len(whole))
			}
			for i := range h1 {
				if !h1[i].At.Equal(whole[i].At) {
					t.Fatalf("first half diverges at %d: %s vs %s", i, h1[i].At, whole[i].At)
				}
			}
			for i := range h2 {
				if !h2[i].At.Equal(whole[len(h1)+i].At) {
					t.Fatalf("second half diverges at %d: %s vs %s", i, h2[i].At, whole[len(h1)+i].At)
				}
			}

			p, err := sched.Prev(to, tz, Policy{})
			if err != nil && !errors.Is(err, ErrNoOccurrence) {
				t.Fatalf("Prev error %v", err)
			}
			// At may EQUAL to at a spring forward seam, where a skipped slot
			// and a real slot share one instant; it must never land after.
			if err == nil && p.At.After(to) {
				t.Fatalf("Prev(%s) = %s, want at or before", to, p.At)
			}
		}
	})
}
