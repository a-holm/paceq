package cronx

import (
	"testing"
	"time"
)

// TestEveryNinetyMinutesIsStableAcrossDST is acceptance criterion 5: an
// interval schedule must produce the same count and the same clock positions
// on a transition day as on any other day, because intervals are pure UTC
// arithmetic.
func TestEveryNinetyMinutesIsStableAcrossDST(t *testing.T) {
	oslo := mustZone(t, "Europe/Oslo")
	every := mustParse(t, "@every 90m")
	p := Policy{}

	dayOffsets := func(day string) []time.Duration {
		start := utc(t, day)
		got, err := every.Between(start, start.Add(24*time.Hour), oslo, p)
		if err != nil {
			t.Fatalf("Between %s: %v", day, err)
		}
		if len(got) != 16 {
			t.Fatalf("%s: got %d occurrences in 24h, want 16: %+v", day, len(got), formatOccs(got))
		}
		var offs []time.Duration
		for _, o := range got {
			offs = append(offs, o.At.Sub(start))
		}
		return offs
	}

	plain := dayOffsets("2026-03-26T00:00:00Z")
	springDay := dayOffsets("2026-03-29T00:00:00Z") // clocks jump this morning
	fallDay := dayOffsets("2026-10-25T00:00:00Z")   // clocks fall back this morning

	for i := range plain {
		if plain[i] != springDay[i] {
			t.Errorf("spring day occurrence %d at +%s, want +%s like the plain day", i, springDay[i], plain[i])
		}
		if plain[i] != fallDay[i] {
			t.Errorf("fall day occurrence %d at +%s, want +%s like the plain day", i, fallDay[i], plain[i])
		}
	}
}

func TestIntervalGridAnchorsToTheEpochNotToCallTime(t *testing.T) {
	u := mustZone(t, "UTC")
	every := mustParse(t, "@every 90m")

	// The epoch anchored grid for 90 minutes runs 00:00, 01:30, 03:00 ... so
	// the first point after 10:45 is 12:00, from any starting era.
	got, err := every.Next(utc(t, "1970-01-01T00:45:00Z"), u, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	wantOcc(t, got, "early era grid point", "1970-01-01T01:30:00Z", "1970-01-01 01:30:00 +00:00", false, "")

	// Same answer shape half a century later: grids do not drift.
	got, err = every.Next(utc(t, "2026-06-01T10:45:00Z"), u, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	wantOcc(t, got, "modern grid point", "2026-06-01T12:00:00Z", "2026-06-01 12:00:00 +00:00", false, "")

	prev, err := every.Prev(utc(t, "2026-06-01T10:45:00Z"), u, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	wantOcc(t, prev, "previous grid point", "2026-06-01T10:30:00Z", "2026-06-01 10:30:00 +00:00", false, "")
}

func TestIntervalScheduleIgnoresDSTPolicy(t *testing.T) {
	oslo := mustZone(t, "Europe/Oslo")
	every := mustParse(t, "@every 60m")

	from := utc(t, "2026-03-28T00:00:00Z")
	to := utc(t, "2026-03-31T00:00:00Z")

	a, err := every.Between(from, to, oslo, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := every.Between(from, to, oslo, Policy{SpringForward: Shift, FallBack: Both})
	if err != nil {
		t.Fatal(err)
	}
	if len(a) == 0 || len(a) != len(b) {
		t.Fatalf("policy changed an interval schedule: %d vs %d occurrences", len(a), len(b))
	}
	for i := range a {
		if !a[i].At.Equal(b[i].At) {
			t.Errorf("occurrence %d differs across policies: %s vs %s", i, a[i].At, b[i].At)
		}
		if a[i].Skipped || a[i].SkipReason != "" {
			t.Errorf("interval occurrence %+v carries DST marking: intervals have no DST", formatOcc(a[i]))
		}
	}
}

func TestIntervalLocalWallRendersInTheGivenZone(t *testing.T) {
	oslo := mustZone(t, "Europe/Oslo")
	every := mustParse(t, "@every 24h")

	// The epoch anchored 24h grid fires at UTC midnight; in Oslo that is
	// 02:00 on a summer day. A schedule wanting local 09:00 says so with a
	// cron expression and the zone, not with an interval.
	got, err := every.Next(utc(t, "2026-06-01T09:59:00Z"), oslo, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	wantOcc(t, got, "daily interval", "2026-06-02T00:00:00Z", "2026-06-02 02:00:00 +02:00", false, "")
}
