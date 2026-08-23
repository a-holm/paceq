package cronx

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Ground truth for every literal below was derived from the IANA database
// with an independent implementation (CPython zoneinfo) before the tests were
// written:
//
//	Europe/Oslo 2026: clocks jump +1h at 2026-03-29T01:00:00Z (02:00 CET does
//	not exist that morning) and back at 2026-10-25T01:00:00Z (02:00 happens
//	twice: once at +02:00, once at +01:00).
//
//	Go resolves a nonexistent wall time through the offset BEFORE the jump,
//	which is exactly the instant the documented "shift" policy fires at.

func TestNextWalksPlainCronAcrossZones(t *testing.T) {
	cases := []struct {
		name string
		expr string
		zone string
		from string
		want string
	}{
		{
			name: "utc daily",
			expr: "0 2 * * *", zone: "UTC",
			from: "2026-01-15T02:30:00Z", want: "2026-01-16T02:00:00Z",
		},
		{
			name: "oslo daily away from transitions",
			expr: "0 2 * * *", zone: "Europe/Oslo",
			from: "2026-03-27T12:00:00Z", want: "2026-03-28T01:00:00Z",
		},
		{
			name: "kolkata has no dst but a half hour offset",
			expr: "0 2 * * *", zone: "Asia/Kolkata",
			from: "2026-03-29T10:00:00Z", want: "2026-03-29T20:30:00Z",
		},
		{
			name: "quarterly minutes",
			expr: "*/15 * * * *", zone: "UTC",
			from: "2026-06-01T10:37:00Z", want: "2026-06-01T10:45:00Z",
		},
		{
			name: "mondays only",
			expr: "0 5 * * mon", zone: "Europe/Oslo",
			from: "2026-06-03T00:00:00Z", want: "2026-06-08T03:00:00Z",
		},
		{
			name: "from inside a skipped minute still lands on the grid",
			expr: "*/15 * * * *", zone: "UTC",
			from: "2026-06-01T10:45:00Z", want: "2026-06-01T11:00:00Z",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, tc.expr)
			got, err := s.Next(utc(t, tc.from), mustZone(t, tc.zone), Policy{})
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			wantOcc(t, got, tc.name, tc.want, got.LocalWall, false, "")
			if got.LocalWall == "" {
				t.Errorf("%s: LocalWall empty, want At rendered in %s", tc.name, tc.zone)
			}
		})
	}
}

// TestDSTPolicyMatrixMatchesTheDocumentedTable is acceptance criterion 1: the
// Oslo daily 02:00 schedule across BOTH 2026 transitions, for all four policy
// combinations, checked as whole occurrence sequences.
func TestDSTPolicyMatrixMatchesTheDocumentedTable(t *testing.T) {
	oslo := mustZone(t, "Europe/Oslo")
	daily := mustParse(t, "0 2 * * *")

	type row struct {
		at      string
		skipped bool
		reason  string
	}

	// Spring: 2026-03-29 02:00 CET (+01:00) does not exist. The normalized
	// instant of the missing slot is 2026-03-29T01:00:00Z.
	springRows := map[SpringForward][]row{
		Skip: {
			{"2026-03-28T01:00:00Z", false, ""},
			{"2026-03-29T01:00:00Z", true, SkipReasonNonexistent}, // the gap is a recorded result
			{"2026-03-30T00:00:00Z", false, ""},
		},
		Shift: {
			{"2026-03-28T01:00:00Z", false, ""},
			{"2026-03-29T01:00:00Z", false, ""}, // shifted slot runs at 03:00 +02:00
			{"2026-03-30T00:00:00Z", false, ""},
		},
	}

	// Fall: 2026-10-25 02:00 happens twice, at 00:00Z (+02:00) and 01:00Z
	// (+01:00). The spring policy must not touch this transition.
	fallRows := map[FallBack][]row{
		First: {
			{"2026-10-24T00:00:00Z", false, ""},
			{"2026-10-25T00:00:00Z", false, ""},
			{"2026-10-25T01:00:00Z", true, SkipReasonDuplicate}, // second instance recorded, not run
			{"2026-10-26T01:00:00Z", false, ""},
		},
		Both: {
			{"2026-10-24T00:00:00Z", false, ""},
			{"2026-10-25T00:00:00Z", false, ""},
			{"2026-10-25T01:00:00Z", false, ""}, // two real runs, distinct UTC instants
			{"2026-10-26T01:00:00Z", false, ""},
		},
	}

	check := func(name string, p Policy, from, to string, rows []row) {
		t.Run(name, func(t *testing.T) {
			got, err := daily.Between(utc(t, from), utc(t, to), oslo, p)
			if err != nil {
				t.Fatalf("Between: %v", err)
			}
			if len(got) != len(rows) {
				t.Fatalf("Between returned %d occurrences, want %d: %+v", len(got), len(rows), formatOccs(got))
			}
			for i, want := range rows {
				label := name
				wantOcc(t, got[i], label, want.at, got[i].LocalWall, want.skipped, want.reason)
				if !got[i].Skipped && got[i].LocalWall == "" {
					t.Errorf("row %d (%s): LocalWall empty", i, want.at)
				}
			}
		})
	}

	for _, sf := range []SpringForward{Skip, Shift} {
		for _, fb := range []FallBack{First, Both} {
			p := Policy{SpringForward: sf, FallBack: fb}
			check(
				"spring "+policyName(uint8(sf))+" fall "+policyName(uint8(fb)), p,
				"2026-03-27T12:00:00Z", "2026-03-30T12:00:00Z",
				springRows[sf],
			)
			check(
				"fall "+policyName(uint8(sf))+" fall "+policyName(uint8(fb)), p,
				"2026-10-23T12:00:00Z", "2026-10-26T12:00:00Z",
				fallRows[fb],
			)
		}
	}
}

func policyName(v uint8) string {
	if v == 0 {
		return "skip"
	}
	return "shift"
}

// TestDSTEdgesThroughNextAndPrevIndividually walks the same edges one step at
// a time: the issue's test plan asks for both Next and Between at every edge.
func TestDSTEdgesThroughNextAndPrevIndividually(t *testing.T) {
	oslo := mustZone(t, "Europe/Oslo")
	daily := mustParse(t, "0 2 * * *")

	t.Run("spring skip: the missing slot is returned then passed over", func(t *testing.T) {
		p := Policy{}
		first, err := daily.Next(utc(t, "2026-03-28T12:00:00Z"), oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, first, "gap slot", "2026-03-29T01:00:00Z", "2026-03-29 02:00:00 +01:00", true, SkipReasonNonexistent)

		second, err := daily.Next(first.At, oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, second, "after gap", "2026-03-30T00:00:00Z", "2026-03-30 02:00:00 +02:00", false, "")
	})

	t.Run("spring shift: the missing slot fires after the jump", func(t *testing.T) {
		p := Policy{SpringForward: Shift}
		got, err := daily.Next(utc(t, "2026-03-28T12:00:00Z"), oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, got, "shifted slot", "2026-03-29T01:00:00Z", "2026-03-29 03:00:00 +02:00", false, "")

		next, err := daily.Next(got.At, oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, next, "after shift", "2026-03-30T00:00:00Z", "2026-03-30 02:00:00 +02:00", false, "")
	})

	t.Run("fall first: second instance is a recorded skip", func(t *testing.T) {
		p := Policy{}
		a, err := daily.Next(utc(t, "2026-10-24T12:00:00Z"), oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, a, "first instance", "2026-10-25T00:00:00Z", "2026-10-25 02:00:00 +02:00", false, "")

		b, err := daily.Next(a.At, oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, b, "second instance", "2026-10-25T01:00:00Z", "2026-10-25 02:00:00 +01:00", true, SkipReasonDuplicate)

		c, err := daily.Next(b.At, oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, c, "next day", "2026-10-26T01:00:00Z", "2026-10-26 02:00:00 +01:00", false, "")
	})

	t.Run("fall both: two real runs", func(t *testing.T) {
		p := Policy{FallBack: Both}
		a, err := daily.Next(utc(t, "2026-10-24T12:00:00Z"), oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, a, "first instance", "2026-10-25T00:00:00Z", "2026-10-25 02:00:00 +02:00", false, "")

		b, err := daily.Next(a.At, oslo, p)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, b, "second instance", "2026-10-25T01:00:00Z", "2026-10-25 02:00:00 +01:00", false, "")
	})

	t.Run("prev mirrors next across both edges", func(t *testing.T) {
		skip := Policy{}

		// Spring: prev of the slot after the gap lands back on the recorded
		// gap occurrence.
		gap, err := daily.Next(utc(t, "2026-03-28T12:00:00Z"), oslo, skip)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, gap, "gap slot", "2026-03-29T01:00:00Z", "2026-03-29 02:00:00 +01:00", true, SkipReasonNonexistent)

		after, err := daily.Next(gap.At, oslo, skip)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, after, "after gap", "2026-03-30T00:00:00Z", "2026-03-30 02:00:00 +02:00", false, "")

		back, err := daily.Prev(after.At, oslo, skip)
		if err != nil {
			t.Fatal(err)
		}
		if !occEqual(back, gap) {
			t.Errorf("Prev(next(gap)) = %+v, want the gap occurrence %+v", formatOcc(back), formatOcc(gap))
		}

		// Fall: walking backwards from Monday lands on the second instance
		// first (it is later in the timetable), then the first.
		both := Policy{FallBack: Both}
		last, err := daily.Prev(utc(t, "2026-10-27T00:00:00Z"), oslo, both)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, last, "latest before monday", "2026-10-26T01:00:00Z", "2026-10-26 02:00:00 +01:00", false, "")

		first := Policy{}
		b, err := daily.Prev(utc(t, "2026-10-25T01:30:00Z"), oslo, first)
		if err != nil {
			t.Fatal(err)
		}
		// 01:30Z sits inside the doubled hour: the largest timetable entry
		// strictly below it is the second pass at 01:00Z (recorded, not run).
		wantOcc(t, b, "inside doubled hour", "2026-10-25T01:00:00Z", "2026-10-25 02:00:00 +01:00", true, SkipReasonDuplicate)

		a, err := daily.Prev(b.At, oslo, first)
		if err != nil {
			t.Fatal(err)
		}
		// Stepping below the second instance reaches the first pass.
		wantOcc(t, a, "first pass", "2026-10-25T00:00:00Z", "2026-10-25 02:00:00 +02:00", false, "")
	})
}

// TestLordHoweThirtyMinuteTransition pins the half hour zone: the delta scan
// must look 30 minutes as well as a full hour, or every rule below silently
// misclassifies. This is the exact trap dagster fell into.
func TestLordHoweThirtyMinuteTransition(t *testing.T) {
	lh := mustZone(t, "Australia/Lord_Howe")

	t.Run("spring gap of thirty minutes", func(t *testing.T) {
		q := mustParse(t, "15 2 * * *") // 02:15 does not exist on 2026-10-04
		got, err := q.Between(utc(t, "2026-10-02T00:00:00Z"), utc(t, "2026-10-05T00:00:00Z"), lh, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d occurrences, want 3: %+v", len(got), formatOccs(got))
		}
		wantOcc(t, got[1], "missing 02:15", "2026-10-03T15:45:00Z", "2026-10-04 02:15:00 +10:30", true, SkipReasonNonexistent)
	})

	t.Run("shift lands thirty minutes later", func(t *testing.T) {
		q := mustParse(t, "15 2 * * *")
		got, err := q.Next(utc(t, "2026-10-02T00:00:00Z"), lh, Policy{SpringForward: Shift})
		if err != nil {
			t.Fatal(err)
		}
		// Wait: Next from Oct 2 first hits the NORMAL Oct 3 02:15 slot; walk
		// one further to reach the transition day.
		next, err := q.Next(got.At, lh, Policy{SpringForward: Shift})
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, next, "shifted", "2026-10-03T15:45:00Z", "2026-10-04 02:45:00 +11:00", false, "")
	})

	t.Run("fall overlap of thirty minutes", func(t *testing.T) {
		halfPastOne := mustParse(t, "30 1 * * *") // 01:30 happens twice on 2026-04-05
		got, err := halfPastOne.Between(utc(t, "2026-04-04T00:00:00Z"), utc(t, "2026-04-06T00:00:00Z"), lh, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d occurrences, want 3: %+v", len(got), formatOccs(got))
		}
		wantOcc(t, got[0], "doubled slot first pass", "2026-04-04T14:30:00Z", "2026-04-05 01:30:00 +11:00", false, "")
		wantOcc(t, got[1], "doubled slot second pass", "2026-04-04T15:00:00Z", "2026-04-05 01:30:00 +10:30", true, SkipReasonDuplicate)
	})

	t.Run("both fires the doubled half hour twice", func(t *testing.T) {
		halfPastOne := mustParse(t, "30 1 * * *")
		got, err := halfPastOne.Between(utc(t, "2026-04-04T00:00:00Z"), utc(t, "2026-04-06T00:00:00Z"), lh, Policy{FallBack: Both})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d occurrences, want 3: %+v", len(got), formatOccs(got))
		}
		if got[0].Skipped || got[1].Skipped {
			t.Errorf("fall_back=both must keep both instances real: %+v", formatOccs(got[:2]))
		}
	})
}

// TestShiftPolicyOnMidnightJumpZones pins spring_forward=shift on zones whose
// transition runs through midnight. Santiago jumps 00:00 to 01:00 on
// 2026-09-06 (04:00Z): Go resolves those gap walls through the post jump
// offset, lands before the transition, and the instant renders as an already
// fired wall of the previous evening, colliding with the real evening slots.
// The policy table promises the first existing local time after the jump
// instead. See classifyEmissions for the read-back rule that fixes it.
func TestShiftPolicyOnMidnightJumpZones(t *testing.T) {
	santiago := mustZone(t, "America/Santiago")
	shift := Policy{SpringForward: Shift}

	t.Run("midnight slot fires at the first existing wall after the jump", func(t *testing.T) {
		s := mustParse(t, "0 0 * * *")
		got, err := s.Next(utc(t, "2026-09-05T12:00:00Z"), santiago, shift)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, got, "shifted midnight", "2026-09-06T04:00:00Z",
			"2026-09-06 01:00:00 -03:00", false, "")
	})

	t.Run("half past midnight keeps its minute one jump later", func(t *testing.T) {
		s := mustParse(t, "30 0 * * *")
		got, err := s.Next(utc(t, "2026-09-05T12:00:00Z"), santiago, shift)
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, got, "shifted 00:30", "2026-09-06T04:30:00Z",
			"2026-09-06 01:30:00 -03:00", false, "")
	})

	t.Run("shifted slots never land on already fired walls", func(t *testing.T) {
		// The old normalization put the missing 00:45 at 03:45Z, the very
		// instant the real 23:45 slot of the same schedule fires on: two
		// occurrences sharing one scheduled_for value.
		s := mustParse(t, "45 23,0 * * *")
		got, err := s.Between(utc(t, "2026-09-05T00:00:00Z"), utc(t, "2026-09-07T00:00:00Z"), santiago, shift)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[int64]int{}
		for _, o := range got {
			seen[o.At.Unix()]++
		}
		for unix, n := range seen {
			if n > 1 {
				t.Errorf("instant %s carried %d occurrences: schedules must never emit the same instant twice",
					time.Unix(unix, 0).Format(time.RFC3339), n)
			}
		}
		var shifted Occurrence
		for _, o := range got {
			if o.LocalWall == "2026-09-06 01:45:00 -03:00" {
				shifted = o
				break
			}
		}
		if shifted.At.IsZero() {
			t.Fatalf("the missing 00:45 slot never surfaced as the first existing wall 01:45: %+v", formatOccs(got))
		}
	})

	t.Run("skip still marks the missing midnight", func(t *testing.T) {
		s := mustParse(t, "0 0 * * *")
		got, err := s.Next(utc(t, "2026-09-05T12:00:00Z"), santiago, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		wantOcc(t, got, "missing midnight marker", "2026-09-06T04:00:00Z",
			"2026-09-06 00:00:00 -04:00", true, SkipReasonNonexistent)
	})
}

func TestBetweenIsHalfOpenOnBothEnds(t *testing.T) {
	minutely := mustParse(t, "* * * * *")
	u := mustZone(t, "UTC")

	t.Run("from excluded to included", func(t *testing.T) {
		got, err := minutely.Between(
			utc(t, "2026-06-01T10:00:00Z"),
			utc(t, "2026-06-01T10:03:00Z"),
			u, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		var ats []string
		for _, o := range got {
			ats = append(ats, o.At.Format(time.RFC3339))
		}
		want := []string{
			"2026-06-01T10:01:00Z",
			"2026-06-01T10:02:00Z",
			"2026-06-01T10:03:00Z",
		}
		if len(ats) != len(want) {
			t.Fatalf("got %v, want %v", ats, want)
		}
		for i := range want {
			if ats[i] != want[i] {
				t.Errorf("occurrence %d = %s, want %s", i, ats[i], want[i])
			}
		}
	})

	t.Run("empty window is empty", func(t *testing.T) {
		at := utc(t, "2026-06-01T10:00:00Z")
		got, err := minutely.Between(at, at, u, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Errorf("Between(t, t) = %+v, want empty", formatOccs(got))
		}
	})

	t.Run("reversed window is empty, not an error", func(t *testing.T) {
		got, err := minutely.Between(
			utc(t, "2026-06-01T10:03:00Z"),
			utc(t, "2026-06-01T10:00:00Z"),
			u, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Errorf("Between(to, from) = %+v, want empty", formatOccs(got))
		}
	})

	t.Run("interval schedules obey the same half open rule", func(t *testing.T) {
		every := mustParse(t, "@every 60m")
		got, err := every.Between(
			utc(t, "2026-06-01T10:00:00Z"),
			utc(t, "2026-06-01T13:00:00Z"),
			u, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		wantAts := []string{"2026-06-01T11:00:00Z", "2026-06-01T12:00:00Z", "2026-06-01T13:00:00Z"}
		if len(got) != len(wantAts) {
			t.Fatalf("got %d, want %d: %+v", len(got), len(wantAts), formatOccs(got))
		}
		for i, w := range wantAts {
			wantWall := w[:10] + " " + w[11:16] + ":00 +00:00"
			wantOcc(t, got[i], "grid point", w, wantWall, false, "")
		}
	})
}

func TestBetweenIncludesSkippedOccurrencesInChronologicalPosition(t *testing.T) {
	oslo := mustZone(t, "Europe/Oslo")
	daily := mustParse(t, "0 2 * * *")
	got, err := daily.Between(utc(t, "2026-03-26T12:00:00Z"), utc(t, "2026-04-01T00:00:00Z"), oslo, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) < 4 {
		t.Fatalf("want a week of occurrences, got %d", len(got))
	}
	var skippedAt int
	for i, o := range got {
		if o.Skipped {
			skippedAt = i
			break
		}
	}
	if skippedAt == 0 || skippedAt == len(got)-1 {
		t.Fatalf("skipped occurrence sits at index %d of %d, want it strictly inside the sequence: %+v", skippedAt, len(got), formatOccs(got))
	}
	if !(got[skippedAt-1].At.Before(got[skippedAt].At) && got[skippedAt].At.Before(got[skippedAt+1].At)) {
		t.Errorf("skipped occurrence breaks chronological order: %+v", formatOccs(got[skippedAt-1:skippedAt+2]))
	}
}

// TestEveryReturnValueIsUTC sweeps the whole API surface: no local time may
// cross the package boundary, ever.
func TestEveryReturnValueIsUTC(t *testing.T) {
	zones := []*time.Location{
		mustZone(t, "UTC"),
		mustZone(t, "Europe/Oslo"),
		mustZone(t, "Asia/Kolkata"),
		mustZone(t, "Australia/Lord_Howe"),
	}
	policies := []Policy{{}, {SpringForward: Shift}, {FallBack: Both}, {SpringForward: Shift, FallBack: Both}}
	exprs := []string{"0 2 * * *", "*/7 * * * *", "@every 90m", "@daily"}

	for _, expr := range exprs {
		s := mustParse(t, expr)
		for _, tz := range zones {
			for _, p := range policies {
				label := expr + " in " + tz.String()
				n, err := s.Next(utc(t, "2026-03-29T00:30:00Z"), tz, p)
				if err != nil {
					t.Fatalf("%s Next: %v", label, err)
				}
				if loc := n.At.Location(); loc != time.UTC {
					t.Errorf("%s: Next().At location = %v, want UTC", label, loc)
				}
				v, err := s.Prev(utc(t, "2026-03-29T23:30:00Z"), tz, p)
				if err != nil {
					t.Fatalf("%s Prev: %v", label, err)
				}
				if loc := v.At.Location(); loc != time.UTC {
					t.Errorf("%s: Prev().At location = %v, want UTC", label, loc)
				}
				bs, err := s.Between(utc(t, "2026-03-28T00:00:00Z"), utc(t, "2026-03-31T00:00:00Z"), tz, p)
				if err != nil {
					t.Fatalf("%s Between: %v", label, err)
				}
				if len(bs) == 0 {
					t.Fatalf("%s: Between empty across the spring transition, the sweep proves nothing", label)
				}
				for _, o := range bs {
					if loc := o.At.Location(); loc != time.UTC {
						t.Errorf("%s: Between().At location = %v, want UTC", label, loc)
					}
				}
			}
		}
	}
}

func TestIterationLimitStopsImpossibleDatesQuickly(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"february thirtieth", "0 0 30 2 *"},
		{"april thirty first", "0 0 31 4 *"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, tc.expr)
			u := mustZone(t, "UTC")

			start := time.Now()
			_, err := s.Next(utc(t, "2026-01-01T00:00:00Z"), u, Policy{})
			elapsed := time.Since(start)
			if !errors.Is(err, ErrNoOccurrence) {
				t.Fatalf("Next error = %v, want ErrNoOccurrence", err)
			}
			if elapsed > 50*time.Millisecond {
				t.Errorf("Next took %s, want under 50ms so an impossible expression cannot spin", elapsed)
			}

			got, err := s.Between(utc(t, "2026-01-01T00:00:00Z"), utc(t, "2027-01-01T00:00:00Z"), u, Policy{})
			if !errors.Is(err, ErrNoOccurrence) {
				t.Fatalf("Between error = %v, want ErrNoOccurrence", err)
			}
			if len(got) != 0 {
				t.Errorf("Between returned %+v alongside ErrNoOccurrence, want nothing collected", formatOccs(got))
			}
		})
	}
}

func TestLeapYearFebruaryTwentyNineIsFoundNotRejected(t *testing.T) {
	s := mustParse(t, "0 0 29 2 *")
	u := mustZone(t, "UTC")

	next, err := s.Next(utc(t, "2026-01-01T00:00:00Z"), u, Policy{})
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	wantOcc(t, next, "next leap day", "2028-02-29T00:00:00Z", "2028-02-29 00:00:00 +00:00", false, "")

	prev, err := s.Prev(utc(t, "2026-01-01T00:00:00Z"), u, Policy{})
	if err != nil {
		t.Fatalf("Prev: %v", err)
	}
	wantOcc(t, prev, "previous leap day", "2024-02-29T00:00:00Z", "2024-02-29 00:00:00 +00:00", false, "")
}

// TestBetweenEqualsIteratedNext is the property from the test plan: for any
// window, Between(from, to) must equal iterating Next until past to. Seeded
// pseudo randomness keeps the case mix fixed between runs.
//
// Windows that touch a DST transition are skipped on purpose: a time.Time
// cursor cannot carry which side of a seam it came from, so chain equals
// Between only away from transitions. The seam shapes have their own
// deterministic contract in TestNextChainContractAtBothSeams; the guard below
// also asserts the draw really produces seam windows, so the skip can never
// silently stop covering anything.
func TestBetweenEqualsIteratedNext(t *testing.T) {
	exprs := []string{"*/7 * * * *", "0 */3 * * *", "15 2 * * 0", "30 1 * * *", "0 0 29 2 *"}
	zones := []string{"UTC", "Europe/Oslo", "Asia/Kolkata", "Australia/Lord_Howe"}
	policies := []Policy{{}, {SpringForward: Shift}, {FallBack: Both}}

	rnd := newRand(47)
	base := utc(t, "2026-01-01T00:00:00Z").Unix()
	seamWindows := 0

	for i := 0; i < 40; i++ {
		expr := exprs[rnd.intn(len(exprs))]
		tz := mustZone(t, zones[rnd.intn(len(zones))])
		p := policies[rnd.intn(len(policies))]
		from := time.Unix(base+int64(rnd.intn(300*24*3600)), 0).UTC()
		to := from.Add(time.Duration(1+rnd.intn(24*14)) * time.Hour)

		s := mustParse(t, expr)

		if touchesZoneTransition(tz, from, to) {
			seamWindows++
			continue
		}

		want, werr := iterateNext(s, from, to, tz, p)
		got, gerr := s.Between(from, to, tz, p)

		if (werr == nil) != (gerr == nil) {
			t.Fatalf("case %d (%s): error mismatch, iterated=%v between=%v", i, expr, werr, gerr)
		}
		if werr != nil {
			continue // horizon trips identically on both sides
		}
		if len(got) != len(want) {
			t.Fatalf("case %d (%s): %d occurrences, want %d\ngot  %+v\nwant %+v", i, expr, len(got), len(want), formatOccs(got), formatOccs(want))
		}
		for j := range got {
			if !occEqual(got[j], want[j]) {
				t.Fatalf("case %d (%s): occurrence %d = %+v, want %+v", i, expr, j, formatOcc(got[j]), formatOcc(want[j]))
			}
		}
	}
	if seamWindows == 0 {
		t.Error("no drawn window touched a zone transition: the seam skip guard never fired, so this property no longer exercises its documented limit")
	}
}

// touchesZoneTransition reports whether a UTC offset change happens inside
// the window. Offsets are sampled at both ends and the midpoint; every real
// transition moves at least one of the three.
func touchesZoneTransition(tz *time.Location, from, to time.Time) bool {
	mid := from.Add(to.Sub(from) / 2)
	o0, _ := from.In(tz).Zone()
	o1, _ := mid.In(tz).Zone()
	o2, _ := to.In(tz).Zone()
	return o0 != o1 || o1 != o2
}

// TestNextChainContractAtBothSeams pins what chaining Next means at the two
// DST seams, so the shapes the package doc promises cannot drift. A bare
// time.Time cursor cannot carry which side of a seam it came from, so a
// chain by At provably cannot reproduce Between there; these assertions pin
// the exact documented shapes instead of pretending equality holds.
func TestNextChainContractAtBothSeams(t *testing.T) {
	s := mustParse(t, "*/15 * * * *")
	oslo := mustZone(t, "Europe/Oslo")

	t.Run("spring forward: a chain by instant loses exactly the tied emissions", func(t *testing.T) {
		from := utc(t, "2026-03-29T00:00:00Z")
		to := utc(t, "2026-03-30T00:00:00Z")
		full, err := s.Between(from, to, oslo, Policy{})
		if err != nil {
			t.Fatalf("Between: %v", err)
		}
		if len(full) != 100 {
			t.Fatalf("Between = %d occurrences, want 100", len(full))
		}

		var chain []Occurrence
		cur := from
		for i := 0; i < 500; i++ {
			o, err := s.Next(cur, oslo, Policy{})
			if err != nil {
				t.Fatalf("Next(%s): %v", cur, err)
			}
			if o.At.After(to) {
				break
			}
			if !o.At.After(cur) {
				t.Fatalf("spring chain stepped backwards at %s -> %s", cur, o.At)
			}
			chain = append(chain, o)
			cur = o.At
		}
		if len(chain) != 96 {
			t.Fatalf("chain by At = %d occurrences, want the documented 96", len(chain))
		}

		type chainKey struct {
			at     int64
			reason string
		}
		seen := map[chainKey]bool{}
		for _, o := range chain {
			seen[chainKey{o.At.Unix(), o.SkipReason}] = true
		}
		var dropped []Occurrence
		for _, o := range full {
			if !seen[chainKey{o.At.Unix(), o.SkipReason}] {
				dropped = append(dropped, o)
			}
		}
		if len(dropped) != 4 {
			t.Fatalf("chain dropped %d emissions, want exactly the 4 tied ones: %s", len(dropped), formatOccs(dropped))
		}
		tieStart := utc(t, "2026-03-29T01:00:00Z")
		tieEnd := utc(t, "2026-03-29T01:45:00Z")
		for _, o := range dropped {
			if o.At.Before(tieStart) || o.At.After(tieEnd) {
				t.Errorf("dropped emission outside the tied hour: %s", formatOcc(o))
			}
		}
	})

	t.Run("fall back: the timetable successor of a marker carries an earlier instant", func(t *testing.T) {
		from := utc(t, "2026-10-25T00:00:00Z")
		to := utc(t, "2026-10-26T00:00:00Z")
		full, err := s.Between(from, to, oslo, Policy{})
		if err != nil {
			t.Fatalf("Between: %v", err)
		}
		if len(full) != 96 {
			t.Fatalf("Between = %d occurrences, want 96", len(full))
		}

		marker := utc(t, "2026-10-25T01:00:00Z") // second instance of doubled 02:00
		next, err := s.Next(marker, oslo, Policy{})
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		wantOcc(t, next, "successor of the doubled 02:00 marker",
			"2026-10-25T00:15:00Z", "2026-10-25 02:15:00 +02:00", false, "")
		if !next.At.Before(marker) {
			t.Errorf("successor At %s is not earlier than the marker cursor %s: the documented backward step vanished",
				next.At, marker)
		}

		// A naive loop that advances by At stops on that non advancing step
		// instead of hanging or spinning; it must see exactly one occurrence
		// before quitting.
		steps := 0
		cur := from
		for i := 0; i < 500; i++ {
			o, err := s.Next(cur, oslo, Policy{})
			if err != nil {
				t.Fatalf("Next(%s): %v", cur, err)
			}
			if o.At.After(to) || !o.At.After(cur) {
				break
			}
			steps++
			cur = o.At
		}
		if steps != 1 {
			t.Errorf("naive fall back chain produced %d occurrences, want 1 before the documented non advancing step", steps)
		}
	})
}

func iterateNext(s Schedule, from, to time.Time, tz *time.Location, p Policy) ([]Occurrence, error) {
	var out []Occurrence
	u := from
	for i := 0; i < maxIterations; i++ { // bounded: a Next that never advances must fail loudly, not hang the suite
		o, err := s.Next(u, tz, p)
		if err != nil {
			return out, err
		}
		if o.At.After(to) {
			return out, nil
		}
		if !o.At.After(u) {
			return out, errors.New("Next(" + u.Format(time.RFC3339) + ") returned " + o.At.Format(time.RFC3339) + ", which does not advance past the previous occurrence")
		}
		out = append(out, o)
		u = o.At
	}
	return out, errors.New("iterating Next exceeded the step budget without passing to")
}

// TestBetweenSplitsIntoIdenticalHalves is G9: the answer may not depend on
// where the caller chops the window. This is what makes catch up safe when a
// daemon processes its backlog in pieces.
func TestBetweenSplitsIntoIdenticalHalves(t *testing.T) {
	exprs := []string{"*/7 * * * *", "0 */3 * * *", "15 2 * * 0", "@every 90m"}
	zones := []string{"UTC", "Europe/Oslo", "Australia/Lord_Howe"}

	rnd := newRand(4747)
	base := utc(t, "2026-03-01T00:00:00Z").Unix()

	for i := 0; i < 30; i++ {
		expr := exprs[rnd.intn(len(exprs))]
		tz := mustZone(t, zones[rnd.intn(len(zones))])
		t0 := time.Unix(base+int64(rnd.intn(20*24*3600)), 0).UTC()
		t1 := t0.Add(time.Duration(48+rnd.intn(240)) * time.Hour)
		tm := t0.Add(time.Duration(rnd.intn(int(t1.Sub(t0)/time.Hour))) * time.Hour)

		s := mustParse(t, expr)

		whole, err := s.Between(t0, t1, tz, Policy{})
		if err != nil {
			t.Fatalf("case %d: whole window: %v", i, err)
		}
		firstHalf, err := s.Between(t0, tm, tz, Policy{})
		if err != nil {
			t.Fatalf("case %d: first half: %v", i, err)
		}
		secondHalf, err := s.Between(tm, t1, tz, Policy{})
		if err != nil {
			t.Fatalf("case %d: second half: %v", i, err)
		}

		joined := append(append([]Occurrence{}, firstHalf...), secondHalf...)
		if len(joined) != len(whole) {
			t.Fatalf("case %d (%s): split gives %d occurrences, whole gives %d\nsplit %+v\nwhole %+v",
				i, expr, len(joined), len(whole), formatOccs(joined), formatOccs(whole))
		}
		for j := range joined {
			if !occEqual(joined[j], whole[j]) {
				t.Fatalf("case %d (%s): split occurrence %d differs\nsplit %+v\nwhole %+v",
					i, expr, j, formatOcc(joined[j]), formatOcc(whole[j]))
			}
		}
	}
}

// TestDayOfMonthAndDayOfWeekFollowTheVixieOrRule pins the classic cron day
// rule: when BOTH day fields are restricted, a day matching EITHER counts;
// when one field is a wildcard, only the restricted field decides. June 2026
// discriminates the two readings sharply: its Fridays are 5, 12, 19, 26 and
// the 13th is a Saturday, so the OR rule fires on five days where an AND
// reading would fire on none.
func TestDayOfMonthAndDayOfWeekFollowTheVixieOrRule(t *testing.T) {
	u := mustZone(t, "UTC")

	t.Run("both restricted: either matching counts", func(t *testing.T) {
		s := mustParse(t, "0 9 13 * fri")
		got, err := s.Between(utc(t, "2026-06-01T00:00:00Z"), utc(t, "2026-07-01T00:00:00Z"), u, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		wantDays := []int{5, 12, 13, 19, 26}
		if len(got) != len(wantDays) {
			t.Fatalf("got %d occurrences %v, want the five OR rule days %v", len(got), formatOccs(got), wantDays)
		}
		for i, d := range wantDays {
			wantAt := fmt.Sprintf("2026-06-%02dT09:00:00Z", d)
			if got[i].At.Format(time.RFC3339) != wantAt {
				t.Errorf("occurrence %d = %s, want %s", i, got[i].At.Format(time.RFC3339), wantAt)
			}
		}

		prev, err := s.Prev(utc(t, "2026-07-01T09:30:00Z"), u, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		if prev.At.Format(time.RFC3339) != "2026-06-26T09:00:00Z" {
			t.Errorf("Prev = %s, want the last Friday June 26th through the same union walk", prev.At.Format(time.RFC3339))
		}
	})

	t.Run("wildcard day of week: only the day of month decides", func(t *testing.T) {
		s := mustParse(t, "0 9 13 * *")
		got, err := s.Between(utc(t, "2026-06-01T00:00:00Z"), utc(t, "2026-07-01T00:00:00Z"), u, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].At.Day() != 13 {
			t.Fatalf("got %v, want exactly June 13th", formatOccs(got))
		}
	})

	t.Run("wildcard day of month: only the weekday decides", func(t *testing.T) {
		s := mustParse(t, "0 9 * * fri")
		got, err := s.Between(utc(t, "2026-06-01T00:00:00Z"), utc(t, "2026-07-01T00:00:00Z"), u, Policy{})
		if err != nil {
			t.Fatal(err)
		}
		wantDays := []int{5, 12, 19, 26}
		if len(got) != len(wantDays) {
			t.Fatalf("got %d occurrences %v, want the four Fridays %v", len(got), formatOccs(got), wantDays)
		}
		for i, d := range wantDays {
			if got[i].At.Day() != d {
				t.Errorf("occurrence %d = day %d, want %d", i, got[i].At.Day(), d)
			}
		}
	})
}

// TestLocalWallFormattingMatchesTimeFormatByteForByte pins the hand rolled
// LocalWall renderer against time.Time.Format itself: same layout, same
// bytes, every zone, every offset shape, including sub minute historical
// offsets where truncation decides what the plus and minus signs even mean.
func TestLocalWallFormattingMatchesTimeFormatByteForByte(t *testing.T) {
	realZones := []*time.Location{
		mustZone(t, "UTC"),
		mustZone(t, "Europe/Oslo"),
		mustZone(t, "Asia/Kolkata"),
		mustZone(t, "Australia/Lord_Howe"),
	}

	sweep := func(t *testing.T, z *time.Location, base time.Time) {
		t.Helper()
		for i := 0; i < 140; i++ {
			at := base.Add(time.Duration(i) * 37 * time.Minute)
			got := formatLocalWall(at, z)
			want := at.In(z).Format(localWallLayout)
			if got != want {
				t.Fatalf("%s in %s: formatLocalWall = %q, Format = %q", at.Format(time.RFC3339), z, got, want)
			}
		}
	}

	// Across both Oslo transitions and a plain stretch of July.
	oslo := realZones[1]
	sweep(t, oslo, utc(t, "2026-03-28T00:00:00Z"))
	sweep(t, oslo, utc(t, "2026-10-24T00:00:00Z"))
	sweep(t, oslo, utc(t, "2026-07-01T00:00:00Z"))
	for _, z := range realZones {
		sweep(t, z, utc(t, "2026-06-01T00:00:00Z"))
	}

	t.Run("odd offsets including sub minute truncation", func(t *testing.T) {
		for _, off := range []int{0, 59, -59, 60, -60, 1800, -1800, 3599, -3599, 3600, -3600, 4772, -4772, 38640, -39600, 86399, -86399} {
			z := time.FixedZone("Probe", off)
			for _, sec := range []int{0, 7, 59} {
				at := time.Date(2026, 3, 29, 2, 30, sec, 0, z)
				got := formatLocalWall(at, z)
				want := at.Format(localWallLayout)
				if got != want {
					t.Errorf("offset %ds sec %d: formatLocalWall = %q, Format = %q", off, sec, got, want)
				}
			}
		}
	})
}

// TestPolicyValuesRejectUnknownNumbers pins that an out of range Policy value
// is rejected when a schedule is APPLIED through Next, not when the
// expression parses.
func TestPolicyValuesRejectUnknownNumbers(t *testing.T) {
	s := mustParse(t, "0 2 * * *")
	oslo := mustZone(t, "Europe/Oslo")

	bad := Policy{SpringForward: SpringForward(9)}
	if _, err := s.Next(utc(t, "2026-03-27T12:00:00Z"), oslo, bad); err == nil {
		t.Fatal("Next with an unknown spring_forward value succeeded, want a named error")
	} else if !strings.Contains(err.Error(), "spring_forward") {
		t.Errorf("error = %q, want it to name spring_forward", err.Error())
	}

	badFall := Policy{FallBack: FallBack(7)}
	if _, err := s.Next(utc(t, "2026-03-27T12:00:00Z"), oslo, badFall); err == nil {
		t.Fatal("Next with an unknown fall_back value succeeded, want a named error")
	} else if !strings.Contains(err.Error(), "fall_back") {
		t.Errorf("error = %q, want it to name fall_back", err.Error())
	}
}

func TestNilTimeZoneIsRejectedNotPanickedOn(t *testing.T) {
	s := mustParse(t, "0 2 * * *")
	from := utc(t, "2026-03-27T12:00:00Z")
	if _, err := s.Next(from, nil, Policy{}); err == nil {
		t.Fatal("Next with a nil zone succeeded, want an error")
	}
	if _, err := s.Prev(from, nil, Policy{}); err == nil {
		t.Fatal("Prev with a nil zone succeeded, want an error")
	}
	if _, err := s.Between(from, from.Add(time.Hour), nil, Policy{}); err == nil {
		t.Fatal("Between with a nil zone succeeded, want an error")
	}
}

func occEqual(a, b Occurrence) bool {
	return a.At.Equal(b.At) &&
		a.LocalWall == b.LocalWall &&
		a.Skipped == b.Skipped &&
		a.SkipReason == b.SkipReason
}

func formatOcc(o Occurrence) string {
	return o.At.Format(time.RFC3339) + " wall=" + o.LocalWall +
		" skipped=" + boolLabel(o.Skipped) + " reason=" + o.SkipReason
}

func formatOccs(os []Occurrence) []string {
	out := make([]string, len(os))
	for i, o := range os {
		out[i] = formatOcc(o)
	}
	return out
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
