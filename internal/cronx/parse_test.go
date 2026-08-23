package cronx

import (
	"strings"
	"testing"
	"time"
)

func mustZone(t *testing.T, name string) *time.Location {
	t.Helper()
	z, err := LoadZone(name)
	if err != nil {
		t.Fatalf("LoadZone(%q): %v", name, err)
	}
	return z
}

func mustParse(t *testing.T, expr string) Schedule {
	t.Helper()
	s, err := Parse(expr)
	if err != nil {
		t.Fatalf("Parse(%q): %v", expr, err)
	}
	return s
}

func utc(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("bad RFC3339 literal %q: %v", s, v)
	}
	return v
}

// wantOcc checks one occurrence against literals. at is RFC3339 UTC, wall is
// the full LocalWall string.
func wantOcc(t *testing.T, got Occurrence, name, at, wall string, skipped bool, reason string) {
	t.Helper()
	wantTime := utc(t, at)
	if !got.At.Equal(wantTime) {
		t.Errorf("%s: At = %s, want %s", name, got.At.Format(time.RFC3339Nano), at)
	}
	if loc := got.At.Location(); loc != time.UTC {
		t.Errorf("%s: At location = %v, want UTC", name, loc)
	}
	if got.LocalWall != wall {
		t.Errorf("%s: LocalWall = %q, want %q", name, got.LocalWall, wall)
	}
	if got.Skipped != skipped {
		t.Errorf("%s: Skipped = %v, want %v", name, got.Skipped, skipped)
	}
	if got.SkipReason != reason {
		t.Errorf("%s: SkipReason = %q, want %q", name, got.SkipReason, reason)
	}
}

func TestParseAcceptsKnownExpressions(t *testing.T) {
	cases := []struct {
		name     string
		expr     string
		wantKind Kind
		wantExpr string
		interval time.Duration
	}{
		{"plain five fields", "0 2 * * *", KindCron, "0 2 * * *", 0},
		{"step list", "*/15 * * * *", KindCron, "0,15,30,45 * * * *", 0},
		{"range with step", "10-30/5 * * * *", KindCron, "10,15,20,25,30 * * * *", 0},
		{"explicit list", "5,35 1 * * *", KindCron, "5,35 1 * * *", 0},
		{"month name", "0 0 1 jul *", KindCron, "0 0 1 7 *", 0},
		{"weekday name with w", "0 0 * * wed", KindCron, "0 0 * * 3", 0},
		{"mixed names upper case", "0 3 1 JAN,FEB *", KindCron, "0 3 1 1,2 *", 0},
		{"dow seven normalises to sunday", "0 0 * * 7", KindCron, "0 0 * * 0", 0},
		{"full ranges collapse to star", "0-59 0-23 1-31 1-12 0-7", KindCron, "* * * * *", 0},
		{"hourly descriptor", "@hourly", KindCron, "0 * * * *", 0},
		{"daily descriptor", "@daily", KindCron, "0 0 * * *", 0},
		{"midnight descriptor", "@midnight", KindCron, "0 0 * * *", 0},
		{"weekly descriptor", "@weekly", KindCron, "0 0 * * 0", 0},
		{"monthly descriptor", "@monthly", KindCron, "0 0 1 * *", 0},
		{"yearly descriptor", "@yearly", KindCron, "0 0 1 1 *", 0},
		{"annually alias", "@annually", KindCron, "0 0 1 1 *", 0},
		{"descriptor upper case", "@DAILY", KindCron, "0 0 * * *", 0},
		{"every prefix", "@every 90m", KindInterval, "@every 1h30m0s", 90 * time.Minute},
		{"bare duration", "15m", KindInterval, "@every 15m0s", 15 * time.Minute},
		{"compound duration", "@every 1h30m", KindInterval, "@every 1h30m0s", 90 * time.Minute},
		{"whitespace is trimmed", "   0 2 * * *   ", KindCron, "0 2 * * *", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.expr)
			if err != nil {
				t.Fatalf("Parse(%q) = error %v, want a schedule", tc.expr, err)
			}
			if got.Kind != tc.wantKind {
				t.Errorf("Kind = %d, want %d", got.Kind, tc.wantKind)
			}
			if got.Expr != tc.wantExpr {
				t.Errorf("Expr = %q, want canonical %q", got.Expr, tc.wantExpr)
			}
			if got.Interval != tc.interval {
				t.Errorf("Interval = %v, want %v", got.Interval, tc.interval)
			}
			// The canonical form must be a fixed point: reparsing it gives
			// the identical schedule.
			again, err := Parse(got.Expr)
			if err != nil {
				t.Fatalf("re-Parse(%q) = error %v: canonical form must parse", got.Expr, err)
			}
			if again != got {
				t.Errorf("re-Parse(%q) = %+v, want the identical schedule %+v", got.Expr, again, got)
			}
		})
	}
}

func TestParseRejectsBrokenExpressions(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want string
	}{
		{"empty", "", "empty schedule expression"},
		{"too few fields", "0 2 * *", "exactly 5 fields"},
		{"six fields with seconds", "0 0 2 * * *", "exactly 5 fields"},
		{"minute out of range", "60 * * * *", "out of range"},
		{"hour out of range", "* 24 * * *", "out of range"},
		{"day of month zero", "0 0 0 * *", "out of range"},
		{"month out of range", "0 0 1 13 *", "out of range"},
		{"unknown name", "0 0 1 foo *", "unknown name"},
		{"backwards range", "0 5-2 * * *", "runs backwards"},
		{"zero step", "*/0 * * * *", "step"},
		{"negative step", "*/-5 * * * *", "step"},
		{"garbage text falls through to the cron error", "abc", "exactly 5 fields"},
		{"malformed every duration", "@every 90x", "bad interval"},
		{"zero interval", "@every 0s", "must be positive"},
		{"negative interval", "@every -5m", "must be positive"},
		{"sub second interval", "@every 500ms", "below one second"},
		{"interval with trailing junk", "@every 90m extra", "bad interval"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.expr)
			if err == nil {
				t.Fatalf("Parse(%q) succeeded, want an error naming %q", tc.expr, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Parse(%q) error = %q, want it to contain %q", tc.expr, err.Error(), tc.want)
			}
		})
	}
}

func TestParseRejectsCronExtensionsByName(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{"last day of month", "0 0 L * *"},
		{"last day of month numbered", "0 0 3L * *"},
		{"nearest weekday", "0 0 15W * *"},
		{"last weekday", "0 0 * * 5L"},
		{"nth weekday", "0 0 * * 1#3"},
		{"nth weekday bare hash", "0 0 * * #3"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.expr)
			if err == nil {
				t.Fatalf("Parse(%q) accepted an L/W/# extension: the 1.0 contract must reject it loudly", tc.expr)
			}
			if !strings.Contains(err.Error(), "not supported in paceq 1.0") {
				t.Errorf("Parse(%q) error = %q, want it to name the 1.0 contract", tc.expr, err.Error())
			}
		})
	}
}

// TestNameRangesAndValueStepsMatchGronx pins the two places where the parser
// must agree with the gronx reference: name ranges like mon-fri are legal on
// both sides of a range (case insensitive), and a single value with a step
// (9/2) walks from that value to the field ceiling, not just the value
// itself. Deliberate divergences stay rejected: reversed name ranges run
// backwards like numeric ones, unknown names inside a range are named by
// their own token, and a start value beyond the field matches no value
// instead of gronx's silent never fire.
func TestNameRangesAndValueStepsMatchGronx(t *testing.T) {
	accept := []struct {
		name     string
		expr     string
		wantExpr string
	}{
		{"weekday name range", "0 9 * * mon-fri", "0 9 * * 1,2,3,4,5"},
		{"month name range", "0 0 * jan-mar *", "0 0 * 1,2,3 *"},
		{"weekday name range upper case", "0 9 * * MON-FRI", "0 9 * * 1,2,3,4,5"},
		{"name list mixed with range", "0 0 * * mon,wed-fri", "0 0 * * 1,3,4,5"},
		{"hour value step walks to the ceiling", "0 9/2 * * *", "0 9,11,13,15,17,19,21,23 * * *"},
		{"minute value step", "30/15 * * * *", "30,45 * * * *"},
		{"weekday value step stops at six", "0 * * * 1/2", "0 * * * 1,3,5"},
		{"weekday name value step", "0 * * * mon/2", "0 * * * 1,3,5"},
		{"month name value step", "0 0 * mar/4 *", "0 0 * 3,7,11 *"},
	}

	for _, tc := range accept {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.expr)
			if err != nil {
				t.Fatalf("Parse(%q) = error %v, want a schedule", tc.expr, err)
			}
			if got.Expr != tc.wantExpr {
				t.Errorf("Expr = %q, want canonical %q", got.Expr, tc.wantExpr)
			}
			again, err := Parse(got.Expr)
			if err != nil || again != got {
				t.Errorf("re-Parse(%q) = (%+v, %v), want the identical schedule", got.Expr, again, err)
			}
		})
	}

	reject := []struct {
		name string
		expr string
		want string
	}{
		{"unknown name inside a range names the token", "0 0 * * moc-fri", `unknown name "moc"`},
		{"reversed name range", "0 0 * * sat-mon", "runs backwards"},
		{"zero step after a value", "0 9/0 * * *", "step"},
		{"start beyond the field matches no value", "* 90/2 * * *", "matches no value"},
	}

	for _, tc := range reject {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.expr)
			if err == nil {
				t.Fatalf("Parse(%q) succeeded, want an error naming %q", tc.expr, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Parse(%q) error = %q, want it to contain %q", tc.expr, err.Error(), tc.want)
			}
		})
	}
}

// TestNameRangeAndStepSchedulesFire pins the semantics behind the canonical
// text with real fires: the weekday name range skips the weekend and a value
// step advances by its step.
func TestNameRangeAndStepSchedulesFire(t *testing.T) {
	t.Run("weekday name range skips the weekend", func(t *testing.T) {
		s := mustParse(t, "0 9 * * mon-fri")
		z := mustZone(t, "Europe/Oslo")
		from := utc(t, "2026-01-02T09:00:00Z") // Friday
		got, err := s.Next(from, z, Policy{})
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		wantOcc(t, got, "next after friday", "2026-01-05T08:00:00Z",
			"2026-01-05 09:00:00 +01:00", false, "")
	})

	t.Run("hour value step advances by two", func(t *testing.T) {
		s := mustParse(t, "0 9/2 * * *")
		z := mustZone(t, "UTC")
		from := utc(t, "2026-06-10T09:00:00Z")
		got, err := s.Next(from, z, Policy{})
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		wantOcc(t, got, "next after 09:00", "2026-06-10T11:00:00Z",
			"2026-06-10 11:00:00 +00:00", false, "")
	})

	t.Run("name range week has five fires in utc", func(t *testing.T) {
		s := mustParse(t, "0 9 * * mon-fri")
		z := mustZone(t, "UTC")
		from := utc(t, "2026-01-04T10:00:00Z") // Sunday evening
		to := utc(t, "2026-01-11T10:00:00Z")
		got, err := s.Between(from, to, z, Policy{})
		if err != nil {
			t.Fatalf("Between: %v", err)
		}
		if len(got) != 5 {
			t.Fatalf("Between = %d occurrences, want 5 working days", len(got))
		}
		for i, o := range got {
			if wd := o.At.Weekday(); wd == time.Saturday || wd == time.Sunday {
				t.Errorf("occurrence %d fires on %v, want monday to friday only", i, wd)
			}
		}
	})
}
