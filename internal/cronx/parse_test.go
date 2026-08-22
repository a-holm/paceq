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
