package spec_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

const cronJob = `name: nightly
steps:
  - name: only
    run: ["/bin/true"]
schedules:
  - name: nightly
    cron: "%EXPR%"
    timezone: Europe/Oslo
`

func cronSource(expr string) string {
	return strings.Replace(cronJob, "%EXPR%", expr, 1)
}

// TestDecoderRefusesTheExpressionsCronxRefuses ties the decoder to the one
// authority on a schedule expression, the way the zone check is tied to
// cronx.LoadZone. Every expression the scheduler parses must decode, and every
// expression it cannot parse must be refused where the file, the line and the
// column are still known: an expression that only fails at the tick loop is
// frozen into a version first and reported as an error tick afterwards.
//
// The fixture is the same job every time and differs only in the expression,
// so a refusal cannot come from anything else in the file. The accepted half
// asserts that the file decodes with no diagnostics at all, which is what
// makes the refused half evidence about the expression.
func TestDecoderRefusesTheExpressionsCronxRefuses(t *testing.T) {
	expressions := []string{
		"0 3 * * *",
		"*/15 * * * *",
		"0 0 1 jan mon-fri",
		"@daily",
		"@every 90m",
		"@every 1s",
		"15m",
		"not a cron expression at all",
		"0 3 * *",
		"0 0 * * 8",
		"0 3 * * * *",
		"@fortnightly",
		"0 0 L * *",
		"@every 500ms",
	}

	for _, expr := range expressions {
		t.Run(expr, func(t *testing.T) {
			source := cronSource(expr)
			_, diags := spec.Parse("j.yaml", []byte(source))

			var refused bool
			for _, d := range diags {
				if d.Code == spec.CodeBadCron {
					refused = true
				}
			}
			_, err := cronx.Parse(expr)
			if refused != (err != nil) {
				t.Fatalf("cron %q: the decoder refused it %v, cronx.Parse refused it %v (%v)",
					expr, refused, err != nil, err)
			}
			if err == nil && len(diags) != 0 {
				t.Errorf("cron %q parses, but the file did not decode clean: %v", expr, diags)
			}
		})
	}
}

// TestAnExpressionWithNoOccurrenceStillDecodes is the boundary the check must
// not cross. "0 0 30 2 *" parses and never matches, and cronx says so with
// ErrNoOccurrence from the horizon search, not from Parse. The decoder asks
// Parse, so the file stays appliable and the scheduler records the truth about
// it on its own wake.
func TestAnExpressionWithNoOccurrenceStillDecodes(t *testing.T) {
	const expr = "0 0 30 2 *"

	schedule, err := cronx.Parse(expr)
	if err != nil {
		t.Fatalf("cronx.Parse(%q) = %v, and this test is about an expression that parses", expr, err)
	}
	zone, err := cronx.LoadZone("UTC")
	if err != nil {
		t.Fatalf("load UTC: %v", err)
	}
	if _, err := schedule.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), zone, cronx.Policy{}); !errors.Is(err, cronx.ErrNoOccurrence) {
		t.Fatalf("Next over %q = %v, want %v: the two questions are no longer separate",
			expr, err, cronx.ErrNoOccurrence)
	}

	job, diags := spec.Parse("j.yaml", []byte(cronSource(expr)))

	if len(diags) != 0 {
		t.Fatalf("an expression that parses but never matches was refused: %v", diags)
	}
	if job == nil || len(job.Schedules) != 1 || job.Schedules[0].Cron != expr {
		t.Fatalf("the job decoded to %+v, want one schedule reading %q", job, expr)
	}
}

// TestBadCronIsRefusedAtItsLineAndColumn is the operator's half of the rule.
// The refusal points at the expression itself, because the caret in the
// excerpt is the whole reason the check happens here and not on the next wake.
func TestBadCronIsRefusedAtItsLineAndColumn(t *testing.T) {
	const expr = "not a cron expression at all"
	source := cronSource(expr)

	_, diags := spec.Parse("j.yaml", []byte(source))

	// The expression is quoted in the file, and a scalar's position is where
	// the scalar starts: the caret lands on the opening quote, not inside it.
	written := `"` + expr + `"`
	wantLine, wantCol := 0, 0
	for i, line := range strings.Split(source, "\n") {
		if idx := strings.Index(line, written); idx >= 0 {
			wantLine, wantCol = i+1, idx+1
		}
	}

	for _, d := range diags {
		if d.Code != spec.CodeBadCron {
			continue
		}
		if d.File != "j.yaml" || d.Line != wantLine || d.Col != wantCol {
			t.Fatalf("the refusal points at %s:%d:%d, want j.yaml:%d:%d",
				d.File, d.Line, d.Col, wantLine, wantCol)
		}
		if !strings.Contains(d.Message, expr) {
			t.Errorf("the message does not quote the expression: %s", d.Message)
		}
		if strings.TrimSpace(d.Hint) == "" {
			t.Errorf("the refusal offers no next step")
		}
		return
	}
	t.Fatalf("cron %q decoded without a %s: %v", expr, spec.CodeBadCron, diags)
}

// TestAnEmptyCronStaysAMissingField keeps the two refusals apart. An empty
// expression is the field not being filled in, which already has a code and a
// hint; parsing it as well would report one mistake twice.
func TestAnEmptyCronStaysAMissingField(t *testing.T) {
	_, diags := spec.Parse("j.yaml", []byte(cronSource("")))

	if len(diags) != 1 {
		t.Fatalf("an empty cron raised %d diagnostics, want the one missing field: %v", len(diags), diags)
	}
	if diags[0].Code != spec.CodeMissingField {
		t.Fatalf("an empty cron raised %s, want %s", diags[0].Code, spec.CodeMissingField)
	}
}

// TestABadDayOfWeekIsRefusedInTheSameWordsEveryTime holds the refusal itself
// to the promise the whole parser rests on: the same file gives the same
// diagnostics. The message quotes cronx, cronx suggests the names the field
// accepts, and those names live in a map. A suggestion list cut to three
// entries before it is ordered names three arbitrary days, so one file refused
// twice reads differently, and it does so intermittently, which is worse than
// always.
//
// The expected clause is the order the field counts in. A list built by
// ranging the map can only come out alphabetical, so it never matches this at
// any seed: the assertion is red on every run, not on one run in thirty-five.
// The repeat loop then holds the property the assertion is a proxy for.
func TestABadDayOfWeekIsRefusedInTheSameWordsEveryTime(t *testing.T) {
	const expr = "0 0 7 12 !"
	const want = `field 5 (day of week) value "!" cannot be read in "!": use numbers or names like sun, mon, tue`

	source := []byte(cronSource(expr))

	first := badCron(t, source)
	if first.Message != want {
		t.Errorf("the refusal reads\n%q\nwant\n%q", first.Message, want)
	}
	for i := range 16 {
		again := badCron(t, source)
		if again != first {
			t.Fatalf("parse %d refused one file differently:\n%+v\n%+v", i+2, first, again)
		}
	}
}

// badCron is the one PQ2011 a file is expected to raise.
func badCron(t *testing.T, source []byte) diag.Diagnostic {
	t.Helper()

	_, diags := spec.Parse("j.yaml", source)
	for _, d := range diags {
		if d.Code == spec.CodeBadCron {
			return d
		}
	}
	t.Fatalf("the file decoded without a %s: %v", spec.CodeBadCron, diags)
	return diag.Diagnostic{}
}
