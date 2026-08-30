package spec_test

import (
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/spec"
)

const zoneJob = `name: nightly
steps:
  - name: only
    run: ["/bin/true"]
schedules:
  - name: nightly
    cron: "0 3 * * *"
    timezone: %ZONE%
`

func zoneSource(zone string) string {
	return strings.Replace(zoneJob, "%ZONE%", zone, 1)
}

// TestDecoderRefusesTheZonesTheSchedulerRefuses ties the decoder to the one
// authority on time zone names. Every zone the scheduler loads through
// cronx.LoadZone must decode, and every zone it refuses must be refused where
// the file, the line and the column are still known: a zone that only fails at
// the tick loop costs an error row on every wake and never a run.
func TestDecoderRefusesTheZonesTheSchedulerRefuses(t *testing.T) {
	for _, zone := range []string{"UTC", "Europe/Oslo", "Local", "Europe/Osloo"} {
		source := zoneSource(zone)
		_, diags := spec.Parse("j.yaml", []byte(source))

		var refused bool
		for _, d := range diags {
			if d.Code == spec.CodeUnknownTimezone {
				refused = true
			}
		}
		_, err := cronx.LoadZone(zone)
		if refused != (err != nil) {
			t.Errorf("timezone %q: the decoder refused it %v, cronx.LoadZone refused it %v (%v)",
				zone, refused, err != nil, err)
		}
	}
}

// TestTimezoneLocalIsRefusedAtItsLineAndColumn is the operator's half of the
// same rule: the refusal has to point at the offending value, because a zone
// that reaches the schedules table is only ever reported as a repeating error
// tick with no file and no line.
func TestTimezoneLocalIsRefusedAtItsLineAndColumn(t *testing.T) {
	source := zoneSource("Local")
	_, diags := spec.Parse("j.yaml", []byte(source))

	lines := strings.Split(source, "\n")
	wantLine := 0
	wantCol := 0
	for i, line := range lines {
		if idx := strings.Index(line, "Local"); idx >= 0 {
			wantLine, wantCol = i+1, idx+1
		}
	}

	for _, d := range diags {
		if d.Code != spec.CodeUnknownTimezone {
			continue
		}
		if d.File != "j.yaml" || d.Line != wantLine || d.Col != wantCol {
			t.Fatalf("the refusal points at %s:%d:%d, want j.yaml:%d:%d",
				d.File, d.Line, d.Col, wantLine, wantCol)
		}
		return
	}
	t.Fatalf("timezone: Local decoded without a %s: %v", spec.CodeUnknownTimezone, diags)
}
