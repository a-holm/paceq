package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// The integrity refusal and the startup sweep (M6-06, R11). A critical
// invariant violation means the database holds something the code cannot
// reason about: a uniqueness rule the schema enforces is broken, or the
// dependency graph has a cycle. Serving such a state would make every later
// fact suspect, so the start is refused - loudly, with the evidence and the
// repair path named, and never repaired around silently.

// fsckSubjectsPerInvariant caps how many subjects one log line names.
const fsckSubjectsPerInvariant = 5

// startupRefusal is the PSQ-FSCK-001 error. The anatomy is the one every
// paceq error keeps - what, where, next step - because an operator meeting
// it at boot is having a bad day and must not have to read source code to
// find the way out.
func startupRefusal(summary string) error {
	return fmt.Errorf("serve: PSQ-FSCK-001: critical invariant violation, startup refused: %s\n"+
		"  next steps:\n"+
		"    paceq fsck --json > /tmp/fsck.json   keep the evidence before touching anything\n"+
		"    paceq fsck --repair --confirm       repair what is safely repairable, after confirming\n"+
		"    or restore the state from its last verified backup", summary)
}

// firstCriticalSummary names the first finding of a sweep that refuses a
// start, empty when there is none. The grade comes from the catalogue through
// store.CriticalViolations, the same reading the quick boot gate and doctor
// take.
func firstCriticalSummary(violations []store.Violation) string {
	for _, v := range store.CriticalViolations(violations) {
		return v.Check + " (" + v.Subject + ": " + v.Detail + ")"
	}
	return ""
}

// recordStartupFindings writes the startup sweep's findings into the
// integrity event log, one row per invariant, and says so in the log. A clean
// sweep writes nothing, which is what makes the log's silence meaningful.
func recordStartupFindings(ctx context.Context, st *store.Store, clk clock.Clock,
	log *slog.Logger, violations []store.Violation,
) error {
	if len(violations) == 0 {
		return nil
	}
	at := clk.Now().UTC()
	counts := map[string]int{}
	subjects := map[string][]string{}
	order := make([]string, 0, len(violations))
	for _, v := range violations {
		if _, seen := counts[v.Check]; !seen {
			order = append(order, v.Check)
		}
		counts[v.Check]++
		if len(subjects[v.Check]) < fsckSubjectsPerInvariant {
			subjects[v.Check] = append(subjects[v.Check], v.Subject)
		}
	}
	findings := make([]store.IntegrityFinding, 0, len(order))
	for _, check := range order {
		findings = append(findings, store.IntegrityFinding{
			Invariant:  check,
			Severity:   store.SeverityOf(check),
			Violations: counts[check],
			Subjects:   subjects[check],
		})
	}
	if err := st.RecordIntegrityFindings(ctx, at, findings); err != nil {
		return fmt.Errorf("record the startup sweep: %w", err)
	}
	for _, f := range findings {
		log.Warn("integrity violation found at startup",
			"invariant", f.Invariant,
			"severity", f.Severity.String(),
			"violations", f.Violations,
			"at", at.Format(time.RFC3339),
		)
	}
	return nil
}
