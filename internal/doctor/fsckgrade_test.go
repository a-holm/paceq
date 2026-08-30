package doctor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/reconcile"
	"github.com/a-holm/paceq/internal/store"
)

// TestDoctorFailsExactlyWhenTheBootGateRefuses is the drift guard the two
// readers used to lack: for every invariant in the catalogue, doctor's fsck
// line is a failure precisely when the boot gate refuses to start on it.
// A finding the daemon serves through must not fail a health script.
func TestDoctorFailsExactlyWhenTheBootGateRefuses(t *testing.T) {
	ctx := context.Background()
	for _, inv := range store.Invariants {
		t.Run(inv.ID, func(t *testing.T) {
			one := []store.Violation{{
				Check:    inv.ID,
				Severity: inv.Severity,
				Subject:  "run 01SUBJECT",
				Detail:   "seeded for the grading guard",
				Remedy:   inv.Remedy,
			}}
			refused := reconcile.CriticalViolationSummary(one) != ""
			finding := doctor.CheckCriticalInvariants(ctx, &fakeDB{fsck: one})
			failed := finding.Level == doctor.Fail
			if failed != refused {
				t.Fatalf("doctor fails=%v, the boot gate refuses=%v for %s (%s): %+v",
					failed, refused, inv.ID, inv.Severity, finding)
			}
			says := strings.Contains(strings.Join(finding.Next, "\n"), "startup is refused")
			if says != refused {
				t.Fatalf("doctor claims startup is refused=%v, the boot gate refuses=%v for %s: %+v",
					says, refused, inv.ID, finding)
			}
		})
	}
}

// TestDoctorsCleanLineCountsWhatTheSweepChecked holds the clean message to
// the subset QuickFsck evaluates, not to the whole catalogue.
func TestDoctorsCleanLineCountsWhatTheSweepChecked(t *testing.T) {
	finding := doctor.CheckCriticalInvariants(context.Background(), &fakeDB{})
	if finding.Level != doctor.OK {
		t.Fatalf("a clean sweep is the healthy case: %+v", finding)
	}
	want := fmt.Sprintf("%d ", len(store.QuickFsckChecks))
	if !strings.HasPrefix(finding.Detail, want) {
		t.Fatalf("the clean line reads %q, want it to count the %d invariants the quick sweep evaluates",
			finding.Detail, len(store.QuickFsckChecks))
	}
}
