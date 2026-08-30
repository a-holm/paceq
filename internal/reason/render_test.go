package reason

import (
	"strings"
	"testing"
)

// storageNames are the columns and tables a reason code is written to. The
// generated page must not name any of them under a level heading, because a
// level is not a promise about storage (#193).
var storageNames = []string{
	"table", "ticks", "triggers", "runs", "steps", "lease_events", "run_events",
}

// TestNoLevelSectionPromisesAStorageTable holds the generated reference to the
// same rule as the Level comment: a level is the object a code explains, and
// it says nothing about where the row lands. The page grouped the catalogue by
// level under one line each that read "Codes stored in the ticks table", which
// is a mapping the writers do not honour. Two run level codes disprove it:
// RUN_REJECTED_DISK_LOW is written on the evaluation and the trigger that
// refused the run, and RUN_INTERRUPTED_SHUTDOWN on the step rows the drain
// handed back; neither ever reaches runs.reason_code.
//
// The writers are pinned in internal/store by
// TestTheDiskHoldRecordsItsCodeOnTheTickAndTheTriggerAndNoRun and
// TestDrainRunRestoresTheInterruptedAttempt. This test is the documentation
// half: it stops the page from promising what those two prove false.
func TestNoLevelSectionPromisesAStorageTable(t *testing.T) {
	page := Render()

	for _, l := range levelOrder {
		intro := levelSectionIntro(t, page, l)
		if intro == "" {
			t.Errorf("the %s level section carries no line under its heading", l)
			continue
		}
		lower := strings.ToLower(intro)
		for _, name := range storageNames {
			if strings.Contains(lower, name) {
				t.Errorf("the %s level section says %q: naming %q under a level heading "+
					"promises a level-to-table mapping that no writer honours", l, intro, name)
			}
		}
	}

	preamble, _, ok := strings.Cut(page, "\n## ")
	if !ok {
		t.Fatal("the page has no level headings, so this test read nothing")
	}
	if !strings.Contains(preamble, string(RUNRejectedDiskLow)) {
		t.Errorf("the page's opening does not cite %s, the code that proves a level "+
			"is the object explained and not the row it lands on", RUNRejectedDiskLow)
	}
}

// levelSectionIntro returns the paragraph the page prints between a level
// heading and the code list under it.
func levelSectionIntro(t *testing.T, page string, l Level) string {
	t.Helper()

	heading := "\n## " + levelHeading(l) + " level\n\n"
	_, after, ok := strings.Cut(page, heading)
	if !ok {
		t.Fatalf("the page has no %q heading", strings.TrimSpace(heading))
	}
	intro, _, ok := strings.Cut(after, "\n\n")
	if !ok {
		t.Fatalf("the %s level heading is not followed by a paragraph", l)
	}
	return intro
}
