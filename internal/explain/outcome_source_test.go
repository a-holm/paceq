package explain

import (
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// The outcome source answers "why was this not run again?" from the row
// (issue #39). It shows only when it is worth a sentence: the direct era's
// rows carry nothing, and 'spool' and 'reconciled' both get their words in
// the text report as well as the data.

func TestStepEntryNamesTheNonDirectSources(t *testing.T) {
	for _, tc := range []struct {
		source   string
		inData   bool
		proseTxt string
	}{
		{"", false, ""},
		{"direct", false, ""},
		{"spool", true, "committed from the attempt's result file"},
		{"reconciled", true, "assumed without a source"},
	} {
		step := store.Step{
			Name:          "only",
			State:         "succeeded",
			OutcomeSource: tc.source,
		}
		e := stepEntry(step)
		got, ok := e.ReasonData["outcome_source"]
		if tc.inData {
			if !ok || got != tc.source {
				t.Fatalf("source %q: reason_data has %v, want %q", tc.source, got, tc.source)
			}
		} else if ok {
			t.Fatalf("source %q: reason_data carries outcome_source %v", tc.source, got)
		}
	}
}

func TestTextReportExplainsTheNonDirectSources(t *testing.T) {
	for _, tc := range []struct {
		source   string
		proseTxt string
	}{
		{"spool", "committed from the attempt's result file"},
		{"reconciled", "assumed without a source"},
	} {
		var b strings.Builder
		r := &Report{Entries: []Entry{{
			Kind:    "run",
			Ref:     "01J0RUN",
			Outcome: "succeeded",
			Children: []Entry{{
				Kind:    "step",
				Ref:     "only",
				Outcome: "succeeded",
				ReasonData: map[string]any{
					"attempt":        1,
					"max_attempts":   1,
					"outcome_source": tc.source,
				},
			}},
		}}}
		renderRunReport(&b, r, StyleASCII())
		if !strings.Contains(b.String(), tc.proseTxt) {
			t.Fatalf("source %q: the report does not explain itself:\n%s", tc.source, b.String())
		}
	}
}
