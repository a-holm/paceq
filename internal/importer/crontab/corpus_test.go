package crontab

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/spec"
)

// corpusFiles collects every fixture under testdata/corpus.
func corpusFiles(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join("testdata", "corpus")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("corpus directory missing: %v", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".crontab") {
			names = append(names, filepath.Join(dir, e.Name()))
		}
	}
	if len(names) < 40 {
		t.Fatalf("the corpus holds %d files, and the corpus gate needs at least 40", len(names))
	}
	return names
}

// corpusOptions maps a fixture to the options it is imported under. The
// ansible-role files model entries cron keeps in /etc/cron.d, so they come
// in with the six-field reading, exactly as --file /etc/cron.d/x would.
func corpusOptions(name string) Options {
	if strings.HasPrefix(filepath.Base(name), "ansible-role-") {
		return Options{SixField: true}
	}
	return Options{}
}

// TestCorpusInterpretationRate is the R6 gate: more than ninety percent of
// the lines in real crontabs must be understood on the first pass, or the
// product is dead the moment a new user runs it. The number is logged so
// every run shows exactly where the boundary sits.
func TestCorpusInterpretationRate(t *testing.T) {
	var totalLines, todoJobs int
	for _, f := range corpusFiles(t) {
		src, err := os.ReadFile(f) // #nosec G304 - testdata path from ReadDir
		if err != nil {
			t.Fatal(err)
		}
		res := Import(src, corpusOptions(f))
		totalLines += res.Report.Lines
		todoJobs += res.Report.NeedsReview
	}
	rate := float64(totalLines-todoJobs) / float64(totalLines)
	t.Logf("interpretation rate %.1f%% (%d of %d lines, %d jobs to review)",
		rate*100, totalLines-todoJobs, totalLines, todoJobs)
	if rate <= 0.90 {
		t.Fatalf("interpretation rate %.1f%% is not above 90%% (09 R6)", rate*100)
	}
}

// TestCorpusRoundTripsThroughSpec is the other half of the promise: every
// job the importer emits must parse as a paceq job file. Anything that does
// not parse is a file the user cannot use, however pretty it looks.
func TestCorpusRoundTripsThroughSpec(t *testing.T) {
	for _, f := range corpusFiles(t) {
		src, err := os.ReadFile(f) // #nosec G304 - testdata path from ReadDir
		if err != nil {
			t.Fatal(err)
		}
		res := Import(src, corpusOptions(f))
		var b strings.Builder
		if emitErr := Emit(res.Docs, nil, &b); emitErr != nil {
			t.Fatalf("%s: emit: %v", f, emitErr)
		}
		docs := SplitDocuments(b.String())
		if len(docs) != len(res.Docs) {
			t.Fatalf("%s: split into %d documents from %d jobs", f, len(docs), len(res.Docs))
		}
		for i, doc := range docs {
			job, diags := spec.Parse(f, []byte(doc))
			if job == nil || diags.HasErrors() {
				msgs := make([]string, 0, len(diags))
				for _, d := range diags {
					msgs = append(msgs, d.Message+" ("+d.Code+")")
				}
				t.Errorf("%s document %d does not parse:\n%s\n--- %s",
					f, i+1, doc, strings.Join(msgs, "; "))
			}
		}
	}
}

// TestCorpusEveryFileProducesTheReportShape checks that no fixture crashes
// the report and that counts stay consistent: jobs plus nothing lost.
func TestCorpusCountsStayConsistent(t *testing.T) {
	for _, f := range corpusFiles(t) {
		src, err := os.ReadFile(f) // #nosec G304 - testdata path from ReadDir
		if err != nil {
			t.Fatal(err)
		}
		res := Import(src, Options{})
		if res.Report.Jobs != len(res.Docs) {
			t.Errorf("%s: report says %d jobs, got %d docs", f, res.Report.Jobs, len(res.Docs))
		}
		if res.Report.NeedsReview > res.Report.Jobs {
			t.Errorf("%s: %d jobs to review out of %d jobs", f, res.Report.NeedsReview, res.Report.Jobs)
		}
		names := make(map[string]bool, len(res.Docs))
		for _, d := range res.Docs {
			if names[d.Name] {
				t.Errorf("%s: duplicate job name %q", f, d.Name)
			}
			names[d.Name] = true
		}
	}
}
