package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// applyProject writes job files, runs paceq init so a state database exists,
// and returns the project directory. apply is the first command after init in
// real life, so the tests walk the same road.
func applyProject(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := project(t, files)
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("paceq init = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	return dir
}

// countVersions reads how many versions one job has by opening the state
// database and closing it again, so a later apply in the same test can still
// take the state lock.
func countVersions(t *testing.T, dir, job string) int {
	t.Helper()

	s, err := store.OpenState(context.Background(), filepath.Join(dir, stateDirName), store.Options{})
	if err != nil {
		t.Fatalf("open the state store: %v", err)
	}
	defer func() { _ = s.Close() }()

	versions, err := s.ListJobVersions(context.Background(), job)
	if err != nil {
		t.Fatalf("list the versions of %s: %v", job, err)
	}
	return len(versions)
}

// writeFile replaces one project file between applies.
func writeFile(t *testing.T, dir, name, body string) error {
	t.Helper()

	return os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600)
}

// TestApplyTwiceLoadsOneVersion is the acceptance criterion that names the
// command: applying an unchanged catalog a second time writes nothing and says
// so. The load alias behaves exactly the same.
func TestApplyTwiceLoadsOneVersion(t *testing.T) {
	dir := applyProject(t, map[string]string{"jobs/nightly.yaml": goodJob})

	first := runCLI(t, dir, nil, "apply", "-o", "text")
	if first.code != ExitOK {
		t.Fatalf("paceq apply = %d, want %d\n%s%s", first.code, ExitOK, first.stdout, first.stderr)
	}
	if !strings.Contains(first.stdout, "nightly") || !strings.Contains(first.stdout, "version 1") {
		t.Errorf("the first apply does not report nightly as version 1:\n%s", first.stdout)
	}

	second := runCLI(t, dir, nil, "load", "-o", "text")
	if second.code != ExitOK {
		t.Fatalf("paceq load = %d, want %d\n%s%s", second.code, ExitOK, second.stdout, second.stderr)
	}
	if !strings.Contains(second.stdout, "unchanged") {
		t.Errorf("the second apply does not say unchanged:\n%s", second.stdout)
	}

	if got := countVersions(t, dir, "nightly"); got != 1 {
		t.Errorf("two applies left %d versions of nightly, want 1", got)
	}
}

// TestApplyCommentsAndKeyOrderAreNotAVersion: the canonical hash decides what
// a change is. Two files that mean the same thing are one version, however
// differently they are written.
func TestApplyCommentsAndKeyOrderAreNotAVersion(t *testing.T) {
	written := `# every night
name: nightly
steps:
  - name: only
    run: ["/bin/true"]
`
	reordered := `name: nightly
# the same job, written differently
steps:
  - name: only
    run: ["/bin/true"]
`
	dir := applyProject(t, map[string]string{"jobs/nightly.yaml": written})

	if got := runCLI(t, dir, nil, "apply"); got.code != ExitOK {
		t.Fatalf("first apply = %d\n%s%s", got.code, got.stdout, got.stderr)
	}

	if err := writeFile(t, dir, "jobs/nightly.yaml", reordered); err != nil {
		t.Fatal(err)
	}
	got := runCLI(t, dir, nil, "apply")
	if got.code != ExitOK {
		t.Fatalf("second apply = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stdout, "unchanged") {
		t.Errorf("a comment and a key order became a new version:\n%s", got.stdout)
	}
	if got := countVersions(t, dir, "nightly"); got != 1 {
		t.Errorf("two spellings of one spec left %d versions, want 1", got)
	}

	if err := writeFile(t, dir, "jobs/nightly.yaml", goodJob+"\n"+`  - name: second
    run: ["/bin/true"]
`); err != nil {
		t.Fatal(err)
	}
	if got := runCLI(t, dir, nil, "apply"); got.code != ExitOK {
		t.Fatalf("third apply = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	if got := countVersions(t, dir, "nightly"); got != 2 {
		t.Errorf("a real change left %d versions, want 2", got)
	}
}

// TestApplyOneBrokenFileAmongGoodOnes: the broken file never reaches the
// database and costs exit 4; the good ones around it still load. The failure
// says file, position and code, and reminds the reader that the last good
// definition stands.
func TestApplyOneBrokenFileAmongGoodOnes(t *testing.T) {
	dir := applyProject(t, map[string]string{
		"jobs/good.yaml":   goodJob,
		"jobs/broken.yaml": brokenJob,
	})

	got := runCLI(t, dir, nil, "apply", "-o", "text", "--no-color")

	if got.code != ExitValidation {
		t.Fatalf("paceq apply = %d, want %d\n%s%s", got.code, ExitValidation, got.stdout, got.stderr)
	}
	for _, want := range []string{"jobs/broken.yaml:", spec.CodeUnknownField} {
		if !strings.Contains(got.stdout, want) && !strings.Contains(got.stderr, want) {
			t.Errorf("the report does not carry %q:\n%s%s", want, got.stdout, got.stderr)
		}
	}

	if got := countVersions(t, dir, "nightly"); got != 1 {
		t.Errorf("the good job was loaded %d times, want once", got)
	}
	if got := countVersions(t, dir, "broken"); got != 0 {
		t.Errorf("the broken file wrote %d versions, want 0", got)
	}
}

// TestApplyABrokenFileKeepsTheLastGoodVersion: nightly loads fine once, the
// file is then broken by an edit, and the next apply refuses the edit but
// leaves version 1 standing with the pointer where it was. A syntax error at
// 02:00 must not take the night's job down with it.
func TestApplyABrokenFileKeepsTheLastGoodVersion(t *testing.T) {
	dir := applyProject(t, map[string]string{"jobs/nightly.yaml": goodJob})

	if got := runCLI(t, dir, nil, "apply"); got.code != ExitOK {
		t.Fatalf("first apply = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	if err := writeFile(t, dir, "jobs/nightly.yaml", brokenJob); err != nil {
		t.Fatal(err)
	}

	got := runCLI(t, dir, nil, "apply")
	if got.code != ExitValidation {
		t.Fatalf("second apply = %d, want %d\n%s%s", got.code, ExitValidation, got.stdout, got.stderr)
	}
	if got := countVersions(t, dir, "nightly"); got != 1 {
		t.Errorf("the broken edit left %d versions behind, want the one good one", got)
	}
}

// TestApplyWithoutAProjectExitsTwo: apply with nothing to write into and
// nothing to read from is a wrong command line, not a database failure.
func TestApplyWithoutAProjectExitsTwo(t *testing.T) {
	dir := t.TempDir()

	got := runCLI(t, dir, nil, "apply")

	if got.code != ExitUsage {
		t.Fatalf("paceq apply = %d, want %d\n%s%s", got.code, ExitUsage, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stderr, "init") {
		t.Errorf("the message does not point at paceq init:\n%s", got.stderr)
	}
}

// TestApplyJSONReportIsAStableContract: scripts read .applied[].job,
// .unchanged[].job and .failed[].file. Nothing here is renamed without a major
// version, and empty groups are lists rather than null.
func TestApplyJSONReportIsAStableContract(t *testing.T) {
	dir := applyProject(t, map[string]string{
		"jobs/good.yaml":   goodJob,
		"jobs/broken.yaml": brokenJob,
	})

	got := runCLI(t, dir, nil, "apply", "-o", "json")
	if got.code != ExitValidation {
		t.Fatalf("paceq apply = %d, want %d\n%s%s", got.code, ExitValidation, got.stdout, got.stderr)
	}

	doc := got.json(t)
	applied, ok := doc["applied"].([]any)
	if !ok || len(applied) != 2 {
		t.Fatalf(".applied is %v, want two entries (the example job plus good.yaml)", doc["applied"])
	}
	var nightly map[string]any
	for _, entry := range applied {
		if e, _ := entry.(map[string]any); e["job"] == "nightly" {
			nightly = e
		}
	}
	if nightly == nil {
		t.Fatalf("no .applied entry names nightly: %v", applied)
	}
	for _, field := range []string{"job", "file", "version", "spec_hash"} {
		if _, ok := nightly[field]; !ok {
			t.Errorf("the nightly entry has no %q", field)
		}
	}
	if failed, ok := doc["failed"].([]any); !ok || len(failed) != 1 {
		t.Fatalf(".failed is %v, want one entry", doc["failed"])
	} else {
		entry, _ := failed[0].(map[string]any)
		if entry["file"] != filepath.Join("jobs", "broken.yaml") {
			t.Errorf(".failed[0].file is %v, want jobs/broken.yaml", entry["file"])
		}
		if _, ok := entry["code"]; !ok {
			t.Error(".failed[0] has no code")
		}
	}
	if unchanged, ok := doc["unchanged"].([]any); !ok || len(unchanged) != 0 {
		t.Errorf(".unchanged is %v, want an empty list", doc["unchanged"])
	}
}

// TestApplyReadsTheJobsDirectoryFromTheEnvironment: PACEQ_JOBS_DIR moves the
// catalog, for setups that keep specs outside the project directory.
func TestApplyReadsTheJobsDirectoryFromTheEnvironment(t *testing.T) {
	dir := applyProject(t, map[string]string{"specs/nightly.yaml": goodJob})

	got := runCLI(t, dir, map[string]string{"PACEQ_JOBS_DIR": "specs"}, "apply")

	if got.code != ExitOK {
		t.Fatalf("paceq apply = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	if got := countVersions(t, dir, "nightly"); got != 1 {
		t.Errorf("the environment catalog loaded %d versions of nightly, want 1", got)
	}
}
