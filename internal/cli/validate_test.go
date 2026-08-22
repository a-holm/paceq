package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/spec"
)

// project writes job files into a temporary directory and returns it. The paths
// are relative to the project, so a test reads the way a user's tree does.
func project(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return dir
}

const goodJob = `name: nightly
steps:
  - name: only
    run: ["/bin/true"]
`

const brokenJob = `name: broken
steps:
  - name: only
    retries: 3
    run: ["/bin/true"]
`

const warningJob = `name: legacy
inherit_env: [SSH_AUTH_SOCK]
steps:
  - name: only
    run: ["rsync -a /a/ /b/"]
    shell: true
`

// TestValidateOnACleanProjectExitsZero is the shape of a passing run: nothing
// on stderr, a summary on stdout, exit 0.
func TestValidateOnACleanProjectExitsZero(t *testing.T) {
	dir := project(t, map[string]string{"jobs/a.yaml": goodJob, "jobs/b.yml": goodJob})

	got := runCLI(t, dir, nil, "validate", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq validate = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	if got.stderr != "" {
		t.Errorf("a clean run wrote to stderr: %q", got.stderr)
	}
	if !strings.Contains(got.stdout, "no problems") {
		t.Errorf("the summary does not say the project is clean:\n%s", got.stdout)
	}
}

// TestValidateReportsPositionedErrors is the acceptance criterion: file, line,
// column, the line quoted, a caret under it and a way forward.
func TestValidateReportsPositionedErrors(t *testing.T) {
	dir := project(t, map[string]string{"jobs/broken.yaml": brokenJob})

	got := runCLI(t, dir, nil, "validate", "-o", "text", "--no-color")

	if got.code != ExitValidation {
		t.Fatalf("paceq validate = %d, want %d\n%s%s", got.code, ExitValidation, got.stdout, got.stderr)
	}
	for _, want := range []string{
		"jobs/broken.yaml:4:5",
		spec.CodeUnknownField,
		`did you mean "retry"`,
		"retries: 3",
		"^",
		"paceq error " + spec.CodeUnknownField,
	} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the report does not carry %q:\n%s", want, got.stdout)
		}
	}
	if !strings.Contains(got.stderr, "\n  ") {
		t.Errorf("the failure has no indented next step:\n%s", got.stderr)
	}
}

// TestValidateJSONIsTheDocumentedStructure. The field names are a contract: a
// script reads .diagnostics[].code and branches on it.
func TestValidateJSONIsTheDocumentedStructure(t *testing.T) {
	dir := project(t, map[string]string{"jobs/broken.yaml": brokenJob})

	got := runCLI(t, dir, nil, "validate")

	if got.code != ExitValidation {
		t.Fatalf("paceq validate = %d, want %d\n%s", got.code, ExitValidation, got.stderr)
	}

	var report struct {
		Diagnostics []struct {
			Code     string `json:"code"`
			Severity string `json:"severity"`
			File     string `json:"file"`
			Line     int    `json:"line"`
			Col      int    `json:"col"`
			Message  string `json:"message"`
			Hint     string `json:"hint"`
		} `json:"diagnostics"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &report); err != nil {
		t.Fatalf("stdout is not the documented document: %v\n%s", err, got.stdout)
	}
	if len(report.Diagnostics) != 1 {
		t.Fatalf("got %d diagnostics, want 1:\n%s", len(report.Diagnostics), got.stdout)
	}

	d := report.Diagnostics[0]
	switch {
	case d.Code != spec.CodeUnknownField:
		t.Errorf("code is %q, want %s", d.Code, spec.CodeUnknownField)
	case d.Severity != "error":
		t.Errorf("severity is %q, want error", d.Severity)
	case d.File != filepath.Join("jobs", "broken.yaml"):
		t.Errorf("file is %q", d.File)
	case d.Line != 4 || d.Col != 5:
		t.Errorf("position is %d:%d, want 4:5", d.Line, d.Col)
	case d.Message == "" || d.Hint == "":
		t.Errorf("the diagnostic is missing a message or a next step: %+v", d)
	}
}

// TestValidateJSONOnACleanProjectIsAnEmptyList. A script that iterates the
// field should not have to special case a clean run.
func TestValidateJSONOnACleanProjectIsAnEmptyList(t *testing.T) {
	dir := project(t, map[string]string{"jobs/a.yaml": goodJob})

	got := runCLI(t, dir, nil, "validate")

	if got.code != ExitOK {
		t.Fatalf("paceq validate = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	if strings.TrimSpace(got.stdout) != `{"diagnostics":[]}` {
		t.Errorf("stdout is %q, want an empty diagnostics list", strings.TrimSpace(got.stdout))
	}
}

// TestWarningsAloneExitZero is the exit code table: a warning is not a failure.
func TestWarningsAloneExitZero(t *testing.T) {
	dir := project(t, map[string]string{"jobs/legacy.yaml": warningJob})

	got := runCLI(t, dir, nil, "validate", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq validate = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	for _, want := range []string{spec.CodeShell, spec.CodeInheritEnv, "WARN"} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the report does not carry %q:\n%s", want, got.stdout)
		}
	}
}

// TestStrictTurnsWarningsIntoFailures is what a pipeline sets.
func TestStrictTurnsWarningsIntoFailures(t *testing.T) {
	dir := project(t, map[string]string{"jobs/legacy.yaml": warningJob})

	got := runCLI(t, dir, nil, "validate", "--strict", "-o", "text")

	if got.code != ExitValidation {
		t.Fatalf("paceq validate --strict = %d, want %d\n%s%s", got.code, ExitValidation, got.stdout, got.stderr)
	}
	if strings.Contains(got.stdout, "WARN") {
		t.Errorf("--strict left a warning as a warning:\n%s", got.stdout)
	}
}

// TestValidateChecksTheFileItIsGiven. A path that names a file is checked
// whatever it is called, so a job kept outside the jobs directory still works.
func TestValidateChecksTheFileItIsGiven(t *testing.T) {
	dir := project(t, map[string]string{"jobs/a.yaml": goodJob, "elsewhere/b.txt": brokenJob})

	clean := runCLI(t, dir, nil, "validate", "jobs/a.yaml", "-o", "text")
	if clean.code != ExitOK {
		t.Errorf("validating one good file = %d, want %d\n%s", clean.code, ExitOK, clean.stdout)
	}

	broken := runCLI(t, dir, nil, "validate", "elsewhere/b.txt", "-o", "text")
	if broken.code != ExitValidation {
		t.Errorf("validating one broken file = %d, want %d\n%s", broken.code, ExitValidation, broken.stdout)
	}
}

// TestValidateWalksADirectoryInAFixedOrder. Two runs over one tree report the
// same thing in the same order, which is what makes the output diffable.
func TestValidateWalksADirectoryInAFixedOrder(t *testing.T) {
	dir := project(t, map[string]string{
		"jobs/c.yaml":       brokenJob,
		"jobs/a.yaml":       brokenJob,
		"jobs/nested/b.yml": brokenJob,
		"jobs/notes.txt":    "this is not a job file",
		"jobs/.hidden.yaml": brokenJob,
	})

	first := runCLI(t, dir, nil, "validate", "-o", "text")
	second := runCLI(t, dir, nil, "validate", "-o", "text")

	if first.stdout != second.stdout {
		t.Errorf("two runs reported different things:\n%s\n---\n%s", first.stdout, second.stdout)
	}
	order := []string{"jobs/a.yaml", "jobs/c.yaml", "jobs/nested/b.yml"}
	at := 0
	for _, want := range order {
		i := strings.Index(first.stdout[at:], want)
		if i < 0 {
			t.Fatalf("%s is missing or out of order:\n%s", want, first.stdout)
		}
		at += i
	}
	if strings.Contains(first.stdout, "notes.txt") {
		t.Errorf("a file that is not a job file was checked:\n%s", first.stdout)
	}
	if strings.Contains(first.stdout, ".hidden.yaml") {
		t.Errorf("a dot file was checked:\n%s", first.stdout)
	}
}

// TestValidateWithNothingToCheckIsAUsageError is exit 2: the command line asks
// for something that cannot be carried out.
func TestValidateWithNothingToCheckIsAUsageError(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "validate")

	if got.code != ExitUsage {
		t.Fatalf("paceq validate with no jobs directory = %d, want %d\n%s", got.code, ExitUsage, got.stderr)
	}
	if !strings.Contains(got.stderr, "paceq init") {
		t.Errorf("the refusal does not say how to get a project:\n%s", got.stderr)
	}
	if got.stdout != "" {
		t.Errorf("a failed command wrote to stdout: %q", got.stdout)
	}
}

// TestValidateWithABadFlagIsAUsageError keeps the other exit 2 path covered on
// this command specifically.
func TestValidateWithABadFlagIsAUsageError(t *testing.T) {
	dir := project(t, map[string]string{"jobs/a.yaml": goodJob})

	for name, args := range map[string][]string{
		"unknown flag":   {"validate", "--nope"},
		"unknown format": {"validate", "-o", "yaml"},
	} {
		t.Run(name, func(t *testing.T) {
			got := runCLI(t, dir, nil, args...)
			if got.code != ExitUsage {
				t.Errorf("paceq %s = %d, want %d\n%s", strings.Join(args, " "), got.code, ExitUsage, got.stderr)
			}
		})
	}
}

// TestValidateOnAPathThatIsNotThereIsNotFound is exit 3, and the message says
// what to do instead of it.
func TestValidateOnAPathThatIsNotThereIsNotFound(t *testing.T) {
	dir := project(t, map[string]string{"jobs/a.yaml": goodJob})

	got := runCLI(t, dir, nil, "validate", "jobs/nope.yaml")

	if got.code != ExitNotFound {
		t.Fatalf("paceq validate on a missing path = %d, want %d\n%s", got.code, ExitNotFound, got.stderr)
	}
	if !strings.Contains(got.stderr, "jobs/nope.yaml") {
		t.Errorf("the refusal does not name the path:\n%s", got.stderr)
	}
	if !strings.Contains(got.stderr, "\n  ") {
		t.Errorf("the refusal has no indented next step:\n%s", got.stderr)
	}
}

// TestValidateOnADirectoryWithNoJobFilesIsNotFound. Exiting 0 having checked
// nothing is the bad kind of green.
func TestValidateOnADirectoryWithNoJobFilesIsNotFound(t *testing.T) {
	dir := project(t, map[string]string{"jobs/notes.txt": "not a job"})

	got := runCLI(t, dir, nil, "validate", "jobs")

	if got.code != ExitNotFound {
		t.Fatalf("paceq validate on an empty jobs directory = %d, want %d\n%s", got.code, ExitNotFound, got.stderr)
	}
	if !strings.Contains(got.stderr, ".yaml") {
		t.Errorf("the refusal does not say what a job file is called:\n%s", got.stderr)
	}
}

// TestWhatInitWritesValidates is the promise init makes, checked end to end.
// The example is the first file anybody copies, so it has to pass the rules the
// files copied from it will be held to.
func TestWhatInitWritesValidates(t *testing.T) {
	dir := t.TempDir()
	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}

	got := runCLI(t, dir, nil, "validate", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("validating what init wrote = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stdout, "no problems") {
		t.Errorf("the example job is not clean:\n%s", got.stdout)
	}
}

// TestEveryCodeTheSpecRaisesIsExplained keeps `paceq error <code>` honest. Every
// message ends by pointing at that command, so a code with no entry is a
// message that sends the reader to a page that does not exist.
func TestEveryCodeTheSpecRaisesIsExplained(t *testing.T) {
	for _, code := range spec.Codes() {
		t.Run(code, func(t *testing.T) {
			got := runCLI(t, t.TempDir(), nil, "error", code, "-o", "text")

			if got.code != ExitOK {
				t.Fatalf("paceq error %s = %d, want %d\n%s", code, got.code, ExitOK, got.stderr)
			}
			if !strings.Contains(got.stdout, code) {
				t.Errorf("the explanation does not name the code:\n%s", got.stdout)
			}
			if !strings.Contains(got.stdout, "→") && !strings.Contains(got.stdout, "->") {
				t.Errorf("the explanation offers no next step:\n%s", got.stdout)
			}
		})
	}
}

// TestValidateNeedsNoState is the property that makes validate usable in CI and
// in an editor: it opens no database and takes no lock, so it works on a
// machine that has never run paceq init and beside a paceq that is running.
func TestValidateNeedsNoState(t *testing.T) {
	dir := project(t, map[string]string{"jobs/a.yaml": goodJob})

	got := runCLI(t, dir, nil, "validate", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq validate without state = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, stateDirName)); err == nil {
		t.Error("validate created a state directory")
	}
}
