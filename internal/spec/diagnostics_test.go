package spec_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

// TestBadFilesMatchTheirDiagnostics is the golden suite over testdata/bad. What
// is compared is the whole rendered message, excerpt and caret included, not
// just the code. The message is the documentation (03 section 1), so a change
// to it has to be a change somebody reads in a diff.
//
// The rendering is fixed: ASCII marks and no colour. A golden whose content
// depends on the terminal it was produced in cannot be compared at all
// (03 risk 10).
func TestBadFilesMatchTheirDiagnostics(t *testing.T) {
	for _, dir := range []string{"testdata/bad", "testdata/warn"} {
		for _, path := range jobFiles(t, dir) {
			t.Run(filepath.Base(path), func(t *testing.T) {
				src := read(t, path)
				_, diags := spec.Parse(path, src)
				if len(diags) == 0 {
					t.Fatalf("%s produced no diagnostics", path)
				}

				var got bytes.Buffer
				if err := diag.ASCII.RenderAll(&got, diags, map[string][]byte{path: src}); err != nil {
					t.Fatalf("render: %v", err)
				}

				golden := strings.TrimSuffix(path, filepath.Ext(path)) + ".golden"
				if *update {
					writeGolden(t, golden, got.Bytes())
					return
				}
				if want := read(t, golden); !bytes.Equal(got.Bytes(), want) {
					t.Errorf("diagnostics for %s do not match %s\n--- got ---\n%s\n--- want ---\n%s",
						path, golden, got.String(), want)
				}
			})
		}
	}
}

// TestEveryDiagnosticIsComplete is 03 section 8.1 as a test: a diagnostic
// without a code, a file, a message and a next step is a dead end. It runs over
// every file in testdata, so a new fixture cannot introduce an incomplete one.
func TestEveryDiagnosticIsComplete(t *testing.T) {
	known := map[string]bool{}
	for _, code := range spec.Codes() {
		known[code] = true
	}

	for _, dir := range []string{"testdata/ok", "testdata/bad", "testdata/warn"} {
		for _, path := range jobFiles(t, dir) {
			_, diags := spec.Parse(path, read(t, path))
			for _, d := range diags {
				switch {
				case d.Code == "":
					t.Errorf("%s: a diagnostic has no code: %s", path, d.Message)
				case !known[d.Code]:
					t.Errorf("%s: code %s is not in spec.Codes()", path, d.Code)
				}
				if d.File != path {
					t.Errorf("%s: a diagnostic names the file as %q", path, d.File)
				}
				if strings.TrimSpace(d.Message) == "" {
					t.Errorf("%s: %s has no message", path, d.Code)
				}
				if strings.TrimSpace(d.Hint) == "" {
					t.Errorf("%s: %s has no next step", path, d.Code)
				}
			}
		}
	}
}

// TestDiagnosticsCarryAPosition keeps the excerpt possible. A diagnostic about
// something written in the file points at where it is written; only the ones
// about the file as a whole are allowed to point at nothing.
func TestDiagnosticsCarryAPosition(t *testing.T) {
	wholeFile := map[string]bool{
		spec.CodeFileTooLarge:    true,
		spec.CodeSyntax:          true,
		spec.CodeMissingField:    true,
		spec.CodeTooManyProblems: true,
	}

	for _, path := range jobFiles(t, "testdata/bad") {
		src := read(t, path)
		_, diags := spec.Parse(path, src)
		lines := bytes.Count(src, []byte("\n")) + 1
		for _, d := range diags {
			if d.Line == 0 {
				if !wholeFile[d.Code] {
					t.Errorf("%s: %s has no line: %s", path, d.Code, d.Message)
				}
				continue
			}
			if d.Line > lines {
				t.Errorf("%s: %s points at line %d of a file with %d", path, d.Code, d.Line, lines)
			}
			if d.Col < 1 {
				t.Errorf("%s: %s has line %d but no column", path, d.Code, d.Line)
			}
		}
	}
}

// TestTheCaretLandsUnderWhatWentWrong checks the position itself rather than
// the rendering of it: the column a diagnostic reports has to be where the
// offending text starts, or the caret in every golden above is decoration.
func TestTheCaretLandsUnderWhatWentWrong(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		code  string
		under string
	}{
		{
			name:  "an unknown field points at the key",
			src:   "name: report\nsteps:\n  - name: only\n    retries: 3\n    run: [\"/bin/true\"]\n",
			code:  spec.CodeUnknownField,
			under: "retries",
		},
		{
			name:  "a duplicate key points at the second one",
			src:   "name: report\ntimeout: 30m\ntimeout: 45m\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n",
			code:  spec.CodeDuplicateKey,
			under: "timeout: 45m",
		},
		{
			name:  "a bad duration points at the value",
			src:   "name: report\ntimeout: 45x\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n",
			code:  spec.CodeBadDuration,
			under: "45x",
		},
		{
			name:  "a relative command points at the first argument",
			src:   "name: report\nsteps:\n  - name: only\n    run: [\"extract\", \"--out\"]\n",
			code:  spec.CodeRunNotAbsolute,
			under: "\"extract\"",
		},
		{
			name:  "max_concurrent points at the number",
			src:   "name: report\nmax_concurrent: 0\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n",
			code:  spec.CodeBadConcurrency,
			under: "0",
		},
		{
			name:  "max_parallel points at the number",
			src:   "name: report\nmax_parallel: 0\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n",
			code:  spec.CodeBadMaxParallel,
			under: "0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, diags := spec.Parse("x.yaml", []byte(tc.src))
			d, ok := find(diags, tc.code)
			if !ok {
				t.Fatalf("no %s in:\n%s", tc.code, renderSource(t, diags, tc.src))
			}

			lines := strings.Split(tc.src, "\n")
			if d.Line < 1 || d.Line > len(lines) {
				t.Fatalf("%s points at line %d of a %d line file", tc.code, d.Line, len(lines))
			}
			line := []rune(lines[d.Line-1])
			if d.Col < 1 || d.Col > len(line)+1 {
				t.Fatalf("%s points at column %d of a %d character line", tc.code, d.Col, len(line))
			}
			if got := string(line[d.Col-1:]); !strings.HasPrefix(got, tc.under) {
				t.Errorf("the caret sits under %q, want it under %q\n%s", got, tc.under, renderSource(t, diags, tc.src))
			}
		})
	}
}

// TestUnknownFieldSuggestsTheNearestOne is the acceptance criterion about
// did-you-mean, checked on the message rather than on the suggester, because
// what matters is that the right candidate list reaches it.
func TestUnknownFieldSuggestsTheNearestOne(t *testing.T) {
	cases := []struct {
		written string
		want    string
		where   string
	}{
		{"retries", "retry", "step"},
		{"retrys", "retry", "step"},
		{"need", "needs", "step"},
		{"shel", "shell", "step"},
		{"cmd", "run", "step"},
		{"command", "run", "step"},
		{"depends_on", "needs", "step"},
		{"workdirs", "workdir", "step"},
		{"job", "name", "job"},
		{"discription", "description", "job"},
		{"envfile", "env_file", "job"},
		{"max_concurent", "max_concurrent", "job"},
		{"schedule", "schedules", "job"},
		{"steps_", "steps", "job"},
	}

	for _, tc := range cases {
		t.Run(tc.written, func(t *testing.T) {
			var src string
			if tc.where == "job" {
				src = fmt.Sprintf("name: report\n%s: x\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n", tc.written)
			} else {
				src = fmt.Sprintf("name: report\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n    %s: x\n", tc.written)
			}

			_, diags := spec.Parse("x.yaml", []byte(src))
			d, ok := find(diags, spec.CodeUnknownField)
			if !ok {
				t.Fatalf("%q was not refused as an unknown field:\n%s", tc.written, renderSource(t, diags, src))
			}
			if !strings.Contains(d.Message, fmt.Sprintf("did you mean %q", tc.want)) {
				t.Errorf("the message does not suggest %q: %s", tc.want, d.Message)
			}
			if !strings.Contains(d.Hint, tc.want) {
				t.Errorf("the next step does not name %q: %s", tc.want, d.Hint)
			}
		})
	}
}

// TestUnknownFieldWithNoNeighbourStillLists. A name nothing is close to gets no
// suggestion, and the message still has to say what the fields are.
func TestUnknownFieldWithNoNeighbourStillLists(t *testing.T) {
	_, diags := spec.Parse("x.yaml", []byte("name: report\nzookeeper: x\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))

	d, ok := find(diags, spec.CodeUnknownField)
	if !ok {
		t.Fatal("an unknown field was not refused")
	}
	if strings.Contains(d.Message, "did you mean") {
		t.Errorf("a name nothing is close to got a suggestion: %s", d.Message)
	}
	for _, field := range []string{"name", "steps", "timeout", "max_concurrent"} {
		if !strings.Contains(d.Hint, field) {
			t.Errorf("the next step does not list %q: %s", field, d.Hint)
		}
	}
}

// TestWarningsAreNotErrors. shell and inherit_env are warnings, and a job that
// carries them still comes back, because paceq will run it.
func TestWarningsAreNotErrors(t *testing.T) {
	path := "testdata/warn/shell-and-inherit.yaml"

	job, diags := spec.Parse(path, read(t, path))

	if diags.HasErrors() {
		t.Fatalf("the warning fixture has errors:\n%s", render(t, diags))
	}
	if job == nil {
		t.Fatal("a job with warnings did not come back")
	}
	if diags.Warnings() != 2 {
		t.Errorf("got %d warnings, want 2: %v", diags.Warnings(), codesOf(diags))
	}
	for _, code := range []string{spec.CodeShell, spec.CodeInheritEnv} {
		d, ok := find(diags, code)
		if !ok {
			t.Errorf("no %s warning", code)
			continue
		}
		if d.IsError() {
			t.Errorf("%s came back as an error", code)
		}
	}
}

// TestPromoteTurnsWarningsIntoErrors is what --strict does, checked where the
// rule lives rather than in the command that uses it.
func TestPromoteTurnsWarningsIntoErrors(t *testing.T) {
	path := "testdata/warn/shell-and-inherit.yaml"
	_, diags := spec.Parse(path, read(t, path))

	strict := diags.Promote()

	if !strict.HasErrors() {
		t.Error("promoting warnings left no errors")
	}
	if diags.HasErrors() {
		t.Error("promoting changed the list it was called on")
	}
	if strict.Errors() != len(diags) {
		t.Errorf("%d of %d diagnostics were promoted", strict.Errors(), len(diags))
	}
}

// TestDiagnosticsComeBackInFileOrder keeps the output readable and the goldens
// comparable: a reader walks a file from the top, and so does the report.
func TestDiagnosticsComeBackInFileOrder(t *testing.T) {
	_, diags := spec.Parse("x.yaml", []byte(`name: report
inherit_env: [PROXY]
steps:
  - name: first
    run: ["/bin/true"]
    shell: true
  - name: second
    run: ["/bin/true"]
    shell: true
`))

	if len(diags) < 3 {
		t.Fatalf("got %d diagnostics, want the three warnings", len(diags))
	}
	for i := 1; i < len(diags); i++ {
		if diags[i-1].Line > diags[i].Line {
			t.Errorf("line %d comes before line %d in the output", diags[i-1].Line, diags[i].Line)
		}
	}
}

func find(diags diag.List, code string) (diag.Diagnostic, bool) {
	for _, d := range diags {
		if d.Code == code {
			return d, true
		}
	}
	return diag.Diagnostic{}, false
}

func codesOf(diags diag.List) []string {
	codes := make([]string, 0, len(diags))
	for _, d := range diags {
		codes = append(codes, d.Code)
	}
	return codes
}

// render draws diagnostics the way a failure message wants them: whole, with
// the excerpt, so a test that fails says what the parser actually reported.
func render(t *testing.T, diags diag.List) string {
	t.Helper()

	sources := map[string][]byte{}
	for _, d := range diags {
		if _, have := sources[d.File]; have {
			continue
		}
		if src, err := readIfPossible(d.File); err == nil {
			sources[d.File] = src
		}
	}
	var b bytes.Buffer
	if err := diag.ASCII.RenderAll(&b, diags, sources); err != nil {
		t.Fatalf("render: %v", err)
	}
	return b.String()
}

func renderSource(t *testing.T, diags diag.List, src string) string {
	t.Helper()

	var b bytes.Buffer
	if err := diag.ASCII.RenderAll(&b, diags, map[string][]byte{"x.yaml": []byte(src)}); err != nil {
		t.Fatalf("render: %v", err)
	}
	return b.String()
}
