package spec_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

// FuzzParseJobSpec drives the parser with whatever the fuzzer produces and
// holds it to the promises the rest of this package rests on.
//
// The properties are the ones a caller relies on without checking:
//
//   - Parse returns. It does not panic, and it does not run out of memory on an
//     input that is a few hundred bytes long.
//   - Parse is a function. The same bytes produce the same job, the same
//     diagnostics and the same canonical form, every time.
//   - A job that comes back is a job the engine can read: its canonical form is
//     JSON, and hashing it twice gives one hash.
//   - Every diagnostic is complete and renderable. A crash in the renderer is
//     the worst kind of parser bug, because it only happens on the input the
//     user needs explained.
//
// The seed corpus is testdata, which is where the interesting shapes already
// are: the anchors, the limits, the broken files and the warnings.
func FuzzParseJobSpec(f *testing.F) {
	for _, dir := range []string{"testdata/ok", "testdata/bad", "testdata/warn"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			f.Fatalf("read %s: %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
				continue
			}
			src, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				f.Fatalf("read %s: %v", entry.Name(), err)
			}
			f.Add(src)
		}
	}
	// The shapes a fuzzer takes a long time to discover on its own, and the
	// ones that would hurt most if they got through.
	f.Add([]byte("name: a\nsteps:\n  - name: b\n    run: [\"/bin/true\"]\n"))
	f.Add([]byte("a: &a [*a]\n"))
	f.Add([]byte("<<: *x\n"))
	f.Add([]byte("!!str x\n"))
	f.Add([]byte("---\n---\n"))
	f.Add([]byte("name: a\ntimeout: 0.0000001s\n"))
	f.Add([]byte("\t\n"))
	f.Add([]byte("{"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, src []byte) {
		const path = "fuzz.yaml"

		job, diags := spec.Parse(path, src)

		// A function of its input, not of the run it happens to be in.
		again, againDiags := spec.Parse(path, src)
		if len(diags) != len(againDiags) {
			t.Fatalf("parsing twice gave %d and %d diagnostics", len(diags), len(againDiags))
		}
		for i := range diags {
			if diags[i] != againDiags[i] {
				t.Fatalf("diagnostic %d differs between two parses:\n%+v\n%+v", i, diags[i], againDiags[i])
			}
		}
		if (job == nil) != (again == nil) {
			t.Fatalf("parsing twice produced a job once and not the other time")
		}

		checkDiagnostics(t, diags, path, src)

		if job == nil {
			if !diags.HasErrors() {
				t.Fatalf("no job and no error for %q", src)
			}
			return
		}
		if diags.HasErrors() {
			t.Fatalf("a job came back with errors: %v", codesOf(diags))
		}

		canonical := spec.Canonical(job)
		if !bytes.Equal(canonical, spec.Canonical(again)) {
			t.Fatalf("two parses of one input canonicalised differently:\n%s\n%s", canonical, spec.Canonical(again))
		}
		if !json.Valid(canonical) {
			t.Fatalf("the canonical form is not JSON: %s", canonical)
		}
		if spec.Hash(canonical) != spec.Compile(job).Hash {
			t.Fatalf("Hash and Compile disagree about %s", canonical)
		}
		if job.MaxConcurrent < 1 {
			t.Fatalf("a job came back with max_concurrent %d", job.MaxConcurrent)
		}
		if job.Timeout <= 0 || job.Timeout > spec.MaxJobTimeout {
			t.Fatalf("a job came back with timeout %v", job.Timeout)
		}
		if len(job.Steps) == 0 {
			t.Fatal("a job came back with no steps")
		}
		for _, step := range job.Steps {
			if len(step.Run) == 0 {
				t.Fatalf("step %q came back with no command", step.Name)
			}
		}
	})
}

// checkDiagnostics is the part of the fuzz target that holds for every input,
// job or no job: a diagnostic is complete, points inside the file, and can be
// rendered without the renderer reaching past the end of it.
func checkDiagnostics(t *testing.T, diags diag.List, path string, src []byte) {
	t.Helper()

	lines := len(diag.Lines(src))
	for _, d := range diags {
		switch {
		case d.Code == "":
			t.Fatalf("a diagnostic has no code: %s", d.Message)
		case strings.TrimSpace(d.Message) == "":
			t.Fatalf("%s has no message", d.Code)
		case strings.TrimSpace(d.Hint) == "":
			t.Fatalf("%s has no next step", d.Code)
		case d.File != path:
			t.Fatalf("%s names the file as %q", d.Code, d.File)
		case d.Line < 0 || d.Col < 0:
			t.Fatalf("%s has the position %d:%d", d.Code, d.Line, d.Col)
		case d.Line > lines:
			t.Fatalf("%s points at line %d of a file with %d", d.Code, d.Line, lines)
		}
	}

	if err := diag.ASCII.RenderAll(io.Discard, diags, map[string][]byte{path: src}); err != nil {
		t.Fatalf("rendering: %v", err)
	}
	if err := diag.Unicode.RenderAll(io.Discard, diags, nil); err != nil {
		t.Fatalf("rendering without a source: %v", err)
	}
}
