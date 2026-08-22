package diag_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/diag"
)

func errorAt(line, col int, message string) diag.Diagnostic {
	return diag.New("PQ1040", diag.SeverityError, "jobs/a.yaml", diag.Position{Line: line, Col: col}, message, "Do the other thing.")
}

// TestRenderDrawsTheAnatomyFrom03Section81: a mark, the position, the code, the
// message, the source around it with a caret, the next step, and where to read
// more. Every part is load bearing, and the golden files in internal/spec pin
// the exact shape.
func TestRenderDrawsTheAnatomyFrom03Section81(t *testing.T) {
	src := []byte("name: report\nsteps:\n  - name: only\n    retries: 3\n    run: [\"/bin/true\"]\n")

	var b bytes.Buffer
	if err := diag.ASCII.Render(&b, errorAt(4, 5, `"retries" is not a field a step has`), src); err != nil {
		t.Fatalf("render: %v", err)
	}

	want := "FAIL jobs/a.yaml:4:5  PQ1040: \"retries\" is not a field a step has\n" +
		"\n" +
		"   2 | steps:\n" +
		"   3 |   - name: only\n" +
		">  4 |     retries: 3\n" +
		"           ^\n" +
		"   5 |     run: [\"/bin/true\"]\n" +
		"\n" +
		"   Do the other thing.\n" +
		"\n" +
		"   paceq error PQ1040  for the full explanation\n"
	if got := b.String(); got != want {
		t.Errorf("render produced\n%s\nwant\n%s", got, want)
	}
}

// TestTheCaretLandsUnderTheColumn checks the arithmetic on its own, at the
// start of a line, in the middle and one past the end.
func TestTheCaretLandsUnderTheColumn(t *testing.T) {
	src := []byte("abcdefgh\n")

	for col := 1; col <= 9; col++ {
		var b bytes.Buffer
		if err := diag.ASCII.Render(&b, errorAt(1, col, "x"), src); err != nil {
			t.Fatalf("render: %v", err)
		}
		lines := strings.Split(b.String(), "\n")
		if len(lines) < 3 {
			t.Fatalf("render produced %d lines", len(lines))
		}
		source, caret := lines[2], lines[3]
		at := strings.Index(caret, "^")
		if at < 0 {
			t.Fatalf("column %d produced no caret:\n%s", col, b.String())
		}
		if want := strings.Index(source, "abcdefgh") + col - 1; at != want {
			t.Errorf("column %d put the caret at %d, want %d:\n%s", col, at, want, b.String())
		}
	}
}

// TestATabInTheSourceKeepsTheCaretAligned. Copying the line's own whitespace is
// what makes this work whatever a terminal decides a tab stop is.
func TestATabInTheSourceKeepsTheCaretAligned(t *testing.T) {
	var b bytes.Buffer

	if err := diag.ASCII.Render(&b, errorAt(1, 3, "x"), []byte("\t\tvalue\n")); err != nil {
		t.Fatalf("render: %v", err)
	}

	caret := strings.Split(b.String(), "\n")[3]
	if !strings.HasSuffix(caret, "\t\t^") {
		t.Errorf("the caret line is %q, want it to copy the two tabs", caret)
	}
}

// TestAPositionOutsideTheFileStillRenders. A parser that reports a line past the
// end of the file is a bug, and a renderer that panics on one is a worse bug:
// it only fires on the input the user needs explained.
func TestAPositionOutsideTheFileStillRenders(t *testing.T) {
	cases := map[string]struct {
		diagnostic diag.Diagnostic
		src        []byte
	}{
		"a line past the end":     {errorAt(99, 1, "x"), []byte("one line\n")},
		"a column past the end":   {errorAt(1, 400, "x"), []byte("one line\n")},
		"no source at all":        {errorAt(1, 1, "x"), nil},
		"no position":             {errorAt(0, 0, "x"), []byte("one line\n")},
		"a line but no column":    {errorAt(1, 0, "x"), []byte("one line\n")},
		"an empty file":           {errorAt(1, 1, "x"), []byte("")},
		"a file of one newline":   {errorAt(1, 1, "x"), []byte("\n")},
		"carriage returns inside": {errorAt(2, 1, "x"), []byte("one\r\ntwo\r\n")},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var b bytes.Buffer
			if err := diag.Unicode.Render(&b, tc.diagnostic, tc.src); err != nil {
				t.Fatalf("render: %v", err)
			}
			if !strings.Contains(b.String(), "PQ1040") {
				t.Errorf("the header is missing:\n%s", b.String())
			}
			if !strings.Contains(b.String(), "Do the other thing.") {
				t.Errorf("the next step is missing:\n%s", b.String())
			}
		})
	}
}

// TestLinesCountsEveryTerminator. A file written on another operating system
// still has to line up with the positions a parser reports.
func TestLinesCountsEveryTerminator(t *testing.T) {
	cases := map[string][]string{
		"a\nb\n":     {"a", "b"},
		"a\r\nb\r\n": {"a", "b"},
		"a\rb\r":     {"a", "b"},
		"a\nb":       {"a", "b"},
		"":           {""},
		"\n":         {""},
	}
	for src, want := range cases {
		t.Run(src, func(t *testing.T) {
			got := diag.Lines([]byte(src))
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Errorf("Lines(%q) = %q, want %q", src, got, want)
			}
		})
	}
}

// TestColourIsOffUnlessAskedFor keeps a golden file comparable and a piped
// report free of escape sequences.
func TestColourIsOffUnlessAskedFor(t *testing.T) {
	src := []byte("name: report\n")

	var plain, coloured bytes.Buffer
	if err := diag.ASCII.Render(&plain, errorAt(1, 1, "x"), src); err != nil {
		t.Fatalf("render: %v", err)
	}
	style := diag.ASCII
	style.Color = true
	if err := style.Render(&coloured, errorAt(1, 1, "x"), src); err != nil {
		t.Fatalf("render: %v", err)
	}

	if strings.Contains(plain.String(), "\x1b[") {
		t.Errorf("the default style wrote escape sequences:\n%q", plain.String())
	}
	if !strings.Contains(coloured.String(), "\x1b[") {
		t.Errorf("the coloured style wrote none:\n%q", coloured.String())
	}
}

// TestSortIsTotal. Two runs over one input report in one order, whatever order
// the checks happened to fire in.
func TestSortIsTotal(t *testing.T) {
	list := diag.List{
		diag.New("PQ2001", diag.SeverityError, "b.yaml", diag.Position{Line: 1, Col: 1}, "m", "h"),
		diag.New("PQ1040", diag.SeverityError, "a.yaml", diag.Position{Line: 9, Col: 1}, "m", "h"),
		diag.New("PQ1041", diag.SeverityError, "a.yaml", diag.Position{Line: 2, Col: 7}, "m", "h"),
		diag.New("PQ1002", diag.SeverityError, "a.yaml", diag.Position{Line: 2, Col: 3}, "m", "h"),
		diag.New("W1001", diag.SeverityWarning, "a.yaml", diag.Position{Line: 2, Col: 3}, "m", "h"),
	}

	list.Sort()

	want := []string{"PQ1002", "W1001", "PQ1041", "PQ1040", "PQ2001"}
	for i, code := range want {
		if list[i].Code != code {
			t.Fatalf("sorted order is %v, want %v", codes(list), want)
		}
	}
}

// TestPromoteLeavesTheOriginalAlone. --strict changes what fails, not what the
// parser found.
func TestPromoteLeavesTheOriginalAlone(t *testing.T) {
	list := diag.List{
		diag.New("W1001", diag.SeverityWarning, "a.yaml", diag.Position{Line: 1, Col: 1}, "m", "h"),
	}

	strict := list.Promote()

	if list.HasErrors() {
		t.Error("Promote changed the list it was called on")
	}
	if !strict.HasErrors() {
		t.Error("Promote left a warning as a warning")
	}
	if list.Warnings() != 1 || strict.Warnings() != 0 {
		t.Errorf("counts are %d and %d, want 1 and 0", list.Warnings(), strict.Warnings())
	}
}

// TestSeverityRoundTripsThroughJSON keeps -o json a format rather than only a
// rendering.
func TestSeverityRoundTripsThroughJSON(t *testing.T) {
	for _, severity := range []diag.Severity{diag.SeverityError, diag.SeverityWarning} {
		encoded, err := json.Marshal(diag.Diagnostic{Code: "PQ1040", Severity: severity})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if !strings.Contains(string(encoded), `"severity":"`+severity.String()+`"`) {
			t.Errorf("%v encoded as %s", severity, encoded)
		}

		var back diag.Diagnostic
		if err := json.Unmarshal(encoded, &back); err != nil {
			t.Fatalf("unmarshal %s: %v", encoded, err)
		}
		if back.Severity != severity {
			t.Errorf("%v came back as %v", severity, back.Severity)
		}
	}

	var back diag.Diagnostic
	if err := json.Unmarshal([]byte(`{"severity":"critical"}`), &back); err == nil {
		t.Error("an unknown severity was accepted")
	}
}

func codes(list diag.List) []string {
	out := make([]string, 0, len(list))
	for _, d := range list {
		out = append(out, d.Code)
	}
	return out
}
