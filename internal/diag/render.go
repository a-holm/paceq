package diag

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Style draws a diagnostic. Two mark sets exist for the same reason the doctor
// report has two: a terminal that cannot render UTF-8 shows a box, which reads
// as damage rather than as a refusal.
type Style struct {
	ErrorMark   string
	WarningMark string
	Color       bool
}

// ASCII and Unicode are the two mark sets. Neither carries colour; the caller
// turns it on, because whether colour is wanted is a decision about the
// terminal and not about the diagnostic.
var (
	ASCII   = Style{ErrorMark: "FAIL", WarningMark: "WARN"}
	Unicode = Style{ErrorMark: "✗", WarningMark: "⚠"}
)

// ANSI colours, written directly. Three sequences is the whole need here.
const (
	colorReset  = "\x1b[0m"
	colorRed    = "\x1b[31m"
	colorYellow = "\x1b[33m"
	colorDim    = "\x1b[2m"
)

// linesBefore and linesAfter are how much of the file is shown around the
// offending line. Two before is enough to see which block the line sits in, one
// after is enough to see it is not the end of the file. Both are frozen: the
// golden tests compare whole messages, and a message whose shape depends on the
// terminal cannot be compared at all.
const (
	linesBefore = 2
	linesAfter  = 1
)

// Render writes one diagnostic. src is the file the diagnostic came from, used
// for the excerpt. A nil src, or a position outside it, prints the header and
// the next step without an excerpt rather than failing.
func (s Style) Render(w io.Writer, d Diagnostic, src []byte) error {
	var b strings.Builder

	b.WriteString(s.mark(d.Severity))
	b.WriteByte(' ')
	b.WriteString(s.location(d))
	b.WriteString("  ")
	b.WriteString(d.Code)
	b.WriteString(": ")
	b.WriteString(d.Message)
	b.WriteByte('\n')

	if excerpt := s.excerpt(d, src); excerpt != "" {
		b.WriteByte('\n')
		b.WriteString(excerpt)
	}
	if d.Hint != "" {
		b.WriteByte('\n')
		b.WriteString(indent(d.Hint, "   "))
	}
	b.WriteByte('\n')
	b.WriteString(indent("paceq error "+d.Code+"  for the full explanation", "   "))

	_, err := io.WriteString(w, b.String())
	return err
}

// RenderAll writes every diagnostic in the list, separated by a blank line.
// sources maps a file name to its contents; a file missing from it renders
// without an excerpt.
func (s Style) RenderAll(w io.Writer, list List, sources map[string][]byte) error {
	for i, d := range list {
		if i > 0 {
			if _, err := io.WriteString(w, "\n"); err != nil {
				return err
			}
		}
		if err := s.Render(w, d, sources[d.File]); err != nil {
			return err
		}
	}
	return nil
}

func (s Style) mark(severity Severity) string {
	mark, colour := s.ErrorMark, colorRed
	if severity == SeverityWarning {
		mark, colour = s.WarningMark, colorYellow
	}
	if s.Color {
		return colour + mark + colorReset
	}
	return mark
}

// location is file:line:col, dropping the parts that are not known. A file with
// no position at all still names the file, because "PQ1001" on its own does not
// tell an operator which of forty job files to open.
func (s Style) location(d Diagnostic) string {
	switch {
	case d.Line <= 0:
		return d.File
	case d.Col <= 0:
		return d.File + ":" + strconv.Itoa(d.Line)
	default:
		return d.File + ":" + strconv.Itoa(d.Line) + ":" + strconv.Itoa(d.Col)
	}
}

// excerpt is the source around the offending line, with a caret under the
// column. It ends with a newline when it is not empty.
func (s Style) excerpt(d Diagnostic, src []byte) string {
	if len(src) == 0 || d.Line <= 0 {
		return ""
	}
	lines := Lines(src)
	if d.Line > len(lines) {
		return ""
	}

	first := max(d.Line-linesBefore, 1)
	last := min(d.Line+linesAfter, len(lines))
	// The gutter is sized on the widest number shown, so the pipes line up and
	// the caret offset is the same arithmetic on every line.
	width := len(strconv.Itoa(last))

	var b strings.Builder
	for n := first; n <= last; n++ {
		text := lines[n-1]
		gutter := fmt.Sprintf("%s%*d | ", markerFor(n == d.Line), width, n)
		if s.Color && n == d.Line {
			b.WriteString(colorDim + gutter + colorReset + text + "\n")
		} else {
			b.WriteString(gutter + text + "\n")
		}
		if n == d.Line && d.Col > 0 {
			b.WriteString(caret(len(gutter), text, d.Col))
		}
	}
	return b.String()
}

// Normalize rewrites every line terminator as a newline.
//
// It exists so that the thing that reports a position and the thing that draws
// the excerpt for it count lines the same way. A parser that treats a carriage
// return as a line break and a renderer that does not will disagree about which
// line is line four, and the caret will land in the wrong place on every file
// an editor on another operating system wrote.
//
// The rule is: a job file is read as if it had newlines, whatever it has.
func Normalize(src []byte) []byte {
	if !bytes.ContainsRune(src, '\r') {
		return src
	}
	normalized := bytes.ReplaceAll(src, []byte("\r\n"), []byte("\n"))
	return bytes.ReplaceAll(normalized, []byte("\r"), []byte("\n"))
}

// Lines is a source file split into the lines a position counts against. A file
// that ends in a terminator does not gain an empty last line from it.
func Lines(src []byte) []string {
	lines := strings.Split(string(Normalize(src)), "\n")
	if n := len(lines); n > 1 && lines[n-1] == "" {
		lines = lines[:n-1]
	}
	return lines
}

// markerFor is the three columns in front of the line number. Both marks are
// the same width, so the pipes and the caret line up whichever line is marked.
func markerFor(marked bool) string {
	if marked {
		return ">  "
	}
	return "   "
}

// caret is the line that points at a column. The indentation copies the source
// line character by character, keeping tabs as tabs, so the caret lands under
// the right character whatever a terminal decides a tab stop is.
func caret(gutter int, text string, col int) string {
	var b strings.Builder
	b.WriteString(strings.Repeat(" ", gutter))
	runes := []rune(text)
	for i := 0; i < col-1; i++ {
		if i < len(runes) && runes[i] == '\t' {
			b.WriteByte('\t')
			continue
		}
		b.WriteByte(' ')
	}
	b.WriteString("^\n")
	return b.String()
}

// indent puts every line of a block behind the same prefix and leaves blank
// lines blank, so a copied YAML fragment keeps its own indentation and no line
// ends in trailing spaces.
func indent(text, prefix string) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n") + "\n"
}
