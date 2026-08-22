package diag

import (
	"fmt"
	"sort"
)

// Severity is whether a diagnostic stops the work or only reports on it.
type Severity int

const (
	// SeverityError is a refusal: the file cannot be used as written.
	SeverityError Severity = iota
	// SeverityWarning is a file paceq accepts and would rather not. --strict
	// turns every warning into an error, which is what CI wants.
	SeverityWarning
)

func (s Severity) String() string {
	if s == SeverityWarning {
		return "warning"
	}
	return "error"
}

// MarshalJSON writes the severity as the word it is called, because a script
// filtering on .severity == "error" reads better than one filtering on a number
// whose meaning is in this file.
func (s Severity) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// UnmarshalJSON reads it back, so the JSON output is a format and not only a
// rendering. An unknown word is an error rather than a silent error severity: a
// consumer that guessed wrong would report a warning as a failure.
func (s *Severity) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case `"error"`:
		*s = SeverityError
	case `"warning"`:
		*s = SeverityWarning
	default:
		return fmt.Errorf("%s is not a severity: it is \"error\" or \"warning\"", data)
	}
	return nil
}

// Position is a place in a source file, counted from one. A zero Line means the
// diagnostic is about the file as a whole, and a zero Col means the line is
// known but the column is not.
type Position struct {
	Line int
	Col  int
}

// Diagnostic is one thing wrong with one file. The JSON field names are the
// stable structure `paceq validate -o json` promises, so a script that reads
// .code and .line keeps working.
type Diagnostic struct {
	Code     string   `json:"code"`
	Severity Severity `json:"severity"`
	File     string   `json:"file"`
	Line     int      `json:"line"`
	Col      int      `json:"col"`
	Message  string   `json:"message"`
	// Hint is what to do now. It may be several lines, and it may carry a YAML
	// fragment to copy. It is never empty: a diagnostic without one is a bug,
	// and the renderer has no way to invent one.
	Hint string `json:"hint"`
}

// New builds a diagnostic. Every argument is required, which is the point: the
// type cannot be constructed without a code, a position and a next step.
func New(code string, severity Severity, file string, pos Position, message, hint string) Diagnostic {
	return Diagnostic{
		Code:     code,
		Severity: severity,
		File:     file,
		Line:     pos.Line,
		Col:      pos.Col,
		Message:  message,
		Hint:     hint,
	}
}

// IsError reports whether the diagnostic refuses the file.
func (d Diagnostic) IsError() bool { return d.Severity == SeverityError }

// Pos is the position the diagnostic points at.
func (d Diagnostic) Pos() Position { return Position{Line: d.Line, Col: d.Col} }

// List is a set of diagnostics from one run over one or more files.
type List []Diagnostic

// Errors counts the diagnostics that refuse a file.
func (l List) Errors() int {
	n := 0
	for _, d := range l {
		if d.IsError() {
			n++
		}
	}
	return n
}

// Warnings counts the rest.
func (l List) Warnings() int { return len(l) - l.Errors() }

// HasErrors reports whether anything in the list refuses a file.
func (l List) HasErrors() bool { return l.Errors() > 0 }

// Promote turns every warning into an error, which is what --strict does. It
// returns a new list: the caller may still want the original severities, and a
// method that edited in place would make that impossible to notice.
func (l List) Promote() List {
	promoted := make(List, len(l))
	for i, d := range l {
		d.Severity = SeverityError
		promoted[i] = d
	}
	return promoted
}

// Sort orders diagnostics the way a reader walks a file: by file, then by line,
// then by column, then by code. The order is total, so two runs over the same
// input print the same thing in the same order.
func (l List) Sort() {
	sort.SliceStable(l, func(i, j int) bool {
		a, b := l[i], l[j]
		switch {
		case a.File != b.File:
			return a.File < b.File
		case a.Line != b.Line:
			return a.Line < b.Line
		case a.Col != b.Col:
			return a.Col < b.Col
		default:
			return a.Code < b.Code
		}
	})
}
