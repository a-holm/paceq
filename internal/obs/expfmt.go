package obs

import (
	"bytes"
	"math"
	"strconv"
	"strings"
)

// The hand written exposition format (SYNTESE section 3.17). The whole
// Prometheus text format this project needs is here rather than behind
// client_golang: a writer that emits HELP/TYPE pairs, labelled samples and the
// three escapes the text format defines. Everything is written into one buffer
// so a scrape is one allocation story and one deterministic byte sequence,
// which is what the golden test pins.

// ContentType is the media type the text format 0.0.4 answers with.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// L is one label pair on a sample.
type L struct {
	Name  string
	Value string
}

// forbiddenLabels are the identities the cardinality discipline forbids as
// label names (06 section 6.4). They are unbounded: every run, step, trigger
// and dedup key would become its own time series until Prometheus dies under
// them. High cardinality data belongs in SQLite and in the log; the closed
// vocabularies (reason codes, configured job and schedule names) stay legal.
var forbiddenLabels = map[string]bool{
	"run_id":     true,
	"step_id":    true,
	"trigger_id": true,
	"run_key":    true,
	"tick_id":    true,
	"id":         true,
}

// Label constructs one label pair, refusing the forbidden identity names.
// The panic is deliberate: a forbidden label is a programming error that must
// never reach production, not a runtime condition to degrade around.
func Label(name, value string) L {
	if forbiddenLabels[name] {
		panic("obs: forbudt høykardinalitets-label " + strconv.Quote(name) + " (06 §6.4)")
	}
	return L{Name: name, Value: value}
}

// Writer accumulates one exposition document.
type Writer struct {
	b bytes.Buffer
}

// NewWriter returns an empty exposition document.
func NewWriter() *Writer { return &Writer{} }

// Help writes the HELP/TYPE pair that introduces a metric family. The pair is
// written even for families that later gain no samples: an always-present
// TYPE line is what lets a scraper tell an empty family from a missing one.
func (w *Writer) Help(name, help, typ string) {
	w.b.WriteString("# HELP ")
	w.b.WriteString(name)
	w.b.WriteByte(' ')
	w.b.WriteString(escapeHelp(help))
	w.b.WriteByte('\n')
	w.b.WriteString("# TYPE ")
	w.b.WriteString(name)
	w.b.WriteByte(' ')
	w.b.WriteString(typ)
	w.b.WriteByte('\n')
}

// Metric writes one sample line.
func (w *Writer) Metric(name string, labels []L, v float64) {
	w.b.WriteString(name)
	if len(labels) > 0 {
		w.b.WriteByte('{')
		for i, l := range labels {
			if i > 0 {
				w.b.WriteByte(',')
			}
			w.b.WriteString(l.Name)
			w.b.WriteString(`="`)
			w.b.WriteString(escapeLabelValue(l.Value))
			w.b.WriteByte('"')
		}
		w.b.WriteByte('}')
	}
	w.b.WriteByte(' ')
	w.b.WriteString(formatValue(v))
	w.b.WriteByte('\n')
}

// Bytes returns the document so far.
func (w *Writer) Bytes() []byte { return w.b.Bytes() }

// Len reports the number of bytes written so far.
func (w *Writer) Len() int { return w.b.Len() }

// escapeHelp applies the two escapes the format defines for HELP text:
// backslash and newline.
func escapeHelp(s string) string {
	if !strings.ContainsAny(s, "\\\n") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// escapeLabelValue applies the three escapes the format defines for label
// values: backslash, double quote and newline.
func escapeLabelValue(s string) string {
	if !strings.ContainsAny(s, `\"
`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// formatValue renders a sample value the way scrapers round-trip it exactly:
// integral values without a decimal point, everything else as the shortest
// float form, NaN and the infinities spelled as the grammar requires. A bare
// "NaN" or "+Inf" is what the text format's own parser matches, never Go's
// dialect.
func formatValue(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case v == math.Trunc(v) && math.Abs(v) < 1<<53:
		return strconv.FormatInt(int64(v), 10)
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}
