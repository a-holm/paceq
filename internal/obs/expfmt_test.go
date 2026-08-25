package obs

import (
	"math"
	"strings"
	"testing"
)

// TestLabelRefusesIdentityLabels is the forbid test (06 section 6.4): the
// guard lives where labels are constructed, so the panic is the contract.
func TestLabelRefusesIdentityLabels(t *testing.T) {
	for _, name := range []string{"run_id", "step_id", "trigger_id", "run_key", "tick_id", "id"} {
		got := func() (panicked any) {
			defer func() { panicked = recover() }()
			_ = Label(name, "x")
			return nil
		}()
		if got == nil {
			t.Fatalf("Label(%q, ...) did not panic; the cardinality guard is broken", name)
		}
		msg, _ := got.(string)
		if !strings.Contains(msg, name) {
			t.Errorf("panic for %q should name it, got: %v", name, got)
		}
	}
}

// TestLabelAllowsClosedVocabularies keeps the guard honest in the other
// direction: reason codes and configured names are bounded and must stay
// legal.
func TestLabelAllowsClosedVocabularies(t *testing.T) {
	for _, name := range []string{"job", "name", "instigator", "status", "state", "reason_code"} {
		if func() (panicked any) {
			defer func() { panicked = recover() }()
			_ = Label(name, "x")
			return nil
		}() != nil {
			t.Errorf("Label(%q, ...) panicked; closed vocabularies must stay legal", name)
		}
	}
}

// TestEscaping pins the three escapes the text format defines - no more, no
// fewer. A job name is untrusted input that can carry quotes, backslashes
// and newlines straight into the exposition.
func TestEscaping(t *testing.T) {
	w := NewWriter()
	w.Help("pulseq_job_test", `help with \ inside`, "gauge")
	w.Metric("pulseq_job_test", []L{Label("job", `say "hi"\now`)}, 1)

	want := "# HELP pulseq_job_test help with \\\\ inside\n" +
		"# TYPE pulseq_job_test gauge\n" +
		"pulseq_job_test{job=\"say \\\"hi\\\"\\\\now\"} 1\n"
	if got := w.Bytes(); string(got) != want {
		t.Fatalf("escaping mismatch\n got: %q\nwant: %q", string(got), want)
	}
}

// TestFormatValue pins the value grammar scrapers round-trip: integral
// numbers without a decimal point, NaN and infinities spelled the way the
// text parser expects them.
func TestFormatValue(t *testing.T) {
	cases := map[float64]string{
		0: "0", 1: "1", -3: "-3",
		0.5: "0.5", 41.2: "41.2", 1e21: "1e+21",
		math.NaN(): "NaN", math.Inf(1): "+Inf", math.Inf(-1): "-Inf",
	}
	for v, want := range cases {
		if got := formatValue(v); got != want {
			t.Errorf("formatValue(%v) = %q, want %q", v, got, want)
		}
	}
}

// TestEmptyFamilyKeepsItsTypeLine documents why Help is written even when no
// sample follows: an always-present TYPE line lets a scraper tell an empty
// family from a missing one.
func TestEmptyFamilyKeepsItsTypeLine(t *testing.T) {
	w := NewWriter()
	w.Help("pulseq_empty_family", "Nothing happened yet.", "counter")
	want := "# HELP pulseq_empty_family Nothing happened yet.\n" +
		"# TYPE pulseq_empty_family counter\n"
	if got := w.Bytes(); string(got) != want {
		t.Fatalf("empty family mismatch\n got: %q\nwant: %q", string(got), want)
	}
}
