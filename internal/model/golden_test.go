package model_test

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

// updateGolden rewrites the transition table instead of comparing against it.
var updateGolden = flag.Bool("update", false, "rewrite transitions.golden.md from the machines")

// goldenPath is the drawn form of both machines. It is generated from the code,
// never written by hand, which is what keeps the documentation and the rules
// from drifting apart. M8-05 renders the reference page from it.
const goldenPath = "transitions.golden.md"

// TestGoldenTransitions fails on any change to what the machines allow. A rule
// change shows up here as a diff, which is the review artefact: a reader sees
// the rule that moved without replaying the switch statements.
func TestGoldenTransitions(t *testing.T) {
	got := renderTransitions()

	if *updateGolden {
		if err := os.WriteFile(goldenPath, []byte(got), 0o600); err != nil {
			t.Fatalf("write %s: %v", goldenPath, err)
		}
		return
	}

	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read %s: %v\nregenerate it with: go test ./internal/model -run TestGoldenTransitions -update", goldenPath, err)
	}
	if want := string(raw); want != got {
		t.Fatalf("the machines differ from %s.\nIs the change intended? Regenerate with: "+
			"go test ./internal/model -run TestGoldenTransitions -update\n\n%s",
			goldenPath, diffLines(want, got))
	}
}

// renderTransitions draws both machines. The cross tables are probed from the
// code over every guard combination; the outcome lists come from the checked in
// tables, which the completeness sweep pins to the code in both directions.
func renderTransitions() string {
	var b strings.Builder

	b.WriteString("# The run and step state machines\n\n")
	b.WriteString("Generated from internal/model. Regenerate with:\n\n")
	b.WriteString("    go test ./internal/model -run TestGoldenTransitions -update\n\n")
	b.WriteString("A cross table cell holds the states an event can lead to, over every combination of the guards. ")
	b.WriteString("A dash is a pair the machine refuses as an illegal transition. ")
	b.WriteString("A pair that leads back to the state it started in is a transition all the same: it writes something.\n")

	for _, m := range machines() {
		fmt.Fprintf(&b, "\n## The %s machine\n\n", m.kind)
		writeCrossTable(&b, m)
		b.WriteString("\n### Transitions\n\n")
		writeOutcomes(&b, m, true)
		b.WriteString("\n### Refusals\n\n")
		writeOutcomes(&b, m, false)
	}
	return b.String()
}

func writeCrossTable(b *strings.Builder, m machine) {
	events := model.AllEvents()

	header := make([]string, 0, len(events)+1)
	header = append(header, "state")
	for _, ev := range events {
		header = append(header, ev.String())
	}
	writeRow(b, header)
	writeRow(b, slices.Repeat([]string{"---"}, len(header)))

	combos := guardCombos()
	for _, from := range m.states {
		row := []string{from.String()}
		for _, ev := range events {
			row = append(row, strings.Join(reachedFrom(m, from, ev, combos), ", "))
		}
		writeRow(b, row)
	}
}

// reachedFrom is every state one pair can lead to, in the order the closed set
// lists them. It is the probe that makes the table generated rather than
// copied: nothing here reads the switch statements, it only calls them.
func reachedFrom(m machine, from model.State, ev model.Event, combos []model.Guards) []string {
	seen := map[string]bool{}
	for _, g := range combos {
		got, _, err := m.next(from, ev, g)
		if err == nil {
			seen[got.String()] = true
		}
	}
	if len(seen) == 0 {
		return []string{"-"}
	}
	var out []string
	for _, s := range m.states {
		if seen[s.String()] {
			out = append(out, s.String())
		}
	}
	return out
}

// writeOutcomes prints the rows of a checked in table: the allowed transitions
// with the effects they demand, or the refusals with the error they carry.
func writeOutcomes(b *strings.Builder, m machine, allowed bool) {
	if allowed {
		writeRow(b, []string{"from", "event", "to", "case", "effects"})
		writeRow(b, slices.Repeat([]string{"---"}, 5))
	} else {
		writeRow(b, []string{"from", "event", "case", "error"})
		writeRow(b, slices.Repeat([]string{"---"}, 4))
	}

	for _, row := range tableRows(m.kind) {
		if (row.err == nil) != allowed {
			continue
		}
		if allowed {
			writeRow(b, []string{row.from, row.event, row.to, row.name, renderEffects(row.effects)})
			continue
		}
		writeRow(b, []string{row.from, row.event, row.name, row.err.Error()})
	}
}

// tableRow is one row of either checked in table, flattened so the renderer
// does not care which machine it came from.
type tableRow struct {
	name    string
	from    string
	event   string
	to      string
	effects []model.Effect
	err     error
}

func tableRows(kind string) []tableRow {
	var out []tableRow
	if kind == "run" {
		for _, tc := range runTable() {
			out = append(out, tableRow{
				name: tc.name, from: tc.from.String(), event: tc.event.String(),
				to: tc.want.String(), effects: tc.effects, err: tc.err,
			})
		}
		return out
	}
	for _, tc := range stepTable() {
		out = append(out, tableRow{
			name: tc.name, from: tc.from.String(), event: tc.event.String(),
			to: tc.want.String(), effects: tc.effects, err: tc.err,
		})
	}
	return out
}

func renderEffects(list []model.Effect) string {
	out := make([]string, 0, len(list))
	for _, e := range list {
		if e.Arg == "" {
			out = append(out, string(e.Kind))
			continue
		}
		out = append(out, fmt.Sprintf("%s(%s)", e.Kind, e.Arg))
	}
	return strings.Join(out, ", ")
}

func writeRow(b *strings.Builder, cells []string) {
	b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
}

// diffLines is the first line the two texts disagree on, with a little context.
// The whole file is too long to read in a test failure, and the first
// difference is what a person needs to see.
func diffLines(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")

	for i := range max(len(wantLines), len(gotLines)) {
		w, g := lineAt(wantLines, i), lineAt(gotLines, i)
		if w == g {
			continue
		}
		return fmt.Sprintf("first difference at line %d:\n  golden: %s\n  now:    %s", i+1, w, g)
	}
	return "the files differ in trailing content only"
}

func lineAt(lines []string, i int) string {
	if i >= len(lines) {
		return "(no line)"
	}
	return lines[i]
}
