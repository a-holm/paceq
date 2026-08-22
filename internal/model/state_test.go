package model_test

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

func TestRunStatesAreTheClosedSet(t *testing.T) {
	want := []model.RunState{
		model.RunQueued, model.RunRunning, model.RunSucceeded, model.RunFailed, model.RunCancelled,
	}
	if got := model.AllRunStates(); !slices.Equal(got, want) {
		t.Errorf("AllRunStates() = %v, want %v", got, want)
	}
	names := []string{"queued", "running", "succeeded", "failed", "cancelled"}
	for i, s := range model.AllRunStates() {
		if s.String() != names[i] {
			t.Errorf("run state %d is named %q, want %q", i, s, names[i])
		}
		if s.Kind() != "run" {
			t.Errorf("run state %q reports kind %q", s, s.Kind())
		}
	}
}

func TestStepStatesAreTheClosedSet(t *testing.T) {
	want := []model.StepState{
		model.StepPending, model.StepRunning, model.StepSucceeded,
		model.StepFailed, model.StepSkipped, model.StepCancelled,
	}
	if got := model.AllStepStates(); !slices.Equal(got, want) {
		t.Errorf("AllStepStates() = %v, want %v", got, want)
	}
	names := []string{"pending", "running", "succeeded", "failed", "skipped", "cancelled"}
	for i, s := range model.AllStepStates() {
		if s.String() != names[i] {
			t.Errorf("step state %d is named %q, want %q", i, s, names[i])
		}
		if s.Kind() != "step" {
			t.Errorf("step state %q reports kind %q", s, s.Kind())
		}
	}
}

// TestTheClosedSetsCannotBeEditedFromOutside is why the sets are functions and
// not variables: a caller that sorts or truncates what it got back must not
// change what the next caller sees. The package holds no mutable global state,
// and this is the part of that rule a test can reach.
func TestTheClosedSetsCannotBeEditedFromOutside(t *testing.T) {
	runs := model.AllRunStates()
	runs[0] = model.RunFailed
	if model.AllRunStates()[0] != model.RunQueued {
		t.Error("editing the slice from AllRunStates() changed the set")
	}

	steps := model.AllStepStates()
	steps[0] = model.StepFailed
	if model.AllStepStates()[0] != model.StepPending {
		t.Error("editing the slice from AllStepStates() changed the set")
	}

	events := model.AllEvents()
	events[0] = model.EvOperatorRetry
	if model.AllEvents()[0] != model.EvClaim {
		t.Error("editing the slice from AllEvents() changed the alphabet")
	}
}

func TestTerminalStatesAreTheFinishedOnes(t *testing.T) {
	runTerminal := map[model.RunState]bool{
		model.RunSucceeded: true, model.RunFailed: true, model.RunCancelled: true,
	}
	for _, s := range model.AllRunStates() {
		if got := s.IsTerminal(); got != runTerminal[s] {
			t.Errorf("run state %q reports IsTerminal() = %v, want %v", s, got, runTerminal[s])
		}
		if got := s.RequiresReasonCode(); got != s.IsTerminal() {
			t.Errorf("run state %q requires a reason code (%v) but is terminal (%v): the two must agree",
				s, got, s.IsTerminal())
		}
	}

	stepTerminal := map[model.StepState]bool{
		model.StepSucceeded: true, model.StepFailed: true,
		model.StepSkipped: true, model.StepCancelled: true,
	}
	for _, s := range model.AllStepStates() {
		if got := s.IsTerminal(); got != stepTerminal[s] {
			t.Errorf("step state %q reports IsTerminal() = %v, want %v", s, got, stepTerminal[s])
		}
		if got := s.RequiresReasonCode(); got != s.IsTerminal() {
			t.Errorf("step state %q requires a reason code (%v) but is terminal (%v): the two must agree",
				s, got, s.IsTerminal())
		}
	}
}

func TestParsingAStoredNameRoundTrips(t *testing.T) {
	for _, want := range model.AllRunStates() {
		got, err := model.ParseRunState(want.String())
		if err != nil || got != want {
			t.Errorf("ParseRunState(%q) = (%q, %v), want (%q, nil)", want, got, err, want)
		}
	}
	for _, want := range model.AllStepStates() {
		got, err := model.ParseStepState(want.String())
		if err != nil || got != want {
			t.Errorf("ParseStepState(%q) = (%q, %v), want (%q, nil)", want, got, err, want)
		}
	}
}

// TestParsingRefusesAnythingOutsideTheSet covers the names that are almost
// right: a state of the other machine, a state that does not exist, and the
// empty string a NULL column would produce.
func TestParsingRefusesAnythingOutsideTheSet(t *testing.T) {
	for _, name := range []string{"", "pending", "skipped", "deferred", "QUEUED", "queued "} {
		if got, err := model.ParseRunState(name); !errors.Is(err, model.ErrUnknownState) {
			t.Errorf("ParseRunState(%q) = (%q, %v), want an unknown state error", name, got, err)
		}
	}
	for _, name := range []string{"", "queued", "deferred", "RUNNING"} {
		if got, err := model.ParseStepState(name); !errors.Is(err, model.ErrUnknownState) {
			t.Errorf("ParseStepState(%q) = (%q, %v), want an unknown state error", name, got, err)
		}
	}

	var detail model.UnknownStateError
	_, err := model.ParseRunState("deferred")
	if !errors.As(err, &detail) {
		t.Fatalf("ParseRunState refused with %v, which carries no detail", err)
	}
	if detail.Kind != "run" || detail.Name != "deferred" {
		t.Errorf("the refusal names (%q, %q), want (run, deferred)", detail.Kind, detail.Name)
	}
}

// TestIsDeferredIsComputedAndNotStored is the whole of the "deferred" state:
// a queued run that is not allowed to start yet. Everything else, including a
// running run with an available_at in the future, is not deferred.
func TestIsDeferredIsComputedAndNotStored(t *testing.T) {
	cases := []struct {
		name        string
		state       model.RunState
		availableAt int64
		want        bool
	}{
		{"a queued run held until later is deferred", model.RunQueued, future, true},
		{"a queued run available now is not", model.RunQueued, now, false},
		{"a queued run available already is not", model.RunQueued, past, false},
		{"a running run is not deferred whatever its available_at says", model.RunRunning, future, false},
		{"a succeeded run is not deferred", model.RunSucceeded, future, false},
		{"a failed run is not deferred", model.RunFailed, future, false},
		{"a cancelled run is not deferred", model.RunCancelled, future, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := model.IsDeferred(tc.state, tc.availableAt, now); got != tc.want {
				t.Errorf("IsDeferred(%q, %d, %d) = %v, want %v", tc.state, tc.availableAt, now, got, tc.want)
			}
		})
	}
}

// deferredWords are the ways a sixth run state would name itself.
var deferredWords = []string{"defer", "postpone", "snooze", "utsatt", "held", "waiting"}

// deferredWord returns the word a name leans on to describe a deferred state,
// or "" when the name is clean. It is pure, so the fixture test below proves
// the rule fires without needing a banned state to exist.
func deferredWord(text string) string {
	lowered := strings.ToLower(text)
	for _, word := range deferredWords {
		if strings.Contains(lowered, word) {
			return word
		}
	}
	return ""
}

// TestDeferredIsNotAState is the acceptance criterion that the simplification
// stayed simple. Two checks, because either one alone has a hole: no value in
// either closed set names a deferred state, and no constant of a state type
// anywhere in the package declares one, whether or not the sets know about it.
//
// The check deliberately reads the state types rather than grepping the package
// for the word. EvDeferred and the defer reason effect carry it legitimately:
// deferral is a thing that happens to a run, it is just not a state it is in.
func TestDeferredIsNotAState(t *testing.T) {
	for _, s := range model.AllRunStates() {
		if word := deferredWord(s.String()); word != "" {
			t.Errorf("run state %q leans on %q: deferred is a queued run with an available_at, not a state", s, word)
		}
	}
	for _, s := range model.AllStepStates() {
		if word := deferredWord(s.String()); word != "" {
			t.Errorf("step state %q leans on %q: deferred is a queued run with an available_at, not a state", s, word)
		}
	}

	fset := token.NewFileSet()
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}

	scanned, constants := 0, 0
	for _, entry := range files {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		scanned++

		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || !isStateType(value.Type) {
					continue
				}
				for i, ident := range value.Names {
					constants++
					if word := deferredWord(ident.Name); word != "" {
						t.Errorf("%s declares the state constant %s, which leans on %q", name, ident.Name, word)
					}
					if i < len(value.Values) {
						if word := deferredWord(literalOf(value.Values[i])); word != "" {
							t.Errorf("%s gives the state constant %s a value that leans on %q", name, ident.Name, word)
						}
					}
				}
			}
		}
	}
	if scanned == 0 {
		t.Fatal("no package file was parsed, so the check proved nothing")
	}
	// The five run states and the six step states. Fewer means the scan
	// stopped reading the file that declares them.
	if want := 11; constants != want {
		t.Errorf("scanned %d state constants, want %d", constants, want)
	}
}

// isStateType reports whether a declared constant type is one of the two state
// types. A constant with no declared type is untyped and is not a state.
func isStateType(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && (ident.Name == "RunState" || ident.Name == "StepState")
}

func literalOf(expr ast.Expr) string {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	text, err := strconv.Unquote(lit.Value)
	if err != nil {
		return ""
	}
	return text
}

// TestTheDeferredCheckCatchesASixthState runs the rule over the names a sixth
// state would use. Without it a check that matched nothing would report a clean
// package.
func TestTheDeferredCheckCatchesASixthState(t *testing.T) {
	for _, name := range []string{"deferred", "RunDeferred", "postponed", "snoozed", "utsatt", "waiting"} {
		if deferredWord(name) == "" {
			t.Errorf("the check accepted %q as a state name", name)
		}
	}
	for _, name := range []string{"queued", "RunQueued", "running", "skipped", "cancelled", "pending"} {
		if word := deferredWord(name); word != "" {
			t.Errorf("the check rejected %q for %q, and that is a state the model has", name, word)
		}
	}
}
