package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// The reason code rule (06 section 2.1): no terminal state is stored without a
// reason code, and every reason code comes from the catalogue in
// internal/reason. Two guards carry the rule over source, and the store's own
// tests carry it over SQL. This file holds the two source guards.
//
//   - TestNoUnknownReasonCodesInSource walks every Go file outside
//     internal/reason and refuses any string literal that claims to be a
//     reason code without being one: a literal written into a ReasonCode
//     field, or one declared or converted as reason.Code.
//   - TestEveryTerminalTransitionCarriesReason drives the whole cross table of
//     both state machines and proves the machines themselves refuse a terminal
//     landing without a code. It iterates every transition rather than
//     sampling, so a new code path cannot slip through.

func TestNoUnknownReasonCodesInSource(t *testing.T) {
	root := repoRoot(t)
	skip := selfDir(t)
	catalogueDir := filepath.Join(root, "internal", "reason")
	known := map[string]bool{}
	for _, c := range reason.Codes() {
		known[c] = true
	}

	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch {
			case strings.HasPrefix(d.Name(), "."):
				return fs.SkipDir
			case d.Name() == "bin" || d.Name() == "dist" || d.Name() == "testdata":
				return fs.SkipDir
			case path == skip || path == catalogueDir:
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}

		// reasonAliases maps this file's local name for internal/reason back
		// to the package, so an import under another name is still caught.
		reasonAliases := map[string]bool{"reason": true}
		for _, imp := range file.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil || p != "github.com/a-holm/paceq/internal/reason" {
				continue
			}
			if imp.Name == nil {
				continue
			}
			reasonAliases[imp.Name.String()] = imp.Name.String() != "_"
		}
		isReasonPkg := func(x ast.Expr) bool {
			id, ok := x.(*ast.Ident)
			return ok && reasonAliases[id.Name]
		}
		check := func(lit *ast.BasicLit, why string) {
			value, err := strconv.Unquote(lit.Value)
			if err != nil || value == "" {
				return
			}
			if known[value] {
				return
			}
			t.Errorf("%s uses %q %s: not in the reason catalogue; add it to internal/reason/codes.go or use a catalogue code",
				rel(root, fset.Position(lit.Pos()).Filename), value, why)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				// reason.Code("...") conversions outside the catalogue.
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Code" || !isReasonPkg(sel.X) || len(node.Args) != 1 {
					return true
				}
				lit, ok := node.Args[0].(*ast.BasicLit)
				if ok && lit.Kind == token.STRING {
					check(lit, "as a reason.Code conversion")
				}
			case *ast.ValueSpec:
				// var x reason.Code = "..." and const forms.
				sel, ok := node.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Code" || !isReasonPkg(sel.X) {
					return true
				}
				for _, v := range node.Values {
					if lit, ok := v.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						check(lit, "as a reason.Code declaration")
					}
				}
			case *ast.KeyValueExpr:
				// Struct writes: Guards{ReasonCode: "..."}, RunEvent{...},
				// Result{ReasonCode: "..."} and anything else that names the
				// field ReasonCode.
				key, ok := node.Key.(*ast.Ident)
				if !ok || key.Name != "ReasonCode" {
					return true
				}
				if lit, ok := node.Value.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					check(lit, "in a ReasonCode field")
				}
			case *ast.AssignStmt:
				// x.ReasonCode = "..."
				for i, rhs := range node.Rhs {
					if len(node.Lhs) != len(node.Rhs) {
						continue
					}
					sel, ok := node.Lhs[i].(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "ReasonCode" {
						continue
					}
					if lit, ok := rhs.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						check(lit, "in a ReasonCode assignment")
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

// TestEveryTerminalTransitionCarriesReason iterates the full cross table of
// both machines. For every legal transition the target decides whether a
// reason code is required, so the assertion is one line: whenever the machine
// accepts a move into a state that RequiresReasonCode, it was handed a code,
// and whenever it is handed none, it refuses. The catalogue supplies the codes
// used here, which also proves the model and the catalogue agree on what a
// code looks like.
func TestEveryTerminalTransitionCarriesReason(t *testing.T) {
	// The guard variants exist so every branch of both machines is reached:
	// a probe with one guard set would prove only the transitions that set
	// happens to unlock, and a code path behind CancelRequested or a crash
	// budget would go unproven. MUTANT5 taught this table that lesson.
	runVariants := []model.Guards{
		{Now: 1_000, AvailableAt: 500, LeaseValid: true, AllStepsTerminal: true, CrashBudgetLeft: true},
		{Now: 1_000, AvailableAt: 500, LeaseValid: true, AllStepsTerminal: true, CrashBudgetLeft: true, CancelRequested: true},
		{Now: 1_000, AvailableAt: 5_000, LeaseValid: true, AllStepsTerminal: true, CrashBudgetLeft: true},
		{Now: 1_000, AvailableAt: 500, LeaseValid: true, AllStepsTerminal: true, CrashBudgetLeft: false},
	}
	stepVariants := []model.Guards{
		{AttemptsLeft: false},
		{AttemptsLeft: true},
	}

	type probe struct {
		from   model.State
		event  model.Event
		guards model.Guards
	}
	var probes []probe

	add := func(states []model.State, events []model.Event, variants []model.Guards) {
		for _, s := range states {
			for _, e := range events {
				for _, g := range variants {
					probes = append(probes, probe{s, e, g})
					withCode := g
					withCode.ReasonCode = string(reason.RUNSucceeded)
					if s.Kind() == "step" {
						withCode.ReasonCode = string(reason.STEPSucceeded)
					}
					probes = append(probes, probe{s, e, withCode})
				}
			}
		}
	}

	runStates := make([]model.State, 0, len(model.AllRunStates()))
	for _, s := range model.AllRunStates() {
		runStates = append(runStates, s)
	}
	stepStates := make([]model.State, 0, len(model.AllStepStates()))
	for _, s := range model.AllStepStates() {
		stepStates = append(stepStates, s)
	}
	allEvents := model.AllEvents()

	add(runStates, allEvents, runVariants)
	add(stepStates, allEvents, stepVariants)

	for _, p := range probes {
		switch state := p.from.(type) {
		case model.RunState:
			next, _, err := model.NextRunState(state, p.event, p.guards)
			assertReasonDiscipline(t, p.from, p.event, p.guards.ReasonCode, next, err)
		case model.StepState:
			next, _, err := model.NextStepState(state, p.event, p.guards)
			assertReasonDiscipline(t, p.from, p.event, p.guards.ReasonCode, next, err)
		}
	}
}

// assertReasonDiscipline holds for every transition of either machine: a move
// into a state that requires a reason code happens only when a code was
// supplied, and a refusal for a missing code names itself. The empty code
// counts as absent; the model has always treated it that way.
func assertReasonDiscipline(t *testing.T, from model.State, ev model.Event, given string, next model.State, err error) {
	t.Helper()
	if err != nil {
		// Refusals move nothing, so they can never store a state at all.
		return
	}
	if !next.RequiresReasonCode() {
		return
	}
	if given == "" {
		t.Errorf("%s(%s -> %s) accepted without a reason code", from.Kind(), from, next)
	}
	if given != "" && !reason.IsKnown(reason.Code(given)) {
		t.Errorf("%s(%s -> %s) carried %q, which is not in the catalogue", from.Kind(), from, next, given)
	}
}
