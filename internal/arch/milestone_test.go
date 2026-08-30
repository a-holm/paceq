package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Issue #252 turns a written rule into a mechanical one: text an operator
// reads names what paceq does, never when it was scheduled to do it.
//
// A milestone identifier in operator-visible prose is a claim with an expiry
// date, and nothing else in the build notices when it expires. The message
// that prompted this rule told the reader "needs parses in M1 and is enforced
// in M4-01 ... until then the steps run in the order they are written" while
// the claim predicate in internal/store/claim.go was already gating every step
// on the frozen edges, so the one person who typed a needs clause and checked
// their work was told to hand-order the steps the graph exists to order for
// them. A milestone that has passed reads exactly like one that has not, which
// is why these are found by a reader who came for something else rather than
// by the suite.
//
// The identifiers also mean nothing to the reader they reach: an operator
// holding a job file has no milestone plan, so the marker costs them a lookup
// and pays nothing back. Naming the behaviour serves both ends, and a version
// number serves the case where the behaviour really is still absent.
var milestonePattern = regexp.MustCompile(`\bM[0-9]+(-[0-9]+)?\b`)

// operatorText is where the rule binds: the surfaces whose whole job is to
// explain to an operator what paceq requires or what it did. internal/spec is
// the validator, so every message and hint it carries is read by someone whose
// file was just refused. internal/reason is the reason catalogue, rendered by
// paceq explain and into docs/reference/reason-codes.md. errorcmd.go is the
// paceq error catalogue, which is where a diagnostic sends the reader for the
// long form.
//
// The boundary stops here on purpose, and the rest of internal/cli is outside
// it. Cobra help strings are operator-visible too and the same rule should
// reach them. They render into docs/reference/cli.md through a staleness gate,
// so widening the prefix to "internal/cli" is a one-word change that has to
// carry a make docs regeneration and the help edits with it.
//
// Packages outside this list carry milestone identifiers that this rule has no
// quarrel with: the fault-injection point names ("M4:claim:after_update") are
// internal identifiers a developer selects by, not prose anyone is shown, and
// the plan references in docs/ are where a schedule belongs.
var operatorText = []string{
	"internal/spec",
	"internal/reason",
	"internal/cli/errorcmd.go",
}

// exemptField is the one field inside those packages that the rule does not
// reach. reason.Entry.ExemptReason argues to a reviewer why a reserved code
// has no explain scenario yet; internal/reason/reason.go documents it as
// checklist metadata, and only internal/explain and internal/reason tests read
// it. It is a note between developers about work that has not happened, which
// is the one place naming the milestone is the honest thing to do.
const exemptField = "ExemptReason"

func TestOperatorTextNamesBehaviourNotAMilestone(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		relPath := filepath.ToSlash(rel(root, path))
		if strings.HasSuffix(relPath, "_test.go") || strings.Contains(relPath, "testdata") {
			return nil
		}
		if !inOperatorText(relPath) {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if kv, ok := n.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == exemptField {
					return false
				}
			}
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(lit.Value)
			if err != nil {
				text = lit.Value
			}
			found := milestonePattern.FindString(text)
			if found == "" {
				return true
			}
			t.Errorf("%s:%d: %s dates behaviour to a milestone an operator cannot look up: %q\n"+
				"\tsay what the field or command does now. If the behaviour is genuinely absent, "+
				"name the version it arrives in.",
				relPath, fset.Position(lit.Pos()).Line, found, oneLine(text))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk the module: %v", err)
	}
}

func inOperatorText(relPath string) bool {
	for _, prefix := range operatorText {
		if relPath == prefix || strings.HasPrefix(relPath, prefix+"/") {
			return true
		}
	}
	return false
}

// oneLine renders a hard-wrapped message on one line, so a failure reads as
// the sentence the operator sees rather than as the source's line breaks.
func oneLine(text string) string {
	flat := strings.Join(strings.Fields(text), " ")
	if len(flat) > 120 {
		return flat[:120] + "..."
	}
	return flat
}

// TestOperatorTextScopeIsReal keeps the prefix list from silently naming a
// path that moved. A scope entry matching no file is a rule that has stopped
// covering anything, and it would pass exactly as loudly as one that works.
func TestOperatorTextScopeIsReal(t *testing.T) {
	root := repoRoot(t)
	covered := make(map[string]int, len(operatorText))

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		relPath := filepath.ToSlash(rel(root, path))
		if strings.HasSuffix(relPath, "_test.go") || strings.Contains(relPath, "testdata") {
			return nil
		}
		for _, prefix := range operatorText {
			if relPath == prefix || strings.HasPrefix(relPath, prefix+"/") {
				covered[prefix]++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the module: %v", err)
	}

	empty := make([]string, 0, len(operatorText))
	for _, prefix := range operatorText {
		if covered[prefix] == 0 {
			empty = append(empty, prefix)
		}
	}
	sort.Strings(empty)
	if len(empty) > 0 {
		t.Errorf("operatorText names paths no source file matches: %v\n"+
			"\tthe rule covers nothing there; point the entry at where the text moved, or drop it.", empty)
	}
}
