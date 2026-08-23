package arch_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Issue #60 makes fencing mechanical. A state write that moves a run or a
// step must carry its holder's fencing token in the statement itself, or be
// written from inside a function that already checked the token against the
// row in the same transaction (store.checkLeaseTx), or say out loud why it
// needs no fence. Anything else is a hole a frozen worker could walk through
// much later, so the guard refuses it at build time instead of at review time.

// mutationPattern matches the statements that can corrupt a run or a step.
var mutationPattern = regexp.MustCompile(
	`^(UPDATE\s+(runs|steps)\b)|(DELETE\s+FROM\s+(runs|steps)\b)`)

// fenceMarker is the escape hatch. Like every nolint it costs a sentence:
// the comment must say why the write is safe without a token.
const fenceMarker = "nolint:fencing"

func TestRunAndStepMutationsCarryTheFence(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, "internal", "store")

	fset := token.NewFileSet()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}
		fenced := fencedFunctions(file)

		for _, decl := range file.Decls {
			specs := mutatingLiterals(fset, file, decl)
			for _, m := range specs {
				if strings.Contains(m.text, "lease_epoch") {
					continue // the statement fences itself
				}
				if fenced[m.decl] {
					continue // the enclosing function checked the token first
				}
				if m.justified {
					continue // an explicit, written exception
				}
				t.Errorf("%s:%d: a run or step mutation with no fence: %s",
					rel(root, path), m.line, firstLine(m.text))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/store: %v", err)
	}
}

// mutationSpec is one SQL literal that mutates a run or a step, with what the
// guard knows about it.
type mutationSpec struct {
	text      string
	line      int
	decl      ast.Node
	justified bool
}

// mutatingLiterals walks every string literal under the declaration and
// returns those that look like a run or step mutation. A literal is justified
// when a comment group directly above it carries the fence marker.
func mutatingLiterals(fset *token.FileSet, file *ast.File, decl ast.Decl) []mutationSpec {
	var out []mutationSpec
	ast.Inspect(decl, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		normalised := strings.Join(strings.Fields(value), " ")
		if !mutationPattern.MatchString(normalised) {
			return true
		}
		out = append(out, mutationSpec{
			text:      normalised,
			line:      fset.Position(lit.Pos()).Line,
			decl:      decl,
			justified: commentedWithFence(fset, file, lit.Pos()),
		})
		return true
	})
	return out
}

// fencedFunctions reports which declarations call store.checkLeaseTx, the
// in-transaction proof that the writer still holds the row at its token.
func fencedFunctions(file *ast.File) map[ast.Node]bool {
	out := make(map[ast.Node]bool)
	for _, decl := range file.Decls {
		ast.Inspect(decl, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "checkLeaseTx" {
				out[decl] = true
			}
			return true
		})
	}
	return out
}

// commentedWithFence looks for the marker among the comments that sit
// immediately above the position: same block, no more than four lines of gap.
func commentedWithFence(fset *token.FileSet, file *ast.File, pos token.Pos) bool {
	line := fset.Position(pos).Line
	for _, group := range file.Comments {
		endLine := fset.Position(group.End()).Line
		if endLine < line-4 || endLine > line {
			continue
		}
		if strings.Contains(group.Text(), fenceMarker) {
			return true
		}
	}
	return false
}

// firstLine trims a statement for an error message.
func firstLine(s string) string {
	if i := strings.Index(s, "\\n"); i > 0 {
		s = s[:i]
	}
	if len(s) > 90 {
		s = s[:90] + "..."
	}
	return s
}
