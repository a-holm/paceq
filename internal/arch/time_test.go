package arch_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// forbiddenTimeFuncs are the package time entry points that read or wait on the
// real clock. Every one of them makes the code that calls it untestable without
// sleeping, which is how a scheduler ends up with flaky tests that get switched
// off. Domain code takes a clock.Clock instead.
var forbiddenTimeFuncs = map[string]bool{
	"Now":       true,
	"Since":     true,
	"Until":     true,
	"After":     true,
	"AfterFunc": true,
	"Sleep":     true,
	"Tick":      true,
	"NewTimer":  true,
	"NewTicker": true,
}

// sleepOnly is the rule for test files in the packages that own time. Their
// tests may read the clock, but a test that sleeps is a test that is slow and
// eventually flaky.
var sleepOnly = map[string]bool{"Sleep": true}

// timeUse is one reference to a forbidden package time function.
type timeUse struct {
	file string
	line int
	call string
}

func (u timeUse) String() string {
	return fmt.Sprintf("%s:%d: %s", u.file, u.line, u.call)
}

// findTimeUses reports every reference to a forbidden package time function in
// one file. It matches the local name of the time import, so an aliased import
// cannot hide a call.
func findTimeUses(fset *token.FileSet, path string, forbidden map[string]bool) ([]timeUse, error) {
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, err
	}

	names := timeImportNames(file)
	if len(names) == 0 {
		return nil, nil
	}

	var uses []timeUse
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || !forbidden[sel.Sel.Name] {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok || !names[ident.Name] || ident.Obj != nil {
			return true
		}
		pos := fset.Position(sel.Pos())
		uses = append(uses, timeUse{
			file: pos.Filename,
			line: pos.Line,
			call: ident.Name + "." + sel.Sel.Name,
		})
		return true
	})
	return uses, nil
}

func timeImportNames(file *ast.File) map[string]bool {
	names := map[string]bool{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != "time" {
			continue
		}
		if imp.Name == nil {
			names["time"] = true
			continue
		}
		if imp.Name.Name != "_" && imp.Name.Name != "." {
			names[imp.Name.Name] = true
		}
	}
	return names
}

// TestTimeStaysInClock is the guard that keeps time injectable. Production code
// outside internal/clock may not touch the real clock at all, and the tests of
// the two packages that own time may not sleep.
//
// Test files elsewhere are deliberately exempt: a test that wants the real
// clock is usually testing something real, and testing/synctest already makes
// the timer-driven ones deterministic. The one way to walk around the check,
// dot importing time so its calls lose their qualifier, is banned by
// TestNoDotImportOfTime.
func TestTimeStaysInClock(t *testing.T) {
	root := repoRoot(t)
	clockDir := filepath.Join(root, "internal", "clock")
	noSleepInTests := map[string]bool{
		clockDir:                              true,
		filepath.Join(root, "internal", "id"): true,
	}

	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "bin", "dist", "testdata":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		dir := filepath.Dir(path)
		isTest := strings.HasSuffix(path, "_test.go")

		var forbidden map[string]bool
		switch {
		case isTest && noSleepInTests[dir]:
			forbidden = sleepOnly
		case isTest:
			return nil
		case dir == clockDir:
			return nil
		default:
			forbidden = forbiddenTimeFuncs
		}

		uses, err := findTimeUses(fset, path, forbidden)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}
		for _, u := range uses {
			u.file = rel(root, u.file)
			if isTest {
				t.Errorf("%s: forbidden, a test in this package must not sleep, advance a clock.Fake or use testing/synctest", u)
				continue
			}
			t.Errorf("%s: forbidden outside internal/clock, take a clock.Clock and call its method instead", u)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

// TestNoDotImportOfTime closes the hole under the selector check. A dot import
// turns time.Now into a bare Now, which no check that reads qualifiers can see,
// and it buys nothing anywhere in this codebase. The ban covers every file,
// tests included, because the point is that the qualifier is always there to
// read. staticcheck ST1001 flags dot imports too; this test is what the rule
// rests on, so the guard does not depend on an external tool's default set.
//
// Unlike TestTimeStaysInClock, this walk deliberately includes testdata. A
// fixture is read by people and copied from, nothing under testdata has any
// reason to dot import, and a fixture is exactly where an unread file would sit.
func TestNoDotImportOfTime(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()

	checked := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "bin", "dist":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		checked++

		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}
		for _, imp := range file.Imports {
			if imp.Name == nil || imp.Name.Name != "." {
				continue
			}
			importPath, err := strconv.Unquote(imp.Path.Value)
			if err != nil || importPath != "time" {
				continue
			}
			pos := fset.Position(imp.Pos())
			t.Errorf("%s:%d: dot imports %q: forbidden, it hides the qualifier the clock check reads",
				rel(root, path), pos.Line, importPath)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if checked == 0 {
		t.Fatal("no Go files walked, the check would pass vacuously")
	}
}

// TestTimeLintCatchesForbiddenUses runs the checker over a fixture that is not
// part of the build. Without it a broken checker would report a clean tree and
// nobody would notice.
func TestTimeLintCatchesForbiddenUses(t *testing.T) {
	root := repoRoot(t)
	fixture := filepath.Join(root, "internal", "arch", "testdata", "badtime", "bad.go")
	fset := token.NewFileSet()

	uses, err := findTimeUses(fset, fixture, forbiddenTimeFuncs)
	if err != nil {
		t.Fatalf("parse %s: %v", rel(root, fixture), err)
	}

	got := map[string]int{}
	for _, u := range uses {
		got[u.call] = u.line
	}

	want := []string{
		"time.Now", "time.Since", "time.Until", "time.After", "time.AfterFunc",
		"time.Sleep", "time.Tick", "time.NewTimer", "time.NewTicker",
	}
	for _, call := range want {
		if _, ok := got[call]; !ok {
			t.Errorf("checker missed %s in the fixture", call)
		}
	}
	if _, ok := got["clock.Now"]; ok {
		t.Error("checker flagged clock.Now, which is the method the rule points people at")
	}
	if _, ok := got["aliased.Now"]; !ok {
		t.Error("checker missed an aliased time import")
	}
	if _, ok := got["time.Parse"]; ok {
		t.Error("checker flagged time.Parse, which does not read the clock")
	}

	sleeps, err := findTimeUses(fset, fixture, sleepOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", rel(root, fixture), err)
	}
	if len(sleeps) != 1 || sleeps[0].call != "time.Sleep" {
		t.Errorf("sleep only rule found %v, want exactly one time.Sleep", sleeps)
	}

	for _, u := range uses {
		message := timeUse{file: rel(root, u.file), line: u.line, call: u.call}.String()
		if !strings.HasPrefix(message, filepath.Join("internal", "arch", "testdata", "badtime", "bad.go")+":") {
			t.Errorf("message %q does not start with the file path", message)
		}
		if !strings.Contains(message, ":"+strconv.Itoa(u.line)+": ") {
			t.Errorf("message %q does not name the line", message)
		}
	}
}
