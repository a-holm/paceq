package arch_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The rule about flaky tests is global, and it is stated once here: a red gate
// is a fact about the code, and nothing anywhere in the gate may retry, rerun
// or tolerate its way past one. A test that fails intermittently has a root
// cause, and the fix is the cause. The gate is .github/workflows, .githooks,
// scripts and the Makefile together, so the guard scans all four.
//
// Three channels turn an intermittent failure into a green run, and each has
// its own check below:
//
//  1. Suppression and retry in the gate itself: continue-on-error, retry keys,
//     || true, until and negated while loops, rerun-fails style tooling.
//  2. A cached pass standing in for a real one: every go test the gate runs
//     carries -count=<n>, and no gate invocation narrows with -short.
//  3. The skip hatch inside the tests: a skip is legitimate only when it states
//     a capability the machine lacks or a mode the run is driven in. A reason
//     that blames timing, load, the runner or CI is an instability excuse, and
//     an empty reason is indistinguishable from a silent pass.

// gateLocations are the directories and files that make up the gate. The walk
// below fails if any of them is missing, so a rename cannot silently empty the
// check the way an empty rule would.
var gateLocations = []string{
	".github/workflows",
	".githooks",
	"scripts",
	"Makefile",
}

// gateFinding is one rule violation in one gate file.
type gateFinding struct {
	line int
	rule string
}

func (f gateFinding) String() string {
	return fmt.Sprintf("line %d: %s", f.line, f.rule)
}

// gateRetryKey matches a YAML key that configures retrying. GitHub Actions has
// no native retry, so such a key only ever appears to feed a retry action.
var gateRetryKey = regexp.MustCompile(`^\s*-?\s*(retry|retries|reruns|rerun-on-failure|rerun_on_failure|attempts|max-attempts|max_attempts)\s*:`)

// goTestCount matches the -count flag with a value, the flag that disables the
// test result cache or forces repeat runs. Both are the honest choices.
var goTestCount = regexp.MustCompile(`-count=\d`)

// shortFlag matches the -short test flag as a standalone word, including after
// an assignment such as GOFLAGS=-short, the flag under which the gate's own
// load tests skip themselves.
var shortFlag = regexp.MustCompile(`(^|[\s=:])-short($|[\s])`)

// scanGateLine reports the findings one line of one gate file earns. It is pure
// so the fixture tests can prove every rule fires, on strings, without touching
// the tree the guard itself runs in. Comments are stripped by the caller.
func scanGateLine(name, line string) []string {
	// $(GO) is how the Makefile spells go, so the go test rule reads the same
	// token in every gate file.
	normalised := strings.ReplaceAll(line, "$(GO)", "go")
	normalised = strings.ReplaceAll(normalised, "${GO}", "go")

	var found []string
	add := func(rule string) {
		found = append(found, rule)
	}

	if strings.Contains(normalised, "continue-on-error") {
		add("continue-on-error tolerates a failing step instead of failing the run")
	}
	if gateRetryKey.MatchString(normalised) {
		add("a retry key asks the gate to run again instead of failing")
	}
	for _, pattern := range []string{"|| true", "|| :", "|| exit 0", "|| echo"} {
		if strings.Contains(normalised, pattern) {
			// A Makefile $(shell ...) assignment computes a value with a
			// fallback, such as the build metadata's "|| echo dev": it
			// decides what a variable holds, never whether the gate
			// passes. Everywhere else the pattern swallows the failure
			// of the command before it.
			if !strings.Contains(normalised, "$(shell") {
				add(pattern + " swallows the failure of the command before it")
			}
			break
		}
	}
	if strings.Contains(normalised, "until ") {
		add("an until loop reruns the command until it goes green")
	}
	if strings.Contains(normalised, "while !") || strings.Contains(normalised, "while true") || strings.Contains(normalised, "while :") {
		add("a while loop around the gate reruns it until it goes green")
	}
	if strings.Contains(normalised, "gotestsum") || strings.Contains(normalised, "--rerun-fails") {
		add("test rerun tooling retries failing tests instead of failing")
	}
	if shortFlag.MatchString(normalised) {
		add("-short narrows the gate, and the tests it skips are the ones that need the time")
	}
	if strings.Contains(normalised, "go test") && !goTestCount.MatchString(normalised) {
		// A line that only delegates to make is checked where the command is
		// spelled out: the Makefile is scanned by the same walk. A workflow
		// step name that says "go test" labels what the step below runs; it
		// executes nothing itself. A direct go test invocation elsewhere must
		// pin -count=<n> itself, or a cached pass can stand in for a real run.
		if !strings.Contains(normalised, "make ") && !strings.Contains(normalised, "- name:") {
			add("go test without -count=<n>: a cached pass can stand in for a real run")
		}
	}
	return found
}

// stripGateComment cuts a comment out of one gate file line. A # counts as a
// comment start at the beginning of the line or after whitespace, which covers
// every shell, Makefile and YAML line in this repository.
func stripGateComment(line string) string {
	if i := strings.Index(line, "#"); i >= 0 && (i == 0 || line[i-1] == ' ' || line[i-1] == '\t') {
		return line[:i]
	}
	return line
}

// gateFindings scans one whole gate file, line by line.
func gateFindings(name, content string) []gateFinding {
	var found []gateFinding
	for i, line := range strings.Split(content, "\n") {
		for _, rule := range scanGateLine(name, stripGateComment(line)) {
			found = append(found, gateFinding{line: i + 1, rule: rule})
		}
	}
	return found
}

// TestGateNeverRetriesOrTolerates walks the gate and fails on any way past a
// red run. It is the enforcement half; the fixture test below is the proof
// that the rules fire.
func TestGateNeverRetriesOrTolerates(t *testing.T) {
	root := repoRoot(t)

	scanned := 0
	for _, loc := range gateLocations {
		path := filepath.Join(root, loc)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", loc, err)
		}
		if !info.IsDir() {
			scanned += scanGateFile(t, root, path)
			continue
		}
		err = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			scanned += scanGateFile(t, root, p)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", loc, err)
		}
	}
	// Makefile, two workflows, two hooks and the cross build script is the
	// floor. Below it the walk has lost a location and the check would pass
	// vacuously.
	if scanned < 6 {
		t.Fatalf("scanned %d gate files, want at least 6: the gate has shrunk under the guard", scanned)
	}
}

func scanGateFile(t *testing.T, root, path string) int {
	t.Helper()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	name := filepath.Base(path)
	for _, f := range gateFindings(name, string(content)) {
		t.Errorf("%s: %s", rel(root, path), f)
	}
	return 1
}

// bannedSkipPhrases are the words a skip reason must not contain. Every entry
// is an instability excuse: it says the test is allowed to disagree with
// itself, which is exactly what a skip that stays skipped hides. Matched
// lowercase against the reason text.
var bannedSkipPhrases = []string{
	"flaky",
	"flake",
	"sometimes",
	"occasionally",
	"intermittent",
	"spurious",
	"unstable",
	"randomly",
	"timing issue",
	"race condition",
	"known issue",
	"known failure",
	"for now",
	"temporarily",
	"under load",
	"on the runner",
	"in ci",
	"on ci",
	"works on my machine",
	"order dependent",
	"order-dependent",
	"passes on retry",
	"expected failure",
}

// skipExcuse returns the banned phrase a skip reason leans on, or "" when the
// reason states a capability or a mode instead. Pure, so the fixture test can
// prove it fires without a Skip call in this file.
func skipExcuse(reason string) string {
	lowered := strings.ToLower(reason)
	for _, phrase := range bannedSkipPhrases {
		if strings.Contains(lowered, phrase) {
			return phrase
		}
	}
	return ""
}

// skipFinding is one problem with one Skip call.
type skipFinding struct {
	pos  token.Pos
	rule string
}

// skipFindings reports the findings of one parsed test file: Skip calls with no
// reason, empty reasons, and reasons that are instability excuses.
func skipFindings(fset *token.FileSet, file *ast.File) []skipFinding {
	var found []skipFinding

	check := func(reason string, pos token.Pos, rule string) {
		if excuse := skipExcuse(reason); excuse != "" {
			found = append(found, skipFinding{pos: pos, rule: fmt.Sprintf("%q leans on %q: an instability excuse, find the root cause instead", reason, excuse)})
			return
		}
		if strings.TrimSpace(reason) == "" {
			found = append(found, skipFinding{pos: pos, rule: "empty skip reason: a silent skip is indistinguishable from a pass"})
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (sel.Sel.Name != "Skip" && sel.Sel.Name != "Skipf") {
			return true
		}
		if _, ok := sel.X.(*ast.Ident); !ok {
			return true
		}
		if len(call.Args) == 0 {
			found = append(found, skipFinding{pos: call.Pos(), rule: "Skip without a reason: a silent skip is indistinguishable from a pass"})
			return true
		}
		// A literal, or literals concatenated with +, is readable here. A
		// computed reason is not, and cannot hide an excuse that is spelled
		// out anywhere else either.
		ast.Inspect(call.Args[0], func(m ast.Node) bool {
			lit, ok := m.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			check(text, lit.Pos(), sel.Sel.Name)
			return true
		})
		return true
	})
	return found
}

// TestSkipsNeverExcuseInstability walks every test file and rejects the skip
// hatch as a way past an intermittent failure. Skips that state a capability
// the machine lacks (no hostname, no boot id) or a mode the run is driven in
// (child process, -short, race detector) pass; that is what every skip in the
// tree says today.
func TestSkipsNeverExcuseInstability(t *testing.T) {
	root := repoRoot(t)

	checked := 0
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
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}
		checked++
		for _, f := range skipFindings(fset, file) {
			t.Errorf("%s: %s", rel(root, fset.Position(f.pos).Filename)+":"+strconv.Itoa(fset.Position(f.pos).Line), f.rule)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if checked == 0 {
		t.Fatal("no test files walked, the check would pass vacuously")
	}
}

// TestGateCheckerCatchesEveryWayPastARedGate feeds one violating line per rule
// to the pure scanner. Without it a broken scanner would report a clean gate
// and nobody would notice, which is the same self-check the time guard runs.
func TestGateCheckerCatchesEveryWayPastARedGate(t *testing.T) {
	cases := []struct {
		name string
		file string
		line string
	}{
		{"continue-on-error", "ci.yml", "  continue-on-error: true"},
		{"retry key", "ci.yml", "  retries: 3"},
		{"attempts key", "ci.yml", "      attempts: 3"},
		{"suppress with true", "pre-push", "make ci || true"},
		{"suppress with colon", "pre-push", "make ci || :"},
		{"suppress with exit 0", "ci.yml", "  run: make gate || exit 0"},
		{"suppress with echo", "ci.yml", "  run: make test || echo 'look later'"},
		{"suppress with echo in make", "Makefile", "	make ci || echo failed; true"},
		{"until loop", "pre-push", "until make ci; do sleep 60; done"},
		{"negated while loop", "ci.yml", "  run: while ! make test; do :; done"},
		{"while true loop", "ci.yml", "  run: while true; do make test && break; done"},
		{"rerun tooling", "Makefile", "\tgotestsum --rerun-fails -- ./..."},
		{"rerun flag", "ci.yml", "  run: go test --rerun-fails ./..."},
		{"short flag", "ci.yml", "  run: make test GOFLAGS=-short"},
		{"cached test run", "Makefile", "\t$(GO) test -race ./..."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scanGateLine(tc.file, tc.line); len(got) == 0 {
				t.Fatalf("scanner missed %q in %s", tc.line, tc.file)
			}
		})
	}

	clean := []struct {
		name string
		file string
		line string
	}{
		{"a gate step", "ci.yml", "  run: make test"},
		{"count with a value", "Makefile", "\tCGO_ENABLED=1 $(GO) test -race -count=1 ./..."},
		{"a for loop that propagates", "Makefile", "		scripts/cross-build.sh \"$${target%/*}\" \"$${target#*/}\" || exit 1; \\"},
		{"a shell value fallback, not a gate decision", "Makefile", "VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)"},
		{"a comment about the rule", "ci.yml", "  # retries are forbidden, see internal/arch"},
	}
	for _, tc := range clean {
		t.Run("clean/"+tc.name, func(t *testing.T) {
			if got := scanGateLine(tc.file, stripGateComment(tc.line)); len(got) != 0 {
				t.Fatalf("scanner flagged a clean line %q: %v", tc.line, got)
			}
		})
	}
}

// TestSkipCheckerCatchesExcuses proves the phrase list fires and that the skip
// reasons the tree legitimately carries stay clean.
func TestSkipCheckerCatchesExcuses(t *testing.T) {
	excuses := []string{
		"flaky under load",
		"sometimes fails on the runner",
		"known issue, tracked upstream",
		"temporarily disabled",
		"unstable ordering",
		"fails in ci",
	}
	for _, reason := range excuses {
		if got := skipExcuse(reason); got == "" {
			t.Errorf("skipExcuse(%q) = \"\", want the excuse named", reason)
		}
	}

	// The skip reasons the tree carries on purpose: capabilities and modes.
	clean := []string{
		"child process, driven by the parent test",
		"no hostname on this machine: %v",
		"this kernel does not expose a boot id",
		"the load window is seconds long",
		"no data race to find here, the gate target runs it",
		"the race detector distorts the timings this test calibrates",
	}
	for _, reason := range clean {
		if got := skipExcuse(reason); got != "" {
			t.Errorf("skipExcuse(%q) = %q, want clean", reason, got)
		}
	}
}

// TestSkipWithoutAReasonIsCaught runs the AST scanner over an in-memory file,
// because no test in this tree carries a reasonless Skip to catch.
func TestSkipWithoutAReasonIsCaught(t *testing.T) {
	src := "package a\n\nimport \"testing\"\n\nfunc TestX(t *testing.T) {\n\tt.Skip()\n}\n"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, 0)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	got := skipFindings(fset, file)
	if len(got) != 1 {
		t.Fatalf("skipFindings reported %d findings, want 1: %v", len(got), got)
	}
	if !strings.Contains(got[0].rule, "without a reason") {
		t.Fatalf("finding %q does not name the missing reason", got[0].rule)
	}
}
