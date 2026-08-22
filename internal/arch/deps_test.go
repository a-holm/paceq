package arch_test

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
)

const (
	modulePath     = "github.com/a-holm/paceq"
	internalPrefix = modulePath + "/internal/"
)

// allowedImports lists, per internal package, the internal packages it may import
// directly. Transitive imports are deliberately not restricted: engine may import
// runner even though cli imports engine and may not import runner itself.
// A package absent from this table carries no direction rule yet.
var allowedImports = map[string][]string{
	"model":    {},
	"id":       {},
	"clock":    {},
	"diag":     {},
	"spec":     {"diag"},
	"store":    {"model", "clock", "id"},
	"engine":   {"model", "store", "runner", "clock", "id", "notify"},
	"doctor":   {"store"},
	"cli":      {"engine", "store", "doctor", "explain", "spec", "diag", "obs", "model", "clock", "id"},
	"testutil": {"model", "clock", "id", "store"},
}

// testOnlyExtra names internal packages that test files may import on top of the
// package's own row in allowedImports. testutil exists to be imported by tests.
var testOnlyExtra = map[string]bool{"testutil": true}

// topPackage sits above everything else: nothing under internal/ may import it.
const topPackage = "cli"

type listedPackage struct {
	ImportPath   string
	Imports      []string
	TestImports  []string
	XTestImports []string
}

// importSet is one list of imports from go list, kept with the label used in
// failure messages so a reader knows whether the edge comes from a test file.
type importSet struct {
	label   string
	imports []string
}

func (p listedPackage) importSets() []importSet {
	return []importSet{
		{"imports", p.Imports},
		{"test-imports", p.TestImports},
		{"external-test-imports", p.XTestImports},
	}
}

func TestDependencyDirection(t *testing.T) {
	pkgs := listPackages(t)

	seen := make(map[string]bool, len(pkgs))
	for _, p := range pkgs {
		if name, ok := internalName(p.ImportPath); ok {
			seen[name] = true
		}
	}
	for name := range allowedImports {
		if !seen[name] {
			t.Errorf("rule table names internal/%s, but no such package exists in the module", name)
		}
	}
	if !seen[topPackage] {
		t.Errorf("top package rule names internal/%s, but no such package exists in the module", topPackage)
	}

	for _, p := range pkgs {
		name, ok := internalName(p.ImportPath)
		if !ok {
			continue
		}
		allowed, constrained := allowedImports[name]

		for _, set := range p.importSets() {
			permitted := permittedFor(allowed, set.label)
			for _, imp := range set.imports {
				target, ok := internalName(imp)
				if !ok || target == name {
					continue
				}
				if target == topPackage {
					t.Errorf("internal/%s %s internal/%s: forbidden, internal/%s is the top layer and nothing under internal/ may import it",
						name, set.label, target, topPackage)
					continue
				}
				if !constrained || permitted[target] {
					continue
				}
				if len(permitted) == 0 {
					t.Errorf("internal/%s %s internal/%s: forbidden, internal/%s must not import anything under internal/",
						name, set.label, target, name)
					continue
				}
				t.Errorf("internal/%s %s internal/%s: forbidden, internal/%s may only import internal/{%s}",
					name, set.label, target, name, strings.Join(sortedKeys(permitted), ", "))
			}
		}
	}
}

func permittedFor(allowed []string, label string) map[string]bool {
	permitted := make(map[string]bool, len(allowed)+len(testOnlyExtra))
	for _, a := range allowed {
		permitted[a] = true
	}
	if label != "imports" {
		for extra := range testOnlyExtra {
			permitted[extra] = true
		}
	}
	return permitted
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func listPackages(t *testing.T) []listedPackage {
	t.Helper()

	out := runGo(t, "list", "-deps", "-json", "./...")

	var pkgs []listedPackage
	dec := json.NewDecoder(strings.NewReader(out))
	for {
		var p listedPackage
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		pkgs = append(pkgs, p)
	}
	return pkgs
}

func internalName(importPath string) (string, bool) {
	if !strings.HasPrefix(importPath, internalPrefix) {
		return "", false
	}
	return strings.TrimPrefix(importPath, internalPrefix), true
}

// runGo runs the go tool from the module root and returns its stdout.
func runGo(t *testing.T, args ...string) string {
	t.Helper()

	return runGoOS(t, "", args...)
}

// runGoOS is runGo for a chosen GOOS. An empty goos keeps the host setting.
func runGoOS(t *testing.T, goos string, args ...string) string {
	t.Helper()

	cmd := exec.Command("go", args...)
	cmd.Dir = repoRoot(t)
	if goos != "" {
		cmd.Env = append(os.Environ(), "GOOS="+goos)
	}
	out, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = ee.Stderr
		}
		t.Fatalf("GOOS=%q go %s: %v\n%s", goos, strings.Join(args, " "), err, stderr)
	}
	return string(out)
}

// repoRoot asks the go tool for the module directory. Deriving it from
// runtime.Caller would break under go test -trimpath, which rewrites that path.
func repoRoot(t *testing.T) string {
	t.Helper()

	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}")
	cmd.Dir = selfDir(t)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -m -f {{.Dir}}: %v", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		t.Fatal("go list -m reported an empty module directory")
	}
	return dir
}

// selfDir is the directory of this package. go test runs the binary there, which
// holds under -trimpath. The SQL locality check skips it, because the checker
// necessarily spells out the patterns it hunts for.
func selfDir(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	return wd
}
