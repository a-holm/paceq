package arch_test

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

const (
	modulePath     = "github.com/a-holm/pulseq"
	internalPrefix = modulePath + "/internal/"
)

// allowedImports lists, per internal package, the internal packages it may import
// directly. Transitive imports are deliberately not restricted: engine may import
// runner even though cli imports engine and may not import runner itself.
// A package absent from this table carries no direction rule yet.
var allowedImports = map[string][]string{
	"model":  {},
	"id":     {},
	"clock":  {},
	"store":  {"model", "clock", "id"},
	"engine": {"model", "store", "runner", "clock", "id", "notify"},
	"cli":    {"engine", "store", "explain", "spec", "obs", "model", "clock", "id"},
}

type listedPackage struct {
	ImportPath string
	Imports    []string
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

	for _, p := range pkgs {
		name, ok := internalName(p.ImportPath)
		if !ok {
			continue
		}
		allowed, constrained := allowedImports[name]
		if !constrained {
			continue
		}
		permitted := make(map[string]bool, len(allowed))
		for _, a := range allowed {
			permitted[a] = true
		}
		for _, imp := range p.Imports {
			target, ok := internalName(imp)
			if !ok || permitted[target] {
				continue
			}
			if len(allowed) == 0 {
				t.Errorf("internal/%s imports internal/%s: forbidden, internal/%s must not import anything under internal/",
					name, target, name)
				continue
			}
			sorted := append([]string(nil), allowed...)
			sort.Strings(sorted)
			t.Errorf("internal/%s imports internal/%s: forbidden, internal/%s may only import internal/{%s}",
				name, target, name, strings.Join(sorted, ", "))
		}
	}
}

func listPackages(t *testing.T) []listedPackage {
	t.Helper()

	cmd := exec.Command("go", "list", "-deps", "-json", "./...")
	cmd.Dir = repoRoot(t)
	out, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = ee.Stderr
		}
		t.Fatalf("go list -deps -json ./...: %v\n%s", err, stderr)
	}

	var pkgs []listedPackage
	dec := json.NewDecoder(strings.NewReader(string(out)))
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

// repoRoot walks up from this test file until it finds the module root.
func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine the path of this test file")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod found above %s", filepath.Dir(file))
		}
		dir = parent
	}
}

// selfDir is the directory of this test file. The SQL locality check skips it,
// because the checker necessarily spells out the patterns it hunts for.
func selfDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine the path of this test file")
	}
	return filepath.Dir(file)
}
