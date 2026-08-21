package arch_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// runtimeDepBudget is the ceiling on direct runtime dependencies. Test-only
// dependencies do not count. Every direct dependency, runtime or test, needs a
// line in docs/adr/0001-foundation.md.
const runtimeDepBudget = 8

// classifyPlatforms are the operating systems the import graph is walked for.
// A dependency behind //go:build windows is a runtime dependency, and a graph
// walked only on the host would not see it.
var classifyPlatforms = []string{"linux", "darwin", "windows"}

// testutilPackage is left out of the runtime roots. It is imported by test files
// only, so what it pulls in ships in test binaries, not in the product.
const testutilPackage = modulePath + "/internal/testutil"

type goModFile struct {
	Require []struct {
		Path     string
		Version  string
		Indirect bool
	}
}

func TestRuntimeDependencyBudget(t *testing.T) {
	root := repoRoot(t)
	direct := directRequires(t, filepath.Join(root, "go.mod"))

	runtimeModules := map[string]bool{}
	testModules := map[string]bool{}
	for _, goos := range classifyPlatforms {
		roots := append([]string{"list", "-deps", "-f", moduleTemplate}, runtimeRoots(t, goos)...)
		mergeInto(runtimeModules, modulesFrom(t, goos, roots...))
		mergeInto(testModules, modulesFrom(t, goos, "list", "-deps", "-test", "-f", moduleTemplate, "./..."))
	}

	var runtimeDeps, testOnlyDeps, unreachable []string
	for _, path := range direct {
		switch {
		case runtimeModules[path]:
			runtimeDeps = append(runtimeDeps, path)
		case testModules[path]:
			testOnlyDeps = append(testOnlyDeps, path)
		default:
			// Reachable only behind a build tag this walk does not set, or stale.
			// Counted as runtime, because guessing the other way opens a hole in
			// the budget. go mod tidy is the authority on staleness, not this test.
			unreachable = append(unreachable, path)
			runtimeDeps = append(runtimeDeps, path)
		}
	}

	if len(runtimeDeps) > runtimeDepBudget {
		t.Errorf("go.mod has %d direct runtime dependencies, budget is %d: %s",
			len(runtimeDeps), runtimeDepBudget, strings.Join(runtimeDeps, ", "))
	}
	t.Logf("direct runtime dependencies: %d of %d used%s", len(runtimeDeps), runtimeDepBudget, listing(runtimeDeps))
	t.Logf("direct test-only dependencies: %d, outside the budget%s", len(testOnlyDeps), listing(testOnlyDeps))
	if len(unreachable) > 0 {
		t.Logf("direct dependencies not reachable on %s, counted as runtime%s",
			strings.Join(classifyPlatforms, "/"), listing(unreachable))
	}
}

func listing(deps []string) string {
	if len(deps) == 0 {
		return ""
	}
	return " (" + strings.Join(deps, ", ") + ")"
}

// moduleTemplate prints the module path of every package, blank for the standard
// library, which has no module.
const moduleTemplate = "{{if .Module}}{{.Module.Path}}{{end}}"

// runtimeRoots lists the module's own packages that ship in the product.
func runtimeRoots(t *testing.T, goos string) []string {
	t.Helper()

	var roots []string
	for _, line := range strings.Split(runGoOS(t, goos, "list", "-f", "{{.ImportPath}}", "./..."), "\n") {
		if line = strings.TrimSpace(line); line != "" && line != testutilPackage {
			roots = append(roots, line)
		}
	}
	if len(roots) == 0 {
		t.Fatalf("go list ./... found no packages for GOOS=%s", goos)
	}
	return roots
}

func modulesFrom(t *testing.T, goos string, args ...string) map[string]bool {
	t.Helper()

	modules := map[string]bool{}
	for _, line := range strings.Split(runGoOS(t, goos, args...), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			modules[line] = true
		}
	}
	return modules
}

func mergeInto(dst, src map[string]bool) {
	for k := range src {
		dst[k] = true
	}
}

// directRequires returns the module paths required directly. The go tool does the
// parsing: go.mod comment placement is subtle enough that hand-parsing it produced
// silent false negatives.
func directRequires(t *testing.T, goModPath string) []string {
	t.Helper()

	cmd := exec.Command("go", "mod", "edit", "-json", goModPath)
	out, err := cmd.Output()
	if err != nil {
		var stderr []byte
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = ee.Stderr
		}
		t.Fatalf("go mod edit -json %s: %v\n%s", goModPath, err, stderr)
	}

	var parsed goModFile
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("decode go mod edit -json output: %v", err)
	}

	var direct []string
	for _, r := range parsed.Require {
		if !r.Indirect {
			direct = append(direct, r.Path)
		}
	}
	sort.Strings(direct)
	return direct
}

func TestDirectRequiresParsing(t *testing.T) {
	cases := []struct {
		name  string
		goMod string
		want  []string
	}{
		{
			name:  "no require at all",
			goMod: "module example.com/x\n\ngo 1.25\n",
		},
		{
			name:  "single line require",
			goMod: "module example.com/x\n\ngo 1.25\n\nrequire example.com/a v1.0.0\n",
			want:  []string{"example.com/a"},
		},
		{
			name: "block with indirect entries",
			goMod: "module example.com/x\n\ngo 1.25\n\nrequire (\n\texample.com/a v1.0.0\n" +
				"\texample.com/b v1.0.0 // indirect\n\texample.com/c v1.0.0\n)\n",
			want: []string{"example.com/a", "example.com/c"},
		},
		{
			name: "block opener carries a trailing comment",
			goMod: "module example.com/x\n\ngo 1.25\n\nrequire ( // runtime deps\n" +
				"\texample.com/a v1.0.0\n\texample.com/b v1.0.0\n)\n",
			want: []string{"example.com/a", "example.com/b"},
		},
		{
			name: "several require blocks",
			goMod: "module example.com/x\n\ngo 1.25\n\nrequire (\n\texample.com/a v1.0.0\n)\n\n" +
				"require example.com/b v1.0.0\n\nrequire (\n\texample.com/c v1.0.0 // indirect\n" +
				"\texample.com/d v1.0.0\n)\n",
			want: []string{"example.com/a", "example.com/b", "example.com/d"},
		},
		{
			name: "comment merely containing the word indirect",
			goMod: "module example.com/x\n\ngo 1.25\n\nrequire (\n" +
				"\texample.com/a v1.0.0 // not indirect, we import it\n" +
				"\texample.com/b v1.0.0 // indirect\n" +
				"\texample.com/c v1.0.0 // indirect; pulled in by a\n)\n",
			want: []string{"example.com/a"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "go.mod")
			if err := os.WriteFile(path, []byte(tc.goMod), 0o600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			got := directRequires(t, path)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("directRequires() = %v, want %v", got, tc.want)
			}
		})
	}
}
