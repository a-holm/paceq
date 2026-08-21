package arch_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// depBudget is the ceiling on direct runtime and test dependencies. Every new
// direct dependency needs a line in docs/adr/0001-foundation.md.
const depBudget = 8

func TestDirectDependencyBudget(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}

	direct := directRequires(string(raw))
	if len(direct) > depBudget {
		t.Errorf("go.mod has %d direct dependencies, budget is %d: %s",
			len(direct), depBudget, strings.Join(direct, ", "))
	}
	t.Logf("direct dependencies: %d of %d used", len(direct), depBudget)
}

// directRequires returns the module paths required directly, meaning every
// require entry that go mod tidy has not marked "// indirect".
func directRequires(goMod string) []string {
	var direct []string
	inBlock := false

	for _, line := range strings.Split(goMod, "\n") {
		line = strings.TrimSpace(line)
		if inBlock {
			if line == ")" {
				inBlock = false
				continue
			}
		} else {
			switch {
			case line == "require (":
				inBlock = true
				continue
			case strings.HasPrefix(line, "require "):
				line = strings.TrimPrefix(line, "require ")
			default:
				continue
			}
		}

		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if comment := strings.Index(line, "//"); comment >= 0 {
			if strings.Contains(line[comment:], "indirect") {
				continue
			}
			line = strings.TrimSpace(line[:comment])
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		direct = append(direct, fields[0])
	}
	return direct
}

func TestDirectRequiresParsing(t *testing.T) {
	cases := []struct {
		name  string
		goMod string
		want  []string
	}{
		{name: "no require at all", goMod: "module example.com/x\n\ngo 1.25\n"},
		{
			name:  "single line require",
			goMod: "module example.com/x\n\ngo 1.25\n\nrequire example.com/a v1.0.0\n",
			want:  []string{"example.com/a"},
		},
		{
			name: "block with indirect entries",
			goMod: "module example.com/x\n\ngo 1.25\n\nrequire (\n\texample.com/a v1.0.0\n" +
				"\texample.com/b v2.0.0 // indirect\n\texample.com/c v3.0.0\n)\n",
			want: []string{"example.com/a", "example.com/c"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := directRequires(tc.goMod)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("directRequires() = %v, want %v", got, tc.want)
			}
		})
	}
}
