package arch_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The needles are built from pieces so this file cannot report itself; the
// spellings they join never appear in product comments either, which is why
// comment lines are still skipped below rather than scanned.
var (
	templateNeedleText = "text/" + "template"
	templateNeedleHTML = "html/" + "template"
	templateNeedleQuot = `"` + "template" + `"`
)

// TestNoTemplatingInAnyProductCode is the mechanical half of #29's AC: the
// event payload, a notifier's argv, a concurrency key - none of it goes near
// a template engine. A substitution language beside SQLite-serialised facts
// would be an injection surface wearing a convenience costume (SYNTESE
// section 3.3), so the import is refused in every internal package.
func TestNoTemplatingInAnyProductCode(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch name := d.Name(); name {
			case ".git", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue // comments document the ban; they must not violate it
			}
			if strings.Contains(line, templateNeedleText) ||
				strings.Contains(line, templateNeedleHTML) ||
				strings.Contains(line, templateNeedleQuot) {
				rel, _ := filepath.Rel(root, path)
				offenders = append(offenders, rel+": line "+itoaArch(i+1)+": "+strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("templating found; paceq substitutes values through fmt and JSON only:\n%s",
			strings.Join(offenders, "\n"))
	}
}

func itoaArch(n int) string {
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}
