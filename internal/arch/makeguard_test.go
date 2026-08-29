//go:build unix

package arch_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSkipGuardsActuallySkip is the guard for #177. Each line of a make recipe
// runs in its own shell, so an `exit 0` inside one line cannot stop the next
// line from running. A target that guards an optional tool like this:
//
//	target:
//		@if ! command -v thing >/dev/null 2>&1; then \
//			echo "SKIP ..."; \
//			exit 0; \
//		fi
//		thing --do-it
//
// prints SKIP and then runs `thing` anyway, failing with 127 where the tool is
// absent. Four targets shipped that shape and one of them killed a push after
// eighteen minutes of green gates. The working form puts the guarded command in
// the guard's own else branch, in the same shell, which is what the shellcheck
// guards in this Makefile already do.
//
// A backslash continues a line, so the unit here is the logical line: an
// `exit 0` only guards what follows it inside the same logical line.
func TestSkipGuardsActuallySkip(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read the Makefile: %v", err)
	}

	type logical struct {
		text string
		line int
	}
	target, targetLine := "", 0
	var recipe []logical
	var pending strings.Builder
	pendingAt := 0

	check := func() {
		for i, l := range recipe {
			if !strings.Contains(l.text, "exit 0") || i == len(recipe)-1 {
				continue
			}
			t.Errorf("Makefile:%d: target %q guards with `exit 0`, then line %d runs %q "+
				"in a new shell the guard cannot reach. Put the guarded command in the "+
				"guard's else branch (#177)",
				l.line, target, recipe[i+1].line, strings.TrimSpace(recipe[i+1].text))
		}
		recipe = nil
	}

	for n, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "\t") {
			check()
			if trimmed := strings.TrimSpace(line); trimmed != "" &&
				!strings.HasPrefix(trimmed, "#") && strings.Contains(line, ":") &&
				!strings.HasPrefix(line, " ") {
				target, targetLine = strings.SplitN(line, ":", 2)[0], n+1
			}
			continue
		}
		if pending.Len() == 0 {
			pendingAt = n + 1
		}
		pending.WriteString(line)
		if strings.HasSuffix(strings.TrimRight(line, " "), "\\") {
			continue // the logical line goes on
		}
		recipe = append(recipe, logical{text: pending.String(), line: pendingAt})
		pending.Reset()
	}
	check()
	_ = targetLine
}
