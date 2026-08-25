package arch_test

import (
	"strings"
	"testing"
)

// The explain package is the product's presentation layer over recorded
// decisions (issue 25, 06 section 1.1 principle 2: explanation and reality
// can never diverge). It reads rows; it must never reach anything that makes
// decisions. The allowlist row in deps_test.go enforces it for direct
// imports; this test names the rule so a reviewer reading the failure sees
// the design constraint, not just a table row.
func TestExplainIsAPresentationLayer(t *testing.T) {
	out := runGo(t, "list", "-deps", "-f", "{{.ImportPath}}",
		modulePath+"/internal/explain")

	forbidden := []string{"internal/scheduler", "internal/sensor", "internal/engine"}
	for _, path := range strings.Split(strings.TrimSpace(out), "\n") {
		for _, f := range forbidden {
			if path == modulePath+"/"+f {
				t.Errorf("internal/explain depends on %s: forbidden, explain is a read-only "+
					"presentation layer and must not import anything that decides", f)
			}
		}
	}
}
