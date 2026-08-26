package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// docsPath is the generated reference page, checked in so it can be reviewed
// and linked. It is written by the generator, never by hand.
const cliDocsPath = "../../docs/reference/cli.md"

// TestCLIDocsAreFresh is the staleness gate: the page is a projection of this
// package's command tree and nothing else. Editing the file by hand, or
// changing any help text, flag or example without regenerating, fails here.
func TestCLIDocsAreFresh(t *testing.T) {
	raw, err := os.ReadFile(filepath.FromSlash(cliDocsPath))
	if err != nil {
		t.Fatalf("read %s: %v\nregenerate it with: go generate ./internal/cli", cliDocsPath, err)
	}
	want := RenderCLIDocs()
	got := string(raw)
	if got == want {
		return
	}
	line := 1
	if i := firstDifference(got, want); i >= 0 {
		line += strings.Count(got[:i], "\n")
	} else {
		// One text is a prefix of the other: the drift is at the end.
		line += strings.Count(got, "\n")
	}
	t.Fatalf("%s is stale at around line %d.\nregenerate it with: go generate ./internal/cli\nthen commit the result together with the help change", cliDocsPath, line)
}

// TestRenderCLIDocsIsDeterministic pins the property the freshness gate leans
// on: rendering twice gives identical bytes, so an unchanged tree cannot
// produce a noisy diff (no dates, no map order).
func TestRenderCLIDocsIsDeterministic(t *testing.T) {
	first := RenderCLIDocs()
	second := RenderCLIDocs()
	if first != second {
		t.Fatal("two renders of one command tree differ; the output is not deterministic")
	}
}

// TestEveryCommandIsDocumented walks the tree and requires a section per
// command path, so a new subcommand cannot ship without appearing in the
// generated reference.
func TestEveryCommandIsDocumented(t *testing.T) {
	docs := RenderCLIDocs()
	var missing []string
	collectMissing(newRoot(docsEnv()), docs, &missing)
	if len(missing) > 0 {
		t.Fatalf("commands without a section in the generated docs: %s", strings.Join(missing, ", "))
	}
}

func collectMissing(cmd *cobra.Command, docs string, missing *[]string) {
	path := cmd.CommandPath()
	if !strings.Contains(docs, "`"+path+"`") {
		*missing = append(*missing, path)
	}
	for _, child := range sortedCommands(cmd) {
		collectMissing(child, docs, missing)
	}
}

func firstDifference(a, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}
