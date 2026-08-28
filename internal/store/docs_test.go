package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// docsPath is the generated reference page, checked in so it can be reviewed
// and linked. It is written by the generator, never by hand.
const docsPath = "../../docs/reference/invarianter.md"

// TestGeneratedDocsAreFresh is the staleness gate (M6-06): the page is a
// projection of the invariant catalogue and nothing else. Editing the file by
// hand, or changing an entry without regenerating, fails here with the one
// command that fixes it - the same discipline the reason catalogue runs under.
func TestGeneratedDocsAreFresh(t *testing.T) {
	raw, err := os.ReadFile(filepath.FromSlash(docsPath))
	if err != nil {
		t.Fatalf("read %s: %v\nregenerate it with: go generate ./internal/store", docsPath, err)
	}
	want := RenderCatalogueDoc()
	got := string(raw)
	if got == want {
		return
	}
	line := 1
	differed := false
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			line += strings.Count(got[:i], "\n") + 1
			differed = true
			break
		}
	}
	if !differed {
		// One text is a prefix of the other: the drift is at the end.
		line += strings.Count(got, "\n")
	}
	t.Fatalf("%s is stale at around line %d.\nregenerate it with: go generate ./internal/store",
		docsPath, line)
}
