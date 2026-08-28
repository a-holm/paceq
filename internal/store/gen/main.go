// Command gen writes docs/reference/invarianter.md from the invariant
// catalogue. It runs through the go:generate directive in internal/store and
// is the only writer of that page, so the reference and the sweep cannot
// drift apart (M6-06).
package main

import (
	"os"
	"path/filepath"

	"github.com/a-holm/paceq/internal/store"
)

func main() {
	// go generate runs with the package directory as cwd, so the page lands
	// two levels up, in docs/reference.
	out := filepath.Join("..", "..", "docs", "reference", "invarianter.md")
	if err := os.MkdirAll(filepath.Dir(out), 0o700); err != nil {
		panic(err)
	}
	if err := os.WriteFile(out, []byte(store.RenderCatalogueDoc()), 0o600); err != nil {
		panic(err)
	}
}
