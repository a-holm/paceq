// Command gen writes docs/reference/reason-codes.md from the reason catalogue.
// It runs through the go:generate directive in internal/reason and is the only
// writer of that page.
package main

import (
	"os"
	"path/filepath"

	"github.com/a-holm/paceq/internal/reason"
)

func main() {
	// go generate runs with the package directory as cwd, so the page lands
	// two levels up, in docs/reference.
	out := filepath.Join("..", "..", "docs", "reference", "reason-codes.md")
	if err := os.MkdirAll(filepath.Dir(out), 0o700); err != nil {
		panic(err)
	}
	if err := os.WriteFile(out, []byte(reason.Render()), 0o600); err != nil {
		panic(err)
	}
}
