// Command gendocs writes docs/reference/cli.md from the live command tree.
//
//	go generate ./internal/cli    # from anywhere
//
// It is wired as go generate ./internal/cli, and the freshness test in
// internal/cli fails when the committed page disagrees with the tree, which
// is what keeps the generated reference from rotting. The page carries no
// date or version on purpose: regenerating an unchanged tree changes nothing.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/a-holm/paceq/internal/cli"
)

func main() {
	root, err := moduleRoot()
	if err != nil {
		log.Fatalf("find the repository root (go.mod): %v", err)
	}
	docsPath := filepath.Join(root, "docs", "reference", "cli.md")
	// #nosec G306 - a checked-in reference page is documentation, meant to be
	// read by every user of the repository; 0644 is the point, not an accident.
	if err := os.WriteFile(docsPath, []byte(cli.RenderCLIDocs()), 0o644); err != nil {
		log.Fatalf("write %s: %v", docsPath, err)
	}
	fmt.Printf("wrote %s\n", docsPath)
}

// moduleRoot walks up from the working directory to the directory holding
// go.mod. go generate runs a generator from the directive's package directory,
// so a path relative to the caller's shell does not survive the hop.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above %s", dir)
		}
		dir = parent
	}
}
