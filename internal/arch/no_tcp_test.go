package arch_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The MVP speaks one transport: HTTP/1.1 over a unix domain socket (M2-08).
// A TCP listener would add a network surface the security model does not
// cover yet: no tokens, no TLS, authorization by file permissions alone.
// So the binary may not contain one, and this scan is the enforceable half
// of that promise: it fails on any net.Listen("tcp...") in shipped code.
//
// Test files are outside the promise (a test may stand up httptest), which
// is why they are skipped; everything else under cmd/ and internal/ ships.
func TestNoTCPListenerInTheBinary(t *testing.T) {
	root := repoRoot(t)

	var hits []string
	for _, dir := range []string{"cmd", "internal"} {
		dirRoot := filepath.Join(root, dir)
		err := filepath.WalkDir(dirRoot, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			name := entry.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for lineNo, line := range strings.Split(string(raw), "\n") {
				if strings.Contains(line, `net.Listen("tcp`) {
					rel, relErr := filepath.Rel(root, path)
					if relErr != nil {
						rel = path
					}
					hits = append(hits, rel+":"+strconv.Itoa(lineNo+1))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dirRoot, err)
		}
	}

	if len(hits) > 0 {
		t.Errorf("found %d TCP listener(s) in shipped code; the MVP listens on a unix socket only:\n%s",
			len(hits), strings.Join(hits, "\n"))
	}
}
