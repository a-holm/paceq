package arch_test

import (
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// zoneReaderExemptions are the directories that may call time.LoadLocation
// directly, with the reason each one is not a second opinion on a schedule's
// zone.
//
// internal/cronx owns the rule and is what everything else asks.
// internal/doctor probes the zone database itself: it reports whether this
// machine can read tzdata at all, and the name it loads is the host's own
// local zone, which is exactly the name a schedule may not carry.
var zoneReaderExemptions = map[string]string{
	filepath.Join("internal", "cronx"):  "owns the rule",
	filepath.Join("internal", "doctor"): "probes the zone database itself",
}

// TestZoneNamesHaveOneAuthority keeps the answer to "is this time zone usable"
// in one function. time.LoadLocation is not that answer: it accepts "Local",
// which reads the host environment and makes a schedule mean different things
// on different machines, so cronx.LoadZone refuses it. A second reader that
// asks time.LoadLocation directly accepts files the scheduler then refuses on
// every wake, which is #214 exactly.
func TestZoneNamesHaveOneAuthority(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	forbidden := map[string]bool{"LoadLocation": true}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "bin", "dist", "testdata":
				return fs.SkipDir
			}
			if strings.HasPrefix(d.Name(), ".") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if _, exempt := zoneReaderExemptions[filepath.Dir(rel(root, path))]; exempt {
			return nil
		}

		uses, err := findTimeUses(fset, path, forbidden)
		if err != nil {
			t.Errorf("parse %s: %v", rel(root, path), err)
			return nil
		}
		for _, u := range uses {
			u.file = rel(root, u.file)
			t.Errorf("%s: forbidden, call cronx.LoadZone instead: it is the rule the scheduler, "+
				"the reconciler and schedules show all read, and it refuses the host dependent "+
				"zone names time.LoadLocation accepts", u)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}
