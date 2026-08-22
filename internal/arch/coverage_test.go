package arch_test

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// coverageFloors are the packages whose statement coverage is gated, and the
// floor each one has to clear. A package earns a line here by being pure: with
// no I/O to mock there is no honest excuse for an unreached statement, and the
// floor is what stops a rule from being added without a test that exercises it.
//
// Go measures statements, not branches. The branch level guarantee for
// internal/model comes from the package's own sweeps, which enumerate every
// pair of state and event against every combination of the guards; this gate is
// what keeps the statements underneath them covered.
var coverageFloors = map[string]float64{
	"model": 100,
}

// TestCoveredPackagesStayCovered runs each gated package with coverage on and
// fails with the uncovered lines named. It runs the package a second time,
// without the race detector, which costs about a second for a package with no
// I/O.
func TestCoveredPackagesStayCovered(t *testing.T) {
	for name, floor := range coverageFloors {
		t.Run(name, func(t *testing.T) {
			profile := filepath.Join(t.TempDir(), "cover.out")
			runGo(t, "test", "-count=1", "-covermode=set", "-coverprofile="+profile, internalPrefix+name)

			covered, total, uncovered := readProfile(t, profile)
			if total == 0 {
				t.Fatalf("the profile for internal/%s holds no statements, so the floor proved nothing", name)
			}

			percent := 100 * float64(covered) / float64(total)
			if percent+1e-9 < floor {
				t.Errorf("internal/%s covers %.1f%% of its %d statements, want at least %.1f%%\nuncovered:\n  %s",
					name, percent, total, floor, strings.Join(uncovered, "\n  "))
			}
		})
	}
}

// readProfile totals the statements in a coverage profile and lists the blocks
// nothing reached, so a failure names lines rather than a percentage.
func readProfile(t *testing.T, path string) (covered, total int, uncovered []string) {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the coverage profile: %v", err)
	}

	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		// mode: set, then one line per block:
		// path/file.go:startLine.col,endLine.col numStatements count
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		block, counts, ok := strings.Cut(line, " ")
		if !ok {
			t.Fatalf("coverage line %q is not a block and its counts", line)
		}
		statements, count, ok := strings.Cut(counts, " ")
		if !ok {
			t.Fatalf("coverage line %q does not carry a statement count and a hit count", line)
		}
		n, err := strconv.Atoi(statements)
		if err != nil {
			t.Fatalf("coverage line %q has an unreadable statement count: %v", line, err)
		}
		hits, err := strconv.Atoi(count)
		if err != nil {
			t.Fatalf("coverage line %q has an unreadable hit count: %v", line, err)
		}

		total += n
		if hits > 0 {
			covered += n
			continue
		}
		uncovered = append(uncovered, blockLocation(block))
	}
	sort.Strings(uncovered)
	return covered, total, uncovered
}

// blockLocation turns a profile block into the file and line a person can open.
func blockLocation(block string) string {
	path, span, ok := strings.Cut(block, ":")
	if !ok {
		return block
	}
	start, _, _ := strings.Cut(span, ",")
	line, _, _ := strings.Cut(start, ".")
	return filepath.Base(filepath.Dir(path)) + "/" + filepath.Base(path) + ":" + line
}
