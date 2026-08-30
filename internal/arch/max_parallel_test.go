package arch_test

import (
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// A run's step budget lives in one column, runs.max_parallel, and the claim
// predicate reads it off the row it is about to write. A run born on the
// schema default therefore runs at whatever the default says instead of at
// what its job file declared, and nothing in the spec, the CLI or the tests
// of either layer can see the difference (#198). The only place that can be
// caught is here: every statement that creates a run has to name the column.
//
// inject.go is the exception, and it is the same one the transition guard
// makes: its whole purpose is planting broken rows for the fsck negative
// proofs, plus one bulk seeder for the retention gate. None of them
// materialises a run from a job version, so none of them has a budget to
// carry, and the schema default is the right answer for all of them.
var maxParallelExemptFiles = map[string]bool{
	"internal/store/inject.go": true,
}

func TestEveryRunInsertNamesItsParallelBudget(t *testing.T) {
	root := repoRoot(t)
	storeDir := filepath.Join(root, "internal", "store")

	entries, err := os.ReadDir(storeDir)
	if err != nil {
		t.Fatalf("read internal/store: %v", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		name := path.Join("internal/store", e.Name())
		if maxParallelExemptFiles[name] {
			continue
		}
		src, err := os.ReadFile(filepath.Join(storeDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(src)
		for i := 0; ; {
			at := strings.Index(text[i:], "INSERT INTO runs")
			if at < 0 {
				break
			}
			at += i
			// The column list is everything between the table name and
			// VALUES, which is where every insert in the package puts it.
			end := strings.Index(text[at:], "VALUES")
			if end < 0 {
				t.Errorf("%s: an INSERT INTO runs has no VALUES clause the guard can read", name)
				break
			}
			columns := text[at : at+end]
			if !strings.Contains(columns, "max_parallel") {
				line := 1 + strings.Count(text[:at], "\n")
				t.Errorf("%s:%d: this INSERT INTO runs does not name max_parallel, so the run it creates "+
					"runs at the schema default instead of the budget its job declared", name, line)
			}
			checked++
			i = at + end
		}
	}
	if checked == 0 {
		t.Fatal("the guard found no INSERT INTO runs at all, so it proved nothing")
	}
}
