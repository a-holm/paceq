package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestOpenSurvivesFragmentAndQueryCharactersInThePath pins the DSN escaping:
// a project directory whose name carries a # or a ? must open exactly like
// any other. The sqlite driver reads the connection string as a URI, so an
// unescaped # silently truncated the filename at the fragment and every
// operation then ran against a different, freshly created database file.
func TestOpenSurvivesFragmentAndQueryCharactersInThePath(t *testing.T) {
	if !strings.Contains(dsn(filepath.Join("x#y", DatabaseFileName), "mode=ro", nil), "%23") {
		t.Fatalf("dsn does not escape #")
	}

	for _, dir := range []string{"jobs#2", "jobs?1"} {
		t.Run(dir, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, dir, DatabaseFileName)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatalf("create %s: %v", filepath.Dir(path), err)
			}
			st, err := Open(context.Background(), path, Options{})
			if err != nil {
				t.Fatalf("open %s: %v", path, err)
			}
			defer func() { _ = st.Close() }()
			if err := st.Migrate(context.Background()); err != nil {
				t.Fatalf("migrate %s: %v", path, err)
			}
			if _, _, err := st.UpsertJobVersion(context.Background(), JobVersionInput{
				JobName:  "escape",
				SpecHash: "sha256:escape",
				SpecJSON: `{"schema":"paceq.job.v1","name":"escape","steps":[{"name":"only","run":["/bin/true"],"shell":false}]}`,
			}); err != nil {
				t.Fatalf("write through an escaped path: %v", err)
			}
			_ = st.Close()

			ro, err := OpenReadOnly(context.Background(), path, Options{})
			if err != nil {
				t.Fatalf("reopen %s read-only: %v", path, err)
			}
			defer func() { _ = ro.Close() }()
			names, err := ro.JobNames(context.Background())
			if err != nil || len(names) != 1 || names[0] != "escape" {
				t.Fatalf("the written job did not survive its own path (names=%v err=%v)", names, err)
			}
		})
	}
}
