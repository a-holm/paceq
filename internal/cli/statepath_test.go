package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// TestAProjectDirectoryWithAURICharacterInItsName drives init, apply and run
// from a directory whose name carries one of the characters SQLite reads inside
// a file: URI. Every other test in this package works in a clean tempdir, which
// is why an unescaped path could ship: the commands all succeeded, against a
// database beside the one the project names.
func TestAProjectDirectoryWithAURICharacterInItsName(t *testing.T) {
	for _, name := range []string{"Sak#42", "jobs?1", "100%42", "my dir", "Ærlig"} {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, name)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatalf("create %s: %v", dir, err)
			}

			if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
				t.Fatalf("paceq init = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
			}
			writeJob(t, dir, "twostep.yaml", twoStepJob)
			if got := runCLI(t, dir, nil, "apply"); got.code != ExitOK {
				t.Fatalf("paceq apply = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
			}

			got := runCLI(t, dir, nil, "run", "twostep", "--wait")
			if got.code != ExitOK {
				t.Fatalf("paceq run twostep = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
			}
			var record runRecord
			if err := json.Unmarshal([]byte(got.stdout), &record); err != nil {
				t.Fatalf("stdout is not one run record:\n%s", got.stdout)
			}
			if record.Run.State != "succeeded" {
				t.Errorf("the run ended %s, want succeeded", record.Run.State)
			}

			// The state directory the project names is the one that holds the
			// work, and it is still readable after every command has closed it.
			s, err := store.OpenState(context.Background(), filepath.Join(dir, stateDirName), store.Options{})
			if err != nil {
				t.Fatalf("open the state store: %v", err)
			}
			defer func() { _ = s.Close() }()
			versions, err := s.ListJobVersions(context.Background(), "twostep")
			if err != nil {
				t.Fatalf("list the versions of twostep: %v", err)
			}
			if len(versions) != 1 {
				t.Errorf("the state holds %d versions of twostep, want 1", len(versions))
			}

			// A URI cut short at # or ? names a file one directory up, which
			// SQLite creates without a word. Nothing may exist beside the
			// project directory.
			entries, err := os.ReadDir(parent)
			if err != nil {
				t.Fatalf("read %s: %v", parent, err)
			}
			for _, entry := range entries {
				if entry.Name() != name {
					t.Errorf("%s appeared beside the project: the DSN named a database outside it",
						filepath.Join(parent, entry.Name()))
				}
			}
		})
	}
}
