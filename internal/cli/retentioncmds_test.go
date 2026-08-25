package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// retentionProject seeds a project whose history is mostly past every
// horizon: one job with old finished runs (plus the keep-minimum's worth of
// even older ones the floor must shield) and one recent run that must
// survive anything. The store is closed before the command runs, exactly as
// a second process would find it.
func retentionProject(t *testing.T, oldRuns int) (dir string, recentRunID string) {
	t.Helper()

	dir = t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(testOrigin))
	ctx := context.Background()

	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}

	ancient := time.Now().UTC().AddDate(-2, 0, 0).Truncate(time.Minute)
	if err := s.SeedOldFinishedRuns(ctx, "nightly", version.ID, oldRuns, 1, ancient); err != nil {
		t.Fatalf("seed old runs: %v", err)
	}

	recent, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the recent run: %v", err)
	}
	if _, _, err := s.ClaimRun(ctx, recent.ID, store.LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ref := store.LeaseRef{Owner: "test", Epoch: 1}
	if err := s.StartStep(ctx, recent.ID, "extract", ref); err != nil {
		t.Fatalf("start extract: %v", err)
	}
	zero := 0
	if err := s.RecordStepOutcome(ctx, recent.ID, "extract", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		ExitCode:   &zero,
	}, ref); err != nil {
		t.Fatalf("finish extract: %v", err)
	}
	if _, err := s.FinishRun(ctx, recent.ID, ref,
		store.FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("finish the recent run: %v", err)
	}
	if err := s.AppendRunEvent(ctx, store.RunEvent{
		RunID: recent.ID, Kind: "run.succeeded", Actor: "test",
	}); err != nil {
		t.Fatalf("append an event: %v", err)
	}
	if err := os.Chmod(filepath.Join(stateDir, store.DatabaseFileName), 0o600); err != nil {
		t.Fatalf("tighten the database mode: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close the seeding store: %v", err)
	}
	return dir, recent.ID
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestPruneDryRunLeavesTheDatabaseBitIdentical(t *testing.T) {
	dir, _ := retentionProject(t, 300)

	dbPath := filepath.Join(dir, stateDirName, store.DatabaseFileName)
	before := sha256File(t, dbPath)

	got := runCLI(t, dir, nil, "prune", "--dry-run", "-o", "text")
	if got.code != ExitOK {
		t.Fatalf("prune --dry-run = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	for _, want := range []string{"runs", "run_keys", "--dry-run"} {
		if !strings.Contains(got.stdout, want) {
			t.Fatalf("the plan never mentions %q:\n%s", want, got.stdout)
		}
	}
	// The newest fifty finished runs of the job are the floor; the recent
	// run is one of them, so only forty-nine old runs join it.
	if !strings.Contains(got.stdout, fmt.Sprint(300-store.DefaultPolicies().RunsKeepMin+1)) {
		t.Fatalf("the plan does not show the deletable count:\n%s", got.stdout)
	}
	if after := sha256File(t, dbPath); before != after {
		t.Fatal("a dry-run changed the database file")
	}
}

func TestPruneAppliesAndKeepsTheFloor(t *testing.T) {
	dir, recent := retentionProject(t, 300)

	got := runCLI(t, dir, nil, "prune")
	if got.code != ExitOK {
		t.Fatalf("prune = %d\n%s%s", got.code, got.stdout, got.stderr)
	}

	ctx := context.Background()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(testOrigin))
	defer func() { _ = s.Close() }()

	oldLeft, err := s.CountRows(ctx, "runs")
	if err != nil {
		t.Fatalf("count runs: %v", err)
	}
	// The newest fifty finished runs of the job survive as the floor, and
	// the recent run is one of them: forty-nine old seeds plus the recent.
	if want := int64(store.DefaultPolicies().RunsKeepMin); oldLeft != want {
		t.Fatalf("%d runs survived, want %d", oldLeft, want)
	}

	detail, err := s.GetRun(ctx, recent)
	if err != nil {
		t.Fatalf("the recent run did not survive: %v", err)
	}
	if detail.State != "succeeded" {
		t.Fatalf("the recent run came back as %q", detail.State)
	}
}

func TestDbCompactWithoutTheFlagRefusesWithExitTwo(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d\n%s%s", got.code, got.stdout, got.stderr)
	}

	got := runCLI(t, dir, nil, "db", "compact")
	if got.code != ExitUsage {
		t.Fatalf("db compact without the flag = %d, want exit %d\n%s%s",
			got.code, ExitUsage, got.stdout, got.stderr)
	}
	for _, want := range []string{"--i-know-this-blocks", "exclusive lock"} {
		if !strings.Contains(got.stderr+got.stdout, want) {
			t.Fatalf("the refusal never says %q:\n%s%s", want, got.stderr, got.stdout)
		}
	}

	ok := runCLI(t, dir, nil, "db", "compact", "--i-know-this-blocks")
	if ok.code != ExitOK {
		t.Fatalf("db compact --i-know-this-blocks = %d\n%s%s", ok.code, ok.stdout, ok.stderr)
	}
}

func TestExportRunBundlesReadableEvidence(t *testing.T) {
	dir, runID := retentionProject(t, 10)

	// A log attempt on disk, laid out exactly as the runner leaves it.
	logRel := filepath.Join(
		time.Now().UTC().Format("2006-01-02"), runID, "extract.1.ndjson")
	logAbs := filepath.Join(dir, stateDirName, "logs", logRel)
	if err := os.MkdirAll(filepath.Dir(logAbs), 0o700); err != nil {
		t.Fatal(err)
	}
	logPayload := []byte(`{"t":1,"line":"first"}` + "\n" + `{"t":2,"line":"second"}` + "\n")
	if err := os.WriteFile(logAbs, logPayload, 0o600); err != nil {
		t.Fatal(err)
	}

	outPath := filepath.Join(t.TempDir(), "proof.tar.gz")
	got := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"},
		"export", "run", runID[:12], "-o", outPath)
	if got.code != ExitOK {
		t.Fatalf("export run = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("no archive was written: %v", err)
	}

	rows := readTarFile(t, outPath, "rows.json")
	var doc struct {
		Run    map[string]any   `json:"run"`
		Events []map[string]any `json:"run_events"`
	}
	if err := json.Unmarshal(rows, &doc); err != nil {
		t.Fatalf("rows.json does not parse: %v", err)
	}
	if doc.Run["ID"] != runID {
		t.Fatalf("rows.json carries run %v, want %s", doc.Run["ID"], runID)
	}
	if len(doc.Events) == 0 {
		t.Fatal("rows.json has no events")
	}

	logged := readTarFile(t, outPath, "logs/extract.1.ndjson")
	if string(logged) != string(logPayload) {
		t.Fatalf("the archived log differs from the file on disk")
	}

	// The manifest names every file with a sha256 that matches the payload.
	manifestRaw := readTarFile(t, outPath, "manifest.json")
	var manifest struct {
		RunID string            `json:"run_id"`
		Files map[string]string `json:"files"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatalf("manifest.json does not parse: %v", err)
	}
	if manifest.RunID != runID {
		t.Fatalf("manifest names run %q", manifest.RunID)
	}
	for name, want := range manifest.Files {
		payload := readTarFile(t, outPath, name)
		sum := sha256.Sum256(payload)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Fatalf("manifest sha256 for %s is %s, archive carries %s", name, want, got)
		}
	}
	for _, required := range []string{"rows.json", "logs/extract.1.ndjson"} {
		if _, ok := manifest.Files[required]; !ok {
			t.Fatalf("the manifest never names %s", required)
		}
	}
	if _, self := manifest.Files["manifest.json"]; self {
		t.Fatal("the manifest names itself; its own hash cannot be inside itself")
	}
}

func TestExportRunNamesAMissingPrefix(t *testing.T) {
	dir, _ := retentionProject(t, 5)
	got := runCLI(t, dir, nil, "export", "run", "ZZZZZZZZZZZZZZZZZZZZZZZZZZ")
	if got.code != ExitNotFound {
		t.Fatalf("export of a nonsense prefix = %d, want exit %d\n%s%s",
			got.code, ExitNotFound, got.stdout, got.stderr)
	}
}

// readTarFile extracts one member of a tar.gz archive by name.
func readTarFile(t *testing.T, archive, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatalf("read %s: %v", archive, err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("%s is not gzip: %v", archive, err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			t.Fatalf("%s holds no member named %s", archive, name)
		}
		if err != nil {
			t.Fatalf("walk %s: %v", archive, err)
		}
		if hdr.Name != name {
			continue
		}
		payload, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read %s from %s: %v", name, archive, err)
		}
		return payload
	}
}
