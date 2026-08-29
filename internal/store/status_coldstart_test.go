package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The performance gate (issue #30, R4): `paceq status` answers in under 100
// milliseconds, cold process start to last byte, with 100 jobs and 100 000
// history rows. This is a user-facing promise, not an aspiration - the
// single-writer database must never become visible to the person who only
// asked "is everything fine?". The measurement is the whole shipped binary,
// process start and migration check included, because that is what cron
// actually runs. It lives in internal/store because the dataset it plants is
// SQL-shaped and every SQL literal belongs to this package.

// perfJobs and perfHistoryRows are the dataset shape the promise names.
const (
	perfJobs         = 100
	perfHistoryRows  = 100_000
	perfColdRuns     = 20 // the p99 sample
	perfBudgetMillis = 100
)

// perfOrigin is when every seeded run happened: inside the confirm window of
// the frozen clock the binary is asked on, far from any real today.
var perfOrigin = time.Date(2026, 12, 9, 8, 0, 0, 0, time.UTC)

func TestStatusColdStartUnder100ms(t *testing.T) {
	if testing.Short() {
		t.Skip("the cold-start budget needs real processes; skipped in -short")
	}

	dir := t.TempDir()
	dsn := filepath.Join(dir, DatabaseFileName)
	bin := buildPerfBinary(t)
	seedStatusPerfDataset(t, dsn)

	// One untimed warm-up so page cache and dynamic linking stop lying
	// about the first sample; the timed loop measures real runs after it.
	warm := exec.Command(bin, "--db", dsn, "status", "-o", "json")
	warm.Env = append(os.Environ(), "LC_ALL=C", "NO_COLOR=1")
	if out, err := warm.CombinedOutput(); err != nil {
		t.Fatalf("warm-up status failed: %v\n%s", err, out)
	}

	samples := make([]int64, 0, perfColdRuns)
	for i := 0; i < perfColdRuns; i++ {
		start := time.Now()
		cmd := exec.Command(bin, "--db", dsn, "status", "-o", "json")
		cmd.Env = append(os.Environ(), "LC_ALL=C", "NO_COLOR=1")
		out, err := cmd.Output()
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("status run %d failed: %v\n%s", i, err, out)
		}
		if !strings.Contains(string(out), `"schema_version":1`) {
			t.Fatalf("status run %d lost its schema_version: %.80s", i, out)
		}
		samples = append(samples, elapsed.Milliseconds())
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p99 := samples[(len(samples)*99+99)/100-1]
	t.Logf("cold-start ms over %d runs: %v (p99 %dms, budget %dms)",
		perfColdRuns, samples, p99, perfBudgetMillis)
	if p99 >= perfBudgetMillis {
		t.Errorf("status cold start p99 = %dms, budget is under %dms with %d jobs and %d history rows",
			p99, perfBudgetMillis, perfJobs, perfHistoryRows)
	}
}

// buildPerfBinary compiles the shipped binary once per test run. The go
// toolchain reads GOFLAGS from the child process environment, which matters:
// a worktree cannot stamp VCS state, and a spawned go build never sees this
// shell's exports.
func buildPerfBinary(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "paceq-perf")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/a-holm/paceq/cmd/paceq")
	cmd.Dir = repoRootForTests(t)
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=readonly -buildvcs=false", "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the perf binary: %v\n%s", err, out)
	}
	return bin
}

// repoRootForTests finds the module root from this file's location, so the
// child go build resolves the main package however the test binary was run.
func repoRootForTests(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("find the module root: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("no go.mod above the store package")
	return ""
}

// seedStatusPerfDataset plants 100 jobs and 100 000 finished runs. Jobs go
// through the store API so every invariant apply enforces holds; the history
// bulk goes through one transaction of batched inserts, because 100 000
// individual transactions would measure the seeder, not the answer. The runs
// reference each job's real current version, so no fixture rows exist
// outside what the store itself would have written.
func seedStatusPerfDataset(t *testing.T, dbPath string) {
	t.Helper()

	ctx := context.Background()
	s, err := Open(ctx, dbPath, Options{Clock: clock.NewFake(perfOrigin)})
	if err != nil {
		t.Fatalf("open the seeding store: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate the seeding store: %v", err)
	}
	for i := 0; i < perfJobs; i++ {
		name := fmt.Sprintf("job-%03d", i)
		spec := fmt.Sprintf(`{"schema":"paceq.job.v1","name":%q,"steps":[{"name":"work","run":["/bin/true"]}]}`, name)
		if _, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
			JobName:  name,
			SpecHash: "sha256:" + name,
			SpecJSON: spec,
		}); err != nil {
			t.Fatalf("seed job %s: %v", name, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close after seeding jobs: %v", err)
	}

	db, err := sql.Open(driverName, testDSN(t, dbPath, "mode=rwc"))
	if err != nil {
		t.Fatalf("open the seeding database: %v", err)
	}
	defer func() { _ = db.Close() }()

	versions := make(map[string]string, perfJobs)
	rows, err := db.QueryContext(ctx, `SELECT name, current_version_id FROM jobs`)
	if err != nil {
		t.Fatalf("read the seeded jobs: %v", err)
	}
	for rows.Next() {
		var name, version string
		if err := rows.Scan(&name, &version); err != nil {
			t.Fatalf("scan a seeded job: %v", err)
		}
		versions[name] = version
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the seeded jobs: %v", err)
	}
	rows.Close()
	if len(versions) != perfJobs {
		t.Fatalf("%d jobs seeded, want %d", len(versions), perfJobs)
	}

	base := perfOrigin.Add(-24 * time.Hour).UnixMilli()
	const batch = 500

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin the seeding transaction: %v", err)
	}
	inserted := 0
	var stmt strings.Builder
	args := make([]any, 0, batch*12)
	for inserted < perfHistoryRows {
		stmt.Reset()
		stmt.WriteString(`INSERT INTO runs (id, job_name, job_version_id, origin, state,
reason_code, available_at, created_at, started_at, finished_at, duration_ms, updated_at) VALUES `)
		args = args[:0]
		for b := 0; b < batch && inserted < perfHistoryRows; b++ {
			if b > 0 {
				stmt.WriteString(",")
			}
			job := fmt.Sprintf("job-%03d", inserted%perfJobs)
			started := base + int64(inserted/10) // ten finishes share each millisecond step
			finished := started + int64(inserted%97+1)
			stmt.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?)")
			args = append(args,
				fmt.Sprintf("SEED%020d", inserted), job, versions[job],
				"schedule", "succeeded", "RUN_SUCCEEDED",
				started, started, started, finished, finished-started, finished)
			inserted++
		}
		if _, err := tx.ExecContext(ctx, stmt.String(), args...); err != nil {
			_ = tx.Rollback()
			t.Fatalf("seed %d history rows: %v", inserted, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit the seeding transaction: %v", err)
	}
}
