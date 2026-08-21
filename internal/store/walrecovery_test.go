//go:build unix

package store

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	// killEnv carries the database path to the child process. Its presence is
	// what turns the helper test into the write burst that gets killed.
	killEnv = "PACEQ_WAL_KILL_DB"

	// nightlyEnv turns the nightly iteration count on. A pull request pays for
	// 25 kills; the long run belongs on a schedule.
	nightlyEnv = "PACEQ_NIGHTLY"
)

// killIterations is 25 on a pull request and 1000 when the nightly variable is
// set. One kill proves the mechanism; the repetition is what catches a recovery
// path that only breaks when the process dies at an unlucky point in the WAL.
func killIterations() int {
	if os.Getenv(nightlyEnv) != "" {
		return 1000
	}
	return 25
}

// readyLine is what the child prints once its burst is committing rows. The
// parent waits for it instead of guessing how long a test binary takes to
// start, which is what keeps the kill inside the burst rather than before it.
const readyLine = "burst-ready"

// killDelay spreads the kill across the burst without a random source. Measured
// from the ready line, the delay walks 20 ms to 65 ms, so successive iterations
// land at different points in the write ahead log and a failing iteration
// reproduces from its index.
func killDelay(iteration int) time.Duration {
	return 20*time.Millisecond + time.Duration(iteration%10)*5*time.Millisecond
}

// waitForBurst reads the child's output until it announces that it is writing.
func waitForBurst(t *testing.T, iteration int, out io.Reader) {
	t.Helper()

	scanner := bufio.NewScanner(out)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == readyLine {
			return
		}
	}
	t.Fatalf("iteration %d: the child never started writing: %v", iteration, scanner.Err())
}

// TestWALRecoveryUnderKill kills a real process mid write burst and reopens the
// database. Recovering the write ahead log is SQLite's job, not ours; what this
// checks is that our settings, journal_size_limit and wal_autocheckpoint among
// them, never leave a file SQLite cannot recover. A committed transaction has
// to survive whole, and integrity_check has to say "ok" every single time.
func TestWALRecoveryUnderKill(t *testing.T) {
	if os.Getenv(killEnv) != "" {
		t.Skip("child process, driven by the parent test")
	}
	if testing.Short() {
		t.Skip("spawns a process per iteration")
	}
	// What this test watches is a file left behind by a process that was shot,
	// which the race detector has nothing to say about. It runs in the gate
	// target instead of paying for a race build of every child.
	if raceEnabled {
		t.Skip("no data race to find here, the gate target runs it")
	}

	iterations := killIterations()
	for i := range iterations {
		path := filepath.Join(t.TempDir(), "state.db")
		cmd := exec.Command(os.Args[0], "-test.run=TestWriteBurstChild", "-test.count=1")
		cmd.Env = append(os.Environ(), killEnv+"="+path)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("iteration %d: pipe the child output: %v", i, err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatalf("iteration %d: start the write burst: %v", i, err)
		}
		waitForBurst(t, i, out)
		time.Sleep(killDelay(i))
		if err := cmd.Process.Kill(); err != nil {
			t.Fatalf("iteration %d: kill the write burst: %v", i, err)
		}
		waitErr := cmd.Wait()

		var status *exec.ExitError
		if !asExitError(waitErr, &status) || status.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("iteration %d: child exited with %v, want death by SIGKILL", i, waitErr)
		}
		assertRecovers(t, i, path)
	}
	t.Logf("survived %d kills mid write burst, set %s to run 1000", iterations, nightlyEnv)
}

// assertRecovers reopens a database whose writer was killed and checks both
// that SQLite can read it and that the rows it holds are the ones that were
// committed: a contiguous run from 1, with no gap where a torn write would be.
func assertRecovers(t *testing.T, iteration int, path string) {
	t.Helper()

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("iteration %d: reopen after SIGKILL: %v", iteration, err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	var integrity string
	if err := s.w.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("iteration %d: integrity_check: %v", iteration, err)
	}
	if integrity != "ok" {
		t.Fatalf("iteration %d: integrity_check reports %q, want \"ok\"", iteration, integrity)
	}

	var rows, highest, distinct int64
	err = s.w.QueryRowContext(ctx,
		"SELECT COUNT(*), COALESCE(MAX(n), 0), COUNT(DISTINCT n) FROM burst").Scan(&rows, &highest, &distinct)
	if err != nil {
		t.Fatalf("iteration %d: read the burst table: %v", iteration, err)
	}
	if rows == 0 {
		t.Fatalf("iteration %d: no committed row survived, so the kill landed before any write "+
			"and the iteration proves nothing", iteration)
	}
	if rows != highest || rows != distinct {
		t.Fatalf("iteration %d: %d rows with a highest n of %d and %d distinct values: a committed "+
			"transaction did not survive whole", iteration, rows, highest, distinct)
	}
}

// readyAfter is how many rows the child commits before it announces itself. A
// handful is enough that the parent's kill lands in a burst already in flight.
const readyAfter = 5

// TestWriteBurstChild is the child half. It writes as fast as it can and never
// returns: the parent kills it. It runs only when the parent sets the
// environment variable holding the database path.
func TestWriteBurstChild(t *testing.T) {
	path := os.Getenv(killEnv)
	if path == "" {
		t.Skip("driven by TestWALRecoveryUnderKill")
	}

	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	ctx := context.Background()
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec("CREATE TABLE burst (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, payload TEXT NOT NULL) STRICT")
		return err
	})
	if err != nil {
		t.Fatalf("create the burst table: %v", err)
	}

	// The payload is wide enough that a transaction spans pages, so a kill has
	// something to land in the middle of.
	payload := strconv.Quote(string(make([]byte, 512)))
	for n := 1; ; n++ {
		if n == readyAfter {
			fmt.Println(readyLine)
		}
		err := s.withTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.Exec("INSERT INTO burst (n, payload) VALUES (?, ?)", n, payload)
			return err
		})
		if err != nil {
			t.Fatalf("write %d: %v", n, err)
		}
	}
}
