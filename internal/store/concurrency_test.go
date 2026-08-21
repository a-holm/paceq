package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// The write model rests on two claims that cost nothing to state and everything
// to get wrong: one writer connection makes SQLITE_BUSY impossible inside this
// process, and WAL lets readers run while that writer holds the lock. This file
// is where both are proven against a real file, under real contention, with the
// numbers the driver decision depends on.
const (
	// gateWriters is the writer count the plan fixes. Real load is under one
	// write per second, so 32 is not realism: it is enough queue pressure that a
	// write model serialising anywhere other than the pool would show it.
	gateWriters = 32

	// gateReaders run against the same file throughout. Their job is to fail if
	// the writer ever blocks a reader.
	gateReaders = 8

	// gateFloor is the throughput gate. Below it the modernc driver decision is
	// reopened while the codebase is small enough to change it.
	gateFloor = 500.0

	// walLimit is PRAGMA journal_size_limit. The WAL staying under it is what
	// proves wal_autocheckpoint is doing its work under sustained writes.
	walLimit = 64 << 20

	// readerPace is the gap a reader leaves between reads.
	readerPace = time.Millisecond

	// readCeiling is the largest read latency compatible with the WAL claim. It
	// is far above any measured value on purpose: what it has to catch is a
	// reader waiting on the write lock, which would show up as seconds.
	readCeiling = time.Second
)

// gateWindow is the load window. The race detector multiplies the cost of every
// transaction, so under it the window shrinks and only the correctness
// assertions run.
func gateWindow() time.Duration {
	if raceEnabled {
		return 2 * time.Second
	}
	return 10 * time.Second
}

// gateStore opens a store on a real file and creates the synthetic tables the
// gate writes to. The domain schema does not exist yet, so the tables mirror
// the shape of a warm write instead: append a row, then move a head row forward
// with RETURNING.
//
// Writers share a key on purpose. A gate where every writer owns its own row
// would never exercise the read-compute-write race that the single writer
// connection exists to serialise.
func gateStore(t *testing.T) *Store {
	t.Helper()

	s, err := Open(context.Background(), tempPath(t), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	err = s.withTx(context.Background(), func(tx *sql.Tx) error {
		stmts := []string{
			`CREATE TABLE gate (
				id INTEGER PRIMARY KEY,
				k INTEGER NOT NULL,
				n INTEGER NOT NULL,
				at INTEGER NOT NULL
			) STRICT`,
			`CREATE UNIQUE INDEX gate_k_n ON gate (k, n)`,
			`CREATE TABLE gate_head (k INTEGER PRIMARY KEY, n INTEGER NOT NULL) STRICT`,
		}
		for _, stmt := range stmts {
			if _, err := tx.Exec(stmt); err != nil {
				return err
			}
		}
		for k := range gateKeys {
			if _, err := tx.Exec("INSERT INTO gate_head (k, n) VALUES (?, 0)", k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("create the gate tables: %v", err)
	}
	return s
}

// gateKeys is how many head rows the writers share. Eight writers per key is
// enough contention that a lost update surfaces within the window.
const gateKeys = 4

// writeTiming splits a write into the two durations that mean different things.
// Queue time is spent waiting for the single write connection and is meant to
// grow with load. Hold time is BEGIN IMMEDIATE to COMMIT, the scarce resource,
// and is the number that predicts production trouble.
type writeTiming struct {
	queue []time.Duration
	hold  []time.Duration
}

// TestConcurrentWriters is the write strategy's existence proof, and it is
// never to be marked flaky. One SQLITE_BUSY here means the model is implemented
// wrong, not that the runner was busy.
//
// It asserts four things at once: no busy error reaches a caller, the write
// lock serialises callbacks to exactly one at a time, concurrent readers keep
// running while the writer works, and the read-compute-write sequence loses no
// update. It measures a fifth: the throughput the driver decision rests on.
func TestConcurrentWriters(t *testing.T) {
	if testing.Short() {
		t.Skip("the load window is seconds long")
	}

	s := gateStore(t)
	window := gateWindow()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()

	var inFlight, peakInFlight atomic.Int64
	timings := make([]writeTiming, gateWriters)
	write := func(ctx context.Context, worker int) error {
		key := worker % gateKeys
		callStart := time.Now()
		var entered time.Time
		err := s.withTx(ctx, func(tx *sql.Tx) error {
			entered = time.Now()
			if n := inFlight.Add(1); n > peakInFlight.Load() {
				peakInFlight.Store(n)
			}
			defer inFlight.Add(-1)

			var n int64
			if err := tx.QueryRow("SELECT n FROM gate_head WHERE k = ?", key).Scan(&n); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO gate (k, n, at) VALUES (?, ?, ?)",
				key, n+1, s.clk.Now().UnixMilli()); err != nil {
				return err
			}
			var moved int64
			if err := tx.QueryRow("UPDATE gate_head SET n = ? WHERE k = ? RETURNING n",
				n+1, key).Scan(&moved); err != nil {
				return err
			}
			if moved != n+1 {
				return fmt.Errorf("head for key %d moved to %d, want %d", key, moved, n+1)
			}
			return nil
		})
		if err == nil {
			timings[worker].queue = append(timings[worker].queue, entered.Sub(callStart))
			timings[worker].hold = append(timings[worker].hold, time.Since(entered))
		}
		return err
	}

	// Readers record the lowest and highest head they see. Two different values
	// prove they ran while writes were committing, which is what makes their
	// latency worth reading at all. The read is a single row by primary key, so
	// its cost does not drift as the writers fill the table.
	var seenLow, seenHigh atomic.Int64
	seenLow.Store(math.MaxInt64)
	read := func(ctx context.Context, _ int) error {
		// Readers pace themselves. They are here to prove that a reader is never
		// blocked by the writer, not to compete with it for CPU: an unthrottled
		// reader loop measures the runner's core count, not the write model.
		defer time.Sleep(readerPace)
		var head int64
		err := s.withRead(ctx, func(ctx context.Context, r reader) error {
			return r.QueryRowContext(ctx, "SELECT n FROM gate_head WHERE k = 0").Scan(&head)
		})
		if err != nil {
			return err
		}
		for low := seenLow.Load(); head < low; low = seenLow.Load() {
			if seenLow.CompareAndSwap(low, head) {
				break
			}
		}
		for high := seenHigh.Load(); head > high; high = seenHigh.Load() {
			if seenHigh.CompareAndSwap(high, head) {
				break
			}
		}
		return nil
	}

	reads := make(chan loadResult, 1)
	go func() { reads <- runLoad(ctx, gateReaders, read) }()
	writes := runLoad(ctx, gateWriters, write)
	readResult := <-reads

	var queue, hold []time.Duration
	for i := range timings {
		queue = append(queue, timings[i].queue...)
		hold = append(hold, timings[i].hold...)
	}
	queued, held := loadResult{lat: queue}, loadResult{lat: hold}

	t.Log(writes.summary(fmt.Sprintf("%d writers", gateWriters)))
	t.Log(readResult.summary(fmt.Sprintf("%d readers", gateReaders)))
	t.Logf("write lock hold p50=%s p99=%s max=%s",
		held.quantile(0.5).Round(time.Microsecond),
		held.quantile(0.99).Round(time.Microsecond),
		held.quantile(1).Round(time.Microsecond))
	t.Logf("writer queue wait p50=%s p99=%s max=%s",
		queued.quantile(0.5).Round(time.Microsecond),
		queued.quantile(0.99).Round(time.Microsecond),
		queued.quantile(1).Round(time.Microsecond))
	t.Logf("wal after the window: %d bytes (limit %d)", walSize(t, s), walLimit)

	for _, err := range writes.failed {
		t.Errorf("writer: %v", err)
	}
	for _, err := range readResult.failed {
		t.Errorf("reader: %v", err)
	}
	if writes.busy+readResult.busy != 0 {
		t.Fatalf("SQLITE_BUSY reached a caller %d times (writers %d, readers %d): with one write "+
			"connection and _txlock=immediate this process cannot compete with itself for the "+
			"write lock, so the write model is implemented wrong",
			writes.busy+readResult.busy, writes.busy, readResult.busy)
	}
	if peak := peakInFlight.Load(); peak != 1 {
		t.Errorf("%d write transactions ran at once, want 1: writes are not serialised by the "+
			"connection pool", peak)
	}
	if writes.ops == 0 {
		t.Fatal("no write committed in the window")
	}

	assertNoLostUpdate(t, s, writes.ops)

	if readResult.ops == 0 {
		t.Fatal("no read completed while the writers ran: the writer is blocking readers, which is " +
			"the whole reason this database runs in WAL mode")
	}
	if seenLow.Load() == seenHigh.Load() {
		t.Errorf("readers only ever saw head %d: they did not overlap the writers, so their "+
			"latency proves nothing", seenHigh.Load())
	}
	if got := readResult.quantile(0.99); got > readCeiling {
		t.Errorf("read p99 is %s, want under %s: a reader waited long enough to have been blocked "+
			"on the write lock", got, readCeiling)
	}
	if size := walSize(t, s); size >= walLimit {
		t.Errorf("wal is %d bytes, at or above the %d byte journal_size_limit: wal_autocheckpoint "+
			"is not keeping up", size, walLimit)
	}

	if raceEnabled {
		t.Logf("throughput gate not asserted: the race detector is on, and %.0f/s under it says "+
			"nothing about the driver", writes.rate())
		return
	}
	if writes.rate() < gateFloor {
		t.Fatalf("PERFORMANCE GATE: %.0f write tx/s is below the floor of %.0f. The modernc.org/sqlite "+
			"decision has to be reopened now, while the codebase is small: see docs/adr/0003-write-throughput.md",
			writes.rate(), gateFloor)
	}
}

// assertNoLostUpdate checks the invariant the single writer exists to hold. Per
// key the rows must be exactly 1..N with no gap and no repeat, the head row
// must equal N, and the rows must add up to what the harness counted. Two
// transactions reading the same head and writing the same n would break all
// three.
func assertNoLostUpdate(t *testing.T, s *Store, commits int) {
	t.Helper()

	var total int64
	err := s.withRead(context.Background(), func(ctx context.Context, r reader) error {
		if err := r.QueryRowContext(ctx, "SELECT COUNT(*) FROM gate").Scan(&total); err != nil {
			return err
		}
		rows, err := r.QueryContext(ctx, `SELECT h.k, h.n, COUNT(g.n), COUNT(DISTINCT g.n), COALESCE(MAX(g.n), 0)
			FROM gate_head h LEFT JOIN gate g ON g.k = h.k GROUP BY h.k ORDER BY h.k`)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var k, head, count, distinct, highest int64
			if err := rows.Scan(&k, &head, &count, &distinct, &highest); err != nil {
				return err
			}
			if count != distinct {
				t.Errorf("key %d has %d rows but only %d distinct values of n: two transactions read "+
					"the same head and both wrote from it", k, count, distinct)
			}
			if highest != count {
				t.Errorf("key %d has %d rows and a highest n of %d: the sequence has a gap", k, count, highest)
			}
			if head != highest {
				t.Errorf("key %d has head %d and highest row %d: the head and the rows disagree", k, head, highest)
			}
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatalf("read back the gate tables: %v", err)
	}
	if total != int64(commits) {
		t.Errorf("the gate table holds %d rows after %d committed transactions", total, commits)
	}
}

// walSize is the size of the write ahead log beside the database file.
func walSize(t *testing.T, s *Store) int64 {
	t.Helper()

	info, err := os.Stat(s.Path() + "-wal")
	if err != nil {
		t.Fatalf("stat the wal: %v", err)
	}
	return info.Size()
}

// TestGateRunsAgainstARealFile guards the one condition the whole gate depends
// on. An in-memory database has no WAL and no file locking, so a gate that
// silently ran against one would prove nothing while staying green.
func TestGateRunsAgainstARealFile(t *testing.T) {
	s := gateStore(t)

	info, err := os.Stat(s.Path())
	if err != nil {
		t.Fatalf("the store is not backed by a file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("the database file is empty")
	}
	if _, err := os.Stat(s.Path() + "-wal"); err != nil {
		t.Errorf("no write ahead log beside the database: %v", err)
	}
}

// TestWritersUnwindWhenTheContextIsCancelledUnderLoad covers the case a caller
// hits in production: a shutdown or a timeout landing while 32 goroutines are
// queued behind one write connection. Every queued writer has to come back with
// the context error rather than a busy error, and the database has to be usable
// straight afterwards.
func TestWritersUnwindWhenTheContextIsCancelledUnderLoad(t *testing.T) {
	s := gateStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int64
	done := make(chan loadResult, 1)
	go func() {
		done <- runLoad(ctx, gateWriters, func(ctx context.Context, worker int) error {
			attempts.Add(1)
			return s.withTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO gate (k, n, at) VALUES (?, ?, ?)",
					worker, attempts.Load(), s.clk.Now().UnixMilli())
				return err
			})
		})
	}()

	for attempts.Load() < gateWriters {
		time.Sleep(time.Millisecond)
	}
	cancel()

	var result loadResult
	select {
	case result = <-done:
	case <-time.After(readDeadline):
		t.Fatalf("writers did not unwind within %s of the context being cancelled", readDeadline)
	}

	for _, err := range result.failed {
		t.Errorf("cancellation surfaced as %v, want the context error", err)
	}
	if result.busy != 0 {
		t.Errorf("cancellation produced %d busy errors", result.busy)
	}

	err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO gate (k, n, at) VALUES (99, 1, 0)")
		return err
	})
	if err != nil {
		t.Fatalf("the store is unusable after a cancellation under load: %v", err)
	}
}

// TestExternalWriterIsAbsorbedByBusyTimeout puts busy_timeout where it belongs.
// It is not the strategy for our own writers, which cannot collide; it is the
// safety net for a second process on the same file, such as the sqlite3 shell
// or the CLI in direct mode. The write here has to wait and then succeed, not
// fail.
func TestExternalWriterIsAbsorbedByBusyTimeout(t *testing.T) {
	const hold = 150 * time.Millisecond

	s := gateStore(t)
	ext := competingWriter(t, s.Path())

	tx, err := ext.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("external writer could not begin: %v", err)
	}
	if _, err := tx.Exec("UPDATE gate_head SET n = n + 1 WHERE k = 0"); err != nil {
		t.Fatalf("external writer could not write: %v", err)
	}

	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(hold)
		if err := tx.Commit(); err != nil {
			t.Errorf("external writer could not commit: %v", err)
		}
	}()

	start := time.Now()
	err = s.withTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("UPDATE gate_head SET n = n + 1 WHERE k = 1")
		return err
	})
	waited := time.Since(start)
	<-released

	if err != nil {
		t.Fatalf("a write behind an external writer failed after %s: %v; busy_timeout is the "+
			"safety net for exactly this case", waited, err)
	}
	if waited < hold/2 {
		t.Fatalf("the write returned after %s, less than the %s the external writer held the lock: "+
			"the external writer never took it, so the test proves nothing", waited, hold)
	}
	t.Logf("waited %s behind an external writer holding the lock for %s", waited.Round(time.Millisecond), hold)
}

// externalWindow is short: the failure it looks for happens within microseconds
// of a read, thousands of times a second, so a long window buys nothing.
const externalWindow = 2 * time.Second

// hammeringWriter is a second connection to the same file with a busy_timeout
// long enough that it keeps making progress while our writers hold the lock.
// The competing writer in the lock tests wants the opposite, a short timeout, so
// that it fails fast.
func hammeringWriter(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", "file:"+path+"?_txlock=immediate&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open the external writer: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestConcurrentWritersBesideAnExternalWriter is the gate for the other half of
// the write model. One write connection is what keeps this process from
// competing with itself; _txlock=immediate is what keeps a transaction from
// taking a read lock first and then failing to upgrade it. Nothing inside this
// process can produce that failure, so the gate brings in a second process's
// worth of pressure: another connection to the same file, committing as fast as
// it can, exactly as the CLI in direct mode or a sqlite3 shell would.
//
// Two assertions, and the second is the sharp one. No busy error may reach a
// caller. And every withTx call must run its callback exactly once: a lock
// upgrade that failed would be retried, so a callback running twice is a
// transaction that began without the write lock, even when the retry hides the
// error.
func TestConcurrentWritersBesideAnExternalWriter(t *testing.T) {
	const writers = 8

	s := gateStore(t)
	ext := hammeringWriter(t, s.Path())
	if err := s.withTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("CREATE TABLE ext (id INTEGER PRIMARY KEY, n INTEGER NOT NULL) STRICT")
		return err
	}); err != nil {
		t.Fatalf("create the external writer's table: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), externalWindow)
	defer cancel()

	// The external writer commits continuously. Every commit invalidates a read
	// snapshot taken before it, which is what turns a deferred transaction's
	// write into an upgrade that cannot succeed.
	hammering := make(chan struct{})
	var extCommits atomic.Int64
	go func() {
		defer close(hammering)
		for ctx.Err() == nil {
			if _, err := ext.ExecContext(ctx, "INSERT INTO ext (n) VALUES (1)"); err == nil {
				extCommits.Add(1)
			}
		}
	}()

	var runs, calls atomic.Int64
	result := runLoad(ctx, writers, func(ctx context.Context, worker int) error {
		key := worker % gateKeys
		calls.Add(1)
		return s.withTx(ctx, func(tx *sql.Tx) error {
			runs.Add(1)
			var n int64
			if err := tx.QueryRow("SELECT n FROM gate_head WHERE k = ?", key).Scan(&n); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO gate (k, n, at) VALUES (?, ?, ?)",
				key, n+1, s.clk.Now().UnixMilli()); err != nil {
				return err
			}
			_, err := tx.Exec("UPDATE gate_head SET n = ? WHERE k = ?", n+1, key)
			return err
		})
	})
	cancel()
	<-hammering

	t.Log(result.summary(fmt.Sprintf("%d writers beside an external writer", writers)))
	t.Logf("the external writer committed %d times, callbacks ran %d times for %d calls",
		extCommits.Load(), runs.Load(), calls.Load())

	for _, err := range result.failed {
		t.Errorf("writer: %v", err)
	}
	if result.busy != 0 {
		t.Errorf("SQLITE_BUSY reached a caller %d times beside an external writer: busy_timeout is "+
			"the safety net for another process, and a transaction that begins with the write lock "+
			"never needs more than that", result.busy)
	}
	if extCommits.Load() == 0 {
		t.Fatal("the external writer never committed, so nothing invalidated a read snapshot and " +
			"the test proves nothing")
	}
	if result.ops == 0 {
		t.Fatal("no write committed beside the external writer")
	}
	if excess := runs.Load() - calls.Load(); excess > 0 {
		t.Errorf("%d write callbacks ran a second time for %d calls: those transactions began "+
			"without the write lock and had to be retried after a failed upgrade, which is what "+
			"_txlock=immediate exists to prevent", excess, calls.Load())
	}
	assertNoLostUpdate(t, s, result.ops)
}
