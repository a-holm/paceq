package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// The gate in TestConcurrentWriters answers one question: is the driver fast
// enough to keep. These benchmarks are where the numbers behind that answer
// come from, including the one the gate deliberately does not assert: what
// durability costs. synchronous=FULL turns every commit into an fsync, so the
// difference between the two write benchmarks is the price of the tradeoff.
//
//	go test ./internal/store -run '^$' -bench . -benchtime 3s

// benchStore opens a store for a benchmark with the given synchronous mode and
// the same synthetic tables the gate writes to.
func benchStore(b *testing.B, sync string) *Store {
	b.Helper()

	path := filepath.Join(b.TempDir(), "state.db")
	s, err := Open(context.Background(), path, Options{Synchronous: sync})
	if err != nil {
		b.Fatalf("Open with synchronous=%s: %v", sync, err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})

	err = s.withTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE bench (
			id INTEGER PRIMARY KEY, k INTEGER NOT NULL, n INTEGER NOT NULL, at INTEGER NOT NULL
		) STRICT`); err != nil {
			return err
		}
		_, err := tx.Exec("CREATE TABLE bench_head (k INTEGER PRIMARY KEY, n INTEGER NOT NULL) STRICT")
		if err != nil {
			return err
		}
		_, err = tx.Exec("INSERT INTO bench_head (k, n) VALUES (0, 0)")
		return err
	})
	if err != nil {
		b.Fatalf("create the bench tables: %v", err)
	}
	return s
}

// benchWrite times the write the gate measures: read the head, append a row,
// move the head forward with RETURNING, all in one IMMEDIATE transaction.
func benchWrite(b *testing.B, sync string) {
	s := benchStore(b, sync)
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		err := s.withTx(ctx, func(tx *sql.Tx) error {
			var n int64
			if err := tx.QueryRow("SELECT n FROM bench_head WHERE k = 0").Scan(&n); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO bench (k, n, at) VALUES (0, ?, ?)",
				n+1, s.clk.Now().UnixMilli()); err != nil {
				return err
			}
			var moved int64
			return tx.QueryRow("UPDATE bench_head SET n = ? WHERE k = 0 RETURNING n", n+1).Scan(&moved)
		})
		if err != nil {
			b.Fatalf("write: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tx/s")
}

// BenchmarkWriteTx is the shipped configuration: synchronous=NORMAL, where a
// commit is a write into the write ahead log and not an fsync. It is the number
// the gate's floor is set against.
func BenchmarkWriteTx(b *testing.B) { benchWrite(b, "normal") }

// BenchmarkWriteTxSyncFull is the same write with synchronous=FULL. It exists
// to keep the durability tradeoff measured rather than assumed: an operator who
// turns FULL on should learn the cost from this number, not from production.
func BenchmarkWriteTxSyncFull(b *testing.B) { benchWrite(b, "full") }

// BenchmarkReadUnderWriteLoad times a primary key read while a writer holds the
// write lock as hard as it can. The read is a single row by key so its cost
// does not drift with the table the writer is filling. In WAL mode the reader
// has its own snapshot, so this should measure the query and nothing else. A number that tracks the write
// benchmark instead means reads are queueing behind writes.
func BenchmarkReadUnderWriteLoad(b *testing.B) {
	s := benchStore(b, "normal")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writing := make(chan struct{})
	go func() {
		defer close(writing)
		for n := int64(1); ctx.Err() == nil; n++ {
			_ = s.withTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO bench (k, n, at) VALUES (1, ?, ?)", n, s.clk.Now().UnixMilli())
				return err
			})
		}
	}()
	// Let the writer get ahead, so the benchmark reads against a moving target.
	time.Sleep(10 * time.Millisecond)

	b.ReportAllocs()
	for b.Loop() {
		var n int64
		err := s.withRead(ctx, func(ctx context.Context, r reader) error {
			return r.QueryRowContext(ctx, "SELECT n FROM bench_head WHERE k = 0").Scan(&n)
		})
		if err != nil {
			b.Fatalf("read: %v", err)
		}
	}
	b.StopTimer()
	cancel()
	<-writing
}
