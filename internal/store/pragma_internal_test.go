package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
)

// The expectations below are copied by hand from the PRAGMA table in the issue,
// not derived from the DSN constants. A test that reads its expectations from
// the code under test proves nothing.

func writerWant(sync string) map[string]string {
	return map[string]string{
		"journal_mode":       "wal",
		"synchronous":        sync,
		"foreign_keys":       "1",
		"busy_timeout":       "10000",
		"temp_store":         "2",
		"cache_size":         "-16000",
		"wal_autocheckpoint": "1000",
		"journal_size_limit": "67108864",
		"query_only":         "0",
		"mmap_size":          "0",
	}
}

func readerWant(sync string) map[string]string {
	return map[string]string{
		"journal_mode": "wal",
		"synchronous":  sync,
		"foreign_keys": "1",
		"busy_timeout": "5000",
		"temp_store":   "2",
		"cache_size":   "-32000",
		"query_only":   "1",
		"mmap_size":    "0",
	}
}

func openTestStore(t *testing.T, opt Options) *Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(context.Background(), path, opt)
	if err != nil {
		t.Fatalf("Open(%q, %+v): %v", path, opt, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func assertPragmas(t *testing.T, db *sql.DB, pool string, want map[string]string) {
	t.Helper()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("%s pool: take connection: %v", pool, err)
	}
	defer func() { _ = conn.Close() }()

	for name, expected := range want {
		var got string
		if err := conn.QueryRowContext(context.Background(), "PRAGMA "+name).Scan(&got); err != nil {
			t.Errorf("%s pool: read PRAGMA %s: %v", pool, name, err)
			continue
		}
		if got != expected {
			t.Errorf("%s pool: PRAGMA %s = %q, want %q", pool, name, got, expected)
		}
	}
}

func TestPragmasReadBackFromBothPools(t *testing.T) {
	cases := []struct {
		name string
		opt  Options
		sync string
	}{
		{name: "default is synchronous NORMAL", opt: Options{}, sync: "1"},
		{name: "synchronous full", opt: Options{Synchronous: "full"}, sync: "2"},
		{name: "synchronous normal spelled out", opt: Options{Synchronous: "normal"}, sync: "1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, tc.opt)
			assertPragmas(t, s.w, "writer", writerWant(tc.sync))
			assertPragmas(t, s.r, "reader", readerWant(tc.sync))
		})
	}
}

func TestPoolLimits(t *testing.T) {
	s := openTestStore(t, Options{})

	if got := s.w.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("writer MaxOpenConnections = %d, want 1", got)
	}
	if got := s.r.Stats().MaxOpenConnections; got != readerPoolSize() {
		t.Errorf("reader MaxOpenConnections = %d, want %d", got, readerPoolSize())
	}
}

// TestReaderPoolKeepsItsConnections pins the effective pool, not the configured
// cap. A reader pool capped at NumCPU but idle-limited to the database/sql
// default of 2 closes and reopens connections under concurrency, and every
// reopen replays the reader DSN pragmas on the hot path.
func TestReaderPoolKeepsItsConnections(t *testing.T) {
	const (
		readers = 8
		queries = 200
	)

	s := openTestStore(t, Options{})

	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range queries {
				err := s.withRead(context.Background(), func(ctx context.Context, r reader) error {
					var one int
					return r.QueryRowContext(ctx, "SELECT 1").Scan(&one)
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("read: %v", err)
	}

	if closed := s.r.Stats().MaxIdleClosed; closed != 0 {
		t.Errorf("reader MaxIdleClosed = %d after %d concurrent readers, want 0: the pool is "+
			"churning connections and replaying the reader pragmas on every reopen",
			closed, readers)
	}
}
