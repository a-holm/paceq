package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyPoolNamesTheMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open(driverName, "file:"+path+"?_pragma=journal_mode(DELETE)&_pragma=foreign_keys(OFF)")
	if err != nil {
		t.Fatalf("open raw pool: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = verifyPool(context.Background(), db, "writer", []pragmaSpec{
		{name: "journal_mode", want: "wal"},
		{name: "foreign_keys", want: "1"},
	})
	if err == nil {
		t.Fatal("verifyPool accepted a pool in journal_mode=DELETE with foreign_keys off, want an error")
	}
	for _, want := range []string{"writer", "journal_mode", "delete", "wal"} {
		if !strings.Contains(strings.ToLower(err.Error()), want) {
			t.Errorf("verifyPool error %q does not mention %q", err, want)
		}
	}
}

func TestVerifyPoolAcceptsAMatchingPool(t *testing.T) {
	s := openTestStore(t, Options{})

	if err := verifyPool(context.Background(), s.w, "writer", writerSpecs("NORMAL")); err != nil {
		t.Errorf("verifyPool on the writer pool: %v", err)
	}
	if err := verifyPool(context.Background(), s.r, "reader", readerSpecs("NORMAL")); err != nil {
		t.Errorf("verifyPool on the reader pool: %v", err)
	}
}
