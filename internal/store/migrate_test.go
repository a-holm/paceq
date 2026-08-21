package store_test

import (
	"context"
	"testing"

	"github.com/a-holm/paceq/internal/testutil"
)

// TestMigrateIsSafeOnEveryStart is the public contract: a process calls Migrate
// after Open, every time, and does not have to know whether the database is new.
func TestMigrateIsSafeOnEveryStart(t *testing.T) {
	s := testutil.TempStore(t)
	ctx := context.Background()

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}
