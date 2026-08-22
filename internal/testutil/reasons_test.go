package testutil

import (
	"context"
	"testing"
)

// TestAssertNoUnknownReasonsPassesOnAnEmptyStore wires the teardown assertion
// to a real store. The teeth of the underlying query are proven against SQL in
// internal/store, including the planted UNKNOWN case; this test exists so the
// helper itself cannot rot into a call that compiles against nothing.
func TestAssertNoUnknownReasonsPassesOnAnEmptyStore(t *testing.T) {
	s := TempStore(t)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	AssertNoUnknownReasons(t, context.Background(), s)
}
