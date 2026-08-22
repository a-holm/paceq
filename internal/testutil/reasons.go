package testutil

import (
	"context"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// AssertNoUnknownReasons is the teardown assertion for every integration test
// from M1 on (06 section 2.1): after a test has driven real work through a
// store, no terminal run, step, tick or trigger may sit without a usable
// reason code. It fails naming each offending row, so a code path that
// learned to skip its explanation is caught the day it is written, in
// whichever test first drives it.
func AssertNoUnknownReasons(t *testing.T, ctx context.Context, s *store.Store) {
	t.Helper()
	rows, err := s.UnexplainedReasons(ctx)
	if err != nil {
		t.Fatalf("audit reason codes: %v", err)
	}
	for _, r := range rows {
		t.Errorf("%s %s ended without a usable reason code", r.Kind, r.Key)
	}
}
