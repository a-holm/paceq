package engine

import (
	"context"

	"github.com/a-holm/paceq/internal/store"
)

// Fsck sweeps the database for broken invariants and returns everything it
// finds. The checks themselves are SQL and live beside the rest of the SQL,
// in internal/store; this wrapper is the entry point the hidden command and
// the crash harness call. A healthy database returns an empty list.
func (e *Engine) Fsck(ctx context.Context) ([]store.Violation, error) {
	return e.Store.Fsck(ctx)
}
