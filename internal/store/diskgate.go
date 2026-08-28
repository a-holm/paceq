package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/a-holm/paceq/internal/reason"
)

// The disk-guard's admission gate (#44). The store owns the verdict (which
// code, which words) because it owns every other verdict in the tick
// transaction; the daemon only supplies the fact it measured. That is the
// same division the runner keeps: one layer names facts, the verdict owner
// names codes.

// RunHold is what an installed gate reports while new runs are refused: the
// catalogue code the refusal is stored under, the operator-facing line beside
// it, and the measurements that make the decision auditable from explain.
type RunHold struct {
	Code reason.Code
	Text string
	Data map[string]any
}

// RunHoldFunc is the gate itself. Returning nil, or a hold without a code,
// admits runs as usual. The daemon installs one that reads the disk-guard's
// state atomically, so the cost inside the admission transaction is one
// pointer load - the decision stays inside the one IMMEDIATE transaction that
// already owns the tick, which is the whole admission idea (07 §4.2).
type RunHoldFunc func() *RunHold

// ErrRunsHeld is what a manual run is refused with while the gate holds.
// The refusal is also recorded - the tick stands down and its trigger is
// stored rejected with the code - so explain answers for the run that never
// happened.
var ErrRunsHeld = errors.New("new runs are held")

// HeldError carries the gate's verdict to the command that asked for a run it
// could not have. Is(ErrRunsHeld) matches it.
type HeldError struct {
	Hold RunHold
}

func (e *HeldError) Error() string {
	if e.Hold.Text != "" {
		return fmt.Sprintf("%s (%s)", e.Hold.Text, e.Hold.Code)
	}
	return "new runs are held (" + string(e.Hold.Code) + ")"
}

func (e *HeldError) Unwrap() error { return ErrRunsHeld }

// SetRunHold installs the gate; nil removes it. Production sets it once at
// daemon startup and never races it, but the swap is atomic anyway: nothing
// outside this file needs a lock to consult it.
func (s *Store) SetRunHold(fn RunHoldFunc) {
	if fn == nil {
		s.runHold.Store(nil)
		return
	}
	s.runHold.Store(&fn)
}

// heldRun consults the gate. ok is false when runs are admitted as usual.
func (s *Store) heldRun() (RunHold, bool) {
	p := s.runHold.Load()
	if p == nil {
		return RunHold{}, false
	}
	h := (*p)()
	if h == nil || h.Code == "" {
		return RunHold{}, false
	}
	return *h, true
}

// holdDataJSON encodes the gate's measurements for reason_data. A hold
// without data stores an empty payload, which every explain reader already
// treats as "no structured facts".
func holdDataJSON(h RunHold) string {
	if len(h.Data) == 0 {
		return ""
	}
	b, err := json.Marshal(h.Data)
	if err != nil {
		return ""
	}
	return string(b)
}

// standDownTickTx records the disk hold on a claimed evaluation: the tick row
// it claimed is converted into the skipped stand-down with the hold's code,
// inside the caller's transaction. One row per fire-time stays the
// idempotency gate's promise, and the gap in the run history keeps its cause
// beside it.
func (s *Store) standDownTickTx(tx *sql.Tx, tickID string, at int64, hold RunHold) error {
	if err := markTickStoodDownTx(tx, tickID, at, hold.Code, hold.Text, holdDataJSON(hold)); err != nil {
		return err
	}
	return nil
}
