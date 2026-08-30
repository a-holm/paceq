package sensor

import (
	"testing"

	"github.com/a-holm/paceq/internal/reason"
)

func resultWithTriggers(n int) *Result {
	out := &Result{
		Outcome:    Triggered,
		ReasonCode: reason.TRIGGERAccepted,
		ReasonData: map[string]any{},
	}
	for i := 0; i < n; i++ {
		out.Triggers = append(out.Triggers, Trigger{RunKey: string(rune('a' + i))})
	}
	return out
}

// TestTruncationKeepsFirstNBounded is the bounded-output proof: a sensor that
// returns far more triggers than the budget gets exactly the first N, the
// rest are counted as dropped, and the tick is marked truncated. A boundary
// answer (<= N) is untouched and not truncated.
func TestTruncationBounded(t *testing.T) {
	// 500 triggers, budget 100 => exactly 100 kept.
	res := resultWithTriggers(500)
	if truncated := ApplyLimit(res, 100); !truncated {
		t.Fatal("ApplyLimit on an oversized batch reported not truncated")
	}
	if len(res.Triggers) != 100 {
		t.Fatalf("kept %d triggers, want 100", len(res.Triggers))
	}
	if got := res.ReasonData["truncated"]; got != true {
		t.Fatalf("ReasonData[truncated] = %v, want true", got)
	}
	if got := res.ReasonData["dropped"]; got != 400 {
		t.Fatalf("ReasonData[dropped] = %v, want 400", got)
	}

	// Boundary: exactly the budget, nothing dropped, not truncated.
	at := resultWithTriggers(100)
	if truncated := ApplyLimit(at, 100); truncated {
		t.Fatal("ApplyLimit on an exact-budget batch reported truncated")
	}
	if len(at.Triggers) != 100 {
		t.Fatalf("boundary kept %d triggers, want 100", len(at.Triggers))
	}

	// Under budget: untouched.
	small := resultWithTriggers(17)
	if truncated := ApplyLimit(small, 100); truncated {
		t.Fatal("ApplyLimit on an under-budget batch reported truncated")
	}
	if len(small.Triggers) != 17 {
		t.Fatalf("under-budget kept %d triggers, want 17", len(small.Triggers))
	}
	if got := small.ReasonData["truncated"]; got != false {
		t.Fatalf("under-budget ReasonData[truncated] = %v, want false", got)
	}
}

// TestTruncationWithoutTrustedCursorDropsTheCursor pins the conservative
// cursor rule (plan 04 section 5.2): without a per-element cursor answer the
// cursor is left unchanged, so a re-evaluation replays and dedup folds the
// duplicates instead of creating twins. This milestone has no trusted partial
// cursor, so every truncation takes the nil-cursor branch.
func TestTruncationWithoutTrustedCursorDropsTheCursor(t *testing.T) {
	res := resultWithTriggers(300)
	before := "00000000000000000000000000000000"
	res.CursorBefore = &before
	res.CursorAfter = &before // a sensor that reported the same cursor
	ApplyLimit(res, 100)
	if res.CursorAfter != nil {
		t.Fatal("truncation advanced the cursor without a trusted partial cursor; cursor_after must stay nil")
	}
	if res.CursorBefore == nil {
		t.Fatal("CursorBefore must survive truncation; only CursorAfter is dropped")
	}
}

// TestTruncationLeavesCursorAloneWhenWithinBudget pins that a normal batched
// answer keeps its cursor: truncation only touches the cursor when it actually
// cuts.
func TestTruncationLeavesCursorAloneWhenWithinBudget(t *testing.T) {
	res := resultWithTriggers(3)
	cur := "mycursor"
	res.CursorAfter = &cur
	res.ReasonData = map[string]any{}
	ApplyLimit(res, 100)
	if res.CursorAfter == nil || *res.CursorAfter != cur {
		t.Fatal("ApplyLimit inside the budget must not drop the cursor")
	}
}
