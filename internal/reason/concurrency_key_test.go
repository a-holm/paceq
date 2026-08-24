package reason

import "testing"

// The concurrency key codes (#17). A deferred-for-key run carries its code on
// the row while it waits; a skipped fire carries the rejection on its trigger
// row. Both need full metadata, like every other entry.
func TestConcurrencyKeyCodesExistWithMetadata(t *testing.T) {
	for _, code := range []Code{RUNDeferredConcurrencyKey, TRIGGERRejectedConcurrencyKey} {
		e, ok := Lookup(code)
		if !ok {
			t.Fatalf("%s has no catalogue entry", code)
		}
		if e.Code != code {
			t.Fatalf("lookup of %s came back as %s", code, e.Code)
		}
		want := map[string]bool{"concurrency_key": false, "blocking_run_id": false}
		for _, key := range e.DataKeys {
			if _, ok := want[key]; ok {
				want[key] = true
			}
		}
		for key, seen := range want {
			if !seen {
				t.Errorf("%s: DataKeys lacks %q", code, key)
			}
		}
	}
}

func TestDeferredConcurrencyKeyCodeIsARunLevelCode(t *testing.T) {
	e, ok := Lookup(RUNDeferredConcurrencyKey)
	if !ok {
		t.Fatal("RUN_DEFERRED_CONCURRENCY_KEY has no entry")
	}
	if e.Level != LevelRun {
		t.Fatalf("the deferral code is level %v, want LevelRun", e.Level)
	}
}

func TestRejectedConcurrencyKeyCodeIsATriggerLevelCode(t *testing.T) {
	e, ok := Lookup(TRIGGERRejectedConcurrencyKey)
	if !ok {
		t.Fatal("TRIGGER_REJECTED_CONCURRENCY_KEY has no entry")
	}
	if e.Level != LevelTrigger {
		t.Fatalf("the rejection code is level %v, want LevelTrigger", e.Level)
	}
}

// RUN_CANCELLED_SUPERSEDED is a reserved name only (#17): the cancel_previous
// policy was declined for 1.0, and the name is parked so a later implementation
// cannot invent a second spelling.
func TestCancelledSupersededIsReserved(t *testing.T) {
	if RUNCancelledSuperseded != Code("RUN_CANCELLED_SUPERSEDED") {
		t.Fatalf("the reserved name drifted: %s", RUNCancelledSuperseded)
	}
}
