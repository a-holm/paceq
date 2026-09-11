package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// busyStub carries the SQLITE_BUSY result code a contended database reports.
// Its message says nothing about being busy on purpose: a classifier that
// passed this test by reading the text would be reading the wrong thing.
//
// The number is written out rather than taken from internal/store, and
// TestIsBusyOnRealDriverErrors there anchors it against a database made
// genuinely busy by lock contention. This package cannot build the real error
// itself: internal/arch forbids every package outside internal/store from
// importing the driver.
type busyStub struct{}

func (busyStub) Error() string { return "a driver error carrying a result code" }
func (busyStub) Code() int     { return 5 }

// openEmptyState creates a state database and hands back a read-only handle
// on it, so a test can make a real store call fail for a real reason.
func openEmptyState(t *testing.T) *store.Store {
	t.Helper()

	stateDir := filepath.Join(t.TempDir(), stateDirName)
	if err := os.MkdirAll(stateDir, store.DirMode); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	writer, err := store.OpenState(t.Context(), stateDir, store.Options{})
	if err != nil {
		t.Fatalf("open the state: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close the state: %v", err)
	}
	ro, err := store.OpenReadOnly(t.Context(), filepath.Join(stateDir, store.DatabaseFileName), store.Options{})
	if err != nil {
		t.Fatalf("open the state read only: %v", err)
	}
	return ro
}

// TestBusyStoreErrorIsNotReportedAsABug is the classification the exit code
// table promises: a contended database is somebody else's write burst, not an
// installation to file a bug about.
func TestBusyStoreErrorIsNotReportedAsABug(t *testing.T) {
	ctx := t.Context()
	busy := fmt.Errorf("list all schedules: %w", busyStub{})

	if got := classify(ctx, busy); got.code != ExitBusy {
		t.Errorf("classify(a busy database) exits %d, want %d\n%s", got.code, ExitBusy, got.Error())
	}
	got := storeFailure(ctx, "could not list schedules", busy)
	if got.code != ExitBusy {
		t.Errorf("storeFailure(a busy database) exits %d, want %d\n%s", got.code, ExitBusy, got.Error())
	}
}

// TestUnknownStoreErrorStaysInternal is the half that matters as much: naming
// the conditions paceq knows must not cost it the one it does not.
func TestUnknownStoreErrorStaysInternal(t *testing.T) {
	got := storeFailure(t.Context(), "could not list schedules", errors.New("disk on fire"))
	if got.code != ExitInternal {
		t.Fatalf("storeFailure(an unrecognised failure) exits %d, want %d\n%s", got.code, ExitInternal, got.Error())
	}
	if !strings.Contains(got.what, "could not list schedules") {
		t.Errorf("the fallback lost what the command was doing:\n%s", got.Error())
	}
	if !strings.Contains(got.Error(), "this is a bug") {
		t.Errorf("an unrecognised failure no longer tells the operator to report it:\n%s", got.Error())
	}
}

// TestResolveScheduleRefLetsClassifySeeTheStoreError drives the failure the
// way the store produces it: the listing runs on a cancelled context and
// returns what the read handle was doing at the time. Naming the class at the
// call site reports that as exit 1, which tells an operator who pressed Ctrl-C
// that their installation is broken.
func TestResolveScheduleRefLetsClassifySeeTheStoreError(t *testing.T) {
	ro := openEmptyState(t)
	defer func() { _ = ro.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := resolveScheduleRef(ctx, ro, "nightly")
	if err == nil {
		t.Fatal("the listing succeeded on a cancelled context, so nothing is being classified")
	}
	if got := classify(ctx, err); got.code != ExitInterrupted {
		t.Fatalf("a cancelled listing exits %d, want %d\n%s", got.code, ExitInterrupted, got.Error())
	}
}

// TestResolveScheduleRefStillReportsAnUnknownStoreErrorAsInternal is the
// negative case at the call site, on a real store error: a closed handle is a
// failure paceq has no better name for, and it has to keep saying so.
func TestResolveScheduleRefStillReportsAnUnknownStoreErrorAsInternal(t *testing.T) {
	ro := openEmptyState(t)
	if err := ro.Close(); err != nil {
		t.Fatalf("close the read handle: %v", err)
	}

	_, err := resolveScheduleRef(t.Context(), ro, "nightly")
	if err == nil {
		t.Fatal("the listing succeeded on a closed handle, so nothing is being classified")
	}
	got := classify(t.Context(), err)
	if got.code != ExitInternal {
		t.Fatalf("a closed handle exits %d, want %d\n%s", got.code, ExitInternal, got.Error())
	}
	if !strings.Contains(got.what, "could not list schedules") {
		t.Errorf("the message no longer says what the command was doing:\n%s", got.Error())
	}
}
