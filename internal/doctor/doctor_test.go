package doctor_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// fixedMode is a database that reports one auto_vacuum mode, which is how a
// database created before paceq set the mode is reproduced without shipping a
// binary fixture.
type fixedMode struct {
	mode store.AutoVacuumMode
	err  error
}

func (f fixedMode) AutoVacuum(context.Context) (store.AutoVacuumMode, error) {
	return f.mode, f.err
}

// TestAutoVacuumNoneWarnsWithAWayOut is the finding an operator has to be able
// to act on: the database never gives disk back, and the check says what to run
// to change that.
func TestAutoVacuumNoneWarnsWithAWayOut(t *testing.T) {
	got := doctor.CheckAutoVacuum(context.Background(), fixedMode{mode: store.AutoVacuumNone})

	if got.Level != doctor.Warn {
		t.Errorf("level is %s, want %s", got.Level, doctor.Warn)
	}
	if !strings.Contains(got.Detail, "NONE") {
		t.Errorf("detail does not name the mode: %q", got.Detail)
	}
	if len(got.Next) == 0 {
		t.Fatal("the finding carries no next step, so an operator cannot act on it")
	}
	joined := strings.Join(got.Next, "\n")
	if !strings.Contains(joined, "VACUUM") {
		t.Errorf("the next step does not say how to rewrite the database:\n%s", joined)
	}
}

// TestAutoVacuumIncrementalIsOK runs against a real database created by this
// project, so the check is wired to the store and not only to a fake.
func TestAutoVacuumIncrementalIsOK(t *testing.T) {
	s := testutil.TempStore(t)

	got := doctor.CheckAutoVacuum(context.Background(), s)

	if got.Level != doctor.OK {
		t.Fatalf("level is %s, want %s: %s", got.Level, doctor.OK, got.Detail)
	}
	if !strings.Contains(got.Detail, "INCREMENTAL") {
		t.Errorf("detail does not name the mode: %q", got.Detail)
	}
	if len(got.Next) != 0 {
		t.Errorf("a healthy database offers next steps: %v", got.Next)
	}
}

// TestAutoVacuumUnreadableFails separates "the setting is wrong" from "the
// question could not be answered". An operator reading a doctor report has to
// be able to tell those apart.
func TestAutoVacuumUnreadableFails(t *testing.T) {
	got := doctor.CheckAutoVacuum(context.Background(), fixedMode{err: errors.New("disk on fire")})

	if got.Level != doctor.Fail {
		t.Errorf("level is %s, want %s", got.Level, doctor.Fail)
	}
	if !strings.Contains(got.Detail, "disk on fire") {
		t.Errorf("detail loses the underlying error: %q", got.Detail)
	}
	if len(got.Next) == 0 {
		t.Fatal("the finding carries no next step, and a report a user reads has one on every line that is not ok")
	}
}
