package spec_test

import (
	"testing"

	"github.com/a-holm/paceq/internal/spec"
)

// TestADuplicateKeyIsCaughtEvenWhenTheFirstIsEmpty. A key written with nothing
// after it is the field left out, and a second one is still a duplicate.
func TestADuplicateKeyIsCaughtEvenWhenTheFirstIsEmpty(t *testing.T) {
	_, diags := spec.Parse("x.yaml", []byte("name: report\ntimeout:\ntimeout: 45m\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))

	d := requireCode(t, diags, spec.CodeDuplicateKey)
	if d.Line != 3 {
		t.Errorf("the refusal points at line %d, want the second key on line 3", d.Line)
	}
}

// TestAKeyWithNothingAfterItLeavesTheDefault. The field is left out, not set to
// a blank, so the IR still carries the default.
func TestAKeyWithNothingAfterItLeavesTheDefault(t *testing.T) {
	job, diags := spec.Parse("x.yaml", []byte("name: report\ntimeout:\nmax_concurrent:\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))

	if diags.HasErrors() {
		t.Fatalf("empty fields were refused:\n%s", render(t, diags))
	}
	if job.Timeout != spec.DefaultTimeout {
		t.Errorf("timeout is %v, want the default %v", job.Timeout, spec.DefaultTimeout)
	}
	if job.MaxConcurrent != spec.DefaultMaxConcurrent {
		t.Errorf("max_concurrent is %d, want the default %d", job.MaxConcurrent, spec.DefaultMaxConcurrent)
	}
}
