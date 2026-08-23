package store

import (
	"context"
	"strings"
	"testing"
)

// The quick health check behind startup reconciliation (#62): three critical
// invariants, read only. Each test plants its violation through the injector
// methods and demands the right check name it.

func TestQuickFsckIsQuietOnASoundGraph(t *testing.T) {
	s := seededStore(t)
	violations, err := s.QuickFsck(context.Background())
	if err != nil {
		t.Fatalf("quick fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("a sound graph reported %+v", violations)
	}
}

func TestQuickFsckNamesDuplicateRunKeys(t *testing.T) {
	s := seededStore(t)
	subject, err := s.InjectDuplicateRunKey(context.Background())
	if err != nil {
		t.Fatalf("plant I3: %v", err)
	}

	violations, err := s.QuickFsck(context.Background())
	if err != nil {
		t.Fatalf("quick fsck: %v", err)
	}
	if len(violations) != 1 || violations[0].Check != "I3" {
		t.Fatalf("violations %+v, want exactly one I3", violations)
	}
	if !strings.Contains(violations[0].Subject, subject[len(subject)-len("planted-duplicate-run-key"):]) {
		t.Errorf("the violation names %q, want it to carry the planted key", violations[0].Subject)
	}
}

func TestQuickFsckNamesADependencyCycle(t *testing.T) {
	s := seededStore(t)
	subject, err := s.InjectDependencyCycle(context.Background())
	if err != nil {
		t.Fatalf("plant I9: %v", err)
	}

	violations, err := s.QuickFsck(context.Background())
	if err != nil {
		t.Fatalf("quick fsck: %v", err)
	}
	found := false
	for _, v := range violations {
		if v.Check == "I9" && strings.Contains(v.Subject, strings.TrimPrefix(subject, "run ")) {
			found = true
		}
	}
	if !found {
		t.Errorf("violations %+v, want an I9 on %s", violations, subject)
	}
}

// The tick-slot check (I6) has no injector on purpose: a UNIQUE constraint
// makes duplicate slots unwritable through SQL, which is the same guarantee
// the checker re-reads. Its healthy path is covered above; the negative path
// is the database's own job.

func TestQuickFsckNamesActiveRunOverflow(t *testing.T) {
	s := seededStore(t)
	subject, err := s.InjectActiveRunOverflow(context.Background())
	if err != nil {
		t.Fatalf("plant I12: %v", err)
	}

	violations, err := s.QuickFsck(context.Background())
	if err != nil {
		t.Fatalf("quick fsck: %v", err)
	}
	found := false
	for _, v := range violations {
		if v.Check == "I12" && strings.Contains(v.Subject, strings.TrimPrefix(subject, "job ")) {
			found = true
		}
	}
	if !found {
		t.Errorf("violations %+v, want an I12 on %s", violations, subject)
	}
}
