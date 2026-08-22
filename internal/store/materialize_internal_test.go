package store

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A transaction boundary cannot be observed from outside by counting rows
// after the fact, so these tests install a trigger that makes SQLite itself
// refuse one statement of the chain. If any earlier statement of the same
// transaction survived, the row count below catches it: that would mean the
// tick, the trigger or the run outlived the failure, which is exactly what
// "one transaction" forbids.

const (
	abortEvents = `CREATE TRIGGER paceq_test_abort_on_run_events BEFORE INSERT ON run_events
BEGIN SELECT RAISE(ABORT, 'injected: events refused'); END`

	abortTriggers = `CREATE TRIGGER paceq_test_abort_on_triggers BEFORE INSERT ON triggers
BEGIN SELECT RAISE(ABORT, 'injected: triggers refused'); END`
)

func dropTestTriggers(t *testing.T, s *Store) {
	t.Helper()
	for _, name := range []string{"paceq_test_abort_on_run_events", "paceq_test_abort_on_triggers"} {
		if _, err := s.w.Exec(`DROP TRIGGER IF EXISTS ` + name); err != nil {
			t.Fatalf("drop %s: %v", name, err)
		}
	}
}

func countRows(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.w.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestMaterializeManualTriggerIsAllOrNothing(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, Options{})
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	version, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:aa",
		SpecJSON: `{"schema":"paceq.job.v1","name":"nightly","max_concurrent":1,"timeout_ms":3600000,` +
			`"steps":[{"name":"build","run":["/bin/true"],"shell":false}]}`,
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	_ = version

	for _, tc := range []struct {
		name    string
		trigger string
	}{
		{"the last write refuses", abortEvents},
		{"a middle write refuses", abortTriggers},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.w.Exec(tc.trigger); err != nil {
				t.Fatalf("install trigger: %v", err)
			}
			defer dropTestTriggers(t, s)

			_, err := s.MaterializeManualTrigger(ctx, ManualTriggerInput{JobName: "nightly"})
			if err == nil {
				t.Fatal("the materialisation went through despite the injected refusal")
			}
			if !strings.Contains(err.Error(), "injected") {
				t.Errorf("error = %v, want the injected refusal named", err)
			}

			// Nothing from the failed attempt survives: no half a decision.
			for _, table := range []string{"ticks", "triggers", "runs", "steps", "step_deps", "run_events"} {
				if n := countRows(t, s, table); n != 0 {
					t.Errorf("%s holds %d rows after a rolled back materialisation", table, n)
				}
			}
		})
	}

	// With the triggers gone the same call succeeds in full, which proves the
	// refusals above came from the injection and not from the method itself.
	out, err := s.MaterializeManualTrigger(ctx, ManualTriggerInput{JobName: "nightly"})
	if err != nil {
		t.Fatalf("clean materialisation: %v", err)
	}
	if out.Run.JobVersionID == "" || errors.Is(err, ErrNotFound) {
		t.Errorf("clean run is missing its version: %+v", out.Run)
	}
}
