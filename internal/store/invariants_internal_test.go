package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// The M6-06 acceptance side of the sweep: the catalogue is complete and every
// entry carries its remedy, the full sweep covers the critical subset, every
// violation is graded and carries a next step, and every statement plans
// through an index so the sweep survives a production-sized database.

// sweepCheckIDs is every check ID the sweep can emit. It is the contract the
// catalogue is tested against, so an invariant added without a catalogue entry
// fails here instead of reporting with a made-up grade.
var sweepCheckIDs = []string{
	"I1", "I2", "I3", "I5", "I6", "I8", "I9", "I10",
	"I11", "I12", "I13", "I14", "I15", "reason",
}

// TestInvariantCatalogueCoversTheSweep is the CI rule the issue fixes: an
// invariant without a non-empty remedy is a diagnosis without a doctor, and
// the build has to refuse it the way it refuses a reason code with no text.
func TestInvariantCatalogueCoversTheSweep(t *testing.T) {
	seen := map[string]bool{}
	for _, inv := range Invariants {
		if seen[inv.ID] {
			t.Errorf("invariant %s is listed twice", inv.ID)
		}
		seen[inv.ID] = true
		if strings.TrimSpace(inv.Title) == "" {
			t.Errorf("invariant %s has no title", inv.ID)
		}
		if strings.TrimSpace(inv.Remedy) == "" {
			t.Errorf("invariant %s has no remedy: every catalogue entry must say what to do", inv.ID)
		}
		switch inv.Severity {
		case Warning, Serious, Critical:
		default:
			t.Errorf("invariant %s has an out-of-range severity %d", inv.ID, inv.Severity)
		}
	}
	for _, id := range sweepCheckIDs {
		if !seen[id] {
			t.Errorf("the sweep can emit %s, but the catalogue does not list it", id)
		}
	}
	if len(seen) != len(sweepCheckIDs) {
		t.Errorf("the catalogue lists %d entries, the sweep emits %d checks", len(seen), len(sweepCheckIDs))
	}
}

// TestFsckTagsEveryFindingWithSeverityAndRemedy plants one warning and one
// serious finding and requires every returned violation to carry its catalogue
// grade and a usable next step, so no consumer ever has to look the severity
// up itself.
func TestFsckTagsEveryFindingWithSeverityAndRemedy(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	// The plants below are rows a live paceq can no longer write: the schema
	// CHECKs refuse both. They are exactly the rows the sweep exists for -
	// what an older version or a hand edit left behind - so the planting runs
	// with the CHECKs relaxed, the way every fsck plant does.
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 1`); err != nil {
		t.Fatalf("relax the CHECKs: %v", err)
	}
	// Warning class: a terminal step with no reason code.
	if _, err := s.w.ExecContext(ctx, `UPDATE steps SET state = 'failed',
attempt = 1, reason_code = NULL, finished_at = started_at WHERE run_id = ?`, runID); err != nil {
		t.Fatalf("plant the reason violation: %v", err)
	}
	// Warning class: a held back run with no defer reason.
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'queued',
reason_code = NULL, available_at = created_at + 3600000, defer_reason = NULL
WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the defer violation: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 0`); err != nil {
		t.Fatalf("restore the CHECKs: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if !hasCheck(violations, "reason") || !hasCheck(violations, "I14") {
		t.Fatalf("fsck missed the planted findings: %+v", violations)
	}
	for _, v := range violations {
		inv, ok := invariantByID(v.Check)
		if !ok {
			t.Fatalf("finding %s is not in the catalogue", v.Check)
		}
		if v.Severity != inv.Severity {
			t.Errorf("finding %s carries severity %v, the catalogue says %v",
				v.Check, v.Severity, inv.Severity)
		}
		if v.Remedy != inv.Remedy || v.Remedy == "" {
			t.Errorf("finding %s carries no catalogue remedy", v.Check)
		}
	}
}

// schemaRuleByInvariant names, for every invariant whose remedy tells an
// operator the database itself refuses the write, the rule that does the
// refusing. The rule text must read verbatim both in the remedy and in the
// golden schema, so a remedy can never send an operator looking for a
// constraint the schema does not hold.
var schemaRuleByInvariant = map[string]string{
	"I3":     "PRIMARY KEY (source_id, epoch, run_key)",
	"I6":     "UNIQUE (source_kind, source_name, scheduled_for)",
	"reason": "reason_code IS NOT NULL",
}

// TestSchemaBackedRemediesNameARuleTheSchemaHolds is the honesty gate on the
// catalogue. A remedy that tells an operator the database itself refused the
// write must name the rule that does the refusing, and that rule must be in
// the schema: a remedy pointing at a constraint the schema does not hold
// sends the operator to a backup restore over a state the product produces
// on purpose.
func TestSchemaBackedRemediesNameARuleTheSchemaHolds(t *testing.T) {
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read %s: %v", goldenPath, err)
	}
	schema := string(raw)

	for _, inv := range Invariants {
		claimsSchema := strings.Contains(inv.Remedy, "schema") ||
			strings.Contains(inv.Remedy, "PRIMARY KEY") ||
			strings.Contains(inv.Remedy, "UNIQUE")
		rule, named := schemaRuleByInvariant[inv.ID]
		if !claimsSchema {
			if named {
				t.Errorf("invariant %s is listed as schema-backed but its remedy claims nothing: %q",
					inv.ID, inv.Remedy)
			}
			continue
		}
		if !named {
			t.Errorf("the remedy of %s claims the schema enforces it but names no rule: %q",
				inv.ID, inv.Remedy)
			continue
		}
		if !strings.Contains(inv.Remedy, rule) {
			t.Errorf("the remedy of %s does not name %q: %q", inv.ID, rule, inv.Remedy)
		}
		if !strings.Contains(schema, rule) {
			t.Errorf("the remedy of %s names %q, which is not in %s", inv.ID, rule, goldenPath)
		}
	}
}

// TestFsckFullSweepCatchesTheCriticalSubset proves the full sweep and the boot
// gate agree: the same plants that refuse a startup come out of the operator's
// sweep graded critical.
func TestFsckFullSweepCatchesTheCriticalSubset(t *testing.T) {
	ctx := context.Background()
	s, runID := plantSeededRun(t)

	// I3: one run claimed by two dedup identities. The run_keys primary key
	// keeps identities apart, so the second claim is planted the way the
	// corruption this check exists for would arrive, behind the gate.
	for _, source := range []string{"src-a", "src-b"} {
		if _, err := s.w.ExecContext(ctx, `INSERT INTO run_keys
(source_id, epoch, run_key, first_seen_at, run_id) VALUES (?, 1, 'shared-key', 1, ?)`,
			source, runID); err != nil {
			t.Fatalf("plant the second dedup identity: %v", err)
		}
	}

	// I9: a dependency on a step the run does not have.
	if _, err := s.w.ExecContext(ctx, `INSERT INTO step_deps (run_id, step_name, depends_on)
VALUES (?, 'build', 'ghost')`, runID); err != nil {
		t.Fatalf("plant the dangling edge: %v", err)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if !hasCheck(violations, "I9") {
		t.Fatalf("the full sweep missed the dangling edge: %+v", violations)
	}
	if !hasCheck(violations, "I3") {
		t.Fatalf("the full sweep missed the double-claimed run: %+v", violations)
	}
	for _, v := range violations {
		if v.Severity != severityOf(v.Check) {
			t.Fatalf("finding %s is graded %v", v.Check, v.Severity)
		}
	}
}

// TestFsckRunsEveryStatementUnderADeadline pins the 07 section 6.4 rule: the
// sweep is a guest on the read pool, and a caller that cancels must not wait
// for any statement. A pre-cancelled context fails the first statement, which
// is the deterministic form of "the deadline is real".
func TestFsckRunsEveryStatementUnderADeadline(t *testing.T) {
	s := internalStore(t)
	if _, err := s.Fsck(context.Background()); err != nil {
		t.Fatalf("fsck on a quiet store: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Fsck(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("fsck ignored the caller's cancellation: err=%v", err)
	}

	if _, err := s.QuickFsck(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("quick fsck ignored the caller's cancellation: err=%v", err)
	}
	if _, err := s.checkDAG(ctx, "fsck I9"); !errors.Is(err, context.Canceled) {
		t.Fatalf("the DAG check ignored the caller's cancellation: err=%v", err)
	}
}

// TestEveryInvariantQueryPlansWithoutScanningRuns is the EXPLAIN QUERY PLAN
// gate: every statement the sweep runs is explained exactly as production runs
// it, and none of them may scan the runs table. A scan there is the difference
// between a five second sweep and a five minute one on a million runs.
func TestEveryInvariantQueryPlansWithoutScanningRuns(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)

	cases := []struct {
		check string
		sql   string
		args  []any
		// uses must all appear in the plan.
		uses []string
	}{
		{check: "I1", sql: fsckI1SQL, args: []any{time.Now().UnixMilli()}, uses: []string{"idx_runs_reaper"}},
		{check: "I2", sql: fsckI2SQL, uses: []string{"SEARCH"}},
		{check: "I3", sql: fsckI3SQL, uses: []string{"COVERING INDEX idx_run_keys_run_id"}},
		{check: "I5", sql: fsckI5SQL},
		{check: "I6", sql: fsckI6SQL, uses: []string{"ticks"}},
		{check: "I8", sql: fsckI8SQL, uses: []string{"SEARCH sd"}},
		{check: "I10", sql: fsckI10SQL, uses: []string{"SEARCH s"}},
		{check: "I11 events", sql: fsckI11EventsSQL, uses: []string{"idx_run_events_run"}},
		{check: "I11 epochs", sql: fsckI11EpochsSQL, uses: []string{"COVERING"}},
		{check: "I13", sql: fsckI13SQL, uses: []string{"COVERING"}},
		{check: "I14", sql: fsckI14SQL, uses: []string{"idx_runs_fsck_defer"}},
		{check: "I15", sql: fsckI15SQL, uses: []string{"idx_run_events_run"}},
		{check: "reason", sql: UnexplainedReasonSQL, uses: []string{"idx_runs_fsck_reason"}},
		{check: "I9 dangling", sql: fsckI9DanglingSQL, uses: []string{"step_deps"}},
		{check: "I9 edges", sql: fsckI9EdgesSQL},
		{check: "I12", sql: activeLimitSQL, uses: []string{"SEARCH r"}},
		{check: "I12 keys", sql: activeKeysSQL, uses: []string{"ux_runs_conc_key"}},
	}

	for _, tc := range cases {
		rows, err := s.r.QueryContext(ctx, "EXPLAIN QUERY PLAN "+tc.sql, tc.args...)
		if err != nil {
			t.Fatalf("explain %s: %v", tc.check, err)
		}
		var plan []string
		for rows.Next() {
			var id, parent, notused int64
			var detail string
			if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
				_ = rows.Close()
				t.Fatalf("scan the %s plan: %v", tc.check, err)
			}
			plan = append(plan, detail)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("read the %s plan: %v", tc.check, err)
		}
		_ = rows.Close()

		joined := strings.Join(plan, "\n")
		if strings.Contains(joined, "SCAN TABLE runs") {
			t.Errorf("invariant %s scans the runs table:\n%s", tc.check, joined)
		}
		for _, want := range tc.uses {
			if !strings.Contains(joined, want) {
				t.Errorf("invariant %s plans without %s:\n%s", tc.check, want, joined)
			}
		}
	}
}

// TestFsckSweepsAProductionSizedDatabaseInsideItsBudget is the performance
// acceptance: a database of a million ticks and a hundred thousand runs sweeps
// in under five seconds. The dataset is built inside SQLite by recursive CTEs,
// so the cost sits in the database engine, not in Go loops, and the budget the
// sweep is held to is the same five seconds its statements are.
func TestFsckSweepsAProductionSizedDatabaseInsideItsBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("the production-sized sweep needs its full dataset: skipped by -short")
	}
	if raceEnabled {
		// The five second budget is a production budget, measured where the
		// acceptance measures it. The detector multiplies every row the
		// driver walks, so a million-row sweep under -race measures the
		// detector, not the plan; the same convention the throughput gate
		// runs under.
		t.Skip("the five second sweep budget is measured in the plain build, not under -race")
	}
	ctx := context.Background()
	s := internalStore(t)

	const runs = 100_000
	const ticks = 1_000_000

	if _, err := s.w.ExecContext(ctx, `INSERT INTO jobs (name, max_concurrent, created_at, updated_at)
VALUES ('bulk', 10000, 1, 1)`); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `INSERT INTO job_versions (id, job_name, version,
	spec_hash, spec_json, created_at) VALUES
	('01BULKVERSION0000000000', 'bulk', 1, 'sha256:bulk', '{}', 1)`); err != nil {
		t.Fatalf("seed the job version: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `WITH RECURSIVE seq(i) AS (
	SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < ?
)
INSERT INTO runs (id, job_name, job_version_id, state, origin, available_at,
	created_at, updated_at, max_attempts, attempt, crash_count, lease_epoch,
	reason_code, started_at, finished_at)
SELECT printf('01BULK%020d', i), 'bulk', '01BULKVERSION0000000000',
	CASE WHEN i % 10 = 0 THEN 'running' ELSE 'succeeded' END,
	'schedule', 1, 1, 1, 1, 1, 0, 0,
	CASE WHEN i % 10 = 0 THEN NULL ELSE 'RUN_SUCCEEDED' END,
	CASE WHEN i % 10 = 0 THEN NULL ELSE 1 END,
	CASE WHEN i % 10 = 0 THEN NULL ELSE 2 END
FROM seq`, runs); err != nil {
		t.Fatalf("seed the runs: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET lease_owner = 'exec',
	lease_expires_at = 9000000000000 WHERE state = 'running'`); err != nil {
		t.Fatalf("lease the seeded runs: %v", err)
	}
	// A claimed run's first step may still be pending; without it the empty
	// step list aggregates to succeeded and every running row reads as drift.
	if _, err := s.w.ExecContext(ctx, `INSERT INTO steps (run_id, name, idx, state, max_attempts)
SELECT id, 'build', 0, 'pending', 1 FROM runs WHERE state = 'running'`); err != nil {
		t.Fatalf("seed the pending steps: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `WITH RECURSIVE seq(i) AS (
	SELECT 1 UNION ALL SELECT i + 1 FROM seq WHERE i < ?
)
INSERT INTO ticks (id, source_kind, source_name, scheduled_for, started_at,
	last_started_at, outcome, reason_code)
SELECT printf('01BULK%020d', i), 'schedule', 'bulk', i, i, i,
	'triggered', NULL
FROM seq`, ticks); err != nil {
		t.Fatalf("seed the ticks: %v", err)
	}

	deadline, cancel := context.WithTimeout(ctx, fsckQueryDeadline)
	defer cancel()
	start := time.Now()
	violations, err := s.Fsck(deadline)
	took := time.Since(start)
	if err != nil {
		t.Fatalf("fsck over the bulk database: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("the sweep took %s and found %d findings in a database sound by construction: %+v",
			took, len(violations), violations[:min(len(violations), 5)])
	}
	if took > fsckQueryDeadline {
		t.Fatalf("the sweep took %s, past its %s budget", took, fsckQueryDeadline)
	}
	t.Logf("bulk sweep of %d runs and %d ticks took %s", runs, ticks, took)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestRecordIntegrityFindingsWritesOneRowPerInvariant covers the event side:
// one sweep, three broken invariants, three rows in one transaction, with the
// subject list capped so a million-row finding cannot bloat the log.
func TestRecordIntegrityFindingsWritesOneRowPerInvariant(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)

	manySubjects := make([]string, 0, integritySubjectCap+5)
	for i := 0; i < integritySubjectCap+5; i++ {
		manySubjects = append(manySubjects, fmt.Sprintf("run %d", i))
	}
	at := time.Date(2027, 1, 21, 8, 2, 0, 0, time.UTC)
	err := s.RecordIntegrityFindings(ctx, at, []IntegrityFinding{
		{Invariant: "I1", Severity: Serious, Violations: 2, Subjects: manySubjects[:2]},
		{Invariant: "I3", Severity: Critical, Violations: 1, Subjects: manySubjects[:1]},
		{
			Invariant: "reason", Severity: Warning, Violations: integritySubjectCap + 5,
			Subjects: manySubjects,
		},
	})
	if err != nil {
		t.Fatalf("record the findings: %v", err)
	}

	read, err := s.MetricsIntegrityViolations(ctx)
	if err != nil {
		t.Fatalf("read the findings back: %v", err)
	}
	if len(read) != 3 {
		t.Fatalf("the newest sweep wrote %d rows, want 3: %+v", len(read), read)
	}
	byInvariant := map[string]MetricsIntegrityViolation{}
	for _, f := range read {
		byInvariant[f.Invariant] = f
	}
	if got := byInvariant["reason"]; got.Violations != integritySubjectCap+5 {
		t.Errorf("the reason row counts %d, want %d", got.Violations, integritySubjectCap+5)
	}
	if got := byInvariant["I1"]; got.Severity != Serious.String() {
		t.Errorf("the I1 row is graded %q", got.Severity)
	}

	last, ok, err := s.MetricsFsckLastRun(ctx)
	if err != nil || !ok {
		t.Fatalf("the last sweep stamp is missing: ok=%v err=%v", ok, err)
	}
	if !last.Equal(at) {
		t.Errorf("the last sweep ran at %s, want %s", last, at)
	}

	// A sweep with nothing to say writes nothing: the log's silence is the
	// clean bill, so a quiet sweep must not move the stamp.
	if err := s.RecordIntegrityFindings(ctx, at.Add(time.Hour), nil); err != nil {
		t.Fatalf("record a clean sweep: %v", err)
	}
	last2, _, err := s.MetricsFsckLastRun(ctx)
	if err != nil {
		t.Fatalf("reread the stamp: %v", err)
	}
	if !last2.Equal(at) {
		t.Errorf("a clean sweep moved the stamp to %s", last2)
	}
}

// TestRecordIntegrityFindingsRefusesAReadOnlyStore pins the write path: the
// read-only pool must not grow an event log, or doctor's RO sweep would write.
func TestRecordIntegrityFindingsRefusesAReadOnlyStore(t *testing.T) {
	s := internalStore(t)
	dbPath := s.Path()
	if err := s.Close(); err != nil {
		t.Fatalf("close the writer: %v", err)
	}
	ro, err := OpenReadOnly(context.Background(), dbPath, Options{})
	if err != nil {
		t.Fatalf("open the store read-only: %v", err)
	}
	defer func() { _ = ro.Close() }()
	if err := ro.RecordIntegrityFindings(context.Background(), time.Now(), []IntegrityFinding{
		{Invariant: "I1", Severity: Serious, Violations: 1},
	}); err == nil {
		t.Fatal("a read-only store recorded integrity findings")
	}
}
