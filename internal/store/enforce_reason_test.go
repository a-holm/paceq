package store

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// The CI rule from 06 section 2.1, held against real SQL: every terminal state
// refuses to be stored without a reason code, every catalogue code fits its
// table, the state sets the schema spells out are the ones the model enforces,
// and the audit query agrees with both. The static half of the rule lives in
// internal/arch; this file is the half that touches the database.

// TestTerminalRowsWithoutReasonAreRefused walks every terminal state of every
// reason carrying table and proves the CHECK constraint rejects the row when
// reason_code is missing. It iterates the states rather than sampling one, so
// loosening any single arm of a constraint fails here naming the state.
func TestTerminalRowsWithoutReasonAreRefused(t *testing.T) {
	s := seededStore(t)
	ctx := context.Background()

	cases := []struct {
		name string
		want string
		stmt string
	}{
		{"terminal run succeeded", "succeeded", runInsert("01J0RUNS1", "succeeded", "")},
		{"terminal run failed", "failed", runInsert("01J0RUNF1", "failed", "")},
		{"terminal run cancelled", "cancelled", runInsert("01J0RUNC1", "cancelled", "")},
		{"terminal step succeeded", "succeeded", stepInsert("probe-s", 90, "succeeded", "")},
		{"terminal step failed", "failed", stepInsert("probe-f", 91, "failed", "")},
		{"terminal step skipped", "skipped", stepInsert("probe-k", 92, "skipped", "")},
		{"terminal step cancelled", "cancelled", stepInsert("probe-c", 93, "cancelled", "")},
		{"skipped tick", "skipped", tickInsert("01J0TICKS1", "skipped", "")},
		{"error tick", "error", tickInsert("01J0TICKE1", "error", "")},
		{"missed tick", "missed", tickInsert("01J0TICKM1", "missed", "")},
		{"deduped trigger", "deduped", triggerInsert("01J0TRGD1", "deduped", "")},
		{"rejected trigger", "rejected", triggerInsert("01J0TRGR1", "rejected", "")},
	}
	for _, tc := range cases {
		_, err := s.w.ExecContext(ctx, tc.stmt)
		if err == nil {
			t.Errorf("%s: a terminal %s was stored without a reason code", tc.name, tc.want)
			continue
		}
		if !strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Errorf("%s: refused, but not by a CHECK constraint: %v", tc.name, err)
		}
		if !strings.Contains(err.Error(), "reason_code IS NOT NULL") {
			t.Errorf("%s: refused by %q, not by the reason rule", tc.name, err)
		}
	}
}

// TestEveryCatalogueCodeIsStorable is the other direction: no entry of the
// catalogue may be refused by the schema. A code the database rejects would
// make the model and the storage disagree about what an explanation is.
//
// Every code goes into every reason carrying table, rather than into the one
// its level names. A level is the object a code explains, never the table it
// lands in (#193): RUN_REJECTED_DISK_LOW is a run level code that only ever
// reaches ticks and triggers, so a fixture keyed on the level asserted
// storability about a row no producer writes and read as a mapping that does
// not exist. Non terminal codes are covered too, because they are stored on
// the same columns; RUN_INTERRUPTED_SHUTDOWN is one, and the old shape did not
// reach it at all.
func TestEveryCatalogueCodeIsStorable(t *testing.T) {
	s := seededStore(t)
	ctx := context.Background()

	tables := []struct {
		name   string
		insert func(n int, code string) string
	}{
		{"runs", func(n int, code string) string {
			return runInsert("01J0CODE"+itoa(n), "failed", code)
		}},
		{"steps", func(n int, code string) string {
			return stepInsert("code-"+itoa(n), 100+n, "failed", code)
		}},
		{"ticks", func(n int, code string) string {
			return tickInsert("01J0CODE"+itoa(n), "skipped", code)
		}},
		{"triggers", func(n int, code string) string {
			return triggerInsert("01J0CODE"+itoa(n), "rejected", code)
		}},
		{"lease_events", func(n int, code string) string {
			return leaseEventInsert("scheduler", "holder-"+itoa(n), 1+n, code)
		}},
	}

	n := 0
	for _, e := range reason.All() {
		for _, table := range tables {
			n++
			if _, err := s.w.ExecContext(ctx, table.insert(n, string(e.Code))); err != nil {
				t.Errorf("%s: the %s table refused a catalogue code: %v", e.Code, table.name, err)
			}
		}
	}
	if n == 0 {
		t.Fatal("the catalogue is empty, so this test proved nothing about it")
	}
}

func runInsert(id, state, code string) string {
	return "INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,\n" +
		"\t\t\treason_code, created_at, updated_at)\n" +
		"\t\t\tVALUES ('" + id + "', 'nightly', '01J0VER1', 'manual', '" + state + "', 2001,\n" +
		"\t\t\t" + sqlOrNull(code) + ", 2001, 2001)"
}

func stepInsert(name string, idx int, state, code string) string {
	return "INSERT INTO steps (run_id, name, idx, state, reason_code)\n" +
		"\t\t\tVALUES ('01J0RUN1', '" + name + "', " + itoa(idx) + ", '" + state + "', " + sqlOrNull(code) + ")"
}

func tickInsert(id, outcome, code string) string {
	return "INSERT INTO ticks (id, source_kind, source_name, started_at, last_started_at, outcome, reason_code)\n" +
		"\t\t\tVALUES ('" + id + "', 'sensor', 'inbox', 2001, 2001, '" + outcome + "', " + sqlOrNull(code) + ")"
}

func triggerInsert(id, outcome, code string) string {
	return "INSERT INTO triggers (id, tick_id, job_name, created_at, outcome, reason_code)\n" +
		"			VALUES ('" + id + "', '01J0TICK1', 'nightly', 2001, '" + outcome + "', " + sqlOrNull(code) + ")"
}

func leaseEventInsert(lease, holder string, epoch int, code string) string {
	return "INSERT INTO lease_events (at, lease, holder, epoch, reason_code)\n" +
		"			VALUES (3001, '" + lease + "', '" + holder + "', " + itoa(epoch) + ", '" + code + "')"
}

// sqlOrNull quotes a code for these fixtures, or writes NULL when it is empty,
// which is what makes the refusal cases above refuse through the reason CHECK
// rather than through a quoting mistake.
func sqlOrNull(v string) string {
	if v == "" {
		return "NULL"
	}
	return "'" + v + "'"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// TestSchemaStateListsMatchTheModel reads the CHECK constraints out of the
// migration files and holds them against internal/model: same closed sets, and
// the states excused from carrying a reason code are exactly the ones the
// model calls non-terminal. Both ends enforce one rule because one end always
// has the bug (07 section 7); this test is what keeps the two copies honest.
func TestSchemaStateListsMatchTheModel(t *testing.T) {
	runs := tableChecks(t, "0004_execution.sql", "runs")
	steps := tableChecks(t, "0004_execution.sql", "steps")
	ticks := tableChecks(t, "0003_decisions.sql", "ticks")
	triggers := tableChecks(t, "0003_decisions.sql", "triggers")

	assertStateSet(t, "runs.state", runs, "state IN", wantNames(model.AllRunStates()))
	assertStateSet(t, "steps.state", steps, "state IN", wantNames(model.AllStepStates()))
	assertExcused(t, "runs", runs, "state IN", nonTerminalNames(model.AllRunStates()))
	assertExcused(t, "steps", steps, "state IN", nonTerminalNames(model.AllStepStates()))

	outcomes := func(names ...string) []string { return names }
	assertStateSet(t, "ticks.outcome", ticks, "outcome IN",
		outcomes("running", "triggered", "skipped", "error", "missed"))
	assertExcused(t, "ticks", ticks, "outcome IN", outcomes("running", "triggered"))
	assertStateSet(t, "triggers.outcome", triggers, "outcome IN",
		outcomes("accepted", "deduped", "rejected"))
	assertExcused(t, "triggers", triggers, "outcome =", outcomes("accepted"))
}

// TestMigrationSQLUsesOnlyCatalogueCodes scans every migration for anything
// shaped like a reason code and demands it be one. A literal invented inside
// SQL would bypass the static guard over Go source, so SQL gets the same gate.
func TestMigrationSQLUsesOnlyCatalogueCodes(t *testing.T) {
	matches := map[string]string{}
	err := filepath.WalkDir("migrations", func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".sql") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range codeShape.FindAllString(string(raw), -1) {
			matches[m] = path
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk migrations: %v", err)
	}
	known := map[string]bool{}
	for _, c := range reason.Codes() {
		known[c] = true
	}
	for code, path := range matches {
		if !known[code] {
			t.Errorf("%s uses %s, which is not in the reason catalogue", path, code)
		}
	}
}

// TestUnexplainedReasonQueryMatchesTheMachines ties the audit query to the
// model: the states it hunts are exactly the terminal ones, per machine. A
// query listing yesterday's states would quietly stop auditing rows the day a
// state is added, which is precisely when the audit matters.
func TestUnexplainedReasonQueryMatchesTheMachines(t *testing.T) {
	for arm, want := range map[string][]string{
		"run":     terminalNamesRun(),
		"step":    terminalNamesStep(),
		"tick":    {"skipped", "error", "missed"},
		"trigger": {"deduped", "rejected"},
	} {
		re := regexp.MustCompile(`SELECT '` + arm + `'[^;]*?IN \(([^)]*)\)`)
		m := re.FindStringSubmatch(UnexplainedReasonSQL)
		if m == nil {
			t.Fatalf("the %s arm of UnexplainedReasonSQL carries no IN list", arm)
		}
		got := quotedValues(m[1])
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("the %s arm audits %v, the machines say %v", arm, got, want)
		}
	}
}

// TestAuditQueryIsCleanAndCatchesRot seeds a fully explained history and
// expects zero rows, then plants one UNKNOWN and expects exactly that row. A
// query that passed on either side of that pair would be decoration.
func TestAuditQueryIsCleanAndCatchesRot(t *testing.T) {
	s := seededStore(t)
	ctx := context.Background()

	finish := []string{
		runInsert("01J0DONE1", "succeeded", "RUN_SUCCEEDED"),
		stepInsert("build-done", 20, "succeeded", "STEP_SUCCEEDED"),
		stepInsert("build-fail", 21, "failed", "STEP_FAILED_NONZERO_EXIT"),
		tickInsert("01J0SKIP1", "skipped", "TICK_SKIPPED_PAUSED"),
		triggerInsert("01J0DEDU1", "deduped", "TRIGGER_DEDUPED_RUN_KEY"),
	}
	for _, stmt := range finish {
		if _, err := s.w.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed an explained history: %v\n%s", err, stmt)
		}
	}

	rows, err := auditUnknownReasons(ctx, s)
	if err != nil {
		t.Fatalf("run the audit query: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("an explained history audited %d bad rows: %v", len(rows), rows)
	}

	if _, err := s.w.ExecContext(ctx,
		`UPDATE runs SET reason_code = 'UNKNOWN' WHERE id = '01J0DONE1'`); err != nil {
		t.Fatalf("plant the rot: %v", err)
	}
	rows, err = auditUnknownReasons(ctx, s)
	if err != nil {
		t.Fatalf("run the audit query: %v", err)
	}
	if len(rows) != 1 || rows[0].Kind != "run" || rows[0].Key != "01J0DONE1" {
		t.Errorf("planted UNKNOWN was not caught exactly once: %v", rows)
	}
}

func auditUnknownReasons(ctx context.Context, s *Store) ([]UnexplainedReason, error) {
	return s.UnexplainedReasons(ctx)
}

// --- helpers shared by the reconciliation tests ---

var (
	codeShape     = regexp.MustCompile(`\b(?:RUN|STEP|TICK|TRIGGER)_[A-Z0-9_]+\b`)
	quotedValueRe = regexp.MustCompile(`'([a-z_]+)'`)
)

func wantNames[T interface{ String() string }](states []T) []string {
	names := make([]string, 0, len(states))
	for _, s := range states {
		names = append(names, s.String())
	}
	return names
}

func nonTerminalNames[T interface {
	String() string
	RequiresReasonCode() bool
}](states []T) []string {
	names := make([]string, 0, len(states))
	for _, s := range states {
		if !s.RequiresReasonCode() {
			names = append(names, s.String())
		}
	}
	return names
}

func terminalNamesRun() []string {
	var out []string
	for _, s := range model.AllRunStates() {
		if s.RequiresReasonCode() {
			out = append(out, s.String())
		}
	}
	return out
}

func terminalNamesStep() []string {
	var out []string
	for _, s := range model.AllStepStates() {
		if s.RequiresReasonCode() {
			out = append(out, s.String())
		}
	}
	return out
}

// tableChecks extracts every CHECK body from one table's definition in one
// migration file, in declaration order. Index predicates and other statements
// after the table's closing STRICT marker are excluded on purpose: the rule
// lives in the table's own constraints.
func tableChecks(t *testing.T, file, table string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("migrations", file))
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	text := string(raw)
	start := strings.Index(text, "CREATE TABLE "+table+" (")
	if start < 0 {
		t.Fatalf("%s declares no table %s", file, table)
	}
	end := strings.Index(text[start:], "\n) STRICT")
	if end < 0 {
		t.Fatalf("%s: table %s has no closing STRICT marker", file, table)
	}
	section := text[start : start+end]

	var checks []string
	for pos := 0; ; {
		at := strings.Index(section[pos:], "CHECK (")
		if at < 0 {
			break
		}
		bodyStart := pos + at + len("CHECK (")
		depth := 1
		i := bodyStart
		for ; i < len(section) && depth > 0; i++ {
			switch section[i] {
			case '(':
				depth++
			case ')':
				depth--
			}
		}
		checks = append(checks, section[bodyStart:i-1])
		pos = i
	}
	if len(checks) == 0 {
		t.Fatalf("%s: table %s carries no CHECK constraints", file, table)
	}
	return checks
}

// assertStateSet finds the constraint that defines the closed set for a column
// and compares it with the model's set, both ways. The reason constraint also
// mentions the column, so the one carrying reason_code is excluded here.
func assertStateSet(t *testing.T, label string, checks []string, pattern string, want []string) {
	t.Helper()
	for _, body := range checks {
		if !strings.Contains(body, pattern) || strings.Contains(body, "reason_code") {
			continue
		}
		got := quotedValues(body)
		sort.Strings(got)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s allows %v, the model says %v", label, got, want)
		}
		return
	}
	t.Fatalf("%s: no %s IN constraint found", label, pattern)
}

// assertExcused finds the CHECK that carries the reason rule and compares the
// values it excuses from carrying a code with the model's non-terminal set.
// It matches either spelling, an IN list or a single equality.
func assertExcused(t *testing.T, label string, checks []string, pattern string, wantExcused []string) {
	t.Helper()
	for _, body := range checks {
		if !strings.Contains(body, "reason_code IS NOT NULL") || !strings.Contains(body, pattern) {
			continue
		}
		got := quotedValues(body)
		sort.Strings(got)
		sort.Strings(wantExcused)
		if strings.Join(got, ",") != strings.Join(wantExcused, ",") {
			t.Errorf("%s excuses %v from a reason code, the model says the non-terminal set is %v",
				label, got, wantExcused)
		}
		return
	}
	t.Fatalf("%s: no constraint demands a reason_code beside %s", label, pattern)
}

func quotedValues(s string) []string {
	ms := quotedValueRe.FindAllStringSubmatch(s, -1)
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m[1])
	}
	return out
}
