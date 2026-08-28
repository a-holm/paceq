package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The repair side of fsck (M6-06): every repair puts the row where the system
// itself would have put it through ordinary reconciliation, writes its own
// audit event as actor fsck, and leaves run_keys and history untouched.

// repairFixture is a seeded run with its first step started, as a live claim
// would leave it.
func repairFixture(t *testing.T) (*Store, string) {
	t.Helper()
	s, runID := plantSeededRun(t)
	ctx := context.Background()
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'running',
lease_owner = 'exec-1', lease_epoch = 1,
lease_expires_at = ?,
started_at = created_at + 1000
WHERE id = ?`, time.Now().Add(time.Hour).UnixMilli(), runID); err != nil {
		t.Fatalf("claim the run: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE steps SET state = 'running',
attempt = 1, started_at = ? WHERE run_id = ? AND name = 'build'`,
		time.Now().UnixMilli(), runID); err != nil {
		t.Fatalf("start the step: %v", err)
	}
	return s, runID
}

func eventCount(t *testing.T, s *Store, where string, args ...any) int {
	t.Helper()
	rows, err := s.r.QueryContext(context.Background(),
		`SELECT COUNT(*) FROM run_events WHERE `+where, args...)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("no count row")
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatalf("scan the count: %v", err)
	}
	return n
}

func TestFsckRepairRequeuesAnOrphanedRunning(t *testing.T) {
	ctx := context.Background()
	s, runID := repairFixture(t)

	// Kill the lease: the run is running with an expired claim, exactly what
	// a dead executor leaves behind.
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET
lease_expires_at = ? WHERE id = ?`,
		time.Now().Add(-time.Hour).UnixMilli(), runID); err != nil {
		t.Fatalf("expire the lease: %v", err)
	}
	if !hasCheck(mustFsck(t, s), "I1") {
		t.Fatal("the planted orphan did not trip I1")
	}

	out, err := s.FsckRepair(ctx, nil, true)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if len(out) != 1 || out[0].Invariant != "I1" || out[0].Repaired != 1 {
		t.Fatalf("the repair reports %+v", out)
	}

	var state string
	var epoch, crashes int
	if err := s.r.QueryRowContext(ctx,
		`SELECT state, lease_epoch, crash_count FROM runs WHERE id = ?`, runID).
		Scan(&state, &epoch, &crashes); err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	if state != "queued" || epoch != 2 || crashes != 1 {
		t.Fatalf("the run is %s at epoch %d with %d crashes, want queued/2/1",
			state, epoch, crashes)
	}
	if n := eventCount(t, s, `run_id = ? AND kind = 'run.requeued' AND actor = 'fsck'`, runID); n != 1 {
		t.Fatalf("the repair wrote %d fsck requeue events, want 1", n)
	}

	// The sweep is clean afterwards, and the token history ends at the new
	// epoch: a repair that broke I11 to fix I1 would be no repair at all.
	violations := mustFsck(t, s)
	for _, v := range violations {
		if v.Check == "I1" || v.Check == "I11" {
			t.Fatalf("the sweep still reports %s after the repair: %+v", v.Check, violations)
		}
	}
}

func TestFsckRepairClosesStrandedSteps(t *testing.T) {
	ctx := context.Background()
	s, runID := repairFixture(t)

	// The run finishes under the step: build is running, deploy pending.
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'succeeded',
reason_code = 'RUN_SUCCEEDED', finished_at = created_at + 2000 WHERE id = ?`, runID); err != nil {
		t.Fatalf("finish the run: %v", err)
	}
	if !hasCheck(mustFsck(t, s), "I2") {
		t.Fatal("the stranded steps did not trip I2")
	}

	out, err := s.FsckRepair(ctx, nil, true)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if len(out) != 1 || out[0].Invariant != "I2" || out[0].Repaired != 2 {
		t.Fatalf("the repair reports %+v", out)
	}
	var states [2]string
	for i, name := range []string{"build", "deploy"} {
		var state, code string
		if err := s.r.QueryRowContext(ctx,
			`SELECT state, reason_code FROM steps WHERE run_id = ? AND name = ?`, runID, name).
			Scan(&state, &code); err != nil {
			t.Fatalf("read step %s back: %v", name, err)
		}
		states[i] = state + "/" + code
	}
	if states[0] != "cancelled/STEP_CANCELLED" {
		t.Errorf("the running step closed as %s, want cancelled/STEP_CANCELLED", states[0])
	}
	if states[1] != "skipped/STEP_CANCELLED" {
		t.Errorf("the pending step closed as %s, want skipped/STEP_CANCELLED", states[1])
	}
	if n := eventCount(t, s, `run_id = ? AND actor = 'fsck' AND kind IN ('step.cancelled','step.skipped')`, runID); n != 2 {
		t.Fatalf("the repair wrote %d step events, want 2", n)
	}
	// The run row itself is history and stays that way.
	var runState string
	if err := s.r.QueryRowContext(ctx, `SELECT state FROM runs WHERE id = ?`, runID).
		Scan(&runState); err != nil || runState != "succeeded" {
		t.Fatalf("the run row moved: %q (err=%v)", runState, err)
	}
	if !hasCheck(mustFsck(t, s), "I2") {
		for _, v := range mustFsck(t, s) {
			if v.Check == "I2" {
				t.Fatalf("I2 survived the repair: %+v", v)
			}
		}
	}
}

func TestFsckRepairStampsAnUnexplainedDeferral(t *testing.T) {
	ctx := context.Background()
	s, runID := repairFixture(t)

	// Park the run in the future with no reason: a row the schema refuses
	// today, so the plant relaxes the CHECKs first.
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 1`); err != nil {
		t.Fatalf("relax the CHECKs: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'queued',
lease_owner = NULL, lease_expires_at = NULL, defer_reason = NULL,
available_at = created_at + 3600000 WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the unexplained hold: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 0`); err != nil {
		t.Fatalf("restore the CHECKs: %v", err)
	}
	if !hasCheck(mustFsck(t, s), "I14") {
		t.Fatal("the unexplained hold did not trip I14")
	}

	if _, err := s.FsckRepair(ctx, nil, false); err != nil {
		t.Fatalf("repair: %v", err)
	}
	var reason string
	if err := s.r.QueryRowContext(ctx,
		`SELECT defer_reason FROM runs WHERE id = ?`, runID).Scan(&reason); err != nil {
		t.Fatalf("read the defer reason back: %v", err)
	}
	if reason != "unspecified" {
		t.Fatalf("the hold says %q, want unspecified", reason)
	}
	if n := eventCount(t, s, `run_id = ? AND kind = 'run.repaired' AND actor = 'fsck'`, runID); n != 1 {
		t.Fatalf("the repair wrote %d events, want 1", n)
	}
	if hasCheck(mustFsck(t, s), "I14") {
		t.Fatal("I14 survived the repair")
	}
}

func TestFsckRepairStampsLegacyReasonCodes(t *testing.T) {
	ctx := context.Background()
	s, runID := repairFixture(t)

	// The run and its step ended without a usable code: rows an older paceq
	// wrote before the discipline existed.
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 1`); err != nil {
		t.Fatalf("relax the CHECKs: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'failed',
reason_code = 'UNKNOWN', finished_at = created_at + 2000 WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the legacy run: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE steps SET state = 'failed',
reason_code = 'UNKNOWN', finished_at = started_at WHERE run_id = ? AND name = 'build'`, runID); err != nil {
		t.Fatalf("plant the legacy step: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 0`); err != nil {
		t.Fatalf("restore the CHECKs: %v", err)
	}
	if !hasCheck(mustFsck(t, s), "reason") {
		t.Fatal("the legacy rows did not trip the reason rule")
	}

	out, err := s.FsckRepair(ctx, nil, false)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	var reasonOut *RepairOutcome
	for i := range out {
		if out[i].Invariant == "reason" {
			reasonOut = &out[i]
		}
	}
	if reasonOut == nil || reasonOut.Repaired != 2 {
		t.Fatalf("the repair reports %+v", out)
	}
	var runCode, stepCode string
	if err := s.r.QueryRowContext(ctx,
		`SELECT reason_code FROM runs WHERE id = ?`, runID).Scan(&runCode); err != nil {
		t.Fatal(err)
	}
	if err := s.r.QueryRowContext(ctx,
		`SELECT reason_code FROM steps WHERE run_id = ? AND name = 'build'`, runID).Scan(&stepCode); err != nil {
		t.Fatal(err)
	}
	if runCode != "RUN_LEGACY_UNSPECIFIED" || stepCode != "STEP_LEGACY_UNSPECIFIED" {
		t.Fatalf("the stamps read %q and %q", runCode, stepCode)
	}
	for _, v := range mustFsck(t, s) {
		if v.Check == "reason" && v.Subject == "run "+runID {
			t.Fatalf("the run still reports without a usable code: %+v", v)
		}
	}
}

// TestFsckRepairDemandsConfirmationOnCriticalFindings is the R11 half of the
// contract: critical findings mean hand edits or corruption, and the repair
// refuses to run around one without the operator's explicit --confirm.
func TestFsckRepairDemandsConfirmationOnCriticalFindings(t *testing.T) {
	ctx := context.Background()
	s, runID := repairFixture(t)

	// Plant the duplicate behind the everyday gate, the way corruption or a
	// hand edit would arrive.
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET run_key = 'shared-key'
WHERE id = ?`, runID); err != nil {
		t.Fatalf("name the run_key: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `INSERT INTO runs (id, job_name, job_version_id,
	state, origin, run_key, available_at, created_at, updated_at)
	SELECT '01DUPRUNKEY0000000000000B', job_name, job_version_id, state, origin,
		run_key, available_at, created_at, updated_at
	FROM runs WHERE id = ?`, runID); err != nil {
		t.Fatalf("plant the duplicate: %v", err)
	}
	if !hasCheck(mustFsck(t, s), "I3") {
		t.Fatal("the planted duplicate did not trip I3")
	}

	if _, err := s.FsckRepair(ctx, nil, false); err == nil {
		t.Fatal("the repair ran with critical findings and no confirmation")
	} else {
		var need *RepairConfirmError
		if !errors.As(err, &need) {
			t.Fatalf("the refusal is %T, want *RepairConfirmError", err)
		}
	}

	// Keys are history: the repair never touches the run_keys table, whatever
	// it repairs.
	var keysBefore, keysAfter int
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_keys`).Scan(&keysBefore); err != nil {
		t.Fatalf("count the run keys: %v", err)
	}
	if _, err := s.FsckRepair(ctx, nil, true); err != nil {
		t.Fatalf("the confirmed repair refused: %v", err)
	}
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_keys`).Scan(&keysAfter); err != nil {
		t.Fatalf("count the run keys: %v", err)
	}
	if keysBefore != keysAfter {
		t.Fatalf("the repair moved the run_keys table: %d -> %d", keysBefore, keysAfter)
	}
}

// TestFsckRepairHonoursTheOnlyList keeps the scope promise: --only repairs
// exactly the named invariants and leaves the rest of the findings standing.
func TestFsckRepairHonoursTheOnlyList(t *testing.T) {
	ctx := context.Background()
	s, runID := repairFixture(t)

	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET
lease_expires_at = ? WHERE id = ?`,
		time.Now().Add(-time.Hour).UnixMilli(), runID); err != nil {
		t.Fatalf("expire the lease: %v", err)
	}
	// An unexplained hold on a second run: a finding the I1 repair cannot
	// cure by side effect.
	version := aJobInternal(t, s, "nightly")
	second, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName: "nightly", JobVersionID: version.ID, Origin: "manual",
		Steps: []NewStep{{Name: "build"}},
	})
	if err != nil {
		t.Fatalf("create the second run: %v", err)
	}
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.w.ExecContext(ctx, `UPDATE runs SET state = 'queued',
defer_reason = NULL, available_at = created_at + 3600000 WHERE id = ?`, second.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.w.ExecContext(ctx, `PRAGMA ignore_check_constraints = 0`); err != nil {
		t.Fatal(err)
	}

	out, err := s.FsckRepair(ctx, []string{"I1"}, false)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	for _, o := range out {
		if o.Invariant != "I1" {
			t.Fatalf("an I1-only pass repaired %+v", o)
		}
	}
	var deferReason []byte
	if err := s.r.QueryRowContext(ctx,
		`SELECT defer_reason FROM runs WHERE id = ?`, second.ID).Scan(&deferReason); err != nil {
		t.Fatal(err)
	}
	if len(deferReason) != 0 {
		t.Fatalf("the I14 finding was repaired by an I1-only pass: defer_reason=%q", deferReason)
	}

	if _, err := s.FsckRepair(ctx, []string{"nope"}, false); err == nil {
		t.Fatal("an unknown invariant name was accepted")
	}
}

func mustFsck(t *testing.T, s *Store) []Violation {
	t.Helper()
	violations, err := s.Fsck(context.Background())
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	return violations
}
