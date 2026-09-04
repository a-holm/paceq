package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/spec"
)

// sensors_store tests walk the real SQLite file in a temp directory, never
// :memory:, and assert the rows and the events together: materialisation is a
// fact of the database, and a sync that forgets its own story would hide here.

func sensorFixture(name string, interval time.Duration) spec.Sensor {
	return spec.Sensor{
		Name:               name,
		Kind:               spec.DefaultSensorKind,
		Run:                []string{"/srv/etl/probe.sh"},
		Interval:           interval,
		MinInterval:        time.Second,
		Timeout:            30 * time.Second,
		MaxTriggersPerTick: 100,
	}
}

func seedSensorsJob(t *testing.T, s *Store, name string) {
	t.Helper()
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:  name,
		SpecHash: "sha256:" + name,
		SpecJSON: `{"schema":"paceq.job.v1","name":"` + name + `","timeout_ms":3600000,"max_concurrent":1,"steps":[{"name":"build","run":["true"]}]}`,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
}

func TestSyncSensorsMaterialisesTheIssueDefaults(t *testing.T) {
	s := migratedStore(t)

	seedSensorsJob(t, s, "import")

	if _, err := s.SyncSensors(context.Background(), "import", []spec.Sensor{sensorFixture("dropzone", 15*time.Second)}); err != nil {
		t.Fatalf("SyncSensors: %v", err)
	}

	var cursor *string
	var dedupEpoch, failures, nextEvalAt int64
	err := s.w.QueryRow(`SELECT cursor, dedup_epoch, consecutive_failures, next_eval_at
FROM sensors WHERE name = 'dropzone'`).Scan(&cursor, &dedupEpoch, &failures, &nextEvalAt)
	if err != nil {
		t.Fatalf("read the sensor row: %v", err)
	}
	if cursor != nil {
		t.Errorf("a fresh sensor has a cursor: %v", *cursor)
	}
	if dedupEpoch != 0 || failures != 0 {
		t.Errorf("a fresh sensor starts with dedup_epoch=%d failures=%d", dedupEpoch, failures)
	}
	if nextEvalAt == 0 {
		t.Error("next_eval_at is not set")
	}
}

func TestSyncSensorsTwiceIsANoOp(t *testing.T) {
	s := migratedStore(t)

	seedSensorsJob(t, s, "import")

	if _, err := s.SyncSensors(context.Background(), "import", []spec.Sensor{sensorFixture("dropzone", 15*time.Second)}); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	var firstUpdated int64
	if err := s.w.QueryRow(`SELECT updated_at FROM sensors WHERE name='dropzone'`).Scan(&firstUpdated); err != nil {
		t.Fatalf("first read: %v", err)
	}

	res, err := s.SyncSensors(context.Background(), "import", []spec.Sensor{sensorFixture("dropzone", 15*time.Second)})
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if len(res.Unchanged) != 1 {
		t.Errorf("second sync reports %+v, want one unchanged", res)
	}
	var secondUpdated int64
	if err := s.w.QueryRow(`SELECT updated_at FROM sensors WHERE name='dropzone'`).Scan(&secondUpdated); err != nil {
		t.Fatalf("second read: %v", err)
	}
	if firstUpdated != secondUpdated {
		t.Errorf("re-apply moved updated_at from %d to %d", firstUpdated, secondUpdated)
	}
}

func TestSyncSensorsLeavesDriftStateAlone(t *testing.T) {
	s := migratedStore(t)

	seedSensorsJob(t, s, "import")

	if _, err := s.SyncSensors(context.Background(), "import", []spec.Sensor{sensorFixture("dropzone", 15*time.Second)}); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	if _, err := s.w.Exec(`UPDATE sensors SET cursor='c-42', dedup_epoch=3, consecutive_failures=2
WHERE name='dropzone'`); err != nil {
		t.Fatalf("stage drift: %v", err)
	}

	if _, err := s.SyncSensors(context.Background(), "import", []spec.Sensor{sensorFixture("dropzone", 30*time.Second)}); err != nil {
		t.Fatalf("re-sync after drift: %v", err)
	}

	var cursor string
	var epoch, failures int64
	var interval int64
	if err := s.w.QueryRow(`SELECT cursor, dedup_epoch, consecutive_failures, interval_ms
FROM sensors WHERE name='dropzone'`).Scan(&cursor, &epoch, &failures, &interval); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if cursor != "c-42" || epoch != 3 || failures != 2 {
		t.Errorf("drift was reset: cursor=%q epoch=%d failures=%d", cursor, epoch, failures)
	}
	if interval != 30000 {
		t.Errorf("the definition update did not land: interval_ms=%d", interval)
	}
}

func TestSyncSensorsRemovesAGoneSensorAndKeepsRunKeys(t *testing.T) {
	s := migratedStore(t)

	seedSensorsJob(t, s, "import")

	if _, err := s.SyncSensors(context.Background(), "import", []spec.Sensor{
		sensorFixture("dropzone", 15*time.Second),
		sensorFixture("archive", 60*time.Second),
	}); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	if _, err := s.w.Exec(`INSERT OR IGNORE INTO run_keys (source_id, epoch, run_key, first_seen_at)
VALUES ('dropzone', 0, 'k1', 1)`); err != nil {
		t.Fatalf("seed a run key: %v", err)
	}

	res, err := s.SyncSensors(context.Background(), "import", []spec.Sensor{sensorFixture("dropzone", 15*time.Second)})
	if err != nil {
		t.Fatalf("sync after removal: %v", err)
	}
	if len(res.Removed) != 1 || res.Removed[0] != "archive" {
		t.Errorf("removal reports %+v", res.Removed)
	}

	var stillThere int
	if err := s.w.QueryRow(`SELECT COUNT(*) FROM sensors WHERE name='archive'`).Scan(&stillThere); err != nil {
		t.Fatalf("count: %v", err)
	}
	if stillThere != 0 {
		t.Errorf("the removed sensor row is still there")
	}
	var keys int
	if err := s.w.QueryRow(`SELECT COUNT(*) FROM run_keys WHERE source_id='dropzone' AND run_key='k1'`).Scan(&keys); err != nil {
		t.Fatalf("count keys: %v", err)
	}
	if keys != 1 {
		t.Errorf("run_keys did not survive the removal")
	}
	var events int
	if err := s.w.QueryRow(`SELECT COUNT(*) FROM sensor_events WHERE sensor_name='archive' AND kind='removed'`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Errorf("no removed event written")
	}
}

func TestApplyJobsMaterialisesSensorsInTheSameTransaction(t *testing.T) {
	s := migratedStore(t)

	results, err := s.ApplyJobs(context.Background(), []JobVersionInput{{
		JobName:  "import",
		SpecHash: "sha256:s1",
		SpecJSON: `{"schema":"paceq.job.v1","name":"import","steps":[{"name":"build","run":["true"]}]}`,
		Sensors:  []spec.Sensor{sensorFixture("dropzone", 15*time.Second)},
	}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(results) != 1 || len(results[0].Sensors.Created) != 1 {
		t.Fatalf("apply result does not carry the sensor sync: %+v", results)
	}
	var count int
	if err := s.w.QueryRow(`SELECT COUNT(*) FROM sensors WHERE job_name='import' AND name='dropzone' AND dedup_epoch=0`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Error("apply did not materialise the sensor row")
	}
}

func TestPausedSensorWithoutAReasonIsRefusedByTheTrigger(t *testing.T) {
	s := migratedStore(t)

	seedSensorsJob(t, s, "import")
	if _, err := s.SyncSensors(context.Background(), "import", []spec.Sensor{sensorFixture("dropzone", 15*time.Second)}); err != nil {
		t.Fatalf("sync: %v", err)
	}

	_, err := s.w.Exec(`UPDATE sensors SET paused = 1 WHERE name = 'dropzone'`)
	if err == nil {
		t.Fatal("the database accepted a pause with no reason")
	}
	if !strings.Contains(err.Error(), "paused") {
		t.Errorf("the trigger says why it refused: %v", err)
	}
}

// sensorJobInput is one job file as apply hands it to the store: a job, the
// hash of its spec, and the sensors it declares.
func sensorJobInput(job, hash string, sensors ...spec.Sensor) JobVersionInput {
	return JobVersionInput{
		JobName:  job,
		SpecHash: "sha256:" + hash,
		SpecJSON: `{"schema":"paceq.job.v1","name":"` + job + `","steps":[{"name":"build","run":["true"]}]}`,
		Sensors:  sensors,
	}
}

// sensorRow is the owner and the drift state of one sensor row. The ownership
// tests compare whole rows, because a name that changes hands and a cursor
// that travels with it are one fact, not two.
type sensorRow struct {
	job    string
	cursor sql.NullString
	epoch  int64
}

func readSensorRow(t *testing.T, s *Store, name string) sensorRow {
	t.Helper()

	var out sensorRow
	if err := s.w.QueryRow(`SELECT job_name, cursor, dedup_epoch FROM sensors WHERE name = ?`, name).
		Scan(&out.job, &out.cursor, &out.epoch); err != nil {
		t.Fatalf("read the sensor row %s: %v", name, err)
	}
	return out
}

// TestApplyJobsRefusesASensorNameAnotherJobOwns is the cross-batch half of the
// rule the in-batch check states: a sensor name is owned by one job, and the
// job that does not own it is refused rather than handed the row, its cursor
// and its dedup epoch.
func TestApplyJobsRefusesASensorNameAnotherJobOwns(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	if _, err := s.ApplyJobs(ctx, []JobVersionInput{
		sensorJobInput("alpha", "a1", sensorFixture("dropzone", 15*time.Second)),
	}); err != nil {
		t.Fatalf("seed alpha: %v", err)
	}
	if _, err := s.w.Exec(`UPDATE sensors SET cursor='c-42', dedup_epoch=3, consecutive_failures=2
WHERE name='dropzone'`); err != nil {
		t.Fatalf("stage drift: %v", err)
	}

	res, err := s.ApplyJobs(ctx, []JobVersionInput{
		sensorJobInput("beta", "b1", sensorFixture("dropzone", 30*time.Second)),
	})
	if err == nil {
		t.Fatalf("beta took the sensor name alpha owns, and apply reported %+v", res)
	}
	var taken *SensorNameTakenError
	if !errors.As(err, &taken) {
		t.Fatalf("the refusal is %T (%v), want a *SensorNameTakenError", err, err)
	}
	if taken.Sensor != "dropzone" || taken.Owner != "alpha" || taken.Taker != "beta" {
		t.Errorf("the refusal reads %+v, want dropzone owned by alpha and declared by beta", *taken)
	}

	var failures, interval int64
	got := readSensorRow(t, s, "dropzone")
	if err := s.w.QueryRow(`SELECT consecutive_failures, interval_ms FROM sensors WHERE name='dropzone'`).
		Scan(&failures, &interval); err != nil {
		t.Fatalf("read the definition columns: %v", err)
	}
	if got.job != "alpha" {
		t.Errorf("the row moved to %s", got.job)
	}
	if got.cursor.String != "c-42" || got.epoch != 3 || failures != 2 {
		t.Errorf("the refusal moved drift state: %+v failures=%d", got, failures)
	}
	if interval != 15000 {
		t.Errorf("the refusal wrote beta's definition anyway: interval_ms=%d", interval)
	}

	var versions int
	if err := s.w.QueryRow(`SELECT COUNT(*) FROM job_versions WHERE job_name='beta'`).Scan(&versions); err != nil {
		t.Fatalf("count beta versions: %v", err)
	}
	if versions != 0 {
		t.Errorf("the refused batch left %d versions of beta behind", versions)
	}
}

// TestSyncSensorsRefusesANameAnotherJobOwns proves the guard in the upsert
// itself, not the check in front of it: a caller that reaches the SQL with a
// name somebody else owns writes nothing and is told why.
func TestSyncSensorsRefusesANameAnotherJobOwns(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	seedSensorsJob(t, s, "alpha")
	seedSensorsJob(t, s, "beta")
	if _, err := s.SyncSensors(ctx, "alpha", []spec.Sensor{sensorFixture("dropzone", 15*time.Second)}); err != nil {
		t.Fatalf("seed alpha: %v", err)
	}

	res, err := s.SyncSensors(ctx, "beta", []spec.Sensor{sensorFixture("dropzone", 30*time.Second)})
	if err == nil {
		t.Fatalf("the upsert moved the row to beta and reported %+v", res)
	}
	var taken *SensorNameTakenError
	if !errors.As(err, &taken) {
		t.Fatalf("the refusal is %T (%v), want a *SensorNameTakenError", err, err)
	}
	if got := readSensorRow(t, s, "dropzone"); got.job != "alpha" {
		t.Errorf("the row moved to %s", got.job)
	}
	var events int
	if err := s.w.QueryRow(`SELECT COUNT(*) FROM sensor_events WHERE job_name='beta'`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 0 {
		t.Errorf("the refused sync wrote %d lifecycle events for beta", events)
	}
}

// TestSensorHandoverIsTheSameInOneBatchAndTwo is the pair the rule has to
// answer identically. One job releases a sensor name and another declares it:
// as one apply, as two applies, and with the two job files in either order.
func TestSensorHandoverIsTheSameInOneBatchAndTwo(t *testing.T) {
	ctx := context.Background()
	dropzone := sensorFixture("dropzone", 15*time.Second)

	// Every case starts from alpha owning the name with a cursor on the row,
	// so a hand-over that carries the cursor is visible in the end state.
	start := func() *Store {
		t.Helper()
		s := migratedStore(t)
		if _, err := s.ApplyJobs(ctx, []JobVersionInput{sensorJobInput("alpha", "a1", dropzone)}); err != nil {
			t.Fatalf("seed alpha: %v", err)
		}
		if _, err := s.w.Exec(`UPDATE sensors SET cursor='c-42', dedup_epoch=3 WHERE name='dropzone'`); err != nil {
			t.Fatalf("stage drift: %v", err)
		}
		return s
	}

	one := start()
	if _, err := one.ApplyJobs(ctx, []JobVersionInput{
		sensorJobInput("alpha", "a2"),
		sensorJobInput("beta", "b1", dropzone),
	}); err != nil {
		t.Fatalf("hand-over in one batch: %v", err)
	}

	two := start()
	if _, err := two.ApplyJobs(ctx, []JobVersionInput{sensorJobInput("alpha", "a2")}); err != nil {
		t.Fatalf("hand-over, first command: %v", err)
	}
	if _, err := two.ApplyJobs(ctx, []JobVersionInput{sensorJobInput("beta", "b1", dropzone)}); err != nil {
		t.Fatalf("hand-over, second command: %v", err)
	}

	gotOne, gotTwo := readSensorRow(t, one, "dropzone"), readSensorRow(t, two, "dropzone")
	if gotOne != gotTwo {
		t.Fatalf("one batch left %+v and two batches left %+v", gotOne, gotTwo)
	}
	if gotOne.job != "beta" {
		t.Errorf("the released name did not reach beta: %+v", gotOne)
	}
	if gotOne.cursor.Valid || gotOne.epoch != 0 {
		t.Errorf("the new owner inherited the old owner's cursor state: %+v", gotOne)
	}

	reversed := start()
	if _, err := reversed.ApplyJobs(ctx, []JobVersionInput{
		sensorJobInput("beta", "b1", dropzone),
		sensorJobInput("alpha", "a2"),
	}); err != nil {
		t.Fatalf("hand-over with the files in the other order: %v", err)
	}
	if got := readSensorRow(t, reversed, "dropzone"); got != gotOne {
		t.Errorf("the file order changed the outcome: %+v, want %+v", got, gotOne)
	}
}

// TestSyncSensorsRenamesWithinOneJob is the move the ownership rule must not
// stand in the way of: one job drops a sensor name and takes another in the
// same sync, and the new row starts with no cursor and no epoch behind it.
func TestSyncSensorsRenamesWithinOneJob(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)

	seedSensorsJob(t, s, "import")
	if _, err := s.SyncSensors(ctx, "import", []spec.Sensor{sensorFixture("dropzone", 15*time.Second)}); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if _, err := s.w.Exec(`UPDATE sensors SET cursor='c-42', dedup_epoch=3 WHERE name='dropzone'`); err != nil {
		t.Fatalf("stage drift: %v", err)
	}

	res, err := s.SyncSensors(ctx, "import", []spec.Sensor{sensorFixture("landing", 15*time.Second)})
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if len(res.Created) != 1 || res.Created[0] != "landing" ||
		len(res.Removed) != 1 || res.Removed[0] != "dropzone" {
		t.Errorf("the rename reports %+v", res)
	}

	var left int
	if err := s.w.QueryRow(`SELECT COUNT(*) FROM sensors WHERE name='dropzone'`).Scan(&left); err != nil {
		t.Fatalf("count the old name: %v", err)
	}
	if left != 0 {
		t.Errorf("the old name is still a row")
	}
	if got := readSensorRow(t, s, "landing"); got.job != "import" || got.cursor.Valid || got.epoch != 0 {
		t.Errorf("the renamed sensor did not start clean: %+v", got)
	}
}
