package store

import (
	"context"
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
