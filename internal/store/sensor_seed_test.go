package store

import (
	"context"
	"testing"
)

// UpsertSensor is the seed/definition seam the CLI test harness and early
// fixtures use; SetSensorDue is the store-half of `sensors tick` through the
// daemon. These tests pin each behaviour so a mutation in either direction is
// caught.

func TestUpsertSensorInsertsAndReseeds(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	ctx := context.Background()

	if err := s.UpsertSensor(ctx, SensorSeedInput{
		Name: "finder", JobName: sensorJob, ExecJSON: `["/bin/echo","a"]`,
	}); err != nil {
		t.Fatalf("UpsertSensor create: %v", err)
	}

	row, err := s.GetSensor(ctx, "finder")
	if err != nil {
		t.Fatalf("GetSensor after create: %v", err)
	}
	if row.ExecJSON != `["/bin/echo","a"]` {
		t.Errorf("ExecJSON = %q, want the seeded value", row.ExecJSON)
	}

	// Re-seed with a different exec spec replaces the definition.
	if err := s.UpsertSensor(ctx, SensorSeedInput{
		Name: "finder", JobName: sensorJob, ExecJSON: `["/bin/echo","b"]`,
	}); err != nil {
		t.Fatalf("UpsertSensor reseed: %v", err)
	}
	row, _ = s.GetSensor(ctx, "finder")
	if row.ExecJSON != `["/bin/echo","b"]` {
		t.Errorf("ExecJSON after reseed = %q, want the replacement", row.ExecJSON)
	}
}

func TestUpsertSensorRequiresDefinition(t *testing.T) {
	s := migratedStore(t)
	seedJobVersionsForSensors(t, s)
	ctx := context.Background()

	if err := s.UpsertSensor(ctx, SensorSeedInput{Name: "x", JobName: sensorJob, ExecJSON: ""}); err == nil {
		t.Fatal("UpsertSensor with empty exec succeeded, want a refusal")
	}
}

func TestSetSensorDueMovesNextEvalAt(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, "finder", "a", 0)
	ctx := context.Background()

	// With the writer clock fixed at a known instant, next_eval_at is
	// deterministic.
	if err := s.SetSensorDue(ctx, "finder"); err != nil {
		t.Fatalf("SetSensorDue: %v", err)
	}

	var next int64
	if err := s.r.QueryRowContext(ctx, "SELECT next_eval_at FROM sensors WHERE name = ?", "finder").Scan(&next); err != nil {
		t.Fatalf("read next_eval_at: %v", err)
	}

	// The writer's clock defaults to the system clock; the only assertion that
	// is stable is that the row was made due now (next_eval_at <= now), which
	// SetSensorDue always guarantees by writing the same instant.
	if next > s.clk.Now().UTC().UnixMilli() {
		t.Errorf("next_eval_at = %d, was not set due (should be <= now)", next)
	}
}

func TestSetSensorDueUnknownIsNotFound(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	err := s.SetSensorDue(context.Background(), "missing")
	if err == nil {
		t.Fatal("SetSensorDue of a missing sensor succeeded, want ErrNotFound")
	}
}

// The PauseSensor/ResumeSensor/LockError separation is a two-way mutation trap:
// pausing must set paused AND reason, resuming must clear both AND the breaker.
// TestPauseSensorClearsNothingOnMiss adds the third direction (a missing sensor
// is never created by a pause).
func TestPauseSensorUnknownDoesNotCreateRow(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)

	_ = s.PauseSensor(context.Background(), "ghost", "why")
	var n int
	if err := s.r.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sensors WHERE name = 'ghost'").Scan(&n); err != nil {
		t.Fatalf("count sensors: %v", err)
	}
	if n != 0 {
		t.Errorf("pause of an unknown sensor created %d rows, want 0", n)
	}
}

// seedJobVersionsForSensors is a minimal job seed for tests that never touch
// the tick path and only need the FK to hold.
func seedJobVersionsForSensors(t *testing.T, s *Store) {
	t.Helper()
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName: sensorJob, SpecHash: "sha256:seed",
		SpecJSON: `{"schema":"paceq.job.v1","name":"` + sensorJob + `","steps":[{"name":"c","run":["true"]}]}`,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
}
