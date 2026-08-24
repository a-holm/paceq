package store

import (
	"context"
	"testing"
)

// The write half of the sensors CLI (issue #11): pause sets the reason, resume
// clears the breaker state. These pin the exact store contract the CLI relies
// on so a mutation that breaks either direction is caught without a GUI.

func TestPauseSensorSetsReason(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, "finder", "a", 0)

	if err := s.PauseSensor(context.Background(), "finder", "deploying"); err != nil {
		t.Fatalf("PauseSensor: %v", err)
	}

	row, err := s.GetSensor(context.Background(), "finder")
	if err != nil {
		t.Fatalf("GetSensor: %v", err)
	}
	if !row.Paused {
		t.Error("Paused = false after pause, want true")
	}
	if row.PausedReason != "deploying" {
		t.Errorf("PausedReason = %q, want deploying", row.PausedReason)
	}
}

func TestPauseSensorDefaultsReason(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, "finder", "a", 0)

	if err := s.PauseSensor(context.Background(), "finder", ""); err != nil {
		t.Fatalf("PauseSensor: %v", err)
	}
	row, _ := s.GetSensor(context.Background(), "finder")
	if !row.Paused {
		t.Error("Paused = false after pause with empty reason")
	}
}

func TestPauseSensorUnknownIsNotFound(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)

	err := s.PauseSensor(context.Background(), "missing", "why")
	if err == nil {
		t.Fatal("PauseSensor of a missing sensor succeeded, want ErrNotFound")
	}
}

func TestResumeSensorClearsBreaker(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, "finder", "a", 0)

	// Plant a paused sensor with a fatigued breaker directly: the write seam is
	// the pause path, so start from the state resume is meant to clear.
	if _, err := s.w.Exec(`UPDATE sensors
SET paused = 1, paused_reason = 'backoff', consecutive_failures = 7
WHERE name = ?`, "finder"); err != nil {
		t.Fatalf("plant a paused breaker sensor: %v", err)
	}

	if err := s.ResumeSensor(context.Background(), "finder"); err != nil {
		t.Fatalf("ResumeSensor: %v", err)
	}

	row, err := s.GetSensor(context.Background(), "finder")
	if err != nil {
		t.Fatalf("GetSensor: %v", err)
	}
	if row.Paused {
		t.Error("Paused = true after resume, want false")
	}
	if row.PausedReason != "" {
		t.Errorf("PausedReason = %q after resume, want empty", row.PausedReason)
	}
	if row.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d after resume, want 0", row.ConsecutiveFailures)
	}
}

func TestResumeSensorUnknownIsNotFound(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)

	err := s.ResumeSensor(context.Background(), "missing")
	if err == nil {
		t.Fatal("ResumeSensor of a missing sensor succeeded, want ErrNotFound")
	}
}
