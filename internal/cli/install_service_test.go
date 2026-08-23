package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallServiceDryRun(t *testing.T) {
	dir := t.TempDir()
	got := runCLI(t, dir, nil, "install-service", "--dry-run")

	if got.code != ExitOK {
		t.Fatalf("install-service --dry-run: %d\n%s", got.code, got.stderr)
	}
	if !strings.Contains(got.stdout, "[Unit]") {
		t.Error("--dry-run should print the unit file content")
	}
	if !strings.Contains(got.stdout, "paceq") {
		t.Error("--dry-run output should mention paceq")
	}
}

func TestInstallServiceWritesToDest(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "etc", "systemd", "system")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}

	got := runCLI(t, dir, nil, "install-service", "--dest", dest)

	if got.code != ExitOK {
		t.Fatalf("install-service: exit %d\n%s", got.code, got.stderr)
	}

	unitPath := filepath.Join(dest, "paceq.service")
	if _, err := os.Stat(unitPath); err != nil {
		t.Errorf("unit file not written: %v", err)
	}

	relaxedPath := filepath.Join(dest, "paceq.service.relaxed")
	if _, err := os.Stat(relaxedPath); err != nil {
		t.Errorf("relaxed file not written: %v", err)
	}
}

func TestInstallServiceIdempotent(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "etc", "systemd", "system")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}

	// First run.
	got1 := runCLI(t, dir, nil, "install-service", "--dest", dest)
	if got1.code != ExitOK {
		t.Fatalf("first install-service: exit %d\n%s", got1.code, got1.stderr)
	}

	// Second run should fail without --force.
	got2 := runCLI(t, dir, nil, "install-service", "--dest", dest)
	if got2.code == ExitOK {
		t.Error("second install-service without --force should fail")
	}
}

func TestInstallServiceForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "etc", "systemd", "system")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}

	// First run.
	got1 := runCLI(t, dir, nil, "install-service", "--dest", dest)
	if got1.code != ExitOK {
		t.Fatalf("first install-service: exit %d\n%s", got1.code, got1.stderr)
	}

	// Second run with --force should succeed.
	got2 := runCLI(t, dir, nil, "install-service", "--dest", dest, "--force")
	if got2.code != ExitOK {
		t.Fatalf("install-service --force: exit %d\n%s", got2.code, got2.stderr)
	}

	unitPath := filepath.Join(dest, "paceq.service")
	if _, err := os.Stat(unitPath); err != nil {
		t.Errorf("unit file should still exist after --force: %v", err)
	}
}

func TestInstallServiceNoOverwriteWithoutForce(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "etc", "systemd", "system")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write a marker file first.
	unitPath := filepath.Join(dest, "paceq.service")
	if err := os.WriteFile(unitPath, []byte("custom content"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := runCLI(t, dir, nil, "install-service", "--dest", dest)
	if got.code == ExitOK {
		t.Error("install-service without --force should error when file exists")
	}

	// The marker content should be preserved.
	data, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "custom content" {
		t.Errorf("existing unit should be preserved, got %q", string(data))
	}
}
