package runner

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestOutcomeTaxonomy is the extracted outcome table from the issue and the
// observability plan, made executable. Each row is a real process driven to
// the condition it names, and the full Result is what the table promises.
// explain later reads these codes; the three failure kinds must never blur.
func TestOutcomeTaxonomy(t *testing.T) {
	fake := fakecmd(t)
	dir := t.TempDir()

	notExecutable := filepath.Join(dir, "not-executable")
	if err := os.WriteFile(notExecutable, []byte("#!/bin/sh\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	type row struct {
		name       string
		spec       func() Spec
		outcome    Outcome
		reason     string
		exitCode   int
		signal     string
		dataKeys   []string
		spawnError bool
	}

	rows := []row{
		{
			name:     "exit 0 is Succeeded",
			spec:     func() Spec { return baseSpec(t, "/bin/true") },
			outcome:  Succeeded,
			reason:   ReasonSucceeded,
			exitCode: 0,
		},
		{
			name:     "exit N is Failed with the code",
			spec:     func() Spec { return baseSpec(t, "/bin/sh", "-c", "exit 3") },
			outcome:  Failed,
			reason:   ReasonNonzeroExit,
			exitCode: 3,
			dataKeys: []string{"exit_code", "transient"},
		},
		{
			name:     "exit 75 is Failed and transient",
			spec:     func() Spec { return baseSpec(t, "/bin/sh", "-c", "exit 75") },
			outcome:  Failed,
			reason:   ReasonNonzeroExit,
			exitCode: 75,
			dataKeys: []string{"exit_code", "transient"},
		},
		{
			name:     "signal death is Signalled",
			spec:     func() Spec { return baseSpec(t, fake, "signal-self", "KILL") },
			outcome:  Signalled,
			reason:   ReasonSignal,
			exitCode: 128 + 9, // SIGKILL on Linux
			signal:   "SIGKILL",
			dataKeys: []string{"signal"},
		},
		{
			name: "deadline is TimedOut",
			spec: func() Spec {
				s := baseSpec(t, fake, "ignore-term", "5m")
				s.Timeout = 200 * time.Millisecond
				return s
			},
			outcome:  TimedOut,
			reason:   ReasonTimeout,
			exitCode: 0,
			dataKeys: []string{"timeout_ms"},
		},
		{
			name: "missing command is SpawnFailed",
			spec: func() Spec { return baseSpec(t, "/paceq/no/such/binary") },
			// exitCode 0: no process ever ran, so there is no exit status
			outcome:    SpawnFailed,
			reason:     ReasonSpawn,
			dataKeys:   []string{"errno"},
			spawnError: true,
		},
		{
			name: "missing workdir is SpawnFailed",
			spec: func() Spec {
				s := baseSpec(t, "/bin/true")
				s.Workdir = filepath.Join(dir, "absent")
				return s
			},
			outcome:    SpawnFailed,
			reason:     ReasonSpawn,
			dataKeys:   []string{"errno"},
			spawnError: true,
		},
	}
	if !runningAsRoot() {
		rows = append(rows, row{
			name:       "not executable is SpawnFailed",
			spec:       func() Spec { return baseSpec(t, notExecutable) },
			outcome:    SpawnFailed,
			reason:     ReasonSpawn,
			dataKeys:   []string{"errno"},
			spawnError: true,
		})
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			s := row.spec()
			s.Stdout = io.Discard
			s.Stderr = io.Discard
			if s.Timeout == 0 {
				s.Timeout = 30 * time.Second
			}
			res, err := runBounded(t, 15*time.Second, context.Background(), s)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}

			if res.Outcome != row.outcome {
				t.Fatalf("outcome = %v, want %v", res.Outcome, row.outcome)
			}
			if res.ReasonCode != row.reason {
				t.Errorf("reason = %q, want %q", res.ReasonCode, row.reason)
			}
			if res.ExitCode != row.exitCode {
				t.Errorf("exit = %d, want %d", res.ExitCode, row.exitCode)
			}
			if res.Signal != row.signal {
				t.Errorf("signal = %q, want %q", res.Signal, row.signal)
			}
			for _, key := range row.dataKeys {
				if _, ok := res.ReasonData[key]; !ok {
					t.Errorf("reason_data missing %q (have %v)", key, res.ReasonData)
				}
			}
			if row.spawnError && res.Err == nil {
				t.Errorf("Err = nil, want the operating system cause preserved")
			}
			if !row.spawnError && res.Err != nil {
				t.Errorf("Err = %v, want nil once the process ran", res.Err)
			}
		})
	}
}

func runningAsRoot() bool { return os.Getuid() == 0 }
