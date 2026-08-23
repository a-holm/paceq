package sensor

import (
	"fmt"
	"time"
)

// Spec is the runnable shape of a sensor: everything the evaluator needs to
// start the subprocess. M3-01 materialises a spec like this from a sensors[]
// entry in the job definition; this package owns the type because the
// contract it drives is frozen here, before any store row exists.
type Spec struct {
	Name        string
	Job         string
	Argv        []string
	Workdir     string
	Env         map[string]string // the sensor's declared environment
	Timeout     time.Duration
	MaxTriggers int

	// Cursor and LastTickAt are the row values the evaluation starts from.
	// They are inputs here, never written.
	Cursor     *string
	LastTickAt *int64
}

// Config is what the evaluator needs beyond a spec. The zero value of each
// field resolves to a sensible default, so a bare Evaluator works in tests
// while production wires every knob from the daemon.
type Config struct {
	// MaxStdout is the cap on the sensor's stdout in bytes. Zero means 1 MiB.
	MaxStdout int64
	// StderrTail is how much of the sensor's stderr is kept on the result.
	// Zero means 4 KiB.
	StderrTail int64
	// KillGrace is the SIGTERM to SIGKILL gap for the process group. Zero
	// means the runner default of 10 seconds.
	KillGrace time.Duration
}

func (c Config) maxStdout() int64 {
	if c.MaxStdout > 0 {
		return c.MaxStdout
	}
	return defaultMaxStdout
}

func (c Config) stderrTail() int64 {
	if c.StderrTail > 0 {
		return c.StderrTail
	}
	return defaultStderrTail
}

func (c Config) killGrace() time.Duration {
	if c.KillGrace > 0 {
		return c.KillGrace
	}
	return defaultKillGrace
}

// The fence defaults live together so every documented limit is one constant.
const (
	defaultMaxStdout  = 1 << 20 // 1 MiB, the strongest bound from SYNTESE 4.4
	defaultStderrTail = 4 << 10
	defaultKillGrace  = 10 * time.Second
)

func (s Spec) validate() error {
	if len(s.Argv) == 0 {
		return fmt.Errorf("sensor %q: argv is empty", s.Name)
	}
	if s.Timeout <= 0 {
		return fmt.Errorf("sensor %q: timeout must be positive", s.Name)
	}
	return nil
}
