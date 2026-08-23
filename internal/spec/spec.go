package spec

import (
	"regexp"
	"time"
)

// SchemaName is what the canonical JSON calls itself. It is the first thing a
// consumer reads and the thing that makes a second version of the IR possible
// without guessing.
const SchemaName = "paceq.job.v1"

// Limits are the parser's refusals, all of them checked before the file is
// expanded (08 T13). They are exported because the messages quote them and the
// tests assert against the same numbers the code uses.
const (
	// MaxFileBytes is one job file. A job definition that needs a megabyte is
	// carrying data that belongs somewhere else.
	MaxFileBytes = 1 << 20
	// MaxDepth is how deeply the file may nest. The schema itself never goes
	// past six.
	MaxDepth = 32
	// MaxAliases is how many aliases one file may use. Anchors are useful for
	// a shared env block; a hundred of them is a program.
	MaxAliases = 100
	// MaxNodes is the size of the syntax tree, before any alias is resolved.
	MaxNodes = 20000
	// MaxSteps is the same ceiling M4-01 puts on a DAG.
	MaxSteps = 200
	// MaxExpandedNodes is the decoder's budget. Every node it visits costs
	// one, including each visit through an alias, so nested aliases cannot
	// multiply their way past the tree limits above.
	MaxExpandedNodes = 200000
)

// Defaults are materialised into the IR before it is hashed, so that leaving a
// field out and writing its default produce the same job and the same hash.
const (
	// DefaultTimeout is what a job without a timeout gets. A timeout is
	// mandatory (08 section 3.2); the default is what makes it mandatory
	// without making every file say so.
	DefaultTimeout = time.Hour
	// MaxJobTimeout is the system ceiling. A job that legitimately runs longer
	// than a day is a service, and paceq does not supervise services.
	MaxJobTimeout = 24 * time.Hour
	// DefaultMaxConcurrent is one. Cron has the opposite default, and the
	// flock wrappers people write around cron jobs are what it costs them
	// (09 section 7, US-02).
	DefaultMaxConcurrent = 1
	// DefaultTimezone is the zone a schedule without one runs in. UTC rather
	// than the daemon's local zone: a schedule that means something different
	// depending on which machine reads it is not explainable.
	DefaultTimezone = "UTC"
)

// Retry defaults, taken from the project defaults block in 03 section 3.4 so
// the two cannot disagree.
const (
	DefaultBackoff  = BackoffExponential
	DefaultInitial  = 30 * time.Second
	DefaultMaxDelay = 10 * time.Minute
	DefaultJitter   = JitterFull
)

// The values the enumerated fields accept.
const (
	BackoffExponential = "exponential"
	BackoffFixed       = "fixed"
	JitterFull         = "full"
	JitterNone         = "none"
)

// The default kind is the only kind. One adapter, a subprocess that writes
// JSON to stdout (10 section 5, F4b): a second kind is a migration and a new
// evaluator, never a flag somebody sets by accident. The value is spelled out
// here so every reader of what sensors accept takes it from one place.
const DefaultSensorKind = "exec"

// The sensor bounds, all of them public because the messages quote them and
// the tests assert against the same numbers the code uses.
const (
	// SensorIntervalMin is the lowest an interval may go. Sensorers are
	// evaluated by one shared runtime; sub-second polling is a non-goal
	// (SCOPE.md section 3.19).
	SensorIntervalMin = time.Second

	// DefaultSensorTimeout is what a sensor without one gets. The same
	// default the design chose (03 section 4.5, 02 section 5.5).
	DefaultSensorTimeout = 30 * time.Second

	// SensorTimeoutMin is the floor every sensor timeout must clear.
	SensorTimeoutMin = time.Second

	// SensorTimeoutMax is the system ceiling. A sensor that needs more than
	// this should chunk its own work via max_triggers_per_tick, not ask the
	// runtime to wait on it (M1-06, the step ceiling, is the same rule).
	SensorTimeoutMax = 5 * time.Minute

	// DefaultSensorMaxTriggers is the default ceiling on one tick.
	DefaultSensorMaxTriggers = 100

	// SensorMaxTriggersHi is what max_triggers_per_tick may be raised to.
	SensorMaxTriggersHi = 10_000
)

// namePattern is what a job, step, schedule or sensor may be called. Lower case
// and no spaces, because a name appears on a command line, in a directory name
// and in a URL, and a name that needs quoting in any of them is a name that
// will be typed wrong at 03:14.
var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// NamePattern is the rule as a message can quote it.
const NamePattern = "^[a-z0-9][a-z0-9_-]{0,63}$"

// Job is one job definition, after parsing and with every default materialised.
// It is the engine facing type: no positions, no source, nothing that only
// makes sense while a file is open.
type Job struct {
	Name          string
	Description   string
	Env           map[string]string
	EnvFile       string
	InheritEnv    []string
	Workdir       string
	Timeout       time.Duration
	MaxConcurrent int
	Steps         []Step
	Schedules     []Schedule
	Sensors       []Sensor
}

// Step is one command in a job.
type Step struct {
	Name string
	// Run is argv. There is no string form, so nothing here is ever split,
	// quoted or expanded by a shell (08 section 3.2).
	Run []string
	// Shell is the explicit opt in that hands Run to a shell. It carries a
	// validation warning wherever it is true.
	Shell   bool
	Workdir string
	// Timeout is this step's own ceiling. Zero means the step is bounded by
	// the job's timeout rather than by one of its own.
	Timeout time.Duration
	Retry   *Retry
	// Needs is carried into the IR and means nothing yet. The cycle detection
	// and the topological order are M4-01; in M1 the steps run in file order.
	Needs []string
}

// Retry is what happens after a step fails.
type Retry struct {
	Max      int
	Backoff  string
	Initial  time.Duration
	MaxDelay time.Duration
	Jitter   string
}

// Schedule is a cron expression and the zone it is read in. It parses and
// validates here and activates in M2: the expression itself is checked by the
// cron parser that arrives with the scheduler. Overlap says what a firing
// does when the job's max_concurrent is already held: "skip" (the default)
// stands down, "queue" defers the run into the future.
type Schedule struct {
	Name     string
	Cron     string
	Timezone string
	Overlap  string
}

const (
	// OverlapSkip stands a tick down when the concurrency limit is held. It
	// is the default, because the user paceq replaces is flock -n, which
	// skips too: the least surprising behaviour, except the stand-down is
	// recorded with its reason instead of vanishing.
	OverlapSkip = "skip"

	// OverlapQueue materialises the run anyway, deferred: queued with
	// available_at in the future and defer_reason set, so it starts when a
	// slot frees instead of being dropped.
	OverlapQueue = "queue"
)

// DefaultOverlap is OverlapSkip. Like DefaultMaxConcurrent this is a product
// decision, not a technical one, and it is spelled out here so both ends of
// the decode read it from one place.
const DefaultOverlap = OverlapSkip

// Sensor is an external trigger. It parses and validates here and materialises
// into the sensors table at apply (M3-01). The type names and their own fields
// land here too; in 1.0 there is exactly one kind, exec.
type Sensor struct {
	// Name is unique within the job and across every job in the catalog,
	// because it is the sensor row's primary key.
	Name string

	// Kind is always "exec" in 1.0. The field is read so a later kind is a
	// validation error with a message that says which release brings it,
	// rather than a value that silently means nothing.
	Kind string

	// Run is argv. There is no string form, so nothing here is ever split,
	// quoted or expanded by a shell (08 section 3.2).
	Run []string

	// Workdir is the directory the sensor process starts in. Empty means the
	// engine's own working directory.
	Workdir string

	// Env is the environment the sensor process is started with, in addition
	// to (never replacing) the job's own inherited baseline.
	Env map[string]string

	// Interval is how often the sensor is evaluated. Minimum one second.
	Interval time.Duration

	// MinInterval is the absolute lower bound between two starts of the same
	// sensor, even when a retry makes an immediate re-evaluation legal. It
	// defaults to one second.
	MinInterval time.Duration

	// Timeout is the ceiling on one evaluation. Defaults to 30s; the hard
	// ceiling is SensorTimeoutMax.
	Timeout time.Duration

	// MaxTriggersPerTick is how many triggers one evaluation may admit. It is
	// the chunking knob that keeps a burst from flooding the queue.
	MaxTriggersPerTick int

	// Paused is the initial state on first materialisation. Pausing a sensor
	// is an operator decision and survives re-apply.
	Paused bool

	// Description is what the sensor is for, for the people who read the job.
	Description string
}

// Hashed is a job and the exact bytes its hash was taken over. The two travel
// together because a hash without the document it covers cannot be checked.
type Hashed struct {
	Job       *Job
	Canonical []byte
	Hash      string
}
