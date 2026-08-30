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
	// MaxSteps is the same ceiling the executed DAG obeys.
	MaxSteps = 200
	// MaxFanOut is how many steps one step may be a dependency of, or how many
	// needs one step may name. A successor count past this is a program wearing
	// a job's clothes, and there is no way to see what it does. Enforced as a
	// semantic refusal.
	MaxFanOut = 100
	// MaxDAGDepth is the longest run of edges from a step with no needs to the
	// deepest step. A deeper graph is a chain no machine should wait on.
	MaxDAGDepth = 100
	// MaxParallelHi is the highest a job's max_parallel may go (M4-02 uses it
	// as the per-run semaphore). The default is four.
	MaxParallelHi = 64
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
	// DefaultMaxParallel is how many steps of one run may run at once
	// (M4-02 uses it as the per-run semaphore). Independent steps share the
	// run, so a fan out needs more than one slot to matter; four is a solid
	// default for a machine that also runs other things.
	DefaultMaxParallel = 4
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

	// ExpectedWithin is how long this job may go without a successful run
	// before monitoring should speak up (#40). It exists so one generic alert
	// rule - time() - last_success > freshness_sla - covers every job forever,
	// instead of one hand maintained rule per job. Zero means the job declares
	// no expectation and gets no SLA series at all: an absent expectation must
	// never render as zero, because zero would alarm on every healthy job
	// that simply said nothing.
	ExpectedWithin time.Duration

	// Notify is the job's alert hooks (#29): named notifiers told about a
	// failed or successful run. nil means the job says nothing about
	// notifications and inherits the daemon's defaults where they apply;
	// an explicit empty list is the deliberate silence. The names resolve
	// against the daemon configuration at dispatch time.
	Notify *Notify
	// MaxParallel is how many steps of one run may be in flight at once
	// (M4-02). 1..MaxParallelHi, default DefaultMaxParallel. It is materialised
	// into the default on parse so a job that leaves it out hashes identically
	// to one that writes it.
	MaxParallel int
	Steps       []Step
	Schedules   []Schedule
	Sensors     []Sensor

	// ConcurrencyKey is the optional mutual exclusion key (#17). nil means
	// every run of this job is unlimited. The closed grammar lives on
	// ConcurrencyKey; the value canonises as <jobname>:<resolved> when a
	// run materialises.
	ConcurrencyKey *ConcurrencyKey

	// OnConflict says what a fire does when another active run already
	// holds the key: defer (the default) queues the run held into the
	// future, skip stands the trigger down with a rejected outcome.
	OnConflict string

	// Shadow puts every schedule of this job into shadow mode (#32):
	// ticks are recorded, nothing executes. False by default and left out
	// of the canonical document at false, so existing hashes are stable.
	Shadow bool
}

// Notify is a job's alert hooks (#29). The names are notifiers defined in the
// daemon configuration; the lists are sets, so saying one name twice hashes
// and delivers like saying it once.
type Notify struct {
	// OnFailure runs when the run ends failed.
	OnFailure []string
	// OnSuccess runs when the run ends succeeded. Empty by design: nobody
	// gets spam by default.
	OnSuccess []string
}

// Empty says whether the block says nothing at all, which is the condition
// the canonical encoder leaves out of the document.
func (n *Notify) Empty() bool {
	return n == nil || (len(n.OnFailure) == 0 && len(n.OnSuccess) == 0)
}

// ConcurrencyKey is the closed grammar of a job's concurrency key. Exactly one
// form is set. There is no templating and no expression language: a constant,
// a named parameter, or the trigger's run key are the whole vocabulary.
type ConcurrencyKey struct {
	// Constant is form 1: the key text itself.
	Constant string
	// Param is form 2: params[this] at fire time. Missing or empty means
	// the run has no key and is unlimited.
	Param string
	// FromRunKey is form 3: the trigger's dedup run key is the value.
	FromRunKey bool
}

// The on_conflict policy values (#17). DefaultOnConflict exists so both ends
// of the decode read the default from one name.
const (
	OnConflictDefer = "defer"
	OnConflictSkip  = "skip"
	// DefaultOnConflict is OnConflictDefer: waiting keeps the event, a
	// silent drop loses it.
	DefaultOnConflict = OnConflictDefer
)

// MaxConcurrencyKeyLength caps the constant form and every resolved value.
// A longer key is a data blob, not a name.
const MaxConcurrencyKeyLength = 200

// Value resolves the raw key text for one fire. params are the trigger's
// parameters and runKey is the dedup key the trigger carried. ok is false
// when this fire has no key at all, which means unlimited; that is a normal
// outcome for the param form, never an error, because params may come from a
// sensor payload that simply lacked the field.
func (k *ConcurrencyKey) Value(params map[string]string, runKey string) (value string, ok bool) {
	switch {
	case k.FromRunKey:
		return runKey, runKey != ""
	case k.Param != "":
		v := params[k.Param]
		return v, v != ""
	default:
		return k.Constant, true
	}
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
	// Needs is this step's upstream edge set. The cycle check and the
	// topological order are here; the engine gates each step on the edges.
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

	// Shadow runs this schedule without executing anything (#32): the
	// scheduler materialises every tick with the decision it would have
	// made, and no run follows. A job-level shadow shadows all of its
	// schedules; this flag is per schedule.
	Shadow bool
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
