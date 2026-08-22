// Package runner executes a single step as an operating system process, owning process groups, signals and log pipes.
//
// The runner is where paceq actually does something: it starts a user
// command, holds it on a short leash and reports the outcome precisely. Three
// ground rules shape everything here.
//
//  1. The orchestrator process never runs user code (plan 04, ground rule 1).
//     Everything user defined is a subprocess with a hard timeout and its own
//     process group. Run is synchronous: one call, one attempt, one verdict.
//
//  2. Setpgid is not optional (plan 05, section 6.5). Without its own process
//     group a job's grandchildren outlive the kill: the shell starts python,
//     python starts curl, and the curl survives as a zombie holding files and
//     ports. Every kill in this package addresses the negative process group
//     id, never a bare pid.
//
//  3. The environment is deny by default (plan 08, section 3.2). A job does
//     not inherit the daemon's environment. "Works in the shell, fails in
//     cron" is the number one cron trap, and it is solved by making the
//     environment explicit instead of inheriting more.
//
// # The frozen environment contract
//
// The child environment is built in layers; a key set by a later layer
// replaces the same key from an earlier one. The PACEQ_ prefix is reserved:
// no job layer may set, inherit or read from a file a key with that prefix.
//
//	Layer 1, baseline (deny by default):
//	  PATH   fixed default, never inherited from the daemon
//	  HOME   taken from the runner process if set
//	  TZ     taken from the runner process if set
//	  LANG   taken from the runner process if set
//
//	Layer 2, the context contract, set on every run:
//	  PACEQ_RUN_ID            the run's ULID
//	  PACEQ_JOB               job name
//	  PACEQ_STEP              step name
//	  PACEQ_ATTEMPT           1-based attempt number
//	  PACEQ_RUN_KEY           dedup key, empty when the run had none
//	  PACEQ_IDEMPOTENCY_KEY   sha256(run_id + ":" + step_name), first 32 hex
//	                          characters; stable across retries and duplicates
//	                          of the same step in the same run, so a user step
//	                          can use it as an idempotency key downstream
//	  PACEQ_SCHEDULED_FOR     RFC3339 UTC, empty for manual runs
//	  PACEQ_PARAMS            the step's params as a JSON object
//	  PACEQ_OUTPUT            path of the NDJSON file the step may write
//	                          artifacts and params to; created by the runner
//	                          before the command starts
//	  PACEQ_INPUTS            merged JSON from upstream steps, "{}" in M1
//
//	Layer 3, InheritEnv: only the named variables, copied from the runner
//	process when they exist there.
//
//	Layer 4, EnvFile: a KEY=VALUE file that must have mode 0600 exactly; a
//	looser mode is refused (fail closed).
//
//	Layer 5, Env: the job's own environment, the most specific and most
//	reviewed layer, wins over every other user layer.
//
// # Outcomes
//
// Run reports one of five outcomes with a reason code each, so explain can
// tell "command not found" from "exit 1" from "killed by a signal" from "hit
// the deadline": three different incidents, three different fixes. The table
// is executable in result_test.go.
//
//	Succeeded    exit 0                          STEP_SUCCEEDED
//	Failed       exit N                          STEP_FAILED_NONZERO_EXIT
//	             with transient=true when N=75, the EX_TEMPFAIL convention
//	Signalled    killed by a signal              STEP_FAILED_SIGNAL
//	             exit code reported as 128+signal
//	TimedOut     deadline hit, group killed      STEP_FAILED_TIMEOUT
//	SpawnFailed  the process never started       STEP_FAILED_SPAWN
//	             (missing binary, no execute permission, missing workdir,
//	             refused path, unreadable env file); no command side effects,
//	             so a retry is always safe
//
// A clean exit status is direct evidence of completion and wins over a
// deadline that fired in the same instant; a signal death under an active
// deadline is the runner's own kill and reports as TimedOut.
//
// # Time
//
// All measured time comes from the clock.Clock in the Spec. The one context
// handed to os/exec carries cancellation plumbing only; the deadline itself
// is a clock timer, so a fake clock drives every timing decision in tests.
//
// # Platform
//
// Process groups and the core limit are unix features. On other platforms the
// runner still works but degrades to pid targeted kills and no rlimit change;
// the supported targets (linux, darwin) all take the unix path.
package runner
