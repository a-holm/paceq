package spec

// The diagnostic codes this package raises. The series are fixed by 03 section
// 8.1: PQ1xxx is parsing and schema, PQ2xxx is semantics, W1xxx is a warning.
//
// A code is a public interface the moment it is printed: it goes into scripts
// that grep for it and into `paceq error PQ1040`. Codes are therefore added,
// never reused for something else.
const (
	// CodeSyntax is a file the YAML parser cannot read at all.
	CodeSyntax = "PQ1000"
	// CodeFileTooLarge is a job file over MaxFileBytes, refused before it is
	// decoded.
	CodeFileTooLarge = "PQ1001"
	// CodeBadName is a missing name, or one that does not match NamePattern.
	CodeBadName = "PQ1002"
	// CodeMissingField is a required field that is not there, or is empty.
	CodeMissingField = "PQ1003"
	// CodeBadValue is a field with the wrong type or a value outside what it
	// accepts.
	CodeBadValue = "PQ1004"
	// CodeTooDeep is nesting past MaxDepth.
	CodeTooDeep = "PQ1005"
	// CodeTooManyAliases is more than MaxAliases aliases, or an alias that
	// refers to itself.
	CodeTooManyAliases = "PQ1006"
	// CodeTooLarge is a file with more nodes than MaxNodes, or one that
	// expands past MaxExpandedNodes.
	CodeTooLarge = "PQ1008"
	// CodeTooManySteps is more than MaxSteps steps.
	CodeTooManySteps = "PQ1009"
	// CodeRunNotAList is run written as a string.
	CodeRunNotAList = "PQ1010"
	// CodeRunEmpty is run written as an empty list.
	CodeRunEmpty = "PQ1011"
	// CodeRunNotAbsolute is a run[0] that is not an absolute path, with no
	// shell to look it up.
	CodeRunNotAbsolute = "PQ1012"
	// CodeBadDuration is a duration paceq does not read.
	CodeBadDuration = "PQ1020"
	// CodeTimeoutTooLong is a timeout over MaxJobTimeout.
	CodeTimeoutTooLong = "PQ1021"
	// CodeBadConcurrency is max_concurrent below one.
	CodeBadConcurrency = "PQ1030"
	// CodeBadMaxParallel is max_parallel outside 1..MaxParallelHi. The field
	// is the per-run semaphore M4-02 reads, so it is validated here, at the
	// door, the same way max_concurrent is.
	CodeBadMaxParallel = "PQ1031"
	// CodeConcurrencyTemplating is a concurrency_key written with {{...}}.
	// Templating is refused for all of 1.0; the message names the decision
	// and the closed forms that exist instead.
	CodeConcurrencyTemplating = "PQ1032"
	// CodeUnknownField is a field name paceq does not know, with the nearest
	// one it does.
	CodeUnknownField = "PQ1040"
	// CodeDuplicateKey is the same key twice in one mapping.
	CodeDuplicateKey = "PQ1041"
	// CodeMergeKey is a << merge key, which paceq does not resolve.
	CodeMergeKey = "PQ1042"
	// CodeTag is an explicit YAML tag, which paceq does not honour.
	CodeTag = "PQ1043"
	// CodeTooManyProblems is where paceq stopped reading a file that had more
	// wrong with it than a report can carry.
	CodeTooManyProblems = "PQ1044"
	// CodeDuplicateStep is two steps with the same name.
	CodeDuplicateStep = "PQ2001"
	// CodeUnknownNeed is a needs entry naming a step that does not exist.
	CodeUnknownNeed = "PQ2002"
	// CodeCycle is a needs graph that loops: one step, somewhere in the graph,
	// needs a step that needs it back, so no step in the loop can ever start.
	CodeCycle = "PQ2003"
	// CodeFanOutLimit is a needs graph where one step is a dependency of more
	// than MaxFanOut steps (or names more than MaxFanOut needs). Beyond that
	// the graph is a program wearing a job's clothes.
	CodeFanOutLimit = "PQ2004"
	// CodeDAGDepthLimit is a needs graph whose longest root-to-leaf run is
	// more than MaxDAGDepth edges. Such a chain holds a machine hostage to a
	// pipeline no stage bounds.
	CodeDAGDepthLimit = "PQ2005"
	// CodeUnknownTimezone is a schedule zone the time zone database does not
	// have.
	CodeUnknownTimezone = "PQ2010"
	// CodeShell is the warning that a step's command reaches a shell.
	CodeShell = "W1001"
	// CodeInheritEnv is the warning that a job takes variables from the
	// environment paceq itself was started in.
	CodeInheritEnv = "W1002"

	// CodeConcurrencyParamUnresolved is the warning that a job's
	// concurrency key reads a trigger parameter: a fire without that
	// parameter runs with no key at all, which means unlimited.
	CodeConcurrencyParamUnresolved = "W1003"

	// CodeSensorBadName is a sensor name that is missing or does not match
	// NamePattern.
	CodeSensorBadName = "PQ4101"
	// CodeSensorNameTaken is a sensor name another sensor in the job, or a
	// sensor in another job, already carries.
	CodeSensorNameTaken = "PQ4102"
	// CodeSensorKind is a kind other than exec: in 1.0 the built in sensor
	// types arrive in v0.3, not earlier.
	CodeSensorKind = "PQ4103"
	// CodeSensorRun is run written as a string, as an empty list, or with an
	// empty argument in it.
	CodeSensorRun = "PQ4104"
	// CodeSensorIntervalMin is an interval under the one second floor.
	CodeSensorIntervalMin = "PQ4105"
	// CodeSensorMinInterval is a min_interval over the interval itself.
	CodeSensorMinInterval = "PQ4106"
	// CodeSensorTimeout is a timeout outside the [1s, 5m] range.
	CodeSensorTimeout = "PQ4107"
	// CodeSensorTriggers is a max_triggers_per_tick outside [1, 10000].
	CodeSensorTriggers = "PQ4108"
	// CodeSensorWorkdir is the warning that a workdir does not exist, or is
	// not an absolute path.
	CodeSensorWorkdir = "PQ4109"
	// CodeSensorEnvKey is an env key with the reserved PULSEQ_ prefix.
	CodeSensorEnvKey = "PQ4110"
)

// Codes is every code this package can raise, in the order they are declared.
// The catalogue behind `paceq error` is checked against it, and a test reads
// the package source to prove nothing raises a code that is missing from here.
func Codes() []string {
	return []string{
		CodeSyntax,
		CodeFileTooLarge,
		CodeBadName,
		CodeMissingField,
		CodeBadValue,
		CodeTooDeep,
		CodeTooManyAliases,
		CodeTooLarge,
		CodeTooManySteps,
		CodeRunNotAList,
		CodeRunEmpty,
		CodeRunNotAbsolute,
		CodeBadDuration,
		CodeTimeoutTooLong,
		CodeBadConcurrency,
		CodeBadMaxParallel,
		CodeConcurrencyTemplating,
		CodeUnknownField,
		CodeDuplicateKey,
		CodeMergeKey,
		CodeTag,
		CodeTooManyProblems,
		CodeDuplicateStep,
		CodeUnknownNeed,
		CodeCycle,
		CodeFanOutLimit,
		CodeDAGDepthLimit,
		CodeUnknownTimezone,
		CodeShell,
		CodeInheritEnv,
		CodeConcurrencyParamUnresolved,
		CodeSensorBadName,
		CodeSensorNameTaken,
		CodeSensorKind,
		CodeSensorRun,
		CodeSensorIntervalMin,
		CodeSensorMinInterval,
		CodeSensorTimeout,
		CodeSensorTriggers,
		CodeSensorWorkdir,
		CodeSensorEnvKey,
	}
}
