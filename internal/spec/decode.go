package spec

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/goccy/go-yaml/ast"

	"github.com/a-holm/paceq/internal/diag"
)

// The fields each mapping in a job file accepts. They are also the candidate
// list a misspelling is measured against, so a field added to a struct and not
// to its list here is a field the parser refuses.
var (
	jobFields      = []string{"name", "description", "env", "env_file", "inherit_env", "workdir", "timeout", "expected_within", "max_concurrent", "max_parallel", "concurrency_key", "on_conflict", "shadow", "steps", "schedules", "sensors"}
	stepFields     = []string{"name", "run", "shell", "workdir", "timeout", "retry", "needs"}
	retryFields    = []string{"max", "backoff", "initial", "max_delay", "jitter"}
	scheduleFields = []string{"name", "cron", "timezone", "overlap", "shadow"}
	sensorFields   = []string{"name", "kind", "run", "workdir", "env", "interval", "min_interval", "timeout", "max_triggers_per_tick", "paused", "description"}
)

// commonMistakes are the names people arrive with, from cron, from Compose,
// from GitHub Actions, and from the earlier drafts of this format. An edit
// distance cannot connect "cmd" to "run", so the connection is written down.
var commonMistakes = map[string]string{
	"argv":        "run",
	"cmd":         "run",
	"command":     "run",
	"exec":        "run",
	"script":      "run",
	"cwd":         "workdir",
	"directory":   "workdir",
	"dir":         "workdir",
	"environment": "env",
	"envfile":     "env_file",
	"job":         "name",
	"sensor":      "name",
	"id":          "name",
	"title":       "name",
	"depends_on":  "needs",
	"after":       "needs",
	"requires":    "needs",
	"schedule":    "schedules",
	"cron":        "schedules",
	"tz":          "timezone",
	"parallelism": "max_concurrent",
	"concurrency": "max_concurrent",
	"attempts":    "retry",
	"tries":       "retry",
	"retries":     "retry",
}

// envNamePattern is what execve will accept as a variable name. A name with an
// equals sign or a newline in it cannot be passed to a process at all, so it is
// refused here rather than at the point where it silently disappears.
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (d *decoder) job(node ast.Node) *Job {
	job := &Job{Timeout: DefaultTimeout, MaxConcurrent: DefaultMaxConcurrent, MaxParallel: DefaultMaxParallel, OnConflict: DefaultOnConflict}

	mapping, ok := d.mapping(node, "the job file")
	if !ok {
		return nil
	}

	seen := d.fields(mapping, "a job", jobFields, func(key string, value ast.Node) {
		switch key {
		case "name":
			job.Name = d.name(value, "the job name")
		case "description":
			job.Description = d.text(value, "description")
		case "env":
			job.Env = d.env(value)
		case "env_file":
			job.EnvFile = d.text(value, "env_file")
		case "inherit_env":
			job.InheritEnv = d.inheritEnv(value)
		case "workdir":
			job.Workdir = d.text(value, "workdir")
		case "timeout":
			job.Timeout = d.timeout(value, DefaultTimeout)
		case "expected_within":
			job.ExpectedWithin = d.expectedWithin(value)
		case "max_concurrent":
			job.MaxConcurrent = d.maxConcurrent(value)
		case "max_parallel":
			job.MaxParallel = d.maxParallel(value)
		case "concurrency_key":
			job.ConcurrencyKey = d.concurrencyKey(value)
		case "on_conflict":
			job.OnConflict = d.oneOf(value, "on_conflict", OnConflictDefer, OnConflictSkip)
		case "shadow":
			job.Shadow = d.flag(value, "shadow")
		case "steps":
			job.Steps = d.steps(value)
		case "schedules":
			job.Schedules = d.schedules(value)
		case "sensors":
			job.Sensors = d.sensors(value)
		}
	})

	if !seen["name"] {
		d.error(CodeBadName, startPosition(mapping),
			"the job has no name",
			"Every job has a name. It is what you type to run it and what a run is filed under:\n\n"+
				"    name: nightly-report")
	}
	if !seen["steps"] {
		d.error(CodeMissingField, startPosition(mapping),
			"the job has no steps",
			"A job runs steps, and needs at least one:\n\n"+
				"    steps:\n"+
				"      - name: say-hello\n"+
				"        run: [\"/bin/echo\", \"hello\"]")
	}
	return job
}

// concurrencyKey reads the closed grammar of #17. A scalar is the constant
// form; a mapping carries exactly one of param or from. There is no
// templating: {{...}} in a constant is refused with the decision named in the
// message, because templating in configuration values was declined for all of
// 1.0 (an injection risk and a complexity spiral for what a third form does).
func (d *decoder) concurrencyKey(node ast.Node) *ConcurrencyKey {
	resolved, ok := d.resolve(node, "concurrency_key")
	if !ok {
		return nil
	}
	if text, isScalar := scalarText(resolved); isScalar {
		return d.constantConcurrencyKey(resolved, text)
	}
	mapping, ok := d.mapping(node, "concurrency_key")
	if !ok {
		return nil
	}

	key := &ConcurrencyKey{}
	seenParam, seenFrom := false, false
	fromRejected := false
	var paramPos diag.Position
	d.fields(mapping, "a concurrency key", []string{"param", "from"}, func(form string, value ast.Node) {
		switch form {
		case "param":
			seenParam = true
			paramPos = position(value)
			key.Param = d.text(value, "concurrency_key param")
		case "from":
			seenFrom = true
			source := d.text(value, "concurrency_key from")
			if source != "" && source != "run_key" {
				d.error(CodeBadValue, position(value),
					fmt.Sprintf("concurrency_key from is %q, and run_key is the only source", source),
					"The key can follow the trigger's dedup key, which makes every distinct\n"+
						"event its own concurrency class:\n\n"+
						"    concurrency_key:\n"+
						"      from: run_key")
				fromRejected = true
				return
			}
			key.FromRunKey = true
		}
	})
	if fromRejected {
		return nil
	}

	switch {
	case seenParam && seenFrom:
		d.error(CodeBadValue, paramPos,
			"a concurrency key is one of param or from, not both",
			"Pick the one form that says where the value comes from:\n\n"+
				"    concurrency_key:\n"+
				"      param: kunde\n"+
				"# or\n"+
				"    concurrency_key:\n"+
				"      from: run_key")
		return nil
	case seenParam:
		if key.Param == "" {
			d.error(CodeBadValue, paramPos,
				"the parameter name in concurrency_key is empty",
				"Name the parameter the key reads at fire time:\n\n"+
					"    concurrency_key:\n"+
					"      param: kunde")
			return nil
		}
		// The warning is the whole contract of the param form: paceq cannot
		// know at apply time whether every trigger will carry it, and a fire
		// without it runs with no key at all. That is a decision to make with
		// open eyes, not an error, because params may come from a sensor.
		d.warn(CodeConcurrencyParamUnresolved, paramPos,
			fmt.Sprintf("concurrency_key reads the parameter %q: a trigger without it runs with no key, which means unlimited", key.Param),
			"The key resolves per fire. A schedule fires with no params at all, so a\n"+
				"scheduled fire of this job is always unlimited; a sensor carries params,\n"+
				"so check the payload it emits:\n\n"+
				"    concurrency_key:\n"+
				"      param: "+key.Param+"\n\n"+
				"A constant or {from: run_key} never leaves a fire unkeyed.")
		return &ConcurrencyKey{Param: key.Param}
	case seenFrom && key.FromRunKey:
		return &ConcurrencyKey{FromRunKey: true}
	default:
		d.error(CodeBadValue, position(resolved),
			"a concurrency key mapping needs one of param or from",
			"The grammar is three forms and nothing else:\n\n"+
				"    concurrency_key: \"nightly-report\"\n"+
				"    concurrency_key: {param: kunde}\n"+
				"    concurrency_key: {from: run_key}")
		return nil
	}
}

// constantConcurrencyKey reads the constant form: text as written, with the
// templating refusal and the length cap applied.
func (d *decoder) constantConcurrencyKey(node ast.Node, text string) *ConcurrencyKey {
	if strings.Contains(text, "{{") {
		d.error(CodeConcurrencyTemplating, position(node),
			fmt.Sprintf("concurrency_key is %q, and paceq refuses templating in key values", text),
			"Templating in configuration is refused for all of 1.0: it is an injection\n"+
				"risk and turns a definition into a program nobody can read. Three closed\n"+
				"forms carry every case it would:\n\n"+
				"    concurrency_key: \"nightly-report\"\n"+
				"    concurrency_key: {param: kunde}\n"+
				"    concurrency_key: {from: run_key}")
		return nil
	}
	if len(text) > MaxConcurrencyKeyLength {
		d.error(CodeBadValue, position(node),
			fmt.Sprintf("concurrency_key is %d characters, and %d is the most a key may carry", len(text), MaxConcurrencyKeyLength),
			"A key names a mutual exclusion class; it is not a place for data. Shorten it.")
		return nil
	}
	if text == "" {
		d.error(CodeBadValue, position(node),
			"concurrency_key is an empty string",
			"Leave the field out when every run should be unlimited, or name the value:\n\n"+
				"    concurrency_key: \"nightly-report\"")
		return nil
	}
	return &ConcurrencyKey{Constant: text}
}

func (d *decoder) steps(node ast.Node) []Step {
	sequence, ok := d.sequence(node, "steps")
	if !ok {
		return nil
	}
	if len(sequence.Values) == 0 {
		d.error(CodeMissingField, position(node),
			"steps is empty",
			"A job runs steps, and needs at least one:\n\n"+
				"    steps:\n"+
				"      - name: say-hello\n"+
				"        run: [\"/bin/echo\", \"hello\"]")
		return nil
	}
	if len(sequence.Values) > MaxSteps {
		d.error(CodeTooManySteps, position(node),
			fmt.Sprintf("the job has %d steps, and paceq runs at most %d", len(sequence.Values), MaxSteps),
			fmt.Sprintf("Split it into several jobs. %d is the same ceiling the dependency graph uses,\n", MaxSteps)+
				"and a graph that large is not something anybody reads at 03:14.")
		return nil
	}

	steps := make([]Step, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		steps = append(steps, d.step(value, i))
	}
	return steps
}

// stepLabel is how a message names a step: by its name once there is one, and
// by its place in the file until then.
func stepLabel(name string, index int) string {
	if name == "" {
		return fmt.Sprintf("step %d", index+1)
	}
	return fmt.Sprintf("the step %q", name)
}

func (d *decoder) step(node ast.Node, index int) Step {
	where := fmt.Sprintf("step %d", index+1)
	positions := stepPositions{name: position(node)}

	step := Step{}
	mapping, ok := d.mapping(node, where)
	if !ok {
		d.stepPos = append(d.stepPos, positions)
		return step
	}

	var runNode ast.Node
	shellPos := startPosition(mapping)
	seen := d.fields(mapping, "a step", stepFields, func(key string, value ast.Node) {
		switch key {
		case "name":
			positions.name = position(value)
			step.Name = d.name(value, where+" name")
		case "run":
			runNode = value
			step.Run = d.run(value, where)
		case "shell":
			shellPos = position(value)
			step.Shell = d.flag(value, where+" shell")
		case "workdir":
			step.Workdir = d.text(value, where+" workdir")
		case "timeout":
			step.Timeout = d.timeout(value, 0)
		case "retry":
			step.Retry = d.retry(value, where)
		case "needs":
			step.Needs, positions.needs = d.needs(value, where)
		}
	})
	d.stepPos = append(d.stepPos, positions)
	label := stepLabel(step.Name, index)

	if !seen["name"] {
		d.error(CodeBadName, startPosition(mapping),
			fmt.Sprintf("%s has no name", where),
			"A step is named so a failure can say which one failed, and so a retry can name it:\n\n"+
				"    - name: extract\n"+
				"      run: [\"/usr/local/bin/extract\"]")
	}
	if !seen["run"] {
		d.error(CodeMissingField, startPosition(mapping),
			fmt.Sprintf("%s has no run", label),
			"A step runs a command, given as a list:\n\n"+
				"    run: [\"/usr/local/bin/extract\", \"--out\", \"/srv/data\"]")
	}

	// The absolute path rule only makes sense once both fields are known: a
	// step that hands its command to a shell is asking the shell to find it.
	if len(step.Run) > 0 && !step.Shell {
		d.checkAbsolute(step.Run[0], runNode)
	}
	if step.Shell {
		d.warn(CodeShell, shellPos,
			fmt.Sprintf("%s runs its command through a shell", label),
			"A shell splits, globs and expands what it is given, so an argument that carries a\n"+
				"space, a quote or a semicolon stops being one argument. With shell off, paceq\n"+
				"starts the process itself and nothing touches the arguments:\n\n"+
				"    run: [\"/bin/sh\", \"-c\", \"echo done\"]\n\n"+
				"Keep it on only when the command genuinely needs a shell, and say why in the\n"+
				"description.")
	}
	return step
}

// checkAbsolute is 08 section 3.2: paceq starts the process itself, so there is
// no PATH lookup and no directory to be relative to.
func (d *decoder) checkAbsolute(command string, node ast.Node) {
	if strings.HasPrefix(command, "/") {
		return
	}
	pos := position(node)
	if sequence, ok := node.(*ast.SequenceNode); ok && len(sequence.Values) > 0 {
		pos = position(sequence.Values[0])
	}
	d.error(CodeRunNotAbsolute, pos,
		fmt.Sprintf("the command %q is not an absolute path", command),
		"paceq starts the process itself. There is no shell to search PATH, and no working\n"+
			"directory for a relative name to be relative to, so the first element has to say\n"+
			"exactly which program to start:\n\n"+
			"    run: [\"/usr/bin/"+lastElement(command)+"\", ...]\n\n"+
			"    command -v "+lastElement(command)+"    prints the path this machine would use")
}

func lastElement(command string) string {
	if i := strings.LastIndex(command, "/"); i >= 0 && i+1 < len(command) {
		return command[i+1:]
	}
	if command == "" {
		return "program"
	}
	return command
}

func (d *decoder) run(node ast.Node, where string) []string {
	resolved, ok := d.resolve(node, where+" run")
	if !ok {
		return nil
	}
	sequence, isSequence := resolved.(*ast.SequenceNode)
	if !isSequence {
		d.error(CodeRunNotAList, position(resolved),
			fmt.Sprintf("run in %s is %s, and run is a list", where, typeName(resolved)),
			"There is no string form. paceq starts the process itself, so it never splits a\n"+
				"command on spaces, and a file name with a space or a semicolon in it stays one\n"+
				"argument:\n\n"+
				"    run: [\"/bin/sh\", \"-c\", \"echo done\"]\n\n"+
				"or, one element per line:\n\n"+
				"    run:\n"+
				"      - /usr/local/bin/extract\n"+
				"      - --out\n"+
				"      - /srv/data/raw.parquet")
		return nil
	}
	if len(sequence.Values) == 0 {
		d.error(CodeRunEmpty, position(resolved),
			fmt.Sprintf("run in %s is an empty list", where),
			"The first element is the program to start, so the list needs at least one:\n\n"+
				"    run: [\"/bin/echo\", \"hello\"]")
		return nil
	}

	argv := make([]string, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		argument, ok := d.stringValue(value, fmt.Sprintf("run[%d] in %s", i, where))
		if !ok {
			continue
		}
		if strings.ContainsRune(argument, 0) {
			d.error(CodeBadValue, position(value),
				fmt.Sprintf("run[%d] in %s carries a NUL byte", i, where),
				"An argument is handed to the kernel as a NUL terminated string, so it cannot\n"+
					"contain one. Remove the escape that produced it.")
			continue
		}
		argv = append(argv, argument)
	}
	return argv
}

func (d *decoder) retry(node ast.Node, where string) *Retry {
	mapping, ok := d.mapping(node, where+" retry")
	if !ok {
		return nil
	}

	retry := &Retry{
		Backoff:  DefaultBackoff,
		Initial:  DefaultInitial,
		MaxDelay: DefaultMaxDelay,
		Jitter:   DefaultJitter,
	}
	seen := d.fields(mapping, "a retry block", retryFields, func(key string, value ast.Node) {
		switch key {
		case "max":
			retry.Max = d.count(value, where+" retry max", 0, MaxSteps)
		case "backoff":
			retry.Backoff = d.oneOf(value, where+" retry backoff", BackoffExponential, BackoffFixed)
		case "initial":
			retry.Initial = d.timeout(value, DefaultInitial)
		case "max_delay":
			retry.MaxDelay = d.timeout(value, DefaultMaxDelay)
		case "jitter":
			retry.Jitter = d.oneOf(value, where+" retry jitter", JitterFull, JitterNone)
		}
	})
	if !seen["max"] {
		d.error(CodeMissingField, position(node),
			fmt.Sprintf("the retry block in %s has no max", where),
			"max is how many attempts a failure is worth. Without it, retry says nothing:\n\n"+
				"    retry:\n"+
				"      max: 3\n"+
				"      initial: 10s")
	}
	if retry.MaxDelay > 0 && retry.Initial > retry.MaxDelay {
		d.error(CodeBadValue, position(node),
			fmt.Sprintf("the retry block in %s starts at %s and is capped at %s",
				where, FormatDuration(retry.Initial), FormatDuration(retry.MaxDelay)),
			"initial is the first wait and max_delay is the ceiling on every wait, so the first\n"+
				"one cannot already be over it. Either lower initial or raise max_delay.")
	}
	return retry
}

func (d *decoder) schedules(node ast.Node) []Schedule {
	sequence, ok := d.sequence(node, "schedules")
	if !ok {
		return nil
	}

	schedules := make([]Schedule, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		where := fmt.Sprintf("schedule %d", i+1)
		mapping, ok := d.mapping(value, where)
		if !ok {
			continue
		}

		schedule := Schedule{Timezone: DefaultTimezone, Overlap: DefaultOverlap}
		seen := d.fields(mapping, "a schedule", scheduleFields, func(key string, value ast.Node) {
			switch key {
			case "name":
				schedule.Name = d.name(value, where+" name")
			case "cron":
				schedule.Cron = d.text(value, where+" cron")
			case "timezone":
				schedule.Timezone = d.timezone(value, where)
			case "overlap":
				schedule.Overlap = d.oneOf(value, where+" overlap", OverlapSkip, OverlapQueue)
			case "shadow":
				schedule.Shadow = d.flag(value, where+" shadow")
			}
		})
		if !seen["name"] {
			d.error(CodeBadName, position(value),
				fmt.Sprintf("%s has no name", where),
				"A schedule is named so a decision can say which one fired:\n\n"+
					"    schedules:\n"+
					"      - name: nightly\n"+
					"        cron: \"0 3 * * *\"")
		}
		if !seen["cron"] || schedule.Cron == "" {
			d.error(CodeMissingField, position(value),
				fmt.Sprintf("%s has no cron expression", where),
				"A schedule is a cron expression and the zone it is read in:\n\n"+
					"    cron: \"0 3 * * *\"\n"+
					"    timezone: Europe/Oslo\n\n"+
					"The expression itself is checked by the scheduler, which arrives in M2.")
		}
		schedules = append(schedules, schedule)
	}
	return schedules
}

func (d *decoder) sensors(node ast.Node) []Sensor {
	sequence, ok := d.sequence(node, "sensors")
	if !ok {
		return nil
	}
	if len(sequence.Values) > MaxSteps {
		d.error(CodeTooManySteps, position(node),
			fmt.Sprintf("the job has %d sensors, and paceq runs at most %d", len(sequence.Values), MaxSteps),
			fmt.Sprintf("Split them between jobs, or raise the ceiling. %d is the same number as the\n", MaxSteps)+
				"step ceiling, because a file that large is not something anybody reads at 03:14.")
		return nil
	}

	sensors := make([]Sensor, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		where := fmt.Sprintf("sensor %d", i+1)
		positions := sensorPositions{name: position(value)}
		mapping, ok := d.mapping(value, where)
		if !ok {
			d.sensorPos = append(d.sensorPos, positions)
			continue
		}

		sensor := Sensor{
			Kind:               DefaultSensorKind,
			Timeout:            DefaultSensorTimeout,
			MaxTriggersPerTick: DefaultSensorMaxTriggers,
		}
		seen := d.fields(mapping, "a sensor", sensorFields, func(key string, value ast.Node) {
			switch key {
			case "name":
				positions.name = position(value)
				sensor.Name = d.name(value, where+" name")
			case "kind":
				sensor.Kind = d.sensorKind(value, where)
			case "run":
				sensor.Run = d.sensorRun(value, where)
			case "workdir":
				positions.workdir = position(value)
				sensor.Workdir = d.text(value, where+" workdir")
			case "env":
				sensor.Env = d.sensorEnv(value, where)
			case "interval":
				sensor.Interval = d.sensorInterval(value, where)
			case "min_interval":
				sensor.MinInterval = d.timeout(value, 0)
			case "timeout":
				sensor.Timeout = d.sensorTimeout(value, where)
			case "max_triggers_per_tick":
				sensor.MaxTriggersPerTick = d.sensorTriggers(value, where)
			case "paused":
				sensor.Paused = d.flag(value, where+" paused")
			case "description":
				sensor.Description = d.text(value, where+" description")
			}
		})
		d.sensorPos = append(d.sensorPos, positions)

		if !seen["name"] {
			d.error(CodeSensorBadName, startPosition(mapping),
				fmt.Sprintf("%s has no name", where),
				"A sensor is named so a failure can say which one fired, and so the row it\n"+
					"materialises into has a key:\n\n"+
					"    sensors:\n"+
					"      - name: dropzone\n"+
					"        run: [\"/srv/etl/nye-objekter.sh\"]\n"+
					"        interval: 15s")
		}
		if !seen["run"] {
			d.error(CodeSensorRun, startPosition(mapping),
				fmt.Sprintf("%s has no run", where),
				"A sensor runs a command, given as a list of arguments:\n\n"+
					"    run: [\"/srv/etl/nye-objekter.sh\"]")
		}
		if !seen["interval"] || sensor.Interval == 0 {
			d.error(CodeSensorIntervalMin, startPosition(mapping),
				fmt.Sprintf("%s has no interval", where),
				"A sensor is evaluated on an interval, and needs one:\n\n"+
					"    interval: 15s")
		}
		if sensor.MinInterval == 0 {
			// The default lower bound is one second. A validated interval is
			// never under it, so min(interval, 1s) is always 1s.
			sensor.MinInterval = SensorIntervalMin
		}
		if sensor.MinInterval > sensor.Interval && sensor.Interval > 0 {
			d.error(CodeSensorMinInterval, startPosition(mapping),
				fmt.Sprintf("%s begins evaluations no more often than every %s, and tries to end them after %s",
					where, FormatDuration(sensor.MinInterval), FormatDuration(sensor.Interval)),
				"min_interval is the absolute lower bound between two starts, and interval is the\n"+
					"desired frequency. The floor cannot be above the rhythm that drives it:\n\n"+
					"    interval: 60s\n"+
					"    min_interval: 10s")
		}
		if sensor.Workdir != "" && !filepath.IsAbs(sensor.Workdir) {
			d.warn(CodeSensorWorkdir, positions.workdir,
				fmt.Sprintf("%s workdir %q is not an absolute path", where, sensor.Workdir),
				"Paceq starts the sensor process itself, so a relative name has no directory to\n"+
					"be relative to, and the directory has to exist when the sensor starts:\n\n"+
					"    workdir: /srv/etl")
		}
		sensors = append(sensors, sensor)
	}
	return sensors
}

// sensorKind reads the kind field. There is exactly one kind in 1.0, exec; a
// value that names a built in type is refused against the release that brings
// it, so nobody ships a sensor that silently means nothing.
func (d *decoder) sensorKind(node ast.Node, where string) string {
	value, ok := d.stringValue(node, where+" kind")
	if !ok {
		return DefaultSensorKind
	}
	if value == DefaultSensorKind {
		return value
	}
	d.error(CodeSensorKind, position(node),
		fmt.Sprintf("%s is %q, and exec is the only kind paceq accepts in 1.0", where, value),
		"The built in sensor types file, http and sql arrive in v0.3 (M7-03). Until then a\n"+
			"sensor runs a subprocess that writes JSON to stdout:\n\n"+
			"    kind: exec\n"+
			"    run: [\"/srv/etl/nye-objekter.sh\"]")
	return DefaultSensorKind
}

// sensorRun reads run as argv. There is no string form, none of it is ever
// split or expanded by a shell, and every element has to carry a command or
// an argument: an empty string would be a hole where an argument is meant to
// be (08 section 3.2, the same rule steps obey).
func (d *decoder) sensorRun(node ast.Node, where string) []string {
	resolved, ok := d.resolve(node, where+" run")
	if !ok {
		return nil
	}
	sequence, isSequence := resolved.(*ast.SequenceNode)
	if !isSequence {
		d.error(CodeSensorRun, position(resolved),
			fmt.Sprintf("run in %s is %s, and run is a list of arguments", where, typeName(resolved)),
			"There is no string form. paceq starts the sensor process itself, so it never\n"+
				"splits a command on spaces, and a file name with a space in it stays one\n"+
				"argument:\n\n"+
				"    run: [\"/usr/bin/ls\", \"/srv/dropzone\"]\n\n"+
				"or, one element per line:\n\n"+
				"    run:\n"+
				"      - /srv/etl/nye-objekter.sh")
		return nil
	}
	if len(sequence.Values) == 0 {
		d.error(CodeSensorRun, position(resolved),
			fmt.Sprintf("run in %s is an empty list", where),
			"The first element is the program to start, so the list needs at least one:\n\n"+
				"    run: [\"/srv/etl/nye-objekter.sh\"]")
		return nil
	}

	argv := make([]string, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		argument, ok := d.stringValue(value, fmt.Sprintf("run[%d] in %s", i, where))
		if !ok {
			continue
		}
		if argument == "" {
			d.error(CodeSensorRun, position(value),
				fmt.Sprintf("run[%d] in %s is an empty argument", i, where),
				"An argument carries a command or a value. An empty string is neither, and it\n"+
					"would disappear the moment the list is turned into a process:\n\n"+
					"    run: [\"/usr/bin/env\", \"BUCKET=acme\"]")
			continue
		}
		if strings.ContainsRune(argument, 0) {
			d.error(CodeBadValue, position(value),
				fmt.Sprintf("run[%d] in %s carries a NUL byte", i, where),
				"An argument is handed to the kernel as a NUL terminated string, so it cannot\n"+
					"contain one. Remove the escape that produced it.")
			continue
		}
		argv = append(argv, argument)
	}
	return argv
}

// sensorInterval reads the interval and enforces the one second floor. The
// value is a duration; a sensor is never evaluated faster than once a second
// no matter how fast its source produces work.
func (d *decoder) sensorInterval(node ast.Node, where string) time.Duration {
	value := d.timeout(node, 0)
	if value > 0 && value < SensorIntervalMin {
		d.error(CodeSensorIntervalMin, position(node),
			fmt.Sprintf("%s interval is %s, and the lowest it goes is 1s", where, FormatDuration(value)),
			"Sensors are evaluated by one shared runtime. Sub-second polling is a non-goal,\n"+
				"and a sensor that pushes faster is better off as a push based source:\n\n"+
				"    interval: 1s\n\n"+
				"    interval: 60s    a calmer default for a source that changes slowly")
		return value
	}
	return value
}

// sensorTimeout reads the timeout and enforces the [1s, 5m] window. Leaving
// the field out is not an error: the default of 30s is materialised instead.
func (d *decoder) sensorTimeout(node ast.Node, where string) time.Duration {
	value := d.timeout(node, DefaultSensorTimeout)
	if value < SensorTimeoutMin || value > SensorTimeoutMax {
		d.error(CodeSensorTimeout, position(node),
			fmt.Sprintf("%s timeout is %s, and paceq allows between 1s and %s",
				where, FormatDuration(value), FormatDuration(SensorTimeoutMax)),
			"A sensor that needs more than five minutes should chunk its own work through\n"+
				"max_triggers_per_tick instead of asking the runtime to wait on it:\n\n"+
				"    timeout: 30s\n"+
				"    max_triggers_per_tick: 100")
		return DefaultSensorTimeout
	}
	return value
}

// sensorTriggers reads how many triggers one evaluation may admit, inside the
// [1, 10000] window.
func (d *decoder) sensorTriggers(node ast.Node, where string) int {
	resolved, ok := d.resolve(node, where+" max_triggers_per_tick")
	if !ok {
		return DefaultSensorMaxTriggers
	}
	value, isNumber := integerValue(resolved)
	if !isNumber {
		written, _ := scalarText(resolved)
		d.error(CodeBadValue, position(resolved),
			fmt.Sprintf("%s max_triggers_per_tick is %q, and it is a whole number", where, written),
			"Write it without quotes or a unit:\n\n"+
				"    max_triggers_per_tick: 100")
		return DefaultSensorMaxTriggers
	}
	if value < 1 || value > SensorMaxTriggersHi {
		d.error(CodeSensorTriggers, position(resolved),
			fmt.Sprintf("%s max_triggers_per_tick is %d, and paceq allows between 1 and %d",
				where, value, SensorMaxTriggersHi),
			"It is how many triggers one evaluation may admit, the chunking knob that keeps\n"+
				"a burst from flooding the queue:\n\n"+
				"    max_triggers_per_tick: 100")
		return DefaultSensorMaxTriggers
	}
	return int(value)
}

// sensorEnv reads the env block. It is the model the job's own env block uses,
// one rule not two, with the reserved prefix bolted on: PULSEQ_ is how paceq
// itself talks to a sensor process, so a definition that claims a name in that
// space is refused before the process starts (08 section 3.2).
func (d *decoder) sensorEnv(node ast.Node, where string) map[string]string {
	mapping, ok := d.mapping(node, where+" env")
	if !ok {
		return nil
	}

	env := map[string]string{}
	seen := map[string]bool{}
	for _, entry := range mapping.Values {
		if !d.spend(entry, where+" env") {
			return env
		}
		name, ok := d.key(entry, where+" env")
		if !ok {
			continue
		}
		if seen[name] {
			d.duplicateKey(name, where+" env", position(entry.Key))
			continue
		}
		seen[name] = true

		if !envNamePattern.MatchString(name) {
			d.error(CodeBadValue, position(entry.Key),
				fmt.Sprintf("%q is not a name a process can be given", name),
				"An environment variable is named with letters, digits and underscores, and\n"+
					"does not start with a digit.")
			continue
		}
		if strings.HasPrefix(name, "PULSEQ_") {
			d.error(CodeSensorEnvKey, position(entry.Key),
				fmt.Sprintf("%q is set in sensor env, and PULSEQ_ is reserved", name),
				"Paceq itself sets every PULSEQ_ variable a sensor process sees. A definition\n"+
					"that sets one would be silently overwritten, so it is refused here:\n\n"+
					"    env:\n"+
					"      BUCKET: acme-dropzone")
			continue
		}
		value, ok := d.stringValue(entry.Value, where+" env "+name)
		if !ok {
			continue
		}
		if strings.ContainsRune(value, 0) {
			d.error(CodeBadValue, position(entry.Value),
				fmt.Sprintf("the value of %s carries a NUL byte", name),
				"An environment entry is handed to the kernel as a NUL terminated string, so\n"+
					"it cannot contain one.")
			continue
		}
		env[name] = value
	}
	return env
}

func (d *decoder) env(node ast.Node) map[string]string {
	mapping, ok := d.mapping(node, "env")
	if !ok {
		return nil
	}

	env := map[string]string{}
	seen := map[string]bool{}
	for _, entry := range mapping.Values {
		if !d.spend(entry, "env") {
			return env
		}
		name, ok := d.key(entry, "env")
		if !ok {
			continue
		}
		if seen[name] {
			d.duplicateKey(name, "env", position(entry.Key))
			continue
		}
		seen[name] = true

		if !envNamePattern.MatchString(name) {
			d.error(CodeBadValue, position(entry.Key),
				fmt.Sprintf("%q is not a name a process can be given", name),
				"An environment variable is named with letters, digits and underscores, and does\n"+
					"not start with a digit. A name with an equals sign or a space in it cannot be\n"+
					"passed to a process at all.")
			continue
		}
		value, ok := d.stringValue(entry.Value, "env "+name)
		if !ok {
			continue
		}
		if strings.ContainsRune(value, 0) {
			d.error(CodeBadValue, position(entry.Value),
				fmt.Sprintf("the value of %s carries a NUL byte", name),
				"An environment entry is handed to the kernel as a NUL terminated string, so it\n"+
					"cannot contain one.")
			continue
		}
		env[name] = value
	}
	return env
}

func (d *decoder) inheritEnv(node ast.Node) []string {
	sequence, ok := d.sequence(node, "inherit_env")
	if !ok {
		return nil
	}

	names := make([]string, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		name, ok := d.stringValue(value, fmt.Sprintf("inherit_env[%d]", i))
		if !ok {
			continue
		}
		if !envNamePattern.MatchString(name) {
			d.error(CodeBadValue, position(value),
				fmt.Sprintf("%q is not a name a process can be given", name),
				"An environment variable is named with letters, digits and underscores, and does\n"+
					"not start with a digit.")
			continue
		}
		names = append(names, name)
	}
	if len(names) > 0 {
		d.warn(CodeInheritEnv, position(node),
			fmt.Sprintf("the job inherits %s from the environment paceq was started in", strings.Join(names, ", ")),
			"A job starts from an empty environment on purpose (08 section 3.2): what it gets is\n"+
				"what the file says, so a run from a terminal and a run from the scheduler are the\n"+
				"same run. Every name here is an exception, and it makes the job depend on how\n"+
				"paceq itself was started.\n\n"+
				"Set the value in the job instead, when it is not a secret:\n\n"+
				"    env:\n"+
				"      NAME: value")
	}
	return names
}

func (d *decoder) needs(node ast.Node, where string) ([]string, []diag.Position) {
	sequence, ok := d.sequence(node, where+" needs")
	if !ok {
		return nil, nil
	}

	names := make([]string, 0, len(sequence.Values))
	positions := make([]diag.Position, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		name, ok := d.stringValue(value, fmt.Sprintf("needs[%d] in %s", i, where))
		if !ok {
			continue
		}
		// A name used twice says nothing a run would act on twice; the first
		// one wins and a job that says it twice means it once.
		if contains(names, name) {
			continue
		}
		names = append(names, name)
		positions = append(positions, position(value))
	}
	return names, positions
}
