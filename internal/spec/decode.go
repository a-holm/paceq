package spec

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/goccy/go-yaml/ast"

	"github.com/a-holm/paceq/internal/diag"
)

// The fields each mapping in a job file accepts. They are also the candidate
// list a misspelling is measured against, so a field added to a struct and not
// to its list here is a field the parser refuses.
var (
	jobFields      = []string{"name", "description", "env", "env_file", "inherit_env", "workdir", "timeout", "max_concurrent", "steps", "schedules", "sensors"}
	stepFields     = []string{"name", "run", "shell", "workdir", "timeout", "retry", "needs"}
	retryFields    = []string{"max", "backoff", "initial", "max_delay", "jitter"}
	scheduleFields = []string{"name", "cron", "timezone"}
	sensorFields   = []string{"name", "type", "interval"}
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
	job := &Job{Timeout: DefaultTimeout, MaxConcurrent: DefaultMaxConcurrent}

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
		case "max_concurrent":
			job.MaxConcurrent = d.maxConcurrent(value)
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

		schedule := Schedule{Timezone: DefaultTimezone}
		seen := d.fields(mapping, "a schedule", scheduleFields, func(key string, value ast.Node) {
			switch key {
			case "name":
				schedule.Name = d.name(value, where+" name")
			case "cron":
				schedule.Cron = d.text(value, where+" cron")
			case "timezone":
				schedule.Timezone = d.timezone(value, where)
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

	sensors := make([]Sensor, 0, len(sequence.Values))
	for i, value := range sequence.Values {
		where := fmt.Sprintf("sensor %d", i+1)
		mapping, ok := d.mapping(value, where)
		if !ok {
			continue
		}

		sensor := Sensor{}
		seen := d.fields(mapping, "a sensor", sensorFields, func(key string, value ast.Node) {
			switch key {
			case "name":
				sensor.Name = d.name(value, where+" name")
			case "type":
				sensor.Type = d.name(value, where+" type")
			case "interval":
				sensor.Interval = d.timeout(value, 0)
			}
		})
		for _, field := range sensorFields {
			if seen[field] {
				continue
			}
			d.error(CodeMissingField, position(value),
				fmt.Sprintf("%s has no %s", where, field),
				"A sensor says what it watches and how often:\n\n"+
					"    sensors:\n"+
					"      - name: dropzone\n"+
					"        type: file\n"+
					"        interval: 15s\n\n"+
					"What each type accepts beyond that arrives with the sensors in M3-01.")
		}
		sensors = append(sensors, sensor)
	}
	return sensors
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
		names = append(names, name)
		positions = append(positions, position(value))
	}
	return names, positions
}
