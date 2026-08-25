package spec

import (
	"fmt"
	"strings"
	"time"

	"github.com/goccy/go-yaml/ast"

	"github.com/a-holm/paceq/internal/diag"
)

// The primitives the decoder is written in: one function per kind of value a
// field can hold, each of them reporting its own diagnostic and returning the
// zero value rather than an error. A field that goes wrong does not stop the
// rest of the file being read, so one run of validate reports everything that
// is wrong with a file instead of the first thing.

// fields walks a mapping, hands every known key to set, and reports the rest.
// It returns the keys it saw, so a caller can tell a field that is missing from
// one that is present and empty.
func (d *decoder) fields(mapping *ast.MappingNode, what string, known []string, set func(key string, value ast.Node)) map[string]bool {
	// Two sets, because they answer different questions. written is every key
	// that appears, and is what makes a second one a duplicate whatever the
	// first one held. given is the fields that carried a value, and is what
	// tells the caller a required field is missing.
	written, given := map[string]bool{}, map[string]bool{}
	for _, entry := range mapping.Values {
		if !d.spend(entry, what) {
			return given
		}
		key, ok := d.key(entry, what)
		if !ok {
			continue
		}
		if written[key] {
			d.duplicateKey(key, what, position(entry.Key))
			continue
		}
		written[key] = true

		if !contains(known, key) {
			d.unknownField(key, what, known, position(entry.Key))
			continue
		}
		value, ok := d.resolve(entry.Value, key)
		if !ok {
			continue
		}
		// A key written with nothing after it is the field left out. Reading
		// it as an empty value would overwrite the default with a blank.
		if _, isNull := value.(*ast.NullNode); isNull {
			continue
		}
		given[key] = true
		set(key, value)
	}
	return given
}

// key is the name of one mapping entry.
func (d *decoder) key(entry *ast.MappingValueNode, what string) (string, bool) {
	if _, isMerge := entry.Key.(*ast.MergeKeyNode); isMerge {
		d.error(CodeMergeKey, position(entry.Key),
			fmt.Sprintf("%s uses a merge key, which paceq does not resolve", what),
			"<< pulls the contents of another mapping into this one, and what wins when both\n"+
				"define a key is a rule nobody reading the file can see. Write the fields out, or\n"+
				"use an anchor on a single value:\n\n"+
				"    timeout: &short 30s")
		return "", false
	}
	key, ok := scalarText(entry.Key)
	if !ok {
		d.error(CodeBadValue, position(entry.Key),
			fmt.Sprintf("a key in %s is %s, and a key is a name", what, typeName(entry.Key)),
			"Every field in a job file is named with plain text:\n\n"+
				"    timeout: 45m")
		return "", false
	}
	return key, true
}

func (d *decoder) duplicateKey(key, what string, pos diag.Position) {
	d.error(CodeDuplicateKey, pos,
		fmt.Sprintf("%q is set twice in %s", key, what),
		"A mapping holds each key once. The second one wins silently in most YAML readers,\n"+
			"which is how a job ends up running with a value nobody can find in the file.\n"+
			"Delete the one you did not mean.")
}

func (d *decoder) unknownField(key, what string, known []string, pos diag.Position) {
	message := fmt.Sprintf("%q is not a field %s has", key, what)
	hint := "The fields here are: " + strings.Join(known, ", ") + "."
	if suggestion := nearestField(key, known); suggestion != "" {
		message += fmt.Sprintf(", did you mean %q", suggestion)
		hint = fmt.Sprintf("Rename it to %q. %s\n\n%s", suggestion, fieldShape(suggestion), hint)
	}
	d.error(CodeUnknownField, pos, message, hint)
}

// nearestField is the did-you-mean (03 section 8.3): the names people actually
// arrive with first, then edit distance over the fields that exist here.
func nearestField(written string, known []string) string {
	if target, ok := commonMistakes[strings.ToLower(written)]; ok && contains(known, target) {
		return target
	}
	return diag.Suggest(written, known)
}

// fieldShape is the one sentence that stops the next mistake: the field the
// suggestion names is often not the shape the wrong one was written in.
func fieldShape(field string) string {
	switch field {
	case "run":
		return "It is a list of arguments, not a string."
	case "retry":
		return "It is a block, not a number."
	case "needs":
		return "It is a list of step names."
	case "env":
		return "It is a mapping of name to value."
	case "steps", "schedules", "sensors", "inherit_env":
		return "It is a list."
	case "timeout", "max_delay", "initial", "interval":
		return "It is a duration, such as 30s or 45m."
	case "max_concurrent", "max":
		return "It is a whole number."
	default:
		return "It is plain text."
	}
}

// startPosition is where a block begins, which is the first key in it rather
// than the punctuation the syntax tree hangs the block off. A diagnostic about
// a block as a whole points there.
func startPosition(mapping *ast.MappingNode) diag.Position {
	if mapping != nil && len(mapping.Values) > 0 {
		return position(mapping.Values[0].Key)
	}
	if mapping != nil && mapping.Start != nil {
		return position(mapping)
	}
	return diag.Position{}
}

func (d *decoder) mapping(node ast.Node, what string) (*ast.MappingNode, bool) {
	resolved, ok := d.resolve(node, what)
	if !ok {
		return nil, false
	}
	if mapping, ok := resolved.(*ast.MappingNode); ok {
		return mapping, true
	}
	// A mapping with one entry parses as the entry itself, so a single field
	// block arrives here as the value rather than as a mapping around it.
	if entry, ok := resolved.(*ast.MappingValueNode); ok {
		return &ast.MappingNode{Start: entry.Start, Values: []*ast.MappingValueNode{entry}}, true
	}
	d.error(CodeBadValue, position(resolved),
		fmt.Sprintf("%s is %s, and it is a block of named fields", what, typeName(resolved)),
		"A block is written as name and value pairs, one per line:\n\n"+
			"    name: nightly-report\n"+
			"    timeout: 45m")
	return nil, false
}

func (d *decoder) sequence(node ast.Node, what string) (*ast.SequenceNode, bool) {
	resolved, ok := d.resolve(node, what)
	if !ok {
		return nil, false
	}
	sequence, isSequence := resolved.(*ast.SequenceNode)
	if !isSequence {
		d.error(CodeBadValue, position(resolved),
			fmt.Sprintf("%s is %s, and it is a list", what, typeName(resolved)),
			"A list is written one entry per line behind a dash, or inline in brackets:\n\n"+
				"    "+lastWord(what)+":\n"+
				"      - first\n"+
				"      - second")
		return nil, false
	}
	for range sequence.Values {
		if !d.spend(sequence, what) {
			return nil, false
		}
	}
	return sequence, true
}

// stringValue is any scalar, read as the text that is in the file.
//
// This is where the Norway problem stops. YAML 1.1 readers turn the country
// code NO into false, and a job that inherits a variable called NO, or sets one
// to the string "no", has every right to expect it back as it was written.
func (d *decoder) stringValue(node ast.Node, what string) (string, bool) {
	resolved, ok := d.resolve(node, what)
	if !ok {
		return "", false
	}
	text, ok := scalarText(resolved)
	if !ok {
		d.error(CodeBadValue, position(resolved),
			fmt.Sprintf("%s is %s, and it is text", what, typeName(resolved)),
			"Write it on one line, quoting it when it contains a colon or starts with a\n"+
				"special character:\n\n"+
				"    "+lastWord(what)+": \"a value\"")
		return "", false
	}
	return text, true
}

// text is stringValue for a field that has no further rules.
func (d *decoder) text(node ast.Node, what string) string {
	value, _ := d.stringValue(node, what)
	return value
}

// name is a text field that has to be usable on a command line, in a directory
// name and in a URL.
func (d *decoder) name(node ast.Node, what string) string {
	value, ok := d.stringValue(node, what)
	if !ok {
		return ""
	}
	if !namePattern.MatchString(value) {
		d.error(CodeBadName, position(node),
			fmt.Sprintf("%s is %q, which is not a name paceq accepts", what, value),
			"A name is lower case letters, digits, underscores and dashes, starts with a letter\n"+
				"or a digit, and is at most 64 characters: "+NamePattern+"\n\n"+
				"    name: "+suggestName(value)+"\n\n"+
				"It has to survive being typed on a command line, used as a directory name and\n"+
				"put in a URL, and a name that needs quoting in any of those gets typed wrong.")
		return ""
	}
	return value
}

// suggestName is what the name would look like if it followed the rule, so the
// message carries a value that can be pasted rather than a rule to apply.
func suggestName(value string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(value) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-':
			b.WriteRune(r)
		case r == ' ' || r == '.' || r == '/' || r == ':':
			b.WriteByte('-')
		}
	}
	cleaned := strings.Trim(b.String(), "-")
	if cleaned == "" {
		return "nightly-report"
	}
	if len(cleaned) > 64 {
		cleaned = cleaned[:64]
	}
	if cleaned[0] == '_' || cleaned[0] == '-' {
		cleaned = strings.TrimLeft(cleaned, "_-")
	}
	if cleaned == "" {
		return "nightly-report"
	}
	return cleaned
}

func (d *decoder) flag(node ast.Node, what string) bool {
	resolved, ok := d.resolve(node, what)
	if !ok {
		return false
	}
	if value, isBool := resolved.(*ast.BoolNode); isBool {
		return value.Value
	}
	written, _ := scalarText(resolved)
	d.error(CodeBadValue, position(resolved),
		fmt.Sprintf("%s is %q, and it is true or false", what, written),
		"YAML has exactly two boolean values, true and false. yes, no, on and off are text,\n"+
			"and reading them as booleans is what turns the country code NO into false.")
	return false
}

// count is a whole number inside a range.
func (d *decoder) count(node ast.Node, what string, low, high int) int {
	resolved, ok := d.resolve(node, what)
	if !ok {
		return low
	}
	value, isNumber := integerValue(resolved)
	if !isNumber {
		written, _ := scalarText(resolved)
		d.error(CodeBadValue, position(resolved),
			fmt.Sprintf("%s is %q, and it is a whole number", what, written),
			fmt.Sprintf("Write it without quotes and without a unit, between %d and %d:\n\n    %s: %d",
				low, high, lastWord(what), low))
		return low
	}
	if value < int64(low) || value > int64(high) {
		d.error(CodeBadValue, position(resolved),
			fmt.Sprintf("%s is %d, and it is between %d and %d", what, value, low, high),
			fmt.Sprintf("Pick a number in range:\n\n    %s: %d", lastWord(what), low))
		return low
	}
	return int(value)
}

// maxConcurrent has its own function because its floor carries a reason worth
// stating: this is the field that replaces the flock wrapper (09 US-02).
func (d *decoder) maxConcurrent(node ast.Node) int {
	resolved, ok := d.resolve(node, "max_concurrent")
	if !ok {
		return DefaultMaxConcurrent
	}
	value, isNumber := integerValue(resolved)
	if !isNumber {
		written, _ := scalarText(resolved)
		d.error(CodeBadConcurrency, position(resolved),
			fmt.Sprintf("max_concurrent is %q, and it is a whole number", written),
			"It is how many runs of this job may be in flight at once:\n\n"+
				"    max_concurrent: 1")
		return DefaultMaxConcurrent
	}
	if value < 1 {
		d.error(CodeBadConcurrency, position(resolved),
			fmt.Sprintf("max_concurrent is %d, and the lowest it goes is 1", value),
			"There is no value that means \"never run\". Remove the schedule, or pause the job,\n"+
				"if that is what you want. 1 is the default and means what a flock wrapper around\n"+
				"a cron line means: a second run waits rather than overlapping.\n\n"+
				"    max_concurrent: 1")
		return DefaultMaxConcurrent
	}
	if value > int64(MaxSteps) {
		d.error(CodeBadConcurrency, position(resolved),
			fmt.Sprintf("max_concurrent is %d, and paceq allows at most %d", value, MaxSteps),
			fmt.Sprintf("Pick a number between 1 and %d.", MaxSteps))
		return DefaultMaxConcurrent
	}
	return int(value)
}

// maxParallel has its own function because its ceiling carries a reason worth
// stating: this is the field that becomes the per-run semaphore in M4-02, and
// the same gate max_concurrent uses applies to it. A number outside 1..64 is
// refused here, at the door, so the engine never has to read a nonsense limit.
func (d *decoder) maxParallel(node ast.Node) int {
	resolved, ok := d.resolve(node, "max_parallel")
	if !ok {
		return DefaultMaxParallel
	}
	value, isNumber := integerValue(resolved)
	if !isNumber {
		written, _ := scalarText(resolved)
		d.error(CodeBadMaxParallel, position(resolved),
			fmt.Sprintf("max_parallel is %q, and it is a whole number", written),
			fmt.Sprintf("It is how many steps of one run may run at once:\n\n    max_parallel: %d", DefaultMaxParallel))
		return DefaultMaxParallel
	}
	if value < 1 {
		d.error(CodeBadMaxParallel, position(resolved),
			fmt.Sprintf("max_parallel is %d, and the lowest it goes is 1", value),
			"There is no value that means \"never run any step in parallel\". Set it to 1,\n"+
				"the floor, if you want the steps of a run to strictly serialise.")
		return DefaultMaxParallel
	}
	if value > int64(MaxParallelHi) {
		d.error(CodeBadMaxParallel, position(resolved),
			fmt.Sprintf("max_parallel is %d, and paceq allows at most %d", value, MaxParallelHi),
			fmt.Sprintf("Pick a number between 1 and %d.", MaxParallelHi))
		return DefaultMaxParallel
	}
	return int(value)
}

// timeout reads a duration field. fallback is what an unreadable value falls
// back to, so the rest of the file is still checked against something sensible.
func (d *decoder) timeout(node ast.Node, fallback time.Duration) time.Duration {
	resolved, ok := d.resolve(node, "a duration")
	if !ok {
		return fallback
	}
	written, ok := scalarText(resolved)
	if !ok {
		d.error(CodeBadDuration, position(resolved),
			fmt.Sprintf("a duration here is %s", typeName(resolved)),
			durationHint(""))
		return fallback
	}

	value, err := ParseDuration(written)
	if err != nil {
		d.error(CodeBadDuration, position(resolved), err.Error(), durationHint(written))
		return fallback
	}
	if value > MaxJobTimeout {
		d.error(CodeTimeoutTooLong, position(resolved),
			fmt.Sprintf("%s is over the ceiling of %s", FormatDuration(value), FormatDuration(MaxJobTimeout)),
			"A timeout is mandatory and it has a ceiling (08 section 3.2). A job that runs\n"+
				"longer than a day is a service, and paceq supervises jobs.\n\n"+
				"Split it into steps with their own timeouts, or run it as a service and let\n"+
				"paceq check on it instead.")
		return fallback
	}
	return value
}

// expectedWithin reads the freshness SLA of #40. A job that declares one at
// all must declare a positive one: zero would mean "must succeed within no
// time", which as a gauge is an alarm on every healthy run, so it is refused
// rather than silently swallowed into "no expectation".
func (d *decoder) expectedWithin(node ast.Node) time.Duration {
	resolved, ok := d.resolve(node, "a duration")
	if !ok {
		return 0
	}
	written, ok := scalarText(resolved)
	if !ok {
		d.error(CodeBadDuration, position(resolved),
			fmt.Sprintf("a duration here is %s", typeName(resolved)),
			durationHint(""))
		return 0
	}

	value, err := ParseDuration(written)
	if err != nil {
		d.error(CodeBadDuration, position(resolved), err.Error(), durationHint(written))
		return 0
	}
	if value <= 0 {
		d.error(CodeBadDuration, position(resolved),
			fmt.Sprintf("expected_within is %s, and an expectation of no time alarms on every healthy run",
				FormatDuration(value)),
			"Give the longest span a healthy gap between successes may reach - 26h for a\n"+
				"job that runs nightly and may overrun its slot - or leave the field out.")
		return 0
	}
	return value
}

func durationHint(written string) string {
	hint := "A duration is a number and a unit: ms, s, m or h. They can be written together.\n\n" +
		"    timeout: 45m\n" +
		"    initial: 10s\n" +
		"    max_delay: 1h30m"
	if written != "" && !strings.ContainsAny(written, "smh") {
		hint = "The number needs a unit. paceq will not guess between seconds and minutes.\n\n" +
			"    timeout: " + written + "s    " + written + " seconds\n" +
			"    timeout: " + written + "m    " + written + " minutes"
	}
	return hint
}

func (d *decoder) oneOf(node ast.Node, what string, allowed ...string) string {
	value, ok := d.stringValue(node, what)
	if !ok {
		return allowed[0]
	}
	if contains(allowed, value) {
		return value
	}
	message := fmt.Sprintf("%s is %q, and it is one of %s", what, value, strings.Join(allowed, " or "))
	hint := fmt.Sprintf("Pick one:\n\n    %s: %s", lastWord(what), allowed[0])
	if suggestion := diag.Suggest(value, allowed); suggestion != "" {
		message += fmt.Sprintf(", did you mean %q", suggestion)
		hint = fmt.Sprintf("    %s: %s", lastWord(what), suggestion)
	}
	d.error(CodeBadValue, position(node), message, hint)
	return allowed[0]
}

func (d *decoder) timezone(node ast.Node, where string) string {
	value, ok := d.stringValue(node, where+" timezone")
	if !ok {
		return DefaultTimezone
	}
	// LoadLocation reads the zone database this machine has, which is the same
	// database the scheduler will read. A zone that does not load here would
	// not load there either.
	if _, err := time.LoadLocation(value); err != nil {
		d.error(CodeUnknownTimezone, position(node),
			fmt.Sprintf("%q is not a time zone this machine knows: %v", value, err),
			"Zones are IANA names, region and city, such as Europe/Oslo or America/New_York.\n"+
				"Three letter abbreviations are ambiguous and are not accepted.\n\n"+
				"    timezone: Europe/Oslo\n\n"+
				"    paceq doctor    reports whether the zone database is readable at all")
		return DefaultTimezone
	}
	return value
}

// crossCheck is the part of validation that needs the whole job: the rules
// about how steps refer to each other.
func (d *decoder) crossCheck(job *Job) {
	if job == nil {
		return
	}

	names := make(map[string]int, len(job.Steps))
	for i, step := range job.Steps {
		if d.stopped {
			return
		}
		if step.Name == "" {
			continue
		}
		if first, taken := names[step.Name]; taken {
			d.error(CodeDuplicateStep, d.stepPosition(i),
				fmt.Sprintf("two steps are called %q", step.Name),
				fmt.Sprintf("Step %d already has that name. A step name is how a failure says which step\n", first+1)+
					"failed, how needs points at it and how a retry names it, so two steps cannot\n"+
					"share one. Rename this one:\n\n"+
					fmt.Sprintf("    - name: %s-2", step.Name))
			continue
		}
		names[step.Name] = i
	}

	for i, step := range job.Steps {
		for j, need := range step.Needs {
			if d.stopped {
				return
			}
			if _, exists := names[need]; exists {
				continue
			}
			d.error(CodeUnknownNeed, d.needPosition(i, j),
				fmt.Sprintf("step %q needs %q, and no step is called that", step.Name, need),
				needHint(need, job.Steps))
		}
	}

	d.checkCycles(job.Steps)
	d.checkGraphBounds(job.Steps)

	sensorNames := make(map[string]int, len(job.Sensors))
	for i, sensor := range job.Sensors {
		if d.stopped {
			return
		}
		if sensor.Name == "" {
			continue
		}
		if first, taken := sensorNames[sensor.Name]; taken {
			d.error(CodeSensorNameTaken, d.sensorPosition(i),
				fmt.Sprintf("two sensors in this job are called %q", sensor.Name),
				fmt.Sprintf("Sensor %d already carries that name, and a sensor name is the row's primary\n", first+1)+
					"key, so two of them can never both exist. It is also the name a failure says\n"+
					"which one fired, so two sharing one means a message nobody can tell apart:\n\n"+
					"    - name: "+sensor.Name+"\n\n"+
					"Rename one of them.\n")
			continue
		}
		sensorNames[sensor.Name] = i
	}
}

func needHint(need string, steps []Step) string {
	known := make([]string, 0, len(steps))
	for _, step := range steps {
		if step.Name != "" {
			known = append(known, step.Name)
		}
	}
	hint := "The steps in this job are: " + strings.Join(known, ", ") + "."
	if suggestion := diag.Suggest(need, known); suggestion != "" {
		hint = fmt.Sprintf("Did you mean %q?\n\n    needs: [%s]\n\n%s", suggestion, suggestion, hint)
	}
	return hint + "\n\nneeds parses in M1 and is enforced in M4-01, where it becomes the graph. Until\n" +
		"then the steps run in the order they are written."
}

func (d *decoder) stepPosition(i int) diag.Position {
	if i < len(d.stepPos) {
		return d.stepPos[i].name
	}
	return diag.Position{}
}

func (d *decoder) sensorPosition(i int) diag.Position {
	if i < len(d.sensorPos) {
		return d.sensorPos[i].name
	}
	return diag.Position{}
}

func (d *decoder) needPosition(step, need int) diag.Position {
	if step < len(d.stepPos) && need < len(d.stepPos[step].needs) {
		return d.stepPos[step].needs[need]
	}
	return d.stepPosition(step)
}

// checkCycles runs after every step is known to exist, so the only problem
// left in the graph is a loop. A loop means each step in it waits on a step
// that waits on it, so none of them can ever start; the diagnostic names the
// steps it goes around, so the reader can find it in the file. The position
// points at the first named step, which is the one the loop comes back to.
func (d *decoder) checkCycles(steps []Step) {
	if d.stopped {
		return
	}
	_, cycle := TopoOrder(steps)
	if cycle == "" {
		return
	}
	subject := cycle
	if line := strings.Index(subject, " -> "); line >= 0 {
		subject = subject[:line]
	}
	where := -1
	for i, step := range steps {
		if step.Name == subject {
			where = i
			break
		}
	}
	d.error(CodeCycle, d.stepPosition(where),
		fmt.Sprintf("the steps depend on each other in a circle: %s", cycle),
		"Every step in that list waits on one before to start, so none of them can:\n"+
			"    "+cycle+"\n\n"+
			"One of them must stop waiting on the one after it. A dependency has to point\n"+
			"backwards, at a step that has already finished.")
}

// checkGraphBounds is the other graph refusal: a graph that avoids a cycle
// but runs deeper than MaxDAGDepth, or fans a single step out wider than
// MaxFanOut, is refused, so an edit pushed into the file cannot push a run
// past the machine's limits at apply.
func (d *decoder) checkGraphBounds(steps []Step) {
	if d.stopped {
		return
	}
	order, cycle := TopoOrder(steps)
	if cycle != "" {
		return
	}
	if len(order) == 0 {
		return
	}

	byName := map[string]int{}
	for i, s := range steps {
		byName[s.Name] = i
	}
	// depth[i] is the longest run of edges from a step with no needs to step
	// i, in the deterministic order, so every need is resolved before its
	// consumer.
	depth := make([]int, len(steps))
	// fanOut[i] is how many steps wait on step i, plus the number of needs
	// step i names; both directions count the same ceiling.
	fanOut := make([]int, len(steps))
	for _, name := range order {
		i := byName[name]
		best := 0
		for _, need := range steps[i].Needs {
			j := byName[need]
			if depth[j]+1 > best {
				best = depth[j] + 1
			}
			fanOut[j]++
		}
		if len(steps[i].Needs) > MaxFanOut {
			d.error(CodeFanOutLimit, d.stepPosition(i),
				fmt.Sprintf("step %q names %d needs, and the most it may is %d", name, len(steps[i].Needs), MaxFanOut),
				fmt.Sprintf("A step that waits on %d others is a graph several machines, not one\n"+
					"step. Split it:\n\n"+
					"    max_fan_out: %d", MaxFanOut, MaxFanOut))
			continue
		}
		depth[i] = best
		if best > MaxDAGDepth {
			d.error(CodeDAGDepthLimit, d.stepPosition(i),
				fmt.Sprintf("step %q is %d steps from the top, and the deepest a run may be is %d", name, best, MaxDAGDepth),
				fmt.Sprintf("A pipeline %d steps deep waits longer than the machine can answer.\nFlatten it into groups:\n\n"+
					"    max_depth: %d", MaxDAGDepth, MaxDAGDepth))
		}
	}
	for i := range steps {
		if fanOut[i] > MaxFanOut {
			d.error(CodeFanOutLimit, d.stepPosition(i),
				fmt.Sprintf("%d steps wait on %q, and the most may is %d", fanOut[i], steps[i].Name, MaxFanOut),
				fmt.Sprintf("A step that %d others depends on is a single point the whole run waits\\n"+
					"on. Fan it out:\\n\\n"+
					"    max_fan_out: %d", MaxFanOut, MaxFanOut))
		}
	}
}

// scalarText is the value of a scalar node as the text the file carries.
func scalarText(n ast.Node) (string, bool) {
	switch v := n.(type) {
	case *ast.StringNode:
		return v.Value, true
	case *ast.LiteralNode:
		if v.Value == nil {
			return "", false
		}
		return v.Value.Value, true
	case *ast.IntegerNode, *ast.FloatNode, *ast.BoolNode, *ast.InfinityNode, *ast.NanNode:
		tk := n.GetToken()
		if tk == nil {
			return "", false
		}
		return tk.Value, true
	}
	return "", false
}

func integerValue(n ast.Node) (int64, bool) {
	number, ok := n.(*ast.IntegerNode)
	if !ok {
		return 0, false
	}
	switch value := number.Value.(type) {
	case int64:
		return value, true
	case uint64:
		// A number this large is out of range for every field that takes one,
		// and the range check the caller runs reports it as such.
		if value > 1<<62 {
			return 1 << 62, true
		}
		return int64(value), true
	}
	return 0, false
}

// typeName is what a message calls a value that is the wrong kind.
func typeName(n ast.Node) string {
	switch n.(type) {
	case *ast.MappingNode, *ast.MappingValueNode:
		return "a block of named fields"
	case *ast.SequenceNode:
		return "a list"
	case *ast.IntegerNode, *ast.FloatNode:
		return "a number"
	case *ast.BoolNode:
		return "true or false"
	case *ast.NullNode:
		return "empty"
	case *ast.StringNode, *ast.LiteralNode:
		return "text"
	case *ast.AliasNode:
		return "an alias"
	}
	return "a value paceq does not read"
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// lastWord is the field name at the end of a context string, so a hint that
// shows how to write the field shows the field rather than the sentence that
// located it.
func lastWord(what string) string {
	if i := strings.LastIndex(what, " "); i >= 0 {
		return what[i+1:]
	}
	return what
}
