package cronx

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/adhocore/gronx"
)

// Kind says which family a schedule belongs to.
type Kind uint8

const (
	// KindCron is a five field cron expression in minute resolution.
	KindCron Kind = iota
	// KindInterval is a fixed duration, pure UTC arithmetic.
	KindInterval
)

// SpringForward chooses what happens to a slot whose local wall time does not
// exist because the clocks jumped ahead. The zero value is the default.
type SpringForward uint8

const (
	// Skip materialises the missing slot as an occurrence with Skipped set
	// and SkipReason dst_nonexistent. Nothing runs, but the gap is recorded.
	Skip SpringForward = iota
	// Shift runs the slot at the instant the requested wall clock maps to
	// after the jump, the way Vixie cron does.
	Shift
)

// FallBack chooses what happens when a local wall time happens twice because
// the clocks fell back. The zero value is the default.
type FallBack uint8

const (
	// First keeps only the first (earliest UTC) instance real. The second
	// instance is materialised as an occurrence with Skipped set and
	// SkipReason dst_duplicate.
	First FallBack = iota
	// Both makes both instances real. Their different UTC instants give them
	// different scheduled_for values downstream without special code.
	Both
)

// Policy carries the DST decisions for one schedule. The zero value is the
// documented default: skip nonexistent times, fire doubled times once.
type Policy struct {
	SpringForward SpringForward
	FallBack      FallBack
}

func (p Policy) validate() error {
	if p.SpringForward > Shift {
		return fmt.Errorf("unknown spring_forward policy value %d: use cronx.Skip or cronx.Shift", p.SpringForward)
	}
	if p.FallBack > Both {
		return fmt.Errorf("unknown fall_back policy value %d: use cronx.First or cronx.Both", p.FallBack)
	}
	return nil
}

// Reasons an occurrence can carry on Skipped. Consumers write these straight
// into tick rows as reason codes.
const (
	SkipReasonNonexistent = "dst_nonexistent"
	SkipReasonDuplicate   = "dst_duplicate"
)

// Schedule is one parsed schedule definition. Expr holds the canonical form:
// descriptors are expanded, fields are normalised numbers joined by single
// spaces, and intervals are spelled "@every <duration>".
type Schedule struct {
	Kind     Kind
	Expr     string
	Interval time.Duration // valid when Kind == KindInterval
}

// Occurrence is one concrete moment a schedule lands on. At is ALWAYS UTC;
// no local time ever crosses this package's boundary. LocalWall renders At in
// the schedule's zone for display, except for a skipped spring forward slot,
// where it shows the requested wall clock with the offset that would have
// applied before the clocks jumped, because that is the time the user asked
// about and the whole point is to explain its absence.
type Occurrence struct {
	At         time.Time // UTC
	LocalWall  string    // display only, see the type comment
	Skipped    bool
	SkipReason string // "" unless Skipped; SkipReasonNonexistent or SkipReasonDuplicate
}

// descriptors maps the @names the package documents to their five field form.
var descriptors = map[string]string{
	"@hourly":   "0 * * * *",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@weekly":   "0 0 * * 0",
	"@monthly":  "0 0 1 * *",
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
}

// Parse validates a schedule expression and returns its canonical Schedule.
//
// Accepted forms:
//
//   - five field cron: "minute hour day-of-month month day-of-week" with
//     *, lists, ranges, steps and the names jan..dec and sun..sat. Day of
//     week accepts 0 and 7 as Sunday. Names work on both sides of a range
//     ("mon-fri", "jan-mar", any case). A step after a single value walks
//     from that value to the field ceiling like gronx: "9/2" is
//     9,11,...,23, and on weekdays "1/2" stops at 6 because 7 folds to
//     Sunday. A start beyond the field ("90/2") is rejected instead of
//     gronx's silent never fire. Seconds fields and the L, W and #
//     extensions are rejected with an explicit error; they are not part of
//     the documented 1.0 contract.
//   - descriptors: @hourly, @daily, @midnight, @weekly, @monthly, @yearly,
//     @annually.
//   - intervals: "@every 90m" or a bare duration like "15m". Resolution below
//     one second is rejected: the scheduler ticks at second level.
func Parse(expr string) (Schedule, error) {
	raw := strings.TrimSpace(expr)
	if raw == "" {
		return Schedule{}, fmt.Errorf("empty schedule expression: give five cron fields like \"*/5 * * * *\", a descriptor like @daily, or an interval like @every 90m")
	}
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "@every ") {
		return parseInterval(strings.TrimSpace(raw[len("@every "):]))
	}
	if s, err := parseInterval(raw); err == nil {
		return s, nil
	}
	return parseCron(raw)
}

func parseInterval(body string) (Schedule, error) {
	d, err := time.ParseDuration(body)
	if err != nil {
		return Schedule{}, fmt.Errorf("bad interval %q: %v; use a Go duration like 90m, 1h30m or 24h", body, err)
	}
	if d <= 0 {
		return Schedule{}, fmt.Errorf("interval %q must be positive", body)
	}
	if d < time.Second {
		return Schedule{}, fmt.Errorf("interval %q is below one second: sub second resolution is not supported, the scheduler ticks at second level", body)
	}
	return Schedule{Kind: KindInterval, Expr: "@every " + d.String(), Interval: d}, nil
}

// fieldSpec describes one cron column.
type fieldSpec struct {
	pos   int
	name  string
	min   int
	max   int
	names map[string]int
}

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dayNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

func cronFields() []fieldSpec {
	return []fieldSpec{
		{pos: 1, name: "minute", min: 0, max: 59},
		{pos: 2, name: "hour", min: 0, max: 23},
		{pos: 3, name: "day of month", min: 1, max: 31},
		{pos: 4, name: "month", min: 1, max: 12, names: monthNames},
		{pos: 5, name: "day of week", min: 0, max: 7, names: dayNames},
	}
}

func parseCron(raw string) (Schedule, error) {
	lower := strings.ToLower(raw)

	canonical := lower
	if expanded, ok := descriptors[lower]; ok {
		canonical = expanded
	}

	fields := strings.Fields(canonical)
	specs := cronFields()
	if len(fields) != len(specs) {
		return Schedule{}, fmt.Errorf("expression %q has %d fields: cron schedules need exactly 5 fields (minute hour day-of-month month day-of-week); seconds fields and descriptors beyond the documented list are not supported", raw, len(fields))
	}

	for i, spec := range specs {
		if err := harnessCheckField(fields[i], spec); err != nil {
			return Schedule{}, err
		}
	}

	canon, err := canonicalFields(fields, specs)
	if err != nil {
		return Schedule{}, err
	}

	// gronx is the parser of record agreed in SYNTHESIS 3.4. The field walk
	// above already enforces our narrower contract; this cross check catches
	// anything the two disagree on before a bad Schedule can escape. A panic
	// is converted to an error so fuzzing sees a rejection, never a crash.
	if !gronxSafeIsValid(canon) {
		return Schedule{}, fmt.Errorf("expression %q was rejected by the cron parser: supported syntax is five fields with * , - / and month or weekday names", canon)
	}

	return Schedule{Kind: KindCron, Expr: canon}, nil
}

// gronxSafeIsValid guards the third party call: gronx must never be able to
// crash Parse, whatever input reaches it.
func gronxSafeIsValid(expr string) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return gronx.IsValid(expr)
}

// harnessCheckField rejects the L, W and # extensions by token shape, checks
// that every name inside a list or range is a real field name, then leaves
// deeper validation to the number walk in canonicalFields. Month and weekday
// names contain the letters l and w (jul, wed), so a bare substring search
// would reject legal expressions; the shape check does not.
func harnessCheckField(field string, spec fieldSpec) error {
	if strings.Contains(field, "#") {
		return fmt.Errorf("field %d (%s) %q uses #: nth weekday extensions are not supported in paceq 1.0; spell the schedule out with plain cron fields", spec.pos, spec.name, field)
	}
	for _, atom := range strings.Split(field, ",") {
		base := atom
		if i := strings.Index(base, "/"); i >= 0 {
			base = base[:i]
		}
		run := lettersOnly(base)
		switch run {
		case "":
			continue
		case "l", "w", "lw":
			return fmt.Errorf("field %d (%s) %q uses %s: last/weekday extensions are not supported in paceq 1.0; spell the schedule out with plain cron fields", spec.pos, spec.name, field, strings.ToUpper(run))
		}
		// Name ranges are legal: validate each side of the dash on its own,
		// so mon-fri reads as two names instead of one collapsed blob.
		for _, tok := range strings.Split(base, "-") {
			if tok == "" {
				return fmt.Errorf("field %d (%s) range %q cannot be read in %q", spec.pos, spec.name, base, field)
			}
			if _, err := strconv.Atoi(tok); err == nil {
				continue
			}
			if _, ok := spec.names[tok]; ok {
				continue
			}
			return fmt.Errorf("field %d (%s) has unknown name %q in %q: use jan..dec for months, sun..sat for weekdays, or numbers", spec.pos, spec.name, tok, field)
		}
	}
	return nil
}

// lettersOnly collects the alphabetic characters of one field atom.
func lettersOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// canonicalFields parses every field into a sorted set of numbers and renders
// the canonical expression. Full ranges collapse to *, so equal semantics
// always give equal Expr strings.
func canonicalFields(fields []string, specs []fieldSpec) (string, error) {
	out := make([]string, len(fields))
	for i, spec := range specs {
		set, err := parseFieldSet(fields[i], spec)
		if err != nil {
			return "", err
		}
		if len(set) == effMax(spec)-spec.min+1 && set[0] == spec.min && set[len(set)-1] == effMax(spec) {
			out[i] = "*"
			continue
		}
		parts := make([]string, len(set))
		for j, v := range set {
			parts[j] = strconv.Itoa(v)
		}
		out[i] = strings.Join(parts, ",")
	}
	return strings.Join(out, " "), nil
}

// parseFieldSet expands one cron column into its sorted unique values.
func parseFieldSet(field string, spec fieldSpec) ([]int, error) {
	seen := map[int]bool{}
	var vals []int

	add := func(v int) error {
		v = normalizeValue(v, spec)
		if v < spec.min || v > maxOf(spec) {
			return fmt.Errorf("field %d (%s) value %d out of range %d..%d in %q", spec.pos, spec.name, v, spec.min, maxOf(spec), field)
		}
		if !seen[v] {
			seen[v] = true
			vals = append(vals, v)
		}
		return nil
	}

	for _, atom := range strings.Split(field, ",") {
		body, stepStr := atom, ""
		hasStep := false
		if i := strings.Index(atom, "/"); i >= 0 {
			body, stepStr, hasStep = atom[:i], atom[i+1:], true
		}
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepStr)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("field %d (%s) step %q must be a positive number in %q", spec.pos, spec.name, stepStr, field)
			}
			step = n
		}

		lo, hi := spec.min, maxOf(spec)
		switch {
		case body == "*":
			if !hasStep {
				for v := lo; v <= hi; v++ {
					if err := add(v); err != nil {
						return nil, err
					}
				}
				continue
			}
		case strings.Contains(body, "-"):
			bounds := strings.SplitN(body, "-", 2)
			a, errA := resolveValue(bounds[0], spec)
			b, errB := resolveValue(bounds[1], spec)
			if errA != nil || errB != nil {
				return nil, fmt.Errorf("field %d (%s) range %q cannot be read in %q", spec.pos, spec.name, body, field)
			}
			if a > b {
				return nil, fmt.Errorf("field %d (%s) range %q runs backwards: put the smaller number first", spec.pos, spec.name, body)
			}
			lo, hi = a, b
		default:
			v, err := resolveValue(body, spec)
			if err != nil {
				return nil, fmt.Errorf("field %d (%s) value %q cannot be read in %q: use numbers%s", spec.pos, spec.name, body, field, nameHint(spec))
			}
			lo, hi = v, v
			if !hasStep {
				if err := add(v); err != nil {
					return nil, err
				}
			} else {
				// A step on a single value walks from that value to the
				// field ceiling, like the gronx reference: 9/2 fires 9,
				// 11 ... 23. The weekday ceiling is 6 because 7 folds to
				// Sunday, so 1/2 gives 1, 3, 5 there too.
				hi = effMax(spec)
			}
		}
		for v := lo; v <= hi; v += step {
			if err := add(v); err != nil {
				return nil, err
			}
		}
	}

	sort.Ints(vals)
	if len(vals) == 0 {
		return nil, fmt.Errorf("field %d (%s) %q matches no value", spec.pos, spec.name, field)
	}
	return vals, nil
}

func maxOf(spec fieldSpec) int {
	if spec.name == "day of week" {
		return 7 // 7 means Sunday, same as 0
	}
	return spec.max
}

// effMax is the largest value a field's set can actually hold after
// normalisation. Day of week folds 7 into 0, so its effective ceiling is 6:
// that is the width a full wildcard collapses at.
func effMax(spec fieldSpec) int {
	if spec.name == "day of week" {
		return 6
	}
	return maxOf(spec)
}

func normalizeValue(v int, spec fieldSpec) int {
	if spec.name == "day of week" && v == 7 {
		return 0
	}
	return v
}

func resolveValue(text string, spec fieldSpec) (int, error) {
	if spec.names != nil {
		if v, ok := spec.names[strings.ToLower(text)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(text)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func nameHint(spec fieldSpec) string {
	if spec.names != nil {
		return " or names like " + firstKeys(spec.names)
	}
	return ""
}

func firstKeys(m map[string]int) string {
	keys := make([]string, 0, 3)
	for k := range m {
		keys = append(keys, k)
		if len(keys) == 3 {
			break
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

// compiled is a parsed cron schedule ready for iteration.
type compiled struct {
	minutes []int
	hours   []int
	doms    []int
	months  []int
	dows    []int
	domWild bool
	dowWild bool
}

// compile re-parses a canonical cron Schedule into iteration sets. Parse
// guarantees the expression compiles, but the error path stays honest for
// hand built Schedule values.
func compile(s Schedule) (*compiled, error) {
	fields := strings.Fields(s.Expr)
	specs := cronFields()
	if s.Kind != KindCron || len(fields) != len(specs) {
		return nil, fmt.Errorf("schedule %q is not a cron expression: rebuild it with cronx.Parse", s.Expr)
	}
	c := &compiled{}
	sets := make([][]int, len(specs))
	for i, spec := range specs {
		set, err := parseFieldSet(fields[i], spec)
		if err != nil {
			return nil, fmt.Errorf("schedule %q no longer parses: %v", s.Expr, err)
		}
		sets[i] = set
	}
	c.minutes, c.hours, c.doms, c.months = sets[0], sets[1], sets[2], sets[3]
	c.dows = sets[4]
	c.domWild = len(c.doms) == 31
	c.dowWild = len(c.dows) == 7
	return c, nil
}
