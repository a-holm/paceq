package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"unicode/utf8"
)

// The canonical form of a job (03 section 3.2). The rules are:
//
//   - Object keys sorted, by byte order, at every level.
//   - Durations as whole milliseconds, in a field named for the unit.
//   - Defaults materialised, so an omitted field and its default are the same
//     document and therefore the same hash.
//   - Empty collections left out, so an empty map and a missing map are the
//     same document too.
//   - No whitespace between tokens, and no HTML escaping.
//
// The encoder is written out here rather than delegated to encoding/json for
// one reason: this output is hashed and stored, so it has to be a decision
// rather than a consequence. encoding/json sorts map keys today; nothing
// promises what it does with a struct field order, an escape or a number
// format in a later Go release, and a job whose hash moved because the
// toolchain moved would read as a job that changed.

// canonicalValue is a node of the document being written.
type canonicalValue interface {
	encode(*bytes.Buffer)
}

type (
	canonicalText   string
	canonicalNumber int64
	canonicalFlag   bool
	canonicalArray  []canonicalValue
)

// canonicalMember is one field of an object. The object sorts them itself, so
// the code that builds one is free to list fields in whatever order reads best.
type canonicalMember struct {
	key   string
	value canonicalValue
}

type canonicalObject []canonicalMember

func (o canonicalObject) encode(b *bytes.Buffer) {
	members := append([]canonicalMember(nil), o...)
	sort.Slice(members, func(i, j int) bool { return members[i].key < members[j].key })

	b.WriteByte('{')
	for i, member := range members {
		if i > 0 {
			b.WriteByte(',')
		}
		encodeCanonicalString(b, member.key)
		b.WriteByte(':')
		member.value.encode(b)
	}
	b.WriteByte('}')
}

func (a canonicalArray) encode(b *bytes.Buffer) {
	b.WriteByte('[')
	for i, value := range a {
		if i > 0 {
			b.WriteByte(',')
		}
		value.encode(b)
	}
	b.WriteByte(']')
}

func (t canonicalText) encode(b *bytes.Buffer)   { encodeCanonicalString(b, string(t)) }
func (n canonicalNumber) encode(b *bytes.Buffer) { b.WriteString(strconv.FormatInt(int64(n), 10)) }

func (f canonicalFlag) encode(b *bytes.Buffer) {
	if f {
		b.WriteString("true")
		return
	}
	b.WriteString("false")
}

// hexDigits is used by the \u escape below, written out rather than taken from
// a format verb so the case of the digits is fixed here.
const hexDigits = "0123456789abcdef"

// encodeCanonicalString writes a JSON string with the minimum escaping RFC 8259
// requires and nothing else. <, > and & are left alone, which is what "no HTML
// escaping" means, and so are U+2028 and U+2029: this document is hashed and
// read by a JSON parser, never pasted into a script tag.
//
// A byte that is not valid UTF-8 becomes U+FFFD. It cannot arrive from a job
// file, which is refused unless it is UTF-8, but the encoder still has to be
// total: a hash function that panics on one input is a hash function with an
// input that has no hash.
func encodeCanonicalString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			i++
			switch c {
			case '"':
				b.WriteString(`\"`)
			case '\\':
				b.WriteString(`\\`)
			case '\b':
				b.WriteString(`\b`)
			case '\f':
				b.WriteString(`\f`)
			case '\n':
				b.WriteString(`\n`)
			case '\r':
				b.WriteString(`\r`)
			case '\t':
				b.WriteString(`\t`)
			default:
				if c < 0x20 {
					b.WriteString(`\u00`)
					b.WriteByte(hexDigits[c>>4])
					b.WriteByte(hexDigits[c&0xf])
					continue
				}
				b.WriteByte(c)
			}
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b.WriteString("�")
			i++
			continue
		}
		b.WriteString(s[i : i+size])
		i += size
	}
	b.WriteByte('"')
}

// Canonical is the job as the engine reads it: paceq.job.v1, with every default
// materialised and every key in a fixed place. Nothing about the result depends
// on map iteration order, on the order fields were written in the YAML file, or
// on the Go release that built the binary.
func Canonical(j *Job) []byte {
	var b bytes.Buffer
	canonicalJob(j).encode(&b)
	return b.Bytes()
}

// Hash is the spec_hash: sha256 over the canonical document, prefixed with the
// algorithm so a stored hash says what produced it.
func Hash(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Compile is the whole pipeline over a job that has already parsed: canonical
// bytes and the hash over them.
func Compile(j *Job) Hashed {
	canonical := Canonical(j)
	return Hashed{Job: j, Canonical: canonical, Hash: Hash(canonical)}
}

func canonicalJob(j *Job) canonicalObject {
	object := canonicalObject{
		{"schema", canonicalText(SchemaName)},
		{"name", canonicalText(j.Name)},
		{"timeout_ms", canonicalNumber(j.Timeout.Milliseconds())},
		{"max_concurrent", canonicalNumber(j.MaxConcurrent)},
		{"steps", canonicalSteps(j.Steps)},
	}
	// max_parallel is left out at the default, the same way overlap is: a
	// job written before the field existed keeps its hash, and a job that
	// says nothing means the default just as much as one that spells it out.
	if j.MaxParallel != DefaultMaxParallel {
		object = append(object, canonicalMember{"max_parallel", canonicalNumber(j.MaxParallel)})
	}
	if j.ConcurrencyKey != nil {
		object = append(object, canonicalMember{"concurrency_key", canonicalConcurrencyKey(j.ConcurrencyKey)})
	}
	// on_conflict is left out at the default, exactly like overlap above: an
	// explicit defer and no policy at all are one document and one hash.
	if j.OnConflict != "" && j.OnConflict != DefaultOnConflict {
		object = appendText(object, "on_conflict", j.OnConflict)
	}
	object = appendText(object, "description", j.Description)
	object = appendText(object, "env_file", j.EnvFile)
	object = appendText(object, "workdir", j.Workdir)
	if len(j.Env) > 0 {
		object = append(object, canonicalMember{"env", canonicalStringMap(j.Env)})
	}
	// inherit_env is a set of variable names, so its order carries no meaning
	// and is sorted away. Two files that list the same names differently are
	// the same job and get the same hash.
	if len(j.InheritEnv) > 0 {
		object = append(object, canonicalMember{"inherit_env", canonicalSortedStrings(j.InheritEnv)})
	}
	if len(j.Schedules) > 0 {
		object = append(object, canonicalMember{"schedules", canonicalSchedules(j.Schedules)})
	}
	if len(j.Sensors) > 0 {
		object = append(object, canonicalMember{"sensors", canonicalSensors(j.Sensors)})
	}
	return object
}

// canonicalConcurrencyKey writes the key in the form the IR reads back.
// Exactly one member rides along, because exactly one form is set.
func canonicalConcurrencyKey(k *ConcurrencyKey) canonicalObject {
	switch {
	case k.FromRunKey:
		return canonicalObject{{"from", canonicalText("run_key")}}
	case k.Param != "":
		return canonicalObject{{"param", canonicalText(k.Param)}}
	default:
		return canonicalObject{{"constant", canonicalText(k.Constant)}}
	}
}

func canonicalSteps(steps []Step) canonicalArray {
	out := make(canonicalArray, 0, len(steps))
	for _, step := range steps {
		object := canonicalObject{
			{"name", canonicalText(step.Name)},
			{"run", canonicalStrings(step.Run)},
			{"shell", canonicalFlag(step.Shell)},
		}
		object = appendText(object, "workdir", step.Workdir)
		// A step without its own timeout is bounded by the job's, which is
		// already in the document. Materialising the job's value here would
		// claim a per step ceiling the job never asked for.
		if step.Timeout > 0 {
			object = append(object, canonicalMember{"timeout_ms", canonicalNumber(step.Timeout.Milliseconds())})
		}
		if step.Retry != nil {
			object = append(object, canonicalMember{"retry", canonicalRetry(step.Retry)})
		}
		// needs is the edge set of the DAG M4-01 builds. A set has no order.
		if len(step.Needs) > 0 {
			object = append(object, canonicalMember{"needs", canonicalSortedStrings(step.Needs)})
		}
		out = append(out, object)
	}
	return out
}

func canonicalRetry(r *Retry) canonicalObject {
	return canonicalObject{
		{"max", canonicalNumber(r.Max)},
		{"backoff", canonicalText(r.Backoff)},
		{"initial_ms", canonicalNumber(r.Initial.Milliseconds())},
		{"max_delay_ms", canonicalNumber(r.MaxDelay.Milliseconds())},
		{"jitter", canonicalText(r.Jitter)},
	}
}

func canonicalSchedules(schedules []Schedule) canonicalArray {
	out := make(canonicalArray, 0, len(schedules))
	for _, schedule := range schedules {
		object := canonicalObject{
			{"name", canonicalText(schedule.Name)},
			{"cron", canonicalText(schedule.Cron)},
			{"timezone", canonicalText(schedule.Timezone)},
		}
		// overlap is left out at the default, so every schedule written
		// before the key existed keeps its hash. A schedule that says
		// nothing overlaps like skip, in the file and in the document.
		if schedule.Overlap != "" && schedule.Overlap != OverlapSkip {
			object = append(object, canonicalMember{"overlap", canonicalText(schedule.Overlap)})
		}
		out = append(out, object)
	}
	return out
}

func canonicalSensors(sensors []Sensor) canonicalArray {
	out := make(canonicalArray, 0, len(sensors))
	for _, sensor := range sensors {
		object := canonicalObject{
			{"name", canonicalText(sensor.Name)},
			{"kind", canonicalText(sensor.Kind)},
			{"run", canonicalStrings(sensor.Run)},
			{"interval_ms", canonicalNumber(sensor.Interval.Milliseconds())},
			{"min_interval_ms", canonicalNumber(sensor.MinInterval.Milliseconds())},
			{"timeout_ms", canonicalNumber(sensor.Timeout.Milliseconds())},
			{"max_triggers_per_tick", canonicalNumber(sensor.MaxTriggersPerTick)},
			{"paused", canonicalFlag(sensor.Paused)},
		}
		object = appendText(object, "workdir", sensor.Workdir)
		if len(sensor.Env) > 0 {
			object = append(object, canonicalMember{"env", canonicalStringMap(sensor.Env)})
		}
		object = appendText(object, "description", sensor.Description)
		out = append(out, object)
	}
	return out
}

func appendText(object canonicalObject, key, value string) canonicalObject {
	if value == "" {
		return object
	}
	return append(object, canonicalMember{key, canonicalText(value)})
}

// canonicalStrings keeps the order it is given. It is for argv, where the order
// is the meaning.
func canonicalStrings(values []string) canonicalArray {
	out := make(canonicalArray, 0, len(values))
	for _, value := range values {
		out = append(out, canonicalText(value))
	}
	return out
}

// canonicalSortedStrings is for the fields that are sets rather than lists.
func canonicalSortedStrings(values []string) canonicalArray {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return canonicalStrings(sorted)
}

func canonicalStringMap(values map[string]string) canonicalObject {
	object := make(canonicalObject, 0, len(values))
	for key, value := range values {
		object = append(object, canonicalMember{key, canonicalText(value)})
	}
	// The object sorts itself when it is encoded, so the range above may hand
	// the members over in whatever order the runtime picked.
	return object
}
