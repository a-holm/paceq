package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// FromIR reads a canonical document back into a Job. The engine materialises
// every run from these bytes, which are frozen in job_versions.spec_json, so
// this function is what makes a run independent of the file it was applied
// from: the version row is immutable, and reading it back through here yields
// exactly the steps, timeouts and edges the hash was taken over.
//
// The decoder is strict about shape rather than permissive: a key the v1
// writer never emits is a corrupted or future document, and running half of
// one would be worse than refusing it. Every refusal names the key path.
func FromIR(data []byte) (*Job, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var root any
	if err := dec.Decode(&root); err != nil {
		return nil, fmt.Errorf("parse the canonical document: %w", err)
	}
	object, ok := root.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the canonical document is %T, want an object", root)
	}

	j := &Job{MaxConcurrent: DefaultMaxConcurrent, MaxParallel: DefaultMaxParallel}
	seenSchema, seenName := false, false
	err := eachMember(object, func(key string, val any) error {
		var err error
		switch key {
		case "schema":
			name, err := text(val, "schema")
			if err != nil {
				return err
			}
			if name != SchemaName {
				return fmt.Errorf("schema is %q, want %q: the bytes are not a v1 document", name, SchemaName)
			}
			seenSchema = true
		case "name":
			if j.Name, err = text(val, "name"); err != nil {
				return err
			}
			seenName = true
		case "description":
			if j.Description, err = text(val, "description"); err != nil {
				return err
			}
		case "env_file":
			if j.EnvFile, err = text(val, "env_file"); err != nil {
				return err
			}
		case "workdir":
			if j.Workdir, err = text(val, "workdir"); err != nil {
				return err
			}
		case "timeout_ms":
			ms, err := milliseconds(val, "timeout_ms")
			if err != nil {
				return err
			}
			j.Timeout = time.Duration(ms) * time.Millisecond
		case "max_concurrent":
			n, err := wholeNumber(val, "max_concurrent")
			if err != nil {
				return err
			}
			j.MaxConcurrent = int(n)
		case "max_parallel":
			n, err := wholeNumber(val, "max_parallel")
			if err != nil {
				return err
			}
			j.MaxParallel = int(n)
		case "concurrency_key":
			j.ConcurrencyKey, err = irConcurrencyKey(val)
			if err != nil {
				return err
			}
		case "on_conflict":
			s, err := text(val, "on_conflict")
			if err != nil {
				return err
			}
			if s != OnConflictDefer && s != OnConflictSkip {
				return fmt.Errorf("on_conflict is %q, want %q or %q", s, OnConflictDefer, OnConflictSkip)
			}
			j.OnConflict = s
		case "env":
			if j.Env, err = textMap(val, "env"); err != nil {
				return err
			}
		case "inherit_env":
			if j.InheritEnv, err = textList(val, "inherit_env"); err != nil {
				return err
			}
		case "steps":
			if j.Steps, err = irSteps(val); err != nil {
				return err
			}
		case "schedules":
			if j.Schedules, err = irSchedules(val); err != nil {
				return err
			}
		case "sensors":
			if j.Sensors, err = irSensors(val); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected key %q: the document is not a paceq.job.v1 encoding", key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if !seenSchema {
		return nil, fmt.Errorf("the document carries no schema name, want %q", SchemaName)
	}
	if !seenName {
		return nil, fmt.Errorf("the document carries no name")
	}
	return j, nil
}

// irSteps reads the steps array. Order is preserved as written: argv order and
// step order are meaning, not presentation.
func irSteps(val any) ([]Step, error) {
	items, err := array(val, "steps")
	if err != nil {
		return nil, err
	}
	steps := make([]Step, 0, len(items))
	for i, item := range items {
		where := fmt.Sprintf("steps[%d]", i)
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s is %T, want an object", where, item)
		}
		step := Step{}
		err := eachMember(object, func(key string, val any) error {
			var err error
			switch key {
			case "name":
				step.Name, err = text(val, where+".name")
			case "run":
				step.Run, err = textList(val, where+".run")
			case "shell":
				step.Shell, err = flag(val, where+".shell")
			case "workdir":
				step.Workdir, err = text(val, where+".workdir")
			case "timeout_ms":
				var ms int64
				ms, err = milliseconds(val, where+".timeout_ms")
				step.Timeout = time.Duration(ms) * time.Millisecond
			case "retry":
				step.Retry, err = irRetry(val, where+".retry")
			case "needs":
				step.Needs, err = textList(val, where+".needs")
			default:
				err = fmt.Errorf("unexpected key %q in %s", key, where)
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, nil
}

func irRetry(val any, where string) (*Retry, error) {
	object, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s is %T, want an object", where, val)
	}
	r := &Retry{}
	err := eachMember(object, func(key string, val any) error {
		var err error
		switch key {
		case "max":
			var n int64
			n, err = wholeNumber(val, where+".max")
			r.Max = int(n)
		case "backoff":
			r.Backoff, err = text(val, where+".backoff")
		case "initial_ms":
			var ms int64
			ms, err = milliseconds(val, where+".initial_ms")
			r.Initial = time.Duration(ms) * time.Millisecond
		case "max_delay_ms":
			var ms int64
			ms, err = milliseconds(val, where+".max_delay_ms")
			r.MaxDelay = time.Duration(ms) * time.Millisecond
		case "jitter":
			r.Jitter, err = text(val, where+".jitter")
		default:
			err = fmt.Errorf("unexpected key %q in %s", key, where)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return r, nil
}

func irSchedules(val any) ([]Schedule, error) {
	items, err := array(val, "schedules")
	if err != nil {
		return nil, err
	}
	out := make([]Schedule, 0, len(items))
	for i, item := range items {
		where := fmt.Sprintf("schedules[%d]", i)
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s is %T, want an object", where, item)
		}
		s := Schedule{}
		err := eachMember(object, func(key string, val any) error {
			var err error
			switch key {
			case "name":
				s.Name, err = text(val, where+".name")
			case "cron":
				s.Cron, err = text(val, where+".cron")
			case "timezone":
				s.Timezone, err = text(val, where+".timezone")
			case "overlap":
				s.Overlap, err = irOverlap(val, where)
			default:
				err = fmt.Errorf("unexpected key %q in %s", key, where)
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// irOverlap reads the overlap policy back. The canonical writer emits the key
// only when the policy is not the default, so an old document without the key
// reads as skip and a future one cannot smuggle in another value.
func irOverlap(val any, where string) (string, error) {
	s, err := text(val, where+".overlap")
	if err != nil {
		return "", err
	}
	if s != OverlapSkip && s != OverlapQueue {
		return "", fmt.Errorf("%s.overlap is %q, want %q or %q", where, s, OverlapSkip, OverlapQueue)
	}
	return s, nil
}

func irSensors(val any) ([]Sensor, error) {
	items, err := array(val, "sensors")
	if err != nil {
		return nil, err
	}
	out := make([]Sensor, 0, len(items))
	for i, item := range items {
		where := fmt.Sprintf("sensors[%d]", i)
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s is %T, want an object", where, item)
		}
		s := Sensor{Kind: DefaultSensorKind}
		err := eachMember(object, func(key string, val any) error {
			var err error
			switch key {
			case "name":
				s.Name, err = text(val, where+".name")
			case "kind":
				s.Kind, err = irSensorKind(val, where)
			case "run":
				s.Run, err = irSensorRun(val, where)
			case "workdir":
				s.Workdir, err = text(val, where+".workdir")
			case "env":
				s.Env, err = textMap(val, where+".env")
			case "interval_ms":
				var ms int64
				ms, err = milliseconds(val, where+".interval_ms")
				s.Interval = time.Duration(ms) * time.Millisecond
			case "min_interval_ms":
				var ms int64
				ms, err = milliseconds(val, where+".min_interval_ms")
				s.MinInterval = time.Duration(ms) * time.Millisecond
			case "timeout_ms":
				var ms int64
				ms, err = milliseconds(val, where+".timeout_ms")
				s.Timeout = time.Duration(ms) * time.Millisecond
			case "max_triggers_per_tick":
				var n int64
				n, err = wholeNumber(val, where+".max_triggers_per_tick")
				s.MaxTriggersPerTick = int(n)
			case "paused":
				s.Paused, err = flag(val, where+".paused")
			case "description":
				s.Description, err = text(val, where+".description")
			default:
				err = fmt.Errorf("unexpected key %q in %s", key, where)
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		if s.Name == "" {
			return nil, fmt.Errorf("%s carries no name", where)
		}
		if len(s.Run) == 0 {
			return nil, fmt.Errorf("%s carries no run", where)
		}
		out = append(out, s)
	}
	return out, nil
}

// irSensorKind reads the kind back. The canonical writer always emits it, and
// anything but exec is a future document a v1 runtime has no behaviour for.
func irSensorKind(val any, where string) (string, error) {
	s, err := text(val, where+".kind")
	if err != nil {
		return "", err
	}
	if s != DefaultSensorKind {
		return "", fmt.Errorf("%s.kind is %q, want %q: this document was written for a later runtime", where, s, DefaultSensorKind)
	}
	return s, nil
}

// irSensorRun reads argv back. Order is the meaning, and an empty element is
// a corrupt document: nothing the v1 writer emits can contain one.
func irSensorRun(val any, where string) ([]string, error) {
	items, err := textList(val, where+".run")
	if err != nil {
		return nil, err
	}
	for i, item := range items {
		if item == "" {
			return nil, fmt.Errorf("%s.run[%d] is empty, want a command or an argument", where, i)
		}
	}
	return items, nil
}

// irConcurrencyKey reads the key back. The closed grammar the file decoder
// accepts is accepted here, and nothing else: these bytes are frozen versions,
// so a shape this runtime never wrote is corruption, not syntax.
func irConcurrencyKey(val any) (*ConcurrencyKey, error) {
	const where = "concurrency_key"
	if s, ok := val.(string); ok {
		if strings.Contains(s, "{{") {
			return nil, fmt.Errorf("%s is %q, and templating in key values does not exist", where, s)
		}
		if len(s) > MaxConcurrencyKeyLength {
			return nil, fmt.Errorf("%s is %d characters, want at most %d", where, len(s), MaxConcurrencyKeyLength)
		}
		if s == "" {
			return nil, fmt.Errorf("%s is an empty string", where)
		}
		return &ConcurrencyKey{Constant: s}, nil
	}
	object, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s is %T, want a string or an object", where, val)
	}
	k := &ConcurrencyKey{}
	err := eachMember(object, func(key string, v any) error {
		switch key {
		case "constant":
			s, err := text(v, where+".constant")
			if err != nil {
				return err
			}
			if s == "" {
				return fmt.Errorf("%s.constant is an empty string", where)
			}
			if len(s) > MaxConcurrencyKeyLength {
				return fmt.Errorf("%s.constant is %d characters, want at most %d", where, len(s), MaxConcurrencyKeyLength)
			}
			k.Constant = s
		case "param":
			s, err := text(v, where+".param")
			if err != nil {
				return err
			}
			if s == "" {
				return fmt.Errorf("%s.param is empty, want a parameter name", where)
			}
			k.Param = s
		case "from":
			s, err := text(v, where+".from")
			if err != nil {
				return err
			}
			if s != "run_key" {
				return fmt.Errorf("%s.from is %q, want %q", where, s, "run_key")
			}
			k.FromRunKey = true
		default:
			return fmt.Errorf("unexpected key %q in %s", key, where)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	forms := 0
	if k.Constant != "" {
		forms++
	}
	if k.Param != "" {
		forms++
	}
	if k.FromRunKey {
		forms++
	}
	if forms != 1 {
		return nil, fmt.Errorf("%s carries exactly one of constant, param or from", where)
	}
	return k, nil
}

// eachMember walks an object's members once. It exists so every reader above
// reads the same way and no key can be visited twice by accident.
func eachMember(object map[string]any, visit func(key string, val any) error) error {
	for key, val := range object {
		if err := visit(key, val); err != nil {
			return err
		}
	}
	return nil
}

func text(val any, where string) (string, error) {
	s, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%s is %T, want a string", where, val)
	}
	return s, nil
}

func flag(val any, where string) (bool, error) {
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("%s is %T, want true or false", where, val)
	}
	return b, nil
}

func wholeNumber(val any, where string) (int64, error) {
	n, ok := val.(json.Number)
	if !ok {
		return 0, fmt.Errorf("%s is %T, want a number", where, val)
	}
	var out int64
	if _, err := fmt.Sscanf(string(n), "%d", &out); err != nil {
		return 0, fmt.Errorf("%s is %q, want a whole number of milliseconds", where, n)
	}
	return out, nil
}

// milliseconds is wholeNumber with the unit in the message, which is the only
// difference that matters to whoever reads the error.
func milliseconds(val any, where string) (int64, error) {
	return wholeNumber(val, where)
}

func array(val any, where string) ([]any, error) {
	list, ok := val.([]any)
	if !ok {
		return nil, fmt.Errorf("%s is %T, want an array", where, val)
	}
	return list, nil
}

func textList(val any, where string) ([]string, error) {
	items, err := array(val, where)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for i, item := range items {
		s, err := text(item, fmt.Sprintf("%s[%d]", where, i))
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func textMap(val any, where string) (map[string]string, error) {
	object, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s is %T, want an object", where, val)
	}
	out := make(map[string]string, len(object))
	for key, item := range object {
		s, err := text(item, where+"."+key)
		if err != nil {
			return nil, err
		}
		out[key] = s
	}
	return out, nil
}
