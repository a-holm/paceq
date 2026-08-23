package sensor

import (
	"fmt"
	"os"
	"sort"
	"strconv"
)

// The baseline names are the only daemon variables a sensor can see without
// declaring them in its own env: a deny by default rule. PATH is replaced
// with a fixed search path so a developer's shell does not leak into the
// sensor; the rest are the small set a process needs to run at all.
var baselineNames = []string{"HOME", "TZ", "LANG"}

// DefaultPath is the fixed PATH every subprocess gets. It is never inherited,
// matching the step contract in internal/runner.
const DefaultPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

// contractKeys are the PACEQ_ variables this package sets. They are reserved:
// a sensor's declared env may not override them, so the contract always wins.
var contractKeys = map[string]bool{
	"PACEQ_SENSOR":       true,
	"PACEQ_JOB":          true,
	"PACEQ_CURSOR":       true,
	"PACEQ_MAX_TRIGGERS": true,
	"PACEQ_DRY_RUN":      true,
}

// buildEnv assembles the sensor's environment in layer order: baseline, then
// the contract keys, then the sensor's declared env. A later layer replaces an
// earlier one key by key, but the declared env may never touch a contract key.
// The output is sorted so a golden test can compare it byte for byte.
func buildEnv(s Spec, in Input) ([]string, error) {
	out := map[string]string{
		"PATH": DefaultPath,
	}
	for _, name := range baselineNames {
		if v, ok := os.LookupEnv(name); ok {
			out[name] = v
		}
	}

	contract := map[string]string{
		"PACEQ_SENSOR":       in.Sensor,
		"PACEQ_JOB":          in.Job,
		"PACEQ_CURSOR":       cursorOrEmpty(in.Cursor), // the empty string is the no-cursor form
		"PACEQ_MAX_TRIGGERS": strconv.Itoa(in.MaxTriggers),
		"PACEQ_DRY_RUN":      boolMap[in.DryRun],
	}
	for k, v := range contract {
		out[k] = v
	}

	for k, v := range s.Env {
		if contractKeys[k] {
			return nil, fmt.Errorf("sensor %q env sets reserved key %s", s.Name, k)
		}
		out[k] = v
	}

	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, k := range keys {
		env = append(env, k+"="+out[k])
	}
	return env, nil
}

var boolMap = map[bool]string{false: "0", true: "1"}

func cursorOrEmpty(c *string) string {
	if c == nil {
		return ""
	}
	return *c
}
