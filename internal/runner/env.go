package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultPath is the fixed PATH every job starts from. It is never inherited:
// a daemon started from a developer shell must not hand its search path to a
// production job.
const DefaultPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

// paceqPrefix is reserved for the context contract. No user layer may set it.
const paceqPrefix = "PACEQ_"

// baselineNames are the only variables allowed through from the runner's own
// environment without an explicit inherit_env entry, and PATH is replaced
// even for them. Everything else is denied by default.
var baselineNames = []string{"HOME", "TZ", "LANG"}

// buildEnv assembles the child environment in the documented layer order:
// baseline, then the PACEQ_ context, then inherit_env, then env_file, then
// the job env. A later layer replaces an earlier one key by key; the output
// is sorted so runs are reproducible byte for byte.
func buildEnv(s Spec, workdir string) ([]string, error) {
	out := map[string]string{}

	out["PATH"] = DefaultPath
	for _, name := range baselineNames {
		if v, ok := os.LookupEnv(name); ok {
			out[name] = v
		}
	}

	key := idempotencyKey(s.Ctx.RunID, s.Ctx.Step)
	params := "{}"
	if len(s.Ctx.Params) > 0 {
		raw, err := json.Marshal(s.Ctx.Params)
		if err != nil {
			return nil, fmt.Errorf("encode params: %w", err)
		}
		params = string(raw)
	}
	scheduledFor := ""
	if !s.Ctx.ScheduledFor.IsZero() {
		scheduledFor = s.Ctx.ScheduledFor.UTC().Format(time.RFC3339)
	}

	// The inputs contract (#13): the merged upstream references travel
	// inline while they are small, and as a file beside the run's other
	// attempt files once they crossed 128 KiB, with PACEQ_INPUTS set to
	// the literal null so a jq pipeline still reads.
	inputs := s.InputsJSON
	if inputs == "" {
		inputs = "{}"
	}
	inputsFile := s.InputsFile
	if inputsFile != "" {
		inputs = "null"
	}
	contextVars := map[string]string{
		"PACEQ_RUN_ID":          s.Ctx.RunID,
		"PACEQ_JOB":             s.Ctx.Job,
		"PACEQ_STEP":            s.Ctx.Step,
		"PACEQ_ATTEMPT":         strconv.Itoa(s.Ctx.Attempt),
		"PACEQ_RUN_KEY":         s.Ctx.RunKey,
		"PACEQ_IDEMPOTENCY_KEY": key,
		"PACEQ_SCHEDULED_FOR":   scheduledFor,
		"PACEQ_PARAMS":          params,
		"PACEQ_OUTPUT":          s.OutputPath,
		"PACEQ_INPUTS":          inputs,
	}
	if inputsFile != "" {
		contextVars["PACEQ_INPUTS_FILE"] = inputsFile
	}
	for k, v := range contextVars {
		out[k] = v
	}

	for _, name := range s.InheritEnv {
		if strings.HasPrefix(name, paceqPrefix) {
			return nil, fmt.Errorf("inherit_env lists reserved key %s", name)
		}
		if v, ok := os.LookupEnv(name); ok {
			out[name] = v
		}
	}

	if s.EnvFile != "" {
		vars, err := loadEnvFile(workdir, s.EnvFile)
		if err != nil {
			return nil, err
		}
		for k, v := range vars {
			out[k] = v
		}
	}

	for k, v := range s.Env {
		if strings.HasPrefix(k, paceqPrefix) {
			return nil, fmt.Errorf("job env sets reserved key %s", k)
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

// idempotencyKey is the documented contract: sha256 of run id and step name,
// first 32 hex characters. It is stable across retries and duplicates of the
// same step in the same run, which is what makes at-least-once delivery safe
// for a downstream INSERT ON CONFLICT.
func idempotencyKey(runID, step string) string {
	sum := sha256.Sum256([]byte(runID + ":" + step))
	return hex.EncodeToString(sum[:])[:32]
}

// parseEnvFile reads one KEY=VALUE file directly. Absolute spec paths resolve
// through the filesystem root so every component, symlinks included, is
// resolved by the kernel rather than trusted. The mode rule applies here too:
// exactly 0600, checked on the open handle so a race cannot slip a looser
// file through.
func parseEnvFile(path string) (map[string]string, error) {
	f, err := openAtFilesystemRoot(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return parseEnvChecked(info, data, filepath.Base(path))
}

// parseEnvChecked enforces regular file and exact 0600 mode, then parses.
// The check reads the already open handle, so there is no window in which a
// chmod can slip past it.
func parseEnvChecked(info os.FileInfo, data []byte, name string) (map[string]string, error) {
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", name)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		return nil, fmt.Errorf("%s has mode %o, refuse anything looser than 0600", name, perm)
	}
	return parseEnvBytes(data, name)
}

// parseEnvBytes parses KEY=VALUE lines. Comments start with #, blank lines are
// skipped, CRLF is tolerated, values may contain equals signs, duplicate keys
// last wins. Anything else fails closed: this file feeds a process
// environment, and guessing what a malformed line meant is how secrets end up
// in the wrong variable.
func parseEnvBytes(data []byte, name string) (map[string]string, error) {
	out := map[string]string{}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, found := strings.Cut(line, "=")
		if !found || k == "" || strings.TrimSpace(k) != k || strings.ContainsAny(k, " 	") {
			return nil, fmt.Errorf("%s line %d: not KEY=VALUE: %q", name, i+1, line)
		}
		if strings.HasPrefix(k, paceqPrefix) {
			return nil, fmt.Errorf("%s line %d: sets reserved key %s", name, i+1, k)
		}
		out[k] = v
	}
	return out, nil
}
