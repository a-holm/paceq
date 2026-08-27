package notify

import (
	"os"
	"strings"
)

// BaseEnv builds a fresh environment from an empty baseline plus the named
// variables of the parent process, then appends the extras. Deny by default:
// anything an inheritor receives is there because configuration said so,
// which keeps secrets like tokens and proxy credentials out of every
// notifier that did not ask for them (08 section 3.2). Output is
// deterministic: inherited names in listed order, extras sorted.
func BaseEnv(inherit []string, extras map[string]string) []string {
	env := make([]string, 0, len(inherit)+len(extras))
	for _, name := range inherit {
		if envNameOK(name) {
			if v, ok := os.LookupEnv(name); ok && !strings.Contains(v, "\n") {
				env = append(env, name+"="+v)
			}
		}
	}
	for _, name := range stableKeys(extras) {
		if envNameOK(name) {
			env = append(env, name+"="+extras[name])
		}
	}
	return env
}

// stableKeys gives BaseEnv deterministic output for tests and logs.
func stableKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	return keys
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// envNameOK re-checks names defensively: an entry with '=' or a newline in it
// cannot be passed to execve at all.
func envNameOK(name string) bool {
	return name != "" &&
		!strings.Contains(name, "=") &&
		!strings.Contains(name, "\n") &&
		!strings.ContainsRune(name, 0)
}
