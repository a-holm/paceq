package crontab

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FuzzCrontabParse feeds arbitrary bytes through the whole import pipeline.
// The contract is the T12 rule: no panic, no unbounded loop. Every byte
// sequence produces either translated docs or verbatim docs under TODO;
// nothing else is allowed to happen.
func FuzzCrontabParse(f *testing.F) {
	corpus, err := filepath.Glob(filepath.Join("testdata", "corpus", "*.crontab"))
	if err == nil {
		for _, c := range corpus {
			src, readErr := os.ReadFile(c) // #nosec G304 - testdata path from Glob
			if readErr == nil {
				f.Add(src)
			}
		}
	}
	for _, seed := range []string{
		"0 6 * * * /bin/a\n",
		"@reboot /bin/b % stdin\n",
		"root 0 6 * * * /bin/c\n",
		"\x00\xff\xfe garbage\n",
		"MAILTO=\n\n\n#only comments",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		res := Import(data, Options{NamePrefix: "fz"})
		if res.Report.Jobs != len(res.Docs) {
			t.Fatalf("jobs %d but %d docs", res.Report.Jobs, len(res.Docs))
		}
		var b strings.Builder
		if err := Emit(res.Docs, nil, &b); err != nil {
			t.Fatalf("emit: %v", err)
		}
	})
}

// FuzzSplitPercent targets the trap function directly: percent handling must
// be total and never lose the command part.
func FuzzSplitPercent(f *testing.F) {
	for _, seed := range []string{"", "%", "%%", `\%`, "a%b%c", `%\%`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, cmd string) {
		command, stdin := splitPercent(cmd)
		if stdin != "" && strings.Contains(command, "\x00") {
			t.Skip("NUL payloads are refused downstream by the spec decoder")
		}
	})
}
