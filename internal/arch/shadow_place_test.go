package arch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The shadow read placement (#32): the whole feature is trustworthy only
// while `shadow` stays a materialisation property. These greps hold the
// architecture together by naming every file that may consult it:
//
//   - the spec layer defines the YAML key (grammar, no behaviour);
//   - serve wires the process-wide switch into scheduler.Config and meta;
//   - the scheduler fills TickInput from its config or the row;
//   - the store acts on the flag inside MaterializeTick and owns every read
//     seam the report stands on;
//   - explain, status and the CLI mark surfaces and render reports.
//
// A new reader anywhere else - an executor checking "am I allowed to run?",
// a janitor pruning differently - would silently fork the product into two
// behaviours, so this test fails until widening the surface is an explicit
// edit here.
func TestShadowIsOnlyReadAtTheMaterialisationSeam(t *testing.T) {
	allowed := map[string]bool{
		filepath.Join("scheduler", "loop.go"):    true,
		filepath.Join("scheduler", "observe.go"): true,
		filepath.Join("store", "schedules.go"):   true,
		// Apply materialises the declared flag into the row. It makes no
		// decision with it; the decision stays at the materialisation seam.
		filepath.Join("store", "schedulesync.go"): true,
		filepath.Join("store", "shadow.go"):       true,
		filepath.Join("cli", "shadowcmd.go"):      true,
		filepath.Join("cli", "statuscmd.go"):      true,
		filepath.Join("cli", "servecmd.go"):       true,
		filepath.Join("cli", "root.go"):           true,
		// The import next-steps hint quotes the future flag; no decision.
		filepath.Join("cli", "importcmd.go"): true,
		// Cutover's help quotes the shadow fence in its safety story; the
		// decision itself goes through the explain report engine.
		filepath.Join("cli", "cutover.go"):         true,
		filepath.Join("status", "build.go"):        true,
		filepath.Join("status", "report.go"):       true,
		filepath.Join("explain", "explain.go"):     true,
		filepath.Join("explain", "report.go"):      true,
		filepath.Join("explain", "render_text.go"): true,
		filepath.Join("explain", "shadow.go"):      true,
		filepath.Join("daemon", "config.go"):       true,
		filepath.Join("daemon", "serve.go"):        true,
		filepath.Join("spec", "spec.go"):           true,
		filepath.Join("spec", "decode.go"):         true,
		filepath.Join("spec", "ir.go"):             true,
		filepath.Join("spec", "canonical.go"):      true,
	}

	hits := []string{}
	root := ".."
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "testdata", "deploy", "test":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		clean, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return err
		}
		clean = strings.TrimPrefix(clean, "internal"+string(filepath.Separator))
		if strings.HasSuffix(clean, "_test.go") ||
			strings.HasPrefix(clean, "arch"+string(filepath.Separator)) {
			return nil
		}
		body, err := os.ReadFile(path) // #nosec G304 - walked source tree, fixed root
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(body), "\n") {
			code := line
			if idx := strings.Index(code, "//"); idx >= 0 {
				code = code[:idx]
			}
			if !strings.Contains(strings.ToLower(code), "shadow") {
				continue
			}
			if allowed[clean] {
				continue
			}
			hits = append(hits, clean+":"+itoa(i+1)+": "+strings.TrimSpace(line))
			break
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(hits) > 0 {
		t.Fatalf("shadow was consulted outside the materialisation seam:\n%s",
			strings.Join(hits, "\n"))
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
