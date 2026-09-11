package arch

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// shadowRole is why one file is allowed to spell shadow at all. The role is the
// argument for the entry, and only one of them changes what happens: a file
// added under any other role has to be true to it.
type shadowRole string

const (
	// roleGrammar defines the YAML keys and canonicalises them. No behaviour.
	roleGrammar shadowRole = "grammar"
	// roleCarries moves the declared flag from a parsed file to the store
	// input. It reads no schedule, compares nothing and decides nothing.
	roleCarries shadowRole = "carries the declaration"
	// roleResolves folds the job-level flag and the per-schedule flag into
	// schedules.shadow, so one row answers for the whole job.
	roleResolves shadowRole = "resolves the declaration into the row"
	// roleDecides acts on the flag. This is the materialisation seam and the
	// loop that feeds it, and it is the only role that changes what happens.
	roleDecides shadowRole = "decides at the materialisation seam"
	// roleWires puts the process-wide switch from the command line into
	// scheduler.Config and the meta table.
	roleWires shadowRole = "wires the instance switch"
	// roleReports marks and renders what the row and the tick rows already
	// say, and gathers the observations the report stands on.
	roleReports shadowRole = "reports"
	// roleQuotes names the flag in help or hint text and nowhere else.
	roleQuotes shadowRole = "quotes it in text"
)

// The shadow placement rule (#32, #203): the feature is trustworthy only while
// every surface gives one answer to "does this fire-time execute?". There is
// one decision, taken at the materialisation seam from schedules.shadow and the
// instance switch; the row carries the whole declaration because apply resolved
// it there; and every other file named below only defines the key, carries it,
// wires the switch, prints it or quotes it.
//
// A file that decides under any other name - an executor asking "am I allowed
// to run?", a janitor pruning differently, a reader that consults the frozen
// spec instead of the row - forks the product into two behaviours, and the
// worse half is the one that tells an operator a shadowed job is running. So
// this grep fails until widening the surface is an explicit edit here, with a
// role beside the file.
//
// The list is also self-pruning: an entry whose file has stopped spelling
// shadow is a standing permission nobody argued for, so it fails too.
var shadowPlaces = map[string]shadowRole{
	filepath.Join("spec", "spec.go"):      roleGrammar,
	filepath.Join("spec", "decode.go"):    roleGrammar,
	filepath.Join("spec", "ir.go"):        roleGrammar,
	filepath.Join("spec", "canonical.go"): roleGrammar,

	// applycmd turns a parsed file into JobVersionInput, which is where the
	// job's own declaration crosses into the store; jobs.go is the field it
	// crosses on. Neither reads a schedule row or compares anything.
	filepath.Join("cli", "applycmd.go"): roleCarries,
	filepath.Join("store", "jobs.go"):   roleCarries,

	filepath.Join("store", "schedulesync.go"): roleResolves,

	filepath.Join("scheduler", "loop.go"):      roleDecides,
	filepath.Join("store", "schedules.go"):     roleDecides,
	filepath.Join("store", "shadow.go"):        roleDecides,
	filepath.Join("cli", "servecmd.go"):        roleWires,
	filepath.Join("daemon", "config.go"):       roleWires,
	filepath.Join("daemon", "serve.go"):        roleWires,
	filepath.Join("scheduler", "observe.go"):   roleReports,
	filepath.Join("cli", "shadowcmd.go"):       roleReports,
	filepath.Join("cli", "statuscmd.go"):       roleReports,
	filepath.Join("cli", "root.go"):            roleReports,
	filepath.Join("status", "build.go"):        roleReports,
	filepath.Join("status", "report.go"):       roleReports,
	filepath.Join("explain", "explain.go"):     roleReports,
	filepath.Join("explain", "report.go"):      roleReports,
	filepath.Join("explain", "render_text.go"): roleReports,
	filepath.Join("explain", "shadow.go"):      roleReports,

	// The import next-steps hint quotes the future flag; cutover's help
	// quotes the shadow fence in its safety story. The decision behind both
	// goes through the explain report engine.
	filepath.Join("cli", "importcmd.go"): roleQuotes,
	filepath.Join("cli", "cutover.go"):   roleQuotes,
}

func TestShadowIsDecidedOnlyAtTheMaterialisationSeam(t *testing.T) {
	hits := []string{}
	spells := map[string]bool{}
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
			spells[clean] = true
			if _, ok := shadowPlaces[clean]; !ok {
				hits = append(hits, clean+":"+itoa(i+1)+": "+strings.TrimSpace(line))
			}
			break
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(hits) > 0 {
		t.Errorf("shadow is spelled outside the placement rule:\n%s\n\nname each file in %s with the role it plays: %s",
			strings.Join(hits, "\n"), "shadow_place_test.go", strings.Join(shadowRoles(), ", "))
	}

	var quiet []string
	for file := range shadowPlaces {
		if !spells[file] {
			quiet = append(quiet, file)
		}
	}
	sort.Strings(quiet)
	if len(quiet) > 0 {
		t.Errorf("the placement rule names files that no longer spell shadow:\n%s\n\nan entry nobody needs is permission for a reader nobody argued for; delete it",
			strings.Join(quiet, "\n"))
	}
}

// shadowRoles lists the roles an entry may claim, for the failure message.
func shadowRoles() []string {
	seen := map[string]bool{}
	for _, role := range shadowPlaces {
		seen[string(role)] = true
	}
	roles := make([]string, 0, len(seen))
	for role := range seen {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	return roles
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
