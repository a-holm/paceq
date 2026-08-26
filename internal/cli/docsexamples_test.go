package cli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The documentation gates (#48). Prose rots silently, so these tests make
// four of its failure modes loud:
//
//  1. Examples that stopped working (TestDocsExamplesRun): every code block
//     marked <!-- run --> is executed against a freshly built binary, in a
//     throwaway directory, and must exit 0.
//  2. Links to nowhere (TestDocsRelativeLinks): every relative link resolves.
//  3. Terminology drift (TestNeverAdvertiseExactlyOnce): the one promise the
//     product never makes appears only in negations.
//  4. Language drift (TestUserFacingDocsAreEnglish): English everywhere
//     user-facing; Norwegian lives in the plan documents.
//
// A fifth, the README contract's one-screen budget, is
// TestREADMEFirstScreenBeforeFirstCommand. Like the generated-pages gates,
// all of them run under make test and therefore make ci.

const (
	readmeBudgetLines = 40 // one screen before the first command
	maxSecondsPerRun  = 120
)

// repoRoot walks up from the working directory to the directory holding
// go.mod. Tests run from the package directory; docs live at the root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the working directory")
		}
		dir = parent
	}
}

// runBlock is one executable slice of a document: the shell text and the
// regular expressions its combined output must match.
type runBlock struct {
	doc     string
	line    int
	script  string
	expects []*regexp.Regexp
}

// extractRunnableBlocks parses one markdown file for fenced bash/sh blocks
// whose preceding non-blank line is exactly <!-- run -->. An optional
// <!-- expect: RE --> comment line after the closing fence adds an output
// assertion. Display-only blocks are ignored: prose shows what it likes,
// runnable blocks answer for themselves.
func extractRunnableBlocks(t *testing.T, path string) []runBlock {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(string(raw), "\n")
	var blocks []runBlock
	for i := 0; i < len(lines); i++ {
		if !strings.HasPrefix(lines[i], "```bash") && !strings.HasPrefix(lines[i], "```sh") {
			continue
		}
		prev := i - 1
		for prev >= 0 && strings.TrimSpace(lines[prev]) == "" {
			prev--
		}
		if prev < 0 || strings.TrimSpace(lines[prev]) != "<!-- run -->" {
			continue
		}
		start := i + 1
		var body []string
		j := start
		for ; j < len(lines) && !strings.HasPrefix(lines[j], "```"); j++ {
			body = append(body, strings.TrimPrefix(lines[j], "$ "))
		}
		if j >= len(lines) {
			t.Fatalf("%s:%d: unterminated runnable code block", path, i+1)
		}
		block := runBlock{doc: filepath.Base(path), line: i + 1, script: strings.Join(body, "\n")}
		for k := j + 1; k < len(lines); k++ {
			trimmed := strings.TrimSpace(lines[k])
			if trimmed == "" {
				continue
			}
			if m := expectRE.FindStringSubmatch(trimmed); m != nil {
				block.expects = append(block.expects, regexp.MustCompile(m[1]))
				continue
			}
			break
		}
		blocks = append(blocks, block)
		i = j
	}
	return blocks
}

var expectRE = regexp.MustCompile(`^<!-- expect: (.+) -->$`)

// runnableDocs lists the documents whose marked blocks must run green, in the
// order a reader meets them.
var runnableDocs = []string{
	"README.md",
	"docs/tutorials/01-from-crontab.md",
	"docs/tutorials/02-first-sensor.md",
}

// TestDocsExamplesRun builds the real binary once and drives every marked
// block through it. Each document runs in its own throwaway directory and
// its blocks share state, because a tutorial is a story told in order.
func TestDocsExamplesRun(t *testing.T) {
	root := repoRoot(t)

	binDir := t.TempDir()
	bin := filepath.Join(binDir, "paceq")
	build := exec.Command("go", "build", "-o", bin, "./cmd/paceq")
	build.Dir = root
	build.Env = append(os.Environ(), "GOFLAGS=-mod=readonly -buildvcs=false", "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the docs binary: %v\n%s", err, out)
	}

	total := 0
	for _, rel := range runnableDocs {
		blocks := extractRunnableBlocks(t, filepath.Join(root, filepath.FromSlash(rel)))
		if len(blocks) == 0 {
			t.Errorf("%s carries no <!-- run --> blocks; either it has no examples or the markers rotted", rel)
			continue
		}
		total += len(blocks)
		dir := t.TempDir()
		for _, b := range blocks {
			block := b
			t.Run(block.doc+"/"+strconv.Itoa(block.line), func(t *testing.T) {
				runDocBlock(t, bin, dir, block)
			})
		}
	}
	if total < 6 {
		t.Fatalf("only %d runnable blocks across the docs; the example suite has thinned", total)
	}
}

func runDocBlock(t *testing.T, bin, dir string, b runBlock) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), maxSecondsPerRun*time.Second)
	defer cancel()

	script := filepath.Join(t.TempDir(), "block.sh")
	if err := os.WriteFile(script, []byte(b.script), 0o755); err != nil {
		t.Fatalf("write block script: %v", err)
	}
	cmd := exec.CommandContext(ctx, "/bin/sh", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PATH="+filepath.Dir(bin)+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("timed out after %ds\noutput:\n%s", maxSecondsPerRun, out)
	}
	if err != nil {
		t.Fatalf("example failed (%v)\nscript:\n%s\noutput:\n%s", err, b.script, out)
	}
	for _, want := range b.expects {
		if !want.Match(out) {
			t.Fatalf("output does not match %s\nscript:\n%s\noutput:\n%s", want, b.script, out)
		}
	}
}

// TestDocsRelativeLinks walks every checked-in markdown file below the repo
// root except docs/plans and testdata, extracts markdown links, and requires
// every relative target to exist. A moved page with a stale link is a broken
// promise to the next reader.
func TestDocsRelativeLinks(t *testing.T) {
	root := repoRoot(t)
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == ".paceq" || name == "plans" || name == "testdata" || name == "node_modules" || name == "bin" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	linkRE := regexp.MustCompile(`\[[^\]]*\]\(([^)\s]+)\)`)
	broken := 0
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		rel, _ := filepath.Rel(root, file)
		for _, m := range linkRE.FindAllStringSubmatch(string(raw), -1) {
			target := m[1]
			if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") ||
				strings.HasPrefix(target, "#") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			clean := target
			if i := strings.Index(clean, "#"); i >= 0 {
				clean = clean[:i]
			}
			if clean == "" {
				continue
			}
			full := filepath.Join(filepath.Dir(file), filepath.FromSlash(clean))
			if _, err := os.Stat(full); err != nil {
				broken++
				t.Errorf("%s: link target missing: %s", rel, target)
			}
		}
	}
	if broken == 0 && len(files) < 10 {
		t.Fatalf("only %d markdown files found; the walk saw too little to be believed", len(files))
	}
}

// TestNeverAdvertiseExactlyOnce is the terminology gate (09 R8, SYNTESE 4.11):
// the phrase may appear only inside a negation. A line that mentions
// exactly-once without saying "no", "not" or "never" somewhere on it fails -
// marketing copy is written one line at a time, and this catches the line.
func TestNeverAdvertiseExactlyOnce(t *testing.T) {
	root := repoRoot(t)
	phrase := regexp.MustCompile(`(?i)exactly[- ]once|garantert kun én gang`)
	negation := regexp.MustCompile(`(?i)\b(no|not|never|nor|nobody|nothing|without|refuse[sd]?)\b`)
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "plans" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	hits := 0
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		rel, _ := filepath.Rel(root, file)
		for i, line := range strings.Split(string(raw), "\n") {
			if phrase.MatchString(line) && !negation.MatchString(line) {
				hits++
				t.Errorf("%s:%d: exactly-once outside a negation: %s", rel, i+1, strings.TrimSpace(line))
			}
		}
	}
	if hits == 0 && len(files) < 10 {
		t.Fatalf("scanned only %d files; the sweep saw too little to be believed", len(files))
	}
}

// TestUserFacingDocsAreEnglish is the language heuristic (09 §9.3): every
// word user-facing material is English; Norwegian survives only in the plan
// documents, which are excluded here. Three unambiguous stop words keep the
// false-positive rate near zero while catching a paragraph written in the
// wrong language.
func TestUserFacingDocsAreEnglish(t *testing.T) {
	root := repoRoot(t)
	stopword := regexp.MustCompile(`\b(ikke|kjøre|jobben)\b`)
	exempt := map[string]bool{
		"docs/PLAN.md":                true,
		"docs/prosjektbeskrivelse.md": true,
	}
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "plans" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	for _, file := range files {
		rel, _ := filepath.Rel(root, file)
		if exempt[rel] {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			if stopword.MatchString(line) {
				t.Errorf("%s:%d: Norwegian stop word in user-facing material (09 §9.3): %s",
					rel, i+1, strings.TrimSpace(line))
			}
		}
	}
}

// TestREADMEFirstScreenBeforeFirstCommand pins the README contract (09 §9.3):
// at most one screen - about forty lines - passes before the reader meets the
// first command to type. Everything else in the file may be long; the top
// may not.
func TestREADMEFirstScreenBeforeFirstCommand(t *testing.T) {
	path := filepath.Join(repoRoot(t), "README.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	lines := strings.Split(string(raw), "\n")
	firstFence := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "```") {
			firstFence = i
			break
		}
	}
	if firstFence < 0 {
		t.Fatal("README carries no command block at all")
	}
	if firstFence > readmeBudgetLines {
		t.Fatalf("first command block starts at line %d; the contract allows %d lines before it",
			firstFence+1, readmeBudgetLines)
	}
}
