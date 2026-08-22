package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/store"
)

// The project layout init creates. jobs is a directory because a project grows
// past one file within the first hour.
const (
	projectFile   = "paceq.yaml"
	jobsDir       = "jobs"
	exampleJob    = "hello.yaml"
	gitignoreFile = ".gitignore"
)

// exampleJobBody is the point of the command (09 section 6.3): what init writes
// has to run as it stands. run is an argv array, never a shell string, so the
// first example a user copies teaches the form that never reaches a shell
// (08 section 3.2).
const exampleJobBody = `# The smallest job that works.
#
# run is a list, not a string: paceq starts the process itself, so nothing here
# is expanded, quoted or split by a shell. The first element is an absolute
# path, because there is no shell to search PATH either.
name: hello
description: The smallest job that works.
steps:
  - name: say-hello
    run: ["/bin/echo", "hello from paceq"]
`

func newInitCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Create a project: a config, an example job and a state database",
		Long: `Create a project in the current directory.

What it writes works as it stands: the example job runs, the database is
migrated, and the state directory is kept out of version control.

init never touches state that is already there. A directory that has been
initialised is refused, and so is state other users can read.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runInit(ctx, env, g, out)
		}),
	}
}

// created is one line of the report: what was written and why it is there.
type created struct {
	Path string `json:"path"`
	Note string `json:"note"`
}

// nextStep is a command to run now, with what it does.
type nextStep struct {
	Command string `json:"command"`
	Note    string `json:"note"`
}

type initReport struct {
	Created   []created  `json:"created"`
	NextSteps []nextStep `json:"next_steps"`
}

func runInit(ctx context.Context, env Env, g *globals, out *ui) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	project := filepath.Join(env.Dir, projectFile)
	job := filepath.Join(env.Dir, jobsDir, exampleJob)

	// Permissions first. State another user can read is a refusal whatever else
	// is wrong with the directory, and saying so before "already initialised"
	// is what stops the operator fixing the wrong problem.
	for path, want := range map[string]fs.FileMode{stateDir: store.DirMode, dbPath: store.DatabaseMode} {
		if err := checkExistingMode(path, want); err != nil {
			return err
		}
	}
	if err := refuseExisting(project, job, dbPath); err != nil {
		return err
	}

	out.note(1, "creating the state directory %s", stateDir)
	if err := os.MkdirAll(stateDir, store.DirMode); err != nil {
		return internalError("could not create the state directory", err)
	}

	out.note(1, "opening the database %s", dbPath)
	schema, err := createState(ctx, env, stateDir, out)
	if err != nil {
		return err
	}

	// The project files are written through a root anchored at the project
	// directory, so no name can resolve outside it through a symlink somebody
	// left in the way.
	root, err := os.OpenRoot(env.Dir)
	if err != nil {
		return internalError("could not open the project directory", err)
	}
	defer func() { _ = root.Close() }()

	out.note(1, "writing %s", project)
	if err := createPrivate(root, projectFile, projectConfig(stateDir, env.Dir)); err != nil {
		return err
	}
	if err := root.MkdirAll(jobsDir, store.DirMode); err != nil {
		return internalError("could not create the jobs directory", err)
	}
	out.note(1, "writing %s", job)
	if err := createPrivate(root, filepath.Join(jobsDir, exampleJob), exampleJobBody); err != nil {
		return err
	}
	entry := gitignoreEntry(env.Dir, stateDir)
	ignored, err := updateGitignore(root, entry)
	if err != nil {
		return err
	}

	report := initReport{
		Created: []created{
			{Path: relative(env.Dir, project), Note: "project config"},
			{Path: relative(env.Dir, job), Note: "an example job that runs as it stands"},
			{Path: relative(env.Dir, dbPath), Note: fmt.Sprintf("state (schema %d, WAL, auto_vacuum INCREMENTAL)", schema)},
		},
		NextSteps: []nextStep{
			{Command: "paceq doctor", Note: "check the installation"},
			{Command: "paceq validate", Note: "check the job files"},
			{Command: "paceq run hello", Note: "run the example job now"},
		},
	}
	if ignored {
		report.Created = append(report.Created, created{Path: gitignoreFile, Note: entry + " added"})
	}
	return writeInitReport(out, report)
}

func writeInitReport(out *ui, report initReport) error {
	if out.mode == modeJSON {
		return out.json(report)
	}

	width := 0
	for _, item := range report.Created {
		if n := len(item.Path); n > width {
			width = n
		}
	}
	for _, step := range report.NextSteps {
		if n := len(step.Command); n > width {
			width = n
		}
	}

	out.print("Created:")
	for _, item := range report.Created {
		out.print("  %s  %s", pad(item.Path, width), item.Note)
	}
	out.print("")
	out.print("Next steps:")
	for _, step := range report.NextSteps {
		out.print("  %s  %s", pad(step.Command, width), step.Note)
	}
	return nil
}

// createState creates the database and brings it up to this build's schema. The
// store takes the state lock first, so a second paceq is refused here rather
// than after both have written.
func createState(ctx context.Context, env Env, stateDir string, out *ui) (int, error) {
	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clkOf(env)})
	if err != nil {
		return 0, err
	}
	defer func() { _ = s.Close() }()

	out.note(1, "applying migrations")
	if err := s.Migrate(ctx); err != nil {
		return 0, err
	}
	schema, err := s.SchemaVersion(ctx)
	if err != nil {
		return 0, err
	}
	out.note(2, "database at schema %d", schema)
	return schema, nil
}

// checkExistingMode applies the fail closed rule to a path that is already
// there. A path that does not exist yet is created by paceq, at the mode paceq
// wants, so there is nothing to check.
func checkExistingMode(path string, want fs.FileMode) error {
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err := store.CheckMode(path, want); err != nil {
		return err
	}
	return nil
}

// refuseExisting is the rule that init never overwrites anything. The refusal
// names every file that is in the way, because fixing them one error at a time
// is the slowest way to learn what is there.
func refuseExisting(paths ...string) error {
	var found []string
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			found = append(found, path)
		}
	}
	if len(found) == 0 {
		return nil
	}
	return usageError(
		fmt.Sprintf("this directory is already initialised: %s", strings.Join(found, ", ")),
		"paceq doctor  reports what is there and whether it is healthy",
		"to start over, move what is in the way aside: mv "+found[0]+" "+found[0]+".bak",
		"or initialise against another state directory: paceq init --db /other/path/"+store.DatabaseFileName,
	)
}

// createPrivate writes a file paceq owns, at the mode paceq requires, and
// refuses to replace one that is already there. The mode is explicit rather
// than left to the umask, because the umask is the environment's decision and
// this one is not, and O_EXCL makes the kernel enforce what refuseExisting
// checked a moment earlier.
func createPrivate(root *os.Root, name, body string) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, store.DatabaseMode)
	if err != nil {
		return internalError("could not write "+name, err)
	}
	if _, err := file.WriteString(body); err != nil {
		_ = file.Close()
		return internalError("could not write "+name, err)
	}
	if err := file.Close(); err != nil {
		return internalError("could not write "+name, err)
	}
	return nil
}

// gitignoreEntry is the line that keeps this run's state out of version
// control. The database is machine state, and a merge conflict in it is
// unrecoverable, so the entry names the directory the state actually landed in
// rather than the default one.
//
// There is nothing to write when that directory is not somewhere inside the
// project: git ignores paths relative to the file, so a state directory beside
// or above the project cannot be named, and one that is the project directory
// itself could only be named by ignoring everything.
func gitignoreEntry(projectDir, stateDir string) string {
	rel, err := filepath.Rel(projectDir, stateDir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ""
	}
	return filepath.ToSlash(rel) + "/"
}

// updateGitignore adds the entry to .gitignore, and reports whether it had to.
// An entry that is already there is left alone, and so is every other line in
// the file.
func updateGitignore(root *os.Root, entry string) (bool, error) {
	if entry == "" {
		return false, nil
	}

	existing, err := root.ReadFile(gitignoreFile)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, internalError("could not read "+gitignoreFile, err)
	}
	for _, line := range strings.Split(string(existing), "\n") {
		if strings.TrimSpace(line) == entry {
			return false, nil
		}
	}

	body := string(existing)
	if body != "" && !strings.HasSuffix(body, "\n") {
		body += "\n"
	}
	body += entry + "\n"
	if err := root.WriteFile(gitignoreFile, []byte(body), store.DatabaseMode); err != nil {
		return false, internalError("could not write "+gitignoreFile, err)
	}
	return true, nil
}

// projectConfig is the file that says where a project keeps its state and its
// jobs. Both values are the ones this init used, so the file describes what is
// actually on disk rather than a default that may not apply.
func projectConfig(stateDir, projectDir string) string {
	return fmt.Sprintf(`# paceq project configuration.
#
# state: where the database and the lock live.
# jobs:  the directory paceq reads job files from.
state: %s
jobs: %s
`, relative(projectDir, stateDir), jobsDir)
}

// relative is a path as it reads from the project directory, and the absolute
// one when it lies outside.
func relative(base, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return path
	}
	return rel
}
