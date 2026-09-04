package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/a-holm/paceq/internal/spec"
)

// The catalog is the set of job files a command reads when nobody names one.
// validate and apply resolve it through the single function below, because two
// commands that read different directories cannot be judging the same files:
// a gate that checks one catalog while the deploy step loads another is worse
// than no gate at all.

// jobFile is one file to check: the path to open, and the path to print. They
// differ because a report reads better relative to the project than as a string
// of absolute paths, while opening a file needs the real one.
type jobFile struct {
	path    string
	display string
}

// catalogCommand is the wording one command lends the shared resolver, so the
// same refusal is spelled for the command the operator actually typed.
type catalogCommand struct {
	// name is the subcommand, for the examples a refusal offers.
	name string
	// verb is what the command does with a file: check, load.
	verb string
	// example is what paceq init leaves behind that this command can use.
	example string
}

var (
	validateCatalog = catalogCommand{name: "validate", verb: "check", example: "an example job that runs as it stands"}
	applyCatalog    = catalogCommand{name: "apply", verb: "load", example: "an example job that applies as it stands"}
)

// catalogFilesFor turns what was typed on the command line into the files to
// read. With nothing given, PACEQ_JOBS_DIR names the catalog and the jobs
// directory beside the project is the fallback. This is the only place the
// variable is read.
//
// It refuses in the two ways that are not a job file's fault: a path that is
// not there, and a place with no job files in it.
func catalogFilesFor(env Env, args []string, c catalogCommand) ([]jobFile, error) {
	roots := args
	if len(roots) == 0 {
		if fromEnv := env.Getenv("PACEQ_JOBS_DIR"); fromEnv != "" {
			roots = []string{fromEnv}
		} else {
			defaultDir := filepath.Join(env.Dir, jobsDir)
			info, err := os.Stat(defaultDir)
			if err != nil || !info.IsDir() {
				return nil, usageError(
					fmt.Sprintf("no paths given, and there is no %s directory here", jobsDir),
					fmt.Sprintf("name what to %s: paceq %s %s/ or paceq %s %s/nightly.yaml",
						c.verb, c.name, jobsDir, c.name, jobsDir),
					"set PACEQ_JOBS_DIR to read a catalog from somewhere else",
					"paceq init  creates a project with a "+jobsDir+" directory and an example job",
				)
			}
			roots = []string{defaultDir}
		}
	}

	resolved := make([]string, 0, len(roots))
	for _, root := range roots {
		path := root
		if !filepath.IsAbs(path) {
			path = filepath.Join(env.Dir, path)
		}
		if _, err := os.Stat(path); err != nil {
			return nil, pathError(c, root, err)
		}
		resolved = append(resolved, path)
	}

	paths, err := spec.Collect(resolved)
	if err != nil {
		return nil, internalError("could not list the job files", err)
	}
	if len(paths) == 0 {
		return nil, notFoundError(
			"no job files to "+c.verb,
			strings.Join(roots, ", "),
			"paceq reads files ending in "+strings.Join(spec.Extensions, " or "),
			"paceq init  creates a project with "+c.example,
		)
	}

	files := make([]jobFile, 0, len(paths))
	for _, path := range paths {
		files = append(files, jobFile{path: path, display: relative(env.Dir, path)})
	}
	return files, nil
}

func pathError(c catalogCommand, named string, err error) *Error {
	if errors.Is(err, fs.ErrPermission) {
		return validationError(fmt.Sprintf("%s cannot be read: %v", named, err), err,
			"ls -l "+named+"  shows who owns it",
			"paceq reads job files as the user it runs as, and does not elevate")
	}
	return notFoundError(fmt.Sprintf("%s is not a path on this machine", named), named,
		fmt.Sprintf("check the spelling, or leave it out: paceq %s  %ss the %s directory",
			c.name, c.verb, jobsDir),
		"paceq init  creates a project with a "+jobsDir+" directory")
}
