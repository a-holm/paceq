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

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

func newValidateCmd(env Env, g *globals) *cobra.Command {
	strict := false

	cmd := &cobra.Command{
		Use:   "validate [path...]",
		Short: "Check job files and say exactly what is wrong with them",
		Long: `Check job files: read them, apply every rule, and report what is wrong with
each one by file, line and column, with the line quoted and a way forward.

With no arguments it checks the jobs directory. A path that names a file checks
that file; a path that names a directory checks every .yaml and .yml file under
it, in a fixed order.

validate reads nothing but the files it is given. It touches no database, starts
no process and needs no daemon.

Warnings do not fail: shell: true and inherit_env are things paceq will run and
would rather you knew about. --strict turns them into failures, which is what a
pipeline wants.`,
		Args: cobra.ArbitraryArgs,
		RunE: runArgsE(env, g, func(_ context.Context, out *ui, args []string) error {
			return runValidate(env, g, out, args, strict)
		}),
	}
	cmd.Flags().BoolVar(&strict, "strict", false, "treat warnings as errors, for CI")
	return cmd
}

// jobFile is one file to check: the path to open, and the path to print. They
// differ because a report reads better relative to the project than as a string
// of absolute paths, while opening a file needs the real one.
type jobFile struct {
	path    string
	display string
}

// validateReport is what -o json writes. The shape is the contract: a script
// reads .diagnostics[].code and branches on it, so nothing here is renamed
// without a major version.
type validateReport struct {
	Diagnostics []diag.Diagnostic `json:"diagnostics"`
}

func runValidate(env Env, g *globals, out *ui, args []string, strict bool) error {
	files, err := jobFilesFor(env, args)
	if err != nil {
		return err
	}

	out.note(1, "checking %d job files", len(files))
	var diags diag.List
	sources := make(map[string][]byte, len(files))
	for _, file := range files {
		out.note(2, "reading %s", file.display)
		source, readDiags := spec.ReadFile(file.path)
		if len(readDiags) > 0 {
			diags = append(diags, rename(readDiags, file.display)...)
			continue
		}
		sources[file.display] = source.Bytes
		_, fileDiags := spec.Parse(file.display, source.Bytes)
		diags = append(diags, fileDiags...)
	}
	if strict {
		diags = diags.Promote()
	}

	if err := writeValidateReport(out, diags, sources); err != nil {
		return err
	}
	if !diags.HasErrors() {
		return nil
	}
	return &Error{
		code: ExitValidation,
		what: fmt.Sprintf("%s in %s", count(diags.Errors(), "problem"), count(len(files), "job file")),
		next: []string{
			"each message above says what to change and where",
			"paceq error <code>  explains any of the codes in full",
		},
	}
}

func writeValidateReport(out *ui, diags diag.List, sources map[string][]byte) error {
	if out.mode == modeJSON {
		// An empty result is an empty list rather than null: a script that
		// iterates the field should not have to special case a clean run.
		report := validateReport{Diagnostics: diags}
		if report.Diagnostics == nil {
			report.Diagnostics = []diag.Diagnostic{}
		}
		return out.json(report)
	}

	style := out.diagStyle()
	for i, d := range diags {
		if i > 0 {
			fmt.Fprintln(out.out)
		}
		if err := style.Render(out.out, d, sources[d.File]); err != nil {
			return internalError("could not write the report", err)
		}
	}
	if len(diags) > 0 {
		fmt.Fprintln(out.out)
	}

	switch {
	case diags.HasErrors():
		out.print("%s, %s", count(diags.Errors(), "error"), count(diags.Warnings(), "warning"))
	case len(diags) > 0:
		out.print("%s, and nothing that stops a run", count(diags.Warnings(), "warning"))
	default:
		out.print("%s %s", out.symbols.ok, "no problems")
	}
	return nil
}

// jobFilesFor turns what was typed into the list of files to read, and refuses
// in the two ways that are not a job file's fault: a path that is not there,
// and a place with no job files in it.
func jobFilesFor(env Env, args []string) ([]jobFile, error) {
	roots := args
	if len(roots) == 0 {
		defaultDir := filepath.Join(env.Dir, jobsDir)
		info, err := os.Stat(defaultDir)
		if err != nil || !info.IsDir() {
			return nil, usageError(
				fmt.Sprintf("no paths given, and there is no %s directory here", jobsDir),
				"name what to check: paceq validate jobs/ or paceq validate jobs/nightly.yaml",
				"paceq init  creates a project with a "+jobsDir+" directory and an example job",
			)
		}
		roots = []string{defaultDir}
	}

	resolved := make([]string, 0, len(roots))
	for _, root := range roots {
		path := root
		if !filepath.IsAbs(path) {
			path = filepath.Join(env.Dir, path)
		}
		if _, err := os.Stat(path); err != nil {
			return nil, pathError(root, err)
		}
		resolved = append(resolved, path)
	}

	paths, err := spec.Collect(resolved)
	if err != nil {
		return nil, internalError("could not list the job files", err)
	}
	if len(paths) == 0 {
		return nil, notFoundError(
			"no job files to check",
			strings.Join(roots, ", "),
			"paceq reads files ending in "+strings.Join(spec.Extensions, " or "),
			"paceq init  creates a project with an example job that runs as it stands",
		)
	}

	files := make([]jobFile, 0, len(paths))
	for _, path := range paths {
		files = append(files, jobFile{path: path, display: relative(env.Dir, path)})
	}
	return files, nil
}

func pathError(named string, err error) *Error {
	if errors.Is(err, fs.ErrPermission) {
		return validationError(fmt.Sprintf("%s cannot be read: %v", named, err), err,
			"ls -l "+named+"  shows who owns it",
			"paceq reads job files as the user it runs as, and does not elevate")
	}
	return notFoundError(fmt.Sprintf("%s is not a path on this machine", named), named,
		"check the spelling, or leave it out: paceq validate  checks the "+jobsDir+" directory",
		"paceq init  creates a project with a "+jobsDir+" directory")
}

// rename points diagnostics at the path the report prints, for the failures
// that happen before a file has been read and named.
func rename(diags diag.List, display string) diag.List {
	renamed := make(diag.List, len(diags))
	for i, d := range diags {
		d.File = display
		renamed[i] = d
	}
	return renamed
}

// count is "1 error" and "2 errors", because a report that says "1 errors" is a
// report somebody stops trusting.
func count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
