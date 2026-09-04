package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

func newValidateCmd(env Env, g *globals) *cobra.Command {
	strict := false

	cmd := &cobra.Command{
		Use:   "validate [path...]",
		Short: "Check job files and say exactly what is wrong with them",
		Long: `Check job files: read them, apply every rule that the files themselves can
answer, and report what is wrong with each one by file, line and column, with
the line quoted and a way forward.

With no arguments it checks the catalog apply loads: the directory named by
PACEQ_JOBS_DIR, or the jobs directory beside the project. A path that names a
file checks that file; a path that names a directory checks every .yaml and
.yml file under it, in a fixed order.

The rules run over each file alone and then over the files together: a sensor
name is a primary key across every job, so two files claiming one are refused
here exactly as apply refuses them.

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

// validateReport is what -o json writes. The shape is the contract: a script
// reads .diagnostics[].code and branches on it, so nothing here is renamed
// without a major version.
type validateReport struct {
	Diagnostics []diag.Diagnostic `json:"diagnostics"`
}

func runValidate(env Env, g *globals, out *ui, args []string, strict bool) error {
	files, err := catalogFilesFor(env, args, validateCatalog)
	if err != nil {
		return err
	}

	out.note(1, "checking %d job files", len(files))
	var diags diag.List
	sources := make(map[string][]byte, len(files))
	parsed := make([]spec.NamedJob, 0, len(files))
	for _, file := range files {
		out.note(2, "reading %s", file.display)
		source, readDiags := spec.ReadFile(file.path)
		if len(readDiags) > 0 {
			diags = append(diags, rename(readDiags, file.display)...)
			continue
		}
		sources[file.display] = source.Bytes
		job, fileDiags := spec.Parse(file.display, source.Bytes)
		diags = append(diags, fileDiags...)
		if job != nil && !fileDiags.HasErrors() {
			parsed = append(parsed, spec.NamedJob{Path: file.display, Job: job})
		}
	}
	// The rules that need more than one file run over what was given, after
	// every file has been read. apply refuses a batch on this one, so validate
	// has to reach the same verdict: a gate that passes what the deploy step
	// refuses costs more than no gate.
	diags = append(diags, spec.CheckGlobalSensorNames(parsed)...)
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
