package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

func newApplyCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "apply [path...]",
		Aliases: []string{"load"},
		Short:   "Load job files into the database as immutable versions",
		Long: `Read every job file under the given paths, parse each one, and record it in
the state database. A spec that hashes to something new becomes a new version;
a spec that was already loaded changes nothing, so running apply twice in a row
is safe and a second run reports every job as unchanged.

Files are the only way a definition enters paceq. An invalid file never reaches
the database: it costs an error with file, line and column, the valid files
around it still load, and the jobs it names keep their last good definition.

With no arguments apply loads the jobs directory, or the directory named by
PACEQ_JOBS_DIR. The whole batch lands in one transaction: either every valid
file is recorded or none of them are.`,
		Args: cobra.ArbitraryArgs,
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runApply(ctx, env, g, out, args)
		}),
	}
	return cmd
}

// loadedSpec is one file that parsed clean and is ready for the database.
type loadedSpec struct {
	display string
	job     *spec.Job
	input   store.JobVersionInput
	fileSHA string
	source  []byte
}

// failedSpec is one file apply could not take: parsing said no.
type failedSpec struct {
	display string
	diags   diag.List
	source  []byte
}

// applyReport is what -o json writes. The field names are the contract scripts
// read, so nothing here is renamed without a major version.
type applyReport struct {
	Applied   []appliedEntry   `json:"applied"`
	Unchanged []unchangedEntry `json:"unchanged"`
	Failed    []failedEntry    `json:"failed"`
}

type appliedEntry struct {
	Job        string `json:"job"`
	File       string `json:"file"`
	Version    int    `json:"version"`
	SpecHash   string `json:"spec_hash"`
	FileSHA256 string `json:"file_sha256"`
}

type unchangedEntry struct {
	Job        string `json:"job"`
	File       string `json:"file"`
	Version    int    `json:"version"`
	SpecHash   string `json:"spec_hash"`
	FileSHA256 string `json:"file_sha256"`
}

type failedEntry struct {
	File    string `json:"file"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func runApply(ctx context.Context, env Env, g *globals, out *ui, args []string) error {
	files, err := applyFilesFor(env, args)
	if err != nil {
		return err
	}

	out.note(1, "reading %d job files", len(files))
	var loaded []loadedSpec
	var failed []failedSpec
	for _, file := range files {
		out.note(2, "parsing %s", file.display)
		job, source, diags := spec.LoadFile(file.path)
		if diags.HasErrors() {
			failed = append(failed, failedSpec{
				display: file.display,
				diags:   rename(diags, file.display),
				source:  source.Bytes,
			})
			continue
		}
		sum := sha256.Sum256(source.Bytes)
		loaded = append(loaded, loadedSpec{
			display: file.display,
			job:     job,
			input: store.JobVersionInput{
				JobName:       job.Name,
				Description:   job.Description,
				SourcePath:    file.display,
				MaxConcurrent: job.MaxConcurrent,
				SpecHash:      spec.Hash(spec.Canonical(job)),
				SpecJSON:      string(spec.Canonical(job)),
				Sensors:       job.Sensors,
				Schedules:     job.Schedules,
			},
			fileSHA: hex.EncodeToString(sum[:]),
			source:  source.Bytes,
		})
	}

	// A sensor name is a primary key across every job, so two files that define
	// the same one cannot both materialise; the later would silently steal the
	// row from the first. The conflict is found against the loaded batch and
	// the files that lose the name are refused here, before the database is
	// touched, with a diagnostic that names both files.
	loaded, failed = refuseSensorNameConflicts(loaded, failed)

	var results []store.JobApplyResult
	if len(loaded) > 0 {
		results, err = applyToStore(ctx, env, g, loaded)
		if err != nil {
			return err
		}
	}

	if err := writeApplyReport(out, results, loaded, failed); err != nil {
		return err
	}
	if len(failed) == 0 {
		return nil
	}
	first := firstError(failed[0].diags)
	return &Error{
		code: ExitValidation,
		what: fmt.Sprintf("%s in %s", count(len(failed), "job file"), count(len(files), "path")),
		next: []string{
			"each message above says what to change and where",
			"a job named by a broken file keeps its last good definition",
			"paceq error " + first.Code + "  explains that code in full",
		},
	}
}

// firstError names the error a report leads with, skipping any warning that
// happens to sit before it.
func firstError(diags diag.List) diag.Diagnostic {
	for _, d := range diags {
		if d.Severity == diag.SeverityError {
			return d
		}
	}
	return diags[0]
}

// refuseSensorNameConflicts moves into failed any loaded file that tries to
// take a sensor name another file in the batch already owns. The sensor names
// are primary keys across every job, so both cannot materialise; refusing the
// later one here beats a database that would silently reassign the row.
func refuseSensorNameConflicts(loaded []loadedSpec, failed []failedSpec) ([]loadedSpec, []failedSpec) {
	named := make([]spec.NamedJob, 0, len(loaded))
	for _, item := range loaded {
		if item.job == nil {
			continue
		}
		named = append(named, spec.NamedJob{Path: item.display, Job: item.job})
	}
	conflicts := spec.CheckGlobalSensorNames(named)
	if len(conflicts) == 0 {
		return loaded, failed
	}

	byFile := map[string]diag.List{}
	for _, d := range conflicts {
		byFile[d.File] = append(byFile[d.File], d)
	}
	kept := loaded[:0]
	for _, item := range loaded {
		if diags, bad := byFile[item.display]; bad {
			failed = append(failed, failedSpec{display: item.display, diags: diags, source: item.source})
			continue
		}
		kept = append(kept, item)
	}
	return kept, failed
}

// applyToStore opens the state database and records the batch. The database
// must exist already: init owns creating one, and apply refuses to guess at a
// project nobody made.
func applyToStore(ctx context.Context, env Env, g *globals, loaded []loadedSpec) ([]store.JobApplyResult, error) {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if _, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		return nil, usageError(
			fmt.Sprintf("there is no state database at %s yet", displayPath(env, dbPath)),
			"paceq init  creates a project with a state directory and an example job",
		)
	}

	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clkOf(env)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()

	out := make([]store.JobVersionInput, len(loaded))
	for i, item := range loaded {
		out[i] = item.input
	}
	results, err := s.ApplyJobs(ctx, out)
	if err != nil {
		return nil, internalError("could not record the job specs", err)
	}
	return results, nil
}

func writeApplyReport(out *ui, results []store.JobApplyResult, loaded []loadedSpec, failed []failedSpec) error {
	if out.mode == modeJSON {
		report := applyReport{
			Applied:   []appliedEntry{},
			Unchanged: []unchangedEntry{},
			Failed:    []failedEntry{},
		}
		byName := make(map[string]loadedSpec, len(loaded))
		for _, item := range loaded {
			byName[item.input.JobName] = item
		}
		for _, r := range results {
			item := byName[r.JobName]
			entry := appliedEntry{
				Job:        r.JobName,
				File:       item.display,
				Version:    r.Version,
				SpecHash:   item.input.SpecHash,
				FileSHA256: item.fileSHA,
			}
			if r.Created {
				report.Applied = append(report.Applied, entry)
			} else {
				report.Unchanged = append(report.Unchanged, unchangedEntry(entry))
			}
		}
		for _, f := range failed {
			first := firstError(f.diags)
			report.Failed = append(report.Failed, failedEntry{
				File:    f.display,
				Code:    first.Code,
				Message: first.Message,
			})
		}
		return out.json(report)
	}

	style := out.diagStyle()
	wrote := 0
	for _, r := range results {
		var item *loadedSpec
		for i := range loaded {
			if loaded[i].input.JobName == r.JobName {
				item = &loaded[i]
			}
		}
		status := "unchanged"
		symbol := out.symbols.ok
		if r.Created && r.Version > 1 {
			status = "updated"
		} else if r.Created {
			status = "new"
		}
		short := item.input.SpecHash
		if cut, ok := strings.CutPrefix(short, "sha256:"); ok {
			short = cut
		}
		if len(short) > 12 {
			short = short[:12]
		}
		out.print("%s %s  %s  version %d  sha256:%s",
			symbol, pad(r.JobName, jobWidth(results)), status, r.Version, short)
		wrote++
	}
	for i, f := range failed {
		if i > 0 || wrote > 0 {
			fmt.Fprintln(out.out)
		}
		for _, d := range f.diags {
			if err := style.Render(out.out, d, f.source); err != nil {
				return internalError("could not write the report", err)
			}
		}
	}
	fmt.Fprintln(out.out)

	switch {
	case len(failed) > 0:
		out.print("%s, %s", count(len(results), "job"), count(len(failed), "failed file"))
	default:
		out.print("%s %s", out.symbols.ok, count(len(results), "job"))
	}
	return nil
}

// jobWidth sizes the name column so the statuses line up.
func jobWidth(results []store.JobApplyResult) int {
	width := 0
	for _, r := range results {
		if len(r.JobName) > width {
			width = len(r.JobName)
		}
	}
	return width
}

// applyFilesFor turns what was typed on the command line into the files to
// load. With nothing given, PACEQ_JOBS_DIR names the catalog and otherwise the
// jobs directory beside the state directory does.
func applyFilesFor(env Env, args []string) ([]jobFile, error) {
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
					"name what to load: paceq apply jobs/ or paceq apply jobs/nightly.yaml",
					"set PACEQ_JOBS_DIR to read a catalog from somewhere else",
					"paceq init  creates a project with a "+jobsDir+" directory",
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
			"no job files to load",
			strings.Join(roots, ", "),
			"paceq reads files ending in "+strings.Join(spec.Extensions, " or "),
			"paceq init  creates a project with an example job that applies as it stands",
		)
	}

	files := make([]jobFile, 0, len(paths))
	for _, path := range paths {
		files = append(files, jobFile{path: path, display: relative(env.Dir, path)})
	}
	return files, nil
}

// displayPath renders a path for a message, relative to the project when it
// lives inside it.
func displayPath(env Env, path string) string {
	if rel, err := filepath.Rel(env.Dir, path); err == nil && !strings.HasPrefix(rel, "..") {
		return rel
	}
	return path
}
