package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/api"
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
			input: store.JobVersionInput{
				JobName:       job.Name,
				Description:   job.Description,
				SourcePath:    file.display,
				MaxConcurrent: job.MaxConcurrent,
				SpecHash:      spec.Hash(spec.Canonical(job)),
				SpecJSON:      string(spec.Canonical(job)),
			},
			fileSHA: hex.EncodeToString(sum[:]),
			source:  source.Bytes,
		})
	}

	var results []store.JobApplyResult
	if len(loaded) > 0 {
		plan := planWrite(ctx, env, g)
		if plan.err != nil {
			return plan.err
		}
		if plan.client != nil {
			defer func() { _ = plan.client.Close() }()
			// The daemon parses the files itself; definitions still enter
			// only through its disk, never through this request.
			return socketApply(ctx, env, out, plan.client, files)
		}
		defer func() { _ = plan.st.Close() }()
		results, err = applyToStore(ctx, env, g, plan.st, loaded)
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

// applyToStore records the batch through a store that resolution already
// opened under the state lock. The database must exist already: init owns
// creating one, and apply refuses to guess at a project nobody made.
func applyToStore(ctx context.Context, env Env, g *globals, st *store.Store, loaded []loadedSpec) ([]store.JobApplyResult, error) {
	out := make([]store.JobVersionInput, len(loaded))
	for i, item := range loaded {
		out[i] = item.input
	}
	results, err := st.ApplyJobs(ctx, out)
	if err != nil {
		return nil, internalError("could not record the job specs", err)
	}
	return results, nil
}

// socketApply sends the file paths to the daemon, which parses and records
// them through its own writer, and renders the report it sends back. The
// success lines match the direct path; a failed file shows its code and
// message instead of a source excerpt, because the excerpt lives on the
// daemon's side of the socket.
func socketApply(ctx context.Context, env Env, out *ui, client *api.Client, files []jobFile) error {
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = file.path
	}
	report, err := client.Apply(ctx, paths)
	if err != nil {
		var wire *api.WireError
		if errors.As(err, &wire) {
			return wireFailure(env, wire)
		}
		return internalError("could not reach the daemon", err)
	}

	if out.mode == modeJSON {
		return out.json(applyReport{
			Applied:   convertApplied(report.Applied),
			Unchanged: convertUnchanged(report.Unchanged),
			Failed:    convertFailed(report.Failed),
		})
	}

	all := append(append([]api.ApplyEntry{}, report.Applied...), report.Unchanged...)
	width := 3
	for _, entry := range all {
		if len(entry.Job) > width {
			width = len(entry.Job)
		}
	}
	for _, entry := range report.Applied {
		status := "new"
		if entry.Version > 1 {
			status = "updated"
		}
		out.print("%s %s  %s  version %d  sha256:%s",
			out.symbols.ok, pad(entry.Job, width), status, entry.Version, shortHash(entry.SpecHash))
	}
	for _, entry := range report.Unchanged {
		out.print("%s %s  unchanged  version %d  sha256:%s",
			out.symbols.ok, pad(entry.Job, width), entry.Version, shortHash(entry.SpecHash))
	}
	for i, failure := range report.Failed {
		if i == 0 && len(all) > 0 {
			fmt.Fprintln(out.out)
		}
		out.print("%s %s", out.symbols.fail, failure.File)
		out.print("    %s: %s", failure.Code, failure.Message)
	}
	fmt.Fprintln(out.out)

	switch {
	case len(report.Failed) > 0:
		out.print("%s, %s", count(len(all), "job"), count(len(report.Failed), "failed file"))
	default:
		out.print("%s %s", out.symbols.ok, count(len(all), "job"))
	}

	if len(report.Failed) == 0 {
		return nil
	}
	return &Error{
		code: ExitValidation,
		what: fmt.Sprintf("%s in %s", count(len(report.Failed), "job file"), count(len(files), "path")),
		next: []string{
			"each failed file above names its code; paceq error <code>  explains it in full",
			"a job named by a broken file keeps its last good definition",
		},
	}
}

// convertApplied and its two siblings move wire entries into the report
// types the direct path renders.
func convertApplied(entries []api.ApplyEntry) []appliedEntry {
	out := make([]appliedEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, appliedEntry{
			Job: entry.Job, File: entry.File, Version: entry.Version,
			SpecHash: entry.SpecHash, FileSHA256: entry.FileSHA256,
		})
	}
	return out
}

func convertUnchanged(entries []api.ApplyEntry) []unchangedEntry {
	out := make([]unchangedEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, unchangedEntry{
			Job: entry.Job, File: entry.File, Version: entry.Version,
			SpecHash: entry.SpecHash, FileSHA256: entry.FileSHA256,
		})
	}
	return out
}

func convertFailed(entries []api.ApplyWireFailure) []failedEntry {
	out := make([]failedEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, failedEntry{File: entry.File, Code: entry.Code, Message: entry.Message})
	}
	return out
}

// shortHash trims a sha256: label to the width the tables show.
func shortHash(hash string) string {
	if cut, ok := strings.CutPrefix(hash, "sha256:"); ok {
		hash = cut
	}
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
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
