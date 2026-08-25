package cli

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/buildinfo"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/store"
)

// export run bundles one run's complete evidence into a tar.gz (06 section
// 9.4): every database row the run produced, the frozen job version it ran
// against, all log attempts from disk, and a manifest naming each file with
// its sha256. The point is a proof that survives retention and reads without
// paceq at all.
type exportFlags struct {
	out string
}

func newExportCmd(env Env, g *globals) *cobra.Command {
	var f exportFlags
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export evidence from the state database",
		Long: `Write self-contained archives that keep the proof of what happened
after retention would have removed it.`,
	}
	run := &cobra.Command{
		Use:   "run <id>",
		Short: "Write one run's full evidence to a tar.gz",
		Long: `Collect everything one run produced: run, steps, step deps,
events and artifacts from the state database; the trigger and tick that
caused it; the exact job version it executed; and every log attempt from
the log directory. Each file's sha256 lands in manifest.json.

Retention may later delete the history this found; an export keeps it.`,
		Args: exactArgs(1, "a run id or unambiguous prefix"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runExportRun(ctx, env, g, out, f, args[0])
		}),
	}
	run.Flags().StringVarP(&f.out, "output", "o", "", "archive path (default: <run id>.tar.gz)")
	cmd.AddCommand(run)
	return cmd
}

type exportManifest struct {
	PulseqVersion string            `json:"pulseq_version"`
	SchemaVersion int               `json:"schema_version"`
	RunID         string            `json:"run_id"`
	ExportedAt    string            `json:"exported_at"`
	Files         map[string]string `json:"files"`
}

func runExportRun(ctx context.Context, env Env, g *globals, out *ui, f exportFlags, runArg string) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if _, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		return notFoundError(
			fmt.Sprintf("there is no paceq state at %s", stateDir),
			stateDir,
			"paceq init  creates a project with its state directory",
			"run the command inside the project directory, or pass --db",
		)
	}

	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	data, err := ro.ExportRun(ctx, runArg)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAmbiguousRunID):
			return notFoundError(
				fmt.Sprintf("%q does not name one run: the prefix matches more than one", runArg),
				ambiguousHint(err),
				"give more characters until the prefix names exactly one run",
			)
		case errors.Is(err, store.ErrRunNotFound):
			return notFoundError(
				fmt.Sprintf("no run matches %q", runArg),
				"any prefix names a run as soon as it can name exactly one",
				"check the id: paceq explains it on every failure it reports",
			)
		case errors.Is(err, id.ErrInvalid):
			return notFoundError(
				fmt.Sprintf("no run matches %q", runArg),
				err.Error(),
				"an id is 26 characters from 0123456789ABCDEFGHJKMNPQRSTVWXYZ; any prefix of one works",
			)
		default:
			return err
		}
	}

	rowsJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return internalError("could not render the run rows", err)
	}

	dst := f.out
	if dst == "" {
		dst = data.Run.ID + ".tar.gz"
	}
	if !filepath.IsAbs(dst) {
		dst = filepath.Join(env.Dir, dst)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return internalError(fmt.Sprintf("could not create %s", filepath.Dir(dst)), err)
	}

	files := map[string]string{}
	// The archive destination is the operator's own -o flag: a chosen output
	// path has no fixed root to be scoped under.
	archive, err := os.Create(dst) // #nosec G304 - operator-selected destination
	if err != nil {
		return internalError(fmt.Sprintf("could not create %s", dst), err)
	}
	defer func() { _ = archive.Close() }()

	gz := gzip.NewWriter(archive)
	tw := tar.NewWriter(gz)

	addFile := func(name string, payload []byte, mode fs.FileMode) error {
		sum := sha256.Sum256(payload)
		files[name] = hex.EncodeToString(sum[:])
		hdr := &tar.Header{
			Name: name,
			Mode: int64(mode),
			Size: int64(len(payload)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		_, err := tw.Write(payload)
		return err
	}

	if err := addFile("rows.json", rowsJSON, 0o600); err != nil {
		return writeFailure(dst, err)
	}

	// Log attempts come straight off disk, newest naming first by spec
	// order. The os.Root keeps every read inside the log directory even if
	// a database row ever carried a hostile relative path.
	logRootFS, err := os.OpenRoot(filepath.Join(stateDir, "logs"))
	if err != nil {
		return internalError("could not open the log directory", err)
	}
	defer func() { _ = logRootFS.Close() }()
	root := logsink.NewRoot(stateDir)
	for _, step := range data.Steps {
		attempts, err := root.AttemptFiles(data.Run.ID, step.Name)
		if err != nil {
			return internalError("could not list the log files of this run", err)
		}
		for _, attempt := range attempts {
			if _, err := root.Abs(attempt.RelPath); err != nil {
				return internalError("could not resolve a log path", err)
			}
			f, err := logRootFS.Open(attempt.RelPath)
			if err != nil {
				return notFoundError(
					fmt.Sprintf("the log file %s is gone from the log directory", attempt.RelPath),
					attempt.RelPath,
					"the database row survives; only the file aged out of the log directory",
					"",
				)
			}
			payload, readErr := io.ReadAll(f)
			_ = f.Close()
			if readErr != nil {
				return internalError("could not read a log attempt", readErr)
			}
			name := fmt.Sprintf("logs/%s.%d.ndjson", step.Name, attempt.Attempt)
			if err := addFile(name, payload, 0o600); err != nil {
				return writeFailure(dst, err)
			}
		}
	}

	manifest := exportManifest{
		PulseqVersion: buildinfo.Version,
		SchemaVersion: schemaVersion(),
		RunID:         data.Run.ID,
		Files:         files,
	}
	if now := clkOf(env).Now(); !now.IsZero() {
		manifest.ExportedAt = now.UTC().Format(time.RFC3339)
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return internalError("could not render the manifest", err)
	}
	if err := addFile("manifest.json", manifestJSON, 0o600); err != nil {
		return writeFailure(dst, err)
	}

	if err := tw.Close(); err != nil {
		return writeFailure(dst, err)
	}
	if err := gz.Close(); err != nil {
		return writeFailure(dst, err)
	}
	if err := archive.Close(); err != nil {
		return writeFailure(dst, err)
	}

	info, err := os.Stat(dst)
	if err != nil {
		return internalError(fmt.Sprintf("could not stat %s", dst), err)
	}
	if out.mode == modeJSON {
		return out.json(struct {
			Path  string `json:"path"`
			Bytes int64  `json:"bytes"`
			Files int    `json:"files"`
			RunID string `json:"run_id"`
		}{dst, info.Size(), len(files), data.Run.ID})
	}
	out.print("Wrote %s (%s): %d files, manifest sha256 verified on read.",
		dst, humanBytes(info.Size()), len(files))
	return nil
}

func writeFailure(dst string, err error) error {
	_ = os.Remove(dst)
	return internalError(fmt.Sprintf("writing %s failed; the partial file was removed", dst), err)
}

// schemaVersion stamps the manifest with the schema the rows came from.
func schemaVersion() int {
	v, err := store.KnownSchemaVersion()
	if err != nil {
		return 0
	}
	return v
}
