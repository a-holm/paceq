package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/store"
	"github.com/spf13/cobra"
)

// runs artifacts is the reference shelf of one run (#13): every name its
// steps published through $PACEQ_OUTPUT, with the uri each step claimed.
// paceq never opened the content and never will; this command only reads
// the references back, in spec order, the way runs show orders steps.

func newRunsArtifactsCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "artifacts <run>",
		Short: "List the artifact references one run published",
		Long: `Every reference the run's steps published through their $PACEQ_OUTPUT
files, by step and name.

A reference is a claim, not a file: paceq never verified that the uri
exists and never touched the content. The listing reads through the read
only pool, so it answers while a daemon writes new runs.`,
		Args: exactArgs(1, "one run id or prefix"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runRunsArtifacts(ctx, env, g, out, args[0])
		}),
	}
}

type artifactRecord struct {
	Step      string `json:"step"`
	Name      string `json:"name"`
	URI       string `json:"uri"`
	SizeBytes *int64 `json:"size_bytes,omitempty"`
	Checksum  string `json:"checksum,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

func artifactRecordOf(a store.Artifact) artifactRecord {
	return artifactRecord{
		Step:      a.StepName,
		Name:      a.Name,
		URI:       a.URI,
		SizeBytes: a.SizeBytes,
		Checksum:  a.Checksum,
		MediaType: a.MediaType,
		CreatedAt: rfc3339(a.CreatedAt),
	}
}

// runRunsArtifacts resolves the id or prefix exactly as runs show does, so
// "this names no run" always says the same thing whatever shelf is asked.
func runRunsArtifacts(ctx context.Context, env Env, g *globals, out *ui, runArg string) error {
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
			"run the command inside the project directory, or pass --db")
	}
	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	detail, err := ro.GetRun(ctx, runArg)
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
				"the ids of finished runs, shortest first: any prefix names a run as soon as it can",
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

	rows, err := ro.RunsArtifacts(ctx, detail.ID)
	if err != nil {
		return internalError("could not read the artifacts of the run", err)
	}

	docs := make([]artifactRecord, 0, len(rows))
	for _, a := range rows {
		docs = append(docs, artifactRecordOf(a))
	}
	if out.mode == modeJSON {
		return out.json(docs)
	}
	if len(docs) == 0 {
		out.print("no references published: a step publishes one by writing a line to its $PACEQ_OUTPUT file")
		return nil
	}
	for _, d := range docs {
		size := ""
		if d.SizeBytes != nil {
			size = fmt.Sprintf("  %s", humanBytes(*d.SizeBytes))
		}
		out.print("  %s  %s  %s%s", d.Step, d.Name, d.URI, size)
	}
	return nil
}
