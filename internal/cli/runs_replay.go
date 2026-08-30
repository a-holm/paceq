package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/store"
)

// runs replay is the second half of the operator pair (M4-04). Where retry
// continues one run in place, replay makes a NEW run beside it: a fresh id,
// replay_of pointing back at the source, no dedup key of its own, and the
// same frozen job version the source ran. Nothing here reads the job's
// current definition, so an apply between the original attempt and the
// replay cannot change what the replay does.

// replayResult is what one replay made, as a script reads it.
type replayResult struct {
	RunID    string   `json:"run_id"`
	ReplayOf string   `json:"replay_of"`
	Reused   []string `json:"reused"`
	Rerun    []string `json:"rerun"`
}

type replayFlags struct {
	from   string
	failed bool
}

func newRunsReplayCmd(env Env, g *globals) *cobra.Command {
	var f replayFlags
	cmd := &cobra.Command{
		Use:   "replay <run>",
		Short: "Run a finished run again as a new run",
		Long: `Make a new run out of a finished one: a fresh id, a link back to the run
it came from, and the same frozen job version the original ran, whatever the
job says now.

By default every step runs again. --from <step> spares the steps the named
step sits on top of: they start already succeeded, their outputs carried over
as references. --failed spares every step that succeeded last time. Give
neither and the whole graph runs from the top.

A replay never takes the original's dedup key, so triggering the job the
ordinary way stays independent of anything replay does.`,
		Args: exactArgs(1, "one run id or prefix"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runRunsReplay(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().StringVar(&f.from, "from", "",
		"start from this step: everything it depends on is reused")
	cmd.Flags().BoolVar(&f.failed, "failed", false,
		"reuse every step that succeeded in the original run")
	return cmd
}

func runRunsReplay(ctx context.Context, env Env, g *globals, out *ui, runArg string, f replayFlags) error {
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

	// Resolve the id or prefix against a read-only store first, so the
	// refusal for a name that matches nothing comes before anything else.
	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return err
	}
	detail, err := ro.GetRun(ctx, runArg)
	closeErr := ro.Close()
	if err != nil {
		return runLookupError(err, runArg)
	}
	if closeErr != nil {
		return internalError("could not close the read only store", closeErr)
	}

	opts := store.ReplayOpts{Actor: cliActor()}
	if f.from != "" {
		opts.From = &f.from
	}
	if f.failed {
		opts.FailedOnly = true
	}

	res, err := replayRunOnce(ctx, env, g, stateDir, detail.ID, opts)
	if err != nil {
		if mapped := replayError(err); mapped != nil {
			return mapped
		}
		return internalError("could not replay the run", err)
	}

	result := replayResult{
		RunID:    res.NewRunID,
		ReplayOf: detail.ID,
		Reused:   res.Reused,
		Rerun:    res.Rerun,
	}
	if out.mode == modeJSON {
		return out.json(result)
	}
	out.print("%s run %s is queued again as %s",
		out.symbols.ok, result.ReplayOf, result.RunID)
	if len(result.Reused) > 0 {
		out.print("  reused as references: %s", joinNames(result.Reused))
	}
	out.print("  running once more: %s", joinNames(result.Rerun))
	return nil
}

// joinNames renders a step list for a human line, and says plainly when the
// list is empty instead of printing nothing after the colon.
func joinNames(names []string) string {
	if len(names) == 0 {
		return "(none)"
	}
	out := names[0]
	for _, name := range names[1:] {
		out += ", " + name
	}
	return out
}

// replayRunOnce performs the materialization on one of the two write paths,
// exactly like its retry twin: through the daemon that serves this state
// directory when there is one, on the database directly when there is not.
// A refusal that came back from the daemon is an answer, not an outage.
func replayRunOnce(ctx context.Context, env Env, g *globals, stateDir, runID string, opts store.ReplayOpts) (store.ReplayResult, error) {
	socketPath, err := daemonSocket(env, g)
	if err != nil {
		return store.ReplayResult{}, socketRefusedError(err)
	}
	if socketPath != "" {
		res, err := replayViaSocket(ctx, socketPath, runID, opts)
		if err == nil {
			return res, nil
		}
		if stop := stopOnRefusal(err); stop != nil {
			return store.ReplayResult{}, stop
		}
		var refusal *socketRefusal
		if errors.As(err, &refusal) {
			return store.ReplayResult{}, err
		}
	}
	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clockForEnv(env)})
	if err != nil {
		return store.ReplayResult{}, err
	}
	defer func() { _ = s.Close() }()
	return s.MaterializeReplay(ctx, runID, opts)
}

// replayError maps the refusals a replay can meet, from either path, onto
// the exit codes the contract fixes. Nil means nothing matched and the
// caller reports an internal failure with the error it holds.
func replayError(err error) error {
	var refusal *socketRefusal
	if errors.As(err, &refusal) {
		switch refusal.code {
		case "not_found":
			return notFoundError(refusal.message, "", "check the id: paceq explains it on every failure it reports")
		case "invalid_state":
			return validationError(refusal.message, err)
		default:
			return internalError("the daemon could not replay the run", err)
		}
	}
	switch {
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrRunNotFound):
		return notFoundError(err.Error(), "", "check the id: paceq explains it on every failure it reports")
	case errors.Is(err, store.ErrConflictingReuse),
		errors.Is(err, store.ErrRunNotTerminal),
		errors.Is(err, store.ErrStepNotInThisRun):
		return validationError(err.Error(), err)
	case errors.Is(err, store.ErrLeaseLost):
		return busyError(err)
	}
	return nil
}

// replayRequest is the body the CLI sends the daemon for one replay request.
type replayRequest struct {
	From   string `json:"from,omitempty"`
	Failed bool   `json:"failed,omitempty"`
}

func replayViaSocket(ctx context.Context, socketPath, runID string, opts store.ReplayOpts) (store.ReplayResult, error) {
	body := replayRequest{Failed: opts.FailedOnly}
	if opts.From != nil {
		body.From = *opts.From
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return store.ReplayResult{}, err
	}
	var res store.ReplayResult
	if err := postForJSON(ctx, socketPath, "/v1/runs/"+runID+"/replay", payload, &res); err != nil {
		return store.ReplayResult{}, err
	}
	return res, nil
}
