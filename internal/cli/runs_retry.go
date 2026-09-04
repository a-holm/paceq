package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// runs retry is the operator surface of the run level reopen (M4-04, T14).
// The command exists so that a person can take a finished run and open it
// again: the run keeps its identity, its failed and skipped steps wait to be
// claimed once more, and the fencing token moves so nothing from the closed
// attempt can write behind the new one. Nothing here runs a step; execution
// belongs to whichever executor claims the run next.

// retryResult is what one retry did, as a script reads it.
type retryResult struct {
	RunID    string   `json:"run_id"`
	NewEpoch int64    `json:"new_epoch"`
	Reopened []string `json:"reopened"`
}

type retryFlags struct {
	step  string
	force bool
}

func newRunsRetryCmd(env Env, g *globals) *cobra.Command {
	var f retryFlags
	cmd := &cobra.Command{
		Use:   "retry <run>",
		Short: "Reopen a failed or cancelled run so it tries again",
		Long: `Open a finished run again, in place: the run keeps its id, its history
and its dedup key, and every step that ended failed or skipped goes back to
pending. Steps that succeeded are never run again; their outputs stay exactly
as they are.

This is the only way a finished run starts again, and it is never done by
paceq itself: an operator types this command, the event history records who,
and the lease epoch rises so a writer from the closed attempt stays shut out.

A step whose outcome was never recorded (its executor died mid flight) is a
deliberate double effect risk: rerunning it may do its work twice. The command
says so and refuses without --force.`,
		Args: exactArgs(1, "one run id or prefix"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runRunsRetry(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().StringVar(&f.step, "step", "",
		"reopen only this step and everything downstream of it")
	cmd.Flags().BoolVar(&f.force, "force", false,
		"reopen even though the last attempt may have an unknown outcome")
	return cmd
}

func runRunsRetry(ctx context.Context, env Env, g *globals, out *ui, runArg string, f retryFlags) error {
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

	// Resolve the id or prefix first against a read-only store, so the
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

	// The unknown outcome veto (AC-10, 02 R14). A run an executor died
	// holding may already have done part of its work: rerunning those
	// steps can do it twice. The command says exactly which evidence it
	// saw, refuses, and goes on only for an operator who passed --force.
	// The check sits before both write paths, so the same refusal comes
	// back whether the reopen travels through the daemon or not.
	if facts := unknownOutcomeFacts(detail); len(facts) > 0 && !f.force {
		return validationError(
			"the last attempt of this run has an outcome nobody recorded; --force is required to retry it",
			nil,
			append(facts,
				"rerunning a step whose outcome was never written may repeat its work",
				"pass --force if that double effect is accepted",
			)...,
		)
	}

	opts := store.ReopenOpts{Forced: f.force}
	if f.step != "" {
		opts.OnlyStep = &f.step
	}

	res, err := retryRunOnce(ctx, env, g, stateDir, detail.ID, opts)
	if err != nil {
		if mapped := reopenError(err); mapped != nil {
			return mapped
		}
		return internalError("could not reopen the run", err)
	}

	result := retryResult{
		RunID:    detail.ID,
		NewEpoch: res.NewEpoch,
		Reopened: res.Reopened,
	}
	if out.mode == modeJSON {
		return out.json(result)
	}
	out.print("%s run %s is queued again at lease epoch %d",
		out.symbols.ok, result.RunID, result.NewEpoch)
	out.print("  pending once more: %s", strings.Join(result.Reopened, ", "))
	return nil
}

// unknownOutcomeFacts collects the evidence that a run's finished attempt
// cannot be assumed clean: executors died holding it (crash_count), or a
// step's verdict was lost with its executor and never written. Empty means
// every outcome on record was reported by a process that lived to see the
// end of its work, so a plain retry carries no double effect risk.
func unknownOutcomeFacts(detail store.RunDetail) []string {
	var facts []string
	if detail.CrashCount > 0 {
		facts = append(facts, fmt.Sprintf("crash_count is %d: an executor died holding this run",
			detail.CrashCount))
	}
	for _, step := range detail.Steps {
		if step.ReasonCode == string(reason.STEPFailedExecutorLost) {
			facts = append(facts, fmt.Sprintf("step %s ended %s with its verdict lost (%s)",
				step.Name, step.State, reason.STEPFailedExecutorLost))
		}
	}
	return facts
}

// retryRunOnce performs the reopen on one of the two write paths. When a
// daemon serves this state directory the work goes over its socket, because
// the daemon owns the state while it runs; without one the command opens the
// store itself. Both paths end in the same transaction, so the answer means
// the same thing whichever one carried it. A refusal that came back from the
// daemon is an answer, not an outage: it is returned as is instead of being
// retried against a database the daemon holds.
func retryRunOnce(ctx context.Context, env Env, g *globals, stateDir, runID string, opts store.ReopenOpts) (store.ReopenResult, error) {
	socketPath, err := daemonSocket(env, g)
	if err != nil {
		return store.ReopenResult{}, socketRefusedError(err)
	}
	if socketPath != "" {
		res, err := retryViaSocket(ctx, socketPath, runID, opts)
		if err == nil {
			return res, nil
		}
		if stop := stopOnRefusal(err); stop != nil {
			return store.ReopenResult{}, stop
		}
		var refusal *socketRefusal
		if errors.As(err, &refusal) {
			return store.ReopenResult{}, err
		}
	}
	if stop := daemonHoldsState(ctx, env, g, runID+" was not retried"); stop != nil {
		return store.ReopenResult{}, stop
	}
	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clockForEnv(env)})
	if err != nil {
		return store.ReopenResult{}, err
	}
	defer func() { _ = s.Close() }()
	return s.ReopenTerminalRunByOperator(ctx, runID, cliActor(), opts)
}

// runLookupError renders the refusals both retry and replay give for a name
// that does not resolve, so the two commands speak with one voice.
func runLookupError(err error, runArg string) error {
	switch {
	case errors.Is(err, store.ErrAmbiguousRunID):
		return notFoundError(
			fmt.Sprintf("%q does not name one run: the prefix matches more than one", runArg),
			ambiguousHint(err),
			"give more characters until the prefix names exactly one run",
		)
	case errors.Is(err, store.ErrRunNotFound), errors.Is(err, id.ErrInvalid):
		return notFoundError(
			fmt.Sprintf("no run matches %q", runArg),
			runArg,
			"the ids of finished runs, shortest first: any prefix names a run as soon as it can",
			"check the id: paceq explains it on every failure it reports",
		)
	default:
		return internalError("could not read the run", err)
	}
}

// reopenError maps the refusals a reopen can meet, from either path, onto
// the exit codes the contract fixes. Nil means nothing matched and the
// caller reports an internal failure with the error it holds.
func reopenError(err error) error {
	var refusal *socketRefusal
	if errors.As(err, &refusal) {
		switch refusal.code {
		case "not_found":
			return notFoundError(refusal.message, "", "check the id: paceq explains it on every failure it reports")
		case "invalid_state":
			return validationError(refusal.message, err)
		default:
			return internalError("the daemon could not reopen the run", err)
		}
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		return notFoundError(err.Error(), "", "check the id: paceq explains it on every failure it reports")
	case errors.Is(err, store.ErrRunNotRetryable),
		errors.Is(err, store.ErrNothingToReopen),
		errors.Is(err, store.ErrRunNotTerminal),
		errors.Is(err, store.ErrStepNotInThisRun):
		return validationError(err.Error(), err)
	case errors.Is(err, store.ErrLeaseLost):
		return busyError(err)
	}
	return nil
}

// retryRequest is the body the CLI sends the daemon for one reopen request.
type retryRequest struct {
	Step  string `json:"step,omitempty"`
	Force bool   `json:"force,omitempty"`
}

func retryViaSocket(ctx context.Context, socketPath, runID string, opts store.ReopenOpts) (store.ReopenResult, error) {
	body := retryRequest{Force: opts.Forced}
	if opts.OnlyStep != nil {
		body.Step = *opts.OnlyStep
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return store.ReopenResult{}, err
	}
	var res store.ReopenResult
	if err := postForJSON(ctx, socketPath, "/v1/runs/"+runID+"/retry", payload, &res); err != nil {
		return store.ReopenResult{}, err
	}
	return res, nil
}
