package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/api"
	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// The run command turns the binary into the executor: the engine is wired up
// inside this process and drives the run to its end. There is no daemon to
// start, which is the whole point of M1, and there is no second execution
// path: everything below hands the run to engine.ExecuteRun.

type runFlags struct {
	wait   bool
	params []string
}

func newRunCmd(env Env, g *globals) *cobra.Command {
	var f runFlags
	cmd := &cobra.Command{
		Use:   "run <job>",
		Short: "Run a job now, in this process, and wait for it",
		Long: `Queue a manual run of the job and execute it right here, waiting for
every step. Nothing else has to be running: the executor lives inside this
command, the state goes to the database, and the logs land under the state
directory like any other run's.

The exit code tells a cron migration what happened: 5 means the job failed
and paceq worked, 1 means paceq itself failed, 8 means you stopped it. A job
that times out failed, so it is 5.

An interrupt asks for a cancellation the way everything else does, durably,
and the run ends cancelled. Pressing it again stops waiting the hard way.`,
		Example: `  paceq run nightly
    Queue a run of nightly and wait for it here.

  paceq run import --param since=2026-09-01 -o json > run.json
    Run with parameters and keep the machine readable record; the exit
    code still tells cron whether the job itself failed (5) or paceq
    did (1).`,
		Args: exactArgs(1, "one job name"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runRun(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().BoolVar(&f.wait, "wait", true,
		"wait for the run to end (a foreground run always waits; the flag says so out loud)")
	cmd.Flags().StringSliceVar(&f.params, "param", nil,
		"a run parameter, as name=value (repeatable, values are strings)")
	return cmd
}

// runRun is the whole five minute experience in one function: queue, execute,
// report, and hand back the exit code the shell will see.
func runRun(ctx context.Context, env Env, g *globals, out *ui, jobName string, f runFlags) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if _, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		return usageError(
			fmt.Sprintf("there is no state database at %s yet", displayPath(env, dbPath)),
			"paceq init  creates a project with a state directory and an example job",
		)
	}
	params, err := paramsJSON(f.params)
	if err != nil {
		return err
	}
	paramPairs, mapErr := paramsMap(f.params)
	if mapErr != nil {
		return mapErr
	}

	// The executor runs detached from the caller's context. An interrupt
	// must reach the database as a cancellation request rather than cut a
	// transaction in half, so the context below never dies on a signal;
	// only the second interrupt cancels it outright.
	execCtx, hardStop := context.WithCancel(context.WithoutCancel(ctx))
	defer hardStop()

	plan := planWrite(execCtx, env, g)
	if plan.err != nil {
		return plan.err
	}
	if plan.client != nil {
		defer func() { _ = plan.client.Close() }()
		return runSocketRun(execCtx, env, g, out, plan.client, stateDir, jobName, paramPairs, hardStop)
	}

	s := plan.st
	defer func() { _ = s.Close() }()

	actor := cliActor()
	queued, err := s.MaterializeManualTrigger(execCtx, store.ManualTriggerInput{
		JobName:    jobName,
		Actor:      actor,
		ParamsJSON: params,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return unknownJobError(execCtx, s, jobName)
		}
		return internalError("could not queue the run", err)
	}

	// Signals arrive on their own channel and become cancellation requests.
	// The main binary also cancels its own context on a signal; this command
	// ignores that context on purpose, as written above.
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	finished := make(chan struct{})
	go func() {
		select {
		case <-sigs:
			_, _ = s.RequestCancel(context.WithoutCancel(execCtx),
				queued.Run.ID, actor, "interrupted")
			select {
			case <-sigs:
				hardStop()
			case <-finished:
			}
		case <-finished:
		}
	}()

	say(out, "run %s  job %s", queued.Run.ID, queued.Run.JobName)

	executor := &engine.Engine{
		Store:   s,
		LogRoot: logsink.NewRoot(stateDir),
		Clock:   clkOf(env),
		Owner:   actor,
	}
	// A foreground run renews its lease from its own goroutine: a long step
	// must never cost the executor its claim just because the renewal could
	// not run inside the work (11 R3).
	hbCtx, stopRenewals := context.WithCancel(execCtx)
	defer stopRenewals()
	go func() { _ = executor.RunLeaseRenewals(hbCtx) }()

	state, err := executor.ExecuteRun(execCtx, queued.Run.ID)
	close(finished)
	if err != nil {
		return err
	}

	detail, err := s.GetRun(context.WithoutCancel(execCtx), queued.Run.ID)
	if err != nil {
		return internalError("could not read back the finished run", err)
	}
	if err := writeRunRecord(out, detail); err != nil {
		return err
	}

	duration := durationOf(detail.StartedAt, detail.FinishedAt)
	switch {
	case state == string(model.RunCancelled):
		return &Error{
			code: ExitInterrupted,
			what: "the run was cancelled before it finished",
			next: []string{
				"nothing was left half written: every change paceq makes is one transaction",
				"paceq logs " + detail.ID + "  shows how far it got",
			},
		}
	case state == string(model.RunFailed):
		// The job failed and paceq worked. That difference is the whole
		// contract, so the two never share an exit code.
		return &Error{
			code: ExitRunFailed,
			what: fmt.Sprintf("the job failed after %s: %s", duration, outcomeText(detail.ReasonCode, detail.ReasonData)),
			next: []string{
				"paceq logs " + detail.ID + "  shows the output of every step",
				"paceq error " + detail.ReasonCode + "  explains that code in full",
			},
		}
	}

	say(out, "%s run %s  ok  %s", out.symbols.ok, detail.ID, duration)
	return nil
}

// runSocketRun is the daemon-up half of `paceq run`: the run is queued over
// the socket, the daemon's own executor carries it out, and this process
// polls the read-only pool until the run ends. The exit codes are the ones
// the local path hands out, so a cron line cannot tell the two apart.
func runSocketRun(ctx context.Context, env Env, g *globals, out *ui, client *api.Client,
	stateDir, jobName string, params map[string]string, hardStop func(),
) error {
	runID, err := client.CreateRun(ctx, jobName, params)
	if err != nil {
		var wire *api.WireError
		if errors.As(err, &wire) {
			return wireFailure(env, wire)
		}
		return internalError("could not reach the daemon", err)
	}

	// Signals become durable cancellation requests on the same wire: the
	// first asks, the second stops waiting the hard way, exactly like the
	// local executor's contract.
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	cancelRequested := false
	go func() {
		select {
		case <-sigs:
			if !cancelRequested {
				cancelRequested = true
				_ = client.CancelRun(context.WithoutCancel(ctx), runID, "interrupted")
				return
			}
			hardStop()
		case <-ctx.Done():
		}
	}()

	say(out, "run %s  job %s", runID, jobName)

	ro, err := store.OpenReadOnly(context.WithoutCancel(ctx),
		filepath.Join(stateDir, store.DatabaseFileName), store.Options{Clock: clkOf(env)})
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	clk := clkOf(env)
	ticker := clk.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var detail store.RunDetail
	for {
		detail, err = ro.GetRun(context.WithoutCancel(ctx), runID)
		switch {
		case err == nil && terminalRunState(detail.State):
			// fall through to the report
		case errors.Is(err, store.ErrRunNotFound) || errors.Is(err, id.ErrInvalid):
			return notFoundError(
				fmt.Sprintf("the daemon queued %s but no such run can be read", runID),
				err.Error(),
				"paceq doctor  checks the installation")
		default:
			select {
			case <-ctx.Done():
				return interruptedError(ctx.Err())
			case <-ticker.C:
				continue
			}
		}
		break
	}

	if err := writeRunRecord(out, detail); err != nil {
		return err
	}

	duration := durationOf(detail.StartedAt, detail.FinishedAt)
	switch {
	case detail.State == string(model.RunCancelled):
		return &Error{
			code: ExitInterrupted,
			what: "the run was cancelled before it finished",
			next: []string{
				"nothing was left half written: every change paceq makes is one transaction",
				"paceq logs " + detail.ID + "  shows how far it got",
			},
		}
	case detail.State == string(model.RunFailed):
		return &Error{
			code: ExitRunFailed,
			what: fmt.Sprintf("the job failed after %s: %s", duration, outcomeText(detail.ReasonCode, detail.ReasonData)),
			next: []string{
				"paceq logs " + detail.ID + "  shows the output of every step",
				"paceq error " + detail.ReasonCode + "  explains that code in full",
			},
		}
	}

	say(out, "%s run %s  ok  %s", out.symbols.ok, detail.ID, duration)
	return nil
}

// terminalRunState says whether a run state ends the wait.
func terminalRunState(state string) bool {
	switch model.RunState(state) {
	case model.RunSucceeded, model.RunFailed, model.RunCancelled:
		return true
	}
	return false
}

// say writes one progress line to stderr. Progress is never data: a pipe of
// run records stays parseable with these on.
func say(out *ui, format string, args ...any) {
	if out.quiet {
		return
	}
	fmt.Fprintf(out.err, format+"\n", args...)
}

// cliActor is who the history books say pressed the button.
func cliActor() string {
	return fmt.Sprintf("cli:%d", os.Getuid())
}

// paramsJSON turns repeated name=value flags into the canonical parameter
// object a run carries. Values are strings: parameters that need structure
// belong in the job file until a real case says otherwise.
func paramsJSON(pairs []string) (string, error) {
	params, err := paramsMap(pairs)
	if err != nil {
		return "", err
	}
	if len(params) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return "", internalError("could not encode the parameters", err)
	}
	return string(encoded), nil
}

// paramsMap is paramsJSON without the encoding, for callers that hand the
// parameters to the socket client instead.
func paramsMap(pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	params := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		name, value, found := strings.Cut(pair, "=")
		if !found || name == "" {
			return nil, usageError(
				fmt.Sprintf("--param %q is not a name=value pair", pair),
				"write it as --param name=value",
			)
		}
		params[name] = value
	}
	return params, nil
}

// unknownJobError is the exit 3 refusal, with the did you mean that teaches
// the verb from the noun (03 section 8.3).
func unknownJobError(ctx context.Context, s *store.Store, name string) error {
	names, err := s.JobNames(ctx)
	if err != nil {
		names = nil
	}
	next := []string{"paceq apply  loads the job files of this project"}
	if suggestion := diag.Suggest(name, names); suggestion != "" {
		next = append([]string{
			fmt.Sprintf("did you mean %q?", suggestion),
			"paceq run " + suggestion + "  starts that job",
		}, next...)
	} else if len(names) > 0 {
		next = append([]string{"the jobs this project knows: " + strings.Join(names, ", ")}, next...)
	}
	return notFoundError(fmt.Sprintf("no job is named %q", name),
		"the jobs recorded by paceq apply", next...)
}

// stepRecord is one step of the machine readable result.
type stepRecord struct {
	Name       string `json:"name"`
	State      string `json:"state"`
	ReasonCode string `json:"reason_code,omitempty"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	LogPath    string `json:"log_path,omitempty"`
}

// runDetailRecord is the run as a script reads it. The shape is the same one
// the later HTTP API exposes, so a jq written today keeps working.
type runDetailRecord struct {
	ID         string       `json:"id"`
	Job        string       `json:"job"`
	Origin     string       `json:"origin,omitempty"`
	State      string       `json:"state"`
	ReasonCode string       `json:"reason_code,omitempty"`
	CreatedAt  string       `json:"created_at,omitempty"`
	StartedAt  string       `json:"started_at,omitempty"`
	FinishedAt string       `json:"finished_at,omitempty"`
	DurationMS int64        `json:"duration_ms,omitempty"`
	Steps      []stepRecord `json:"steps"`
}

type runEnvelope struct {
	Run      runDetailRecord `json:"run"`
	DaemonUp *bool           `json:"daemon_up,omitempty"`
}

// writeRunRecord puts the finished run on stdout in JSON mode. It is written
// for failures too: a script that branches on the exit code often wants the
// reason beside it, and stderr carries the words for people.
func writeRunRecord(out *ui, detail store.RunDetail) error {
	return writeRunRecordField(out, detail, nil)
}

// writeRunRecordField is writeRunRecord with an optional daemon_up field for
// the envelope. Nil keeps the document exactly as it was before dual mode.
func writeRunRecordField(out *ui, detail store.RunDetail, daemonUp *bool) error {
	if out.mode != modeJSON {
		return nil
	}
	record := runDetailRecord{
		ID:         detail.ID,
		Job:        detail.JobName,
		Origin:     detail.Origin,
		State:      detail.State,
		ReasonCode: detail.ReasonCode,
		CreatedAt:  rfc3339(detail.CreatedAt),
		StartedAt:  rfc3339(detail.StartedAt),
		FinishedAt: rfc3339(detail.FinishedAt),
		DurationMS: millisBetween(detail.StartedAt, detail.FinishedAt),
		Steps:      make([]stepRecord, 0, len(detail.Steps)),
	}
	for _, step := range detail.Steps {
		var exit *int
		if step.HasExitCode {
			code := step.ExitCode
			exit = &code
		}
		record.Steps = append(record.Steps, stepRecord{
			Name:       step.Name,
			State:      step.State,
			ReasonCode: step.ReasonCode,
			ExitCode:   exit,
			DurationMS: step.DurationMS,
			LogPath:    step.LogPath,
		})
	}
	return out.json(runEnvelope{Run: record, DaemonUp: daemonUp})
}

// outcomeText is a reason code as a sentence fragment. The failed step's name
// rides in reason_data, and it is the one fact the operator needs first.
func outcomeText(code, data string) string {
	if code == string(reason.RUNFailedStep) {
		var d struct {
			Step string `json:"step"`
		}
		if json.Unmarshal([]byte(data), &d) == nil && d.Step != "" {
			return fmt.Sprintf("step %q failed (%s)", d.Step, code)
		}
	}
	if entry, ok := reason.Lookup(reason.Code(code)); ok {
		return fmt.Sprintf("%s (%s)", entry.Short, code)
	}
	return code
}

// rfc3339 renders a stamp for JSON output. A missing stamp stays missing.
func rfc3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

// millisBetween is a duration as the database stores it. Either end missing
// means no duration is known.
func millisBetween(start, end time.Time) int64 {
	if start.IsZero() || end.IsZero() {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

// durationOf renders a duration the way the tables do.
func durationOf(start, end time.Time) string {
	ms := millisBetween(start, end)
	if ms <= 0 {
		return "0ms"
	}
	return spec.FormatDuration(time.Duration(ms) * time.Millisecond)
}
