package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/explain"
	"github.com/a-holm/paceq/internal/status"
)

// status is the morning view (#30): one line per job on one screen, problems
// at the top, an aggregate above them, and a hint pointing at explain under
// every deviation. It reads through the read-only pool, so it answers with
// the daemon down, and its exit code carries the verdict for MOTD and
// monitoring scripts: 0 everything inside its window, 5 a job needs a human,
// 3 a reference that names nothing.

type statusFlags struct {
	all bool
}

func newStatusCmd(env Env, g *globals) *cobra.Command {
	f := &statusFlags{}
	cmd := &cobra.Command{
		Use:   "status [ref]",
		Short: "One screen of job health: last outcome, next run, deviations first",
		Long: `Read the whole project at a glance, or one subject in full.

With no argument, one line per job: its newest finished run, how long it
took, what happened, and when it fires next. Deviations sort to the top, an
aggregate line opens the view, and every deviation carries a hint naming the
explain command that tells its story. More jobs than fit one screen fold
into "... and N more"; --all lifts the fold.

With a reference - job/x, schedule/job.name, sensor/x or run/<id>, bare
names resolve heuristically - the command draws that subject's status block.

The command reads the state database directly, so the daemon being down
changes nothing except the daemon field itself, which then reads up:false
and says so in the text form.

Exit codes are half the contract (monitoring runs this in a cron): 0 when
everything is inside its window, 5 when a job has an unconfirmed failure, a
stuck run or a breached freshness SLA, 1 when paceq itself fails, 3 when a
reference names nothing. A paused job is an operator decision, not a
deviation, and never moves the exit code away from 0.`,
		Args: cobra.MaximumNArgs(1),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			ref := ""
			if len(args) == 1 {
				ref = args[0]
			}
			return runStatus(ctx, env, g, out, ref, *f)
		}),
	}
	cmd.Flags().BoolVar(&f.all, "all", false,
		"list every job instead of folding past one screen")
	return cmd
}

func runStatus(ctx context.Context, env Env, g *globals, out *ui, ref string, f statusFlags) error {
	if _, statErr := g.stateDir(env); statErr != nil {
		return statErr
	}
	if ref != "" {
		return runStatusRef(ctx, env, g, out, ref)
	}
	return runStatusOverview(ctx, env, g, out, f)
}

// probeDaemon asks whether a daemon answers right now: a socket file paceq
// trusts gets one short dial, everything else reads as down. A socket it
// refuses is named on stderr and then read as down too, because a status
// report that will not print is a worse answer than one that says so.
func probeDaemon(env Env, g *globals, out *ui) bool {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return false
	}
	socketPath, err := daemonSocket(stateDir)
	if err != nil {
		fmt.Fprintf(out.err, "paceq: %v\n", err)
		return false
	}
	return daemonResponds(socketPath)
}

// runStatusOverview builds and renders the whole-project report.
func runStatusOverview(ctx context.Context, env Env, g *globals, out *ui, f statusFlags) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}

	daemonUp := probeDaemon(env, g, out)
	rep, err := status.Build(ctx, ro, status.Options{
		Clock:    clkOf(env),
		DaemonUp: daemonUp,
	})
	// The instance marker is read beside the report rather than inside it:
	// #32 wants the morning view to say plainly when nothing below executes.
	var shadowLive bool
	if info, infoErr := ro.ShadowRuntime(ctx); infoErr == nil && info.Running {
		shadowLive = true
	}
	_ = ro.Close()
	if err != nil {
		return internalError("could not read the status", err)
	}

	if out.mode == modeJSON {
		if err := out.json(rep); err != nil {
			return err
		}
		return statusExitError(rep.Summary.Deviations, rep.Summary.Jobs)
	}
	if shadowLive {
		fmt.Fprintln(out.out, "== "+shadowBanner+" ==")
	}

	style := status.StyleASCII()
	if unicodeOutput(env) {
		style = status.StyleUnicode()
	}
	status.RenderText(out.out, rep, status.RenderOptions{
		Style: style,
		Color: out.color && !out.quiet,
		All:   f.all,
		Quiet: out.quiet,
	})
	return statusExitError(rep.Summary.Deviations, rep.Summary.Jobs)
}

// statusExitError turns a deviation count into exit 5. The report itself went
// to stdout as data; what comes back here is only the verdict a script branches
// on, said once, quietly.
func statusExitError(deviations, jobs int) error {
	if deviations == 0 {
		return nil
	}
	return &Error{
		code: ExitRunFailed,
		what: fmt.Sprintf("%d of %d jobs need attention", deviations, jobs),
		next: []string{
			"each flagged job above names the paceq explain command that tells its story",
		},
	}
}

// runStatusRef resolves one reference and renders its status block.
func runStatusRef(ctx context.Context, env Env, g *globals, out *ui, ref string) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}

	resolved, err := explain.Resolve(ctx, ro, ref)
	if err != nil {
		_ = ro.Close()
		return explainResolveError(err)
	}

	daemonUp := probeDaemon(env, g, out)
	rep, err := status.BuildSubject(ctx, ro, status.SubjectRef{
		Kind:     string(resolved.Kind),
		Job:      resolved.Job,
		Schedule: resolved.Schedule,
		Sensor:   resolved.Sensor,
		RunID:    resolved.RunID,
	}, status.Options{Clock: clkOf(env), DaemonUp: daemonUp})
	_ = ro.Close()
	if err != nil {
		return internalError("could not read the status of "+ref, err)
	}

	if out.mode == modeJSON {
		if err := out.json(rep); err != nil {
			return err
		}
	} else {
		renderRefBlock(out, rep)
	}
	if status.IsDeviation(rep.State) || refStateFailed(rep) {
		return &Error{
			code: ExitRunFailed,
			what: fmt.Sprintf("%s %s needs attention", rep.Subject.Kind, refTail(rep)),
			next: []string{"the block above names the explain command that answers why"},
		}
	}
	return nil
}

// refStateFailed covers the kinds whose own vocabulary, not the job-state
// set, decides the exit: a run reference exits 5 exactly when the run failed,
// a sensor reference when it sits in the failure path.
func refStateFailed(rep *status.RefReport) bool {
	switch rep.Subject.Kind {
	case "run":
		return rep.Run != nil && rep.Run.State == "failed"
	case "sensor":
		return rep.Sensor != nil && rep.Sensor.ConsecutiveFailures > 0
	}
	return false
}

func refTail(rep *status.RefReport) string {
	switch rep.Subject.Kind {
	case "schedule":
		return rep.Subject.Job + "." + rep.Subject.Schedule
	case "sensor":
		return rep.Subject.Sensor
	case "run":
		return rep.Subject.RunID
	default:
		return rep.Subject.Job
	}
}

// renderRefBlock draws one subject's status block: kind header, then facts,
// then the hint when there is something to explain.
func renderRefBlock(out *ui, rep *status.RefReport) {
	style := status.StyleASCII()
	if out.symbols == unicodeSymbols {
		style = status.StyleUnicode()
	}
	dash := style.Dash
	fmt.Fprintf(out.out, "%s %s\n", rep.Subject.Kind, refTail(rep))
	if rep.InstanceShadow || rep.ScheduleShadow {
		// The line that must be impossible to miss (#32).
		fmt.Fprintf(out.out, "SHADOW MODE: nothing executes - every tick is a recorded decision only\n")
	}
	fmt.Fprintf(out.out, "state: %s\n", rep.State)
	if !rep.Daemon.Up {
		fmt.Fprintf(out.out, "daemon: down\n")
	}
	switch rep.Subject.Kind {
	case "job":
		if rep.Paused {
			fmt.Fprintf(out.out, "paused by an operator - schedules stand down, manual runs still go\n")
		}
		if l := rep.LastRun; l != nil {
			took := dash
			if l.DurationMS > 0 {
				took = durationWords(l.DurationMS)
			}
			at := l.FinishedAt
			if at == "" {
				at = dash
			}
			fmt.Fprintf(out.out, "last run: %s  %s  took %s  outcome %s\n",
				shortID(l.ID), at, took, l.Outcome)
		} else {
			fmt.Fprintf(out.out, "no finished run yet\n")
		}
		if next := rep.NextRunAt; next != "" {
			fmt.Fprintf(out.out, "next run: %s\n", next)
		}
	case "schedule":
		s := rep.Schedule
		fmt.Fprintf(out.out, "%s %s (%s)\n", s.Kind, s.Expr, s.Timezone)
		if s.NextTickAt != "" {
			fmt.Fprintf(out.out, "next tick: %s\n", s.NextTickAt)
		}
		if rep.Paused {
			fmt.Fprintf(out.out, "paused by an operator\n")
		}
	case "sensor":
		s := rep.Sensor
		fmt.Fprintf(out.out, "every %s  consecutive failures %d\n",
			durationWords(s.IntervalMS), s.ConsecutiveFailures)
		if s.LastOutcome != "" {
			fmt.Fprintf(out.out, "last outcome: %s\n", s.LastOutcome)
		}
		if s.NextEvalAt != "" {
			fmt.Fprintf(out.out, "next evaluation: %s\n", s.NextEvalAt)
		}
		if rep.Paused {
			fmt.Fprintf(out.out, "paused: %s\n", strings.TrimSpace(" "+s.PausedReason))
		}
	case "run":
		r := rep.Run
		fmt.Fprintf(out.out, "job %s  origin %s  state %s\n", r.JobName, r.Origin, r.State)
		if r.ReasonCode != "" {
			fmt.Fprintf(out.out, "reason: %s\n", r.ReasonCode)
		}
		if r.StartedAt != "" {
			took := dash
			if r.DurationMS > 0 {
				took = durationWords(r.DurationMS)
			}
			fmt.Fprintf(out.out, "started %s  finished %s  took %s\n",
				r.StartedAt, orDash(r.FinishedAt, dash), took)
		}
	}
	if rep.Hint != "" {
		fmt.Fprintf(out.out, "  %s run `%s`\n", style.Branch, rep.Hint)
	}
}

func shortID(id string) string {
	const idWidth = 12
	if len(id) <= idWidth {
		return id
	}
	return id[:idWidth]
}

func orDash(s, dash string) string {
	if s == "" {
		return dash
	}
	return s
}

// durationWords renders milliseconds the way the status blocks say them.
func durationWords(ms int64) string {
	if ms <= 0 {
		return "0s"
	}
	d := time.Duration(ms) * time.Millisecond
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", ms)
	case d < time.Minute:
		return fmt.Sprintf("%.0fs", d.Seconds())
	case d < time.Hour:
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		if s > 0 {
			return fmt.Sprintf("%dm%ds", m, s)
		}
		return fmt.Sprintf("%dm", m)
	default:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		if m > 0 {
			return fmt.Sprintf("%dh%dm", h, m)
		}
		return fmt.Sprintf("%dh", h)
	}
}
