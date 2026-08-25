package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/explain"
)

// paceq explain answers the only question an operator asks after silence:
// why did nothing happen. It reads the decisions the system already recorded,
// never re-derives them, and works whether the daemon is up or not.

const explainDefaultSince = 48 * time.Hour

type explainFlags struct {
	since string
}

func newExplainCmd(env Env, g *globals) *cobra.Command {
	f := &explainFlags{}
	cmd := &cobra.Command{
		Use:   "explain [job|schedule|sensor|run] <ref>",
		Short: "Explain what a job, schedule, sensor or run did, and why nothing happened",
		Long: `Answer "why did nothing happen" from what was actually decided.

Every evaluation, trigger, run and step records its outcome and its reason
code. explain reads those rows back - it never guesses what should have
happened - and prints them newest first, each terminal decision with what to
do next.

References follow one shape:

  job/nightly-report          a whole job: every schedule and sensor feeding it
  schedule/nightly.hourly     one schedule of one job (job/name works too)
  sensor/dropzone             one sensor
  run/01JQ9F2M4K              one run, by id or any prefix that names it

A bare name resolves heuristically; when a name could mean two things, the
refusal says exactly which forms to type.

A bare reference works too - paceq explain nightly - and is resolved
heuristically; when a name could mean two things the refusal lists every
reading.

explain reads the state database directly, so it answers with the daemon
stopped. The window is set with --since (a duration such as 48h or 10m).`,
	}
	cmd.PersistentFlags().StringVar(&f.since, "since", "",
		"how far back to look (a duration: 48h, 10m; default 48h)")
	cmd.RunE = runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
		if len(args) != 1 {
			return usageError(fmt.Sprintf("explain takes one bare reference, got %d arguments", len(args)),
				"paceq explain job|schedule|sensor|run <ref>  names the subject kind")
		}
		return runExplain(ctx, env, g, out, "", args[0], *f)
	})
	for _, noun := range []string{"job", "schedule", "sensor", "run"} {
		cmd.AddCommand(newExplainNounCmd(env, g, noun, f))
	}
	return cmd
}

func newExplainNounCmd(env Env, g *globals, noun string, f *explainFlags) *cobra.Command {
	sub := &cobra.Command{
		Use:   noun + " <ref>",
		Short: explainShortFor(noun),
		Args:  exactArgs(1, "one reference such as "+noun+"/<name>"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runExplain(ctx, env, g, out, noun, args[0], *f)
		}),
	}
	return sub
}

func explainShortFor(noun string) string {
	switch noun {
	case "run":
		return "Explain one run: every step, attempt and reason"
	default:
		return fmt.Sprintf("Explain %s <ref>: the decision timeline behind it", noun)
	}
}

// runExplain resolves the reference, builds the report and renders it. Exit
// codes are the pinned contract: 0 a report was produced (even when
// everything is fine), 2 a bad reference or flag, 3 a name that matches
// nothing. explain never exits 5: it reports failures, it does not have them.
func runExplain(ctx context.Context, env Env, g *globals, out *ui, noun, ref string, f explainFlags) error {
	if mismatch := refKindMismatch(noun, ref); mismatch != "" {
		return usageError(mismatch,
			"write job/<name>, schedule/<job>.<name>, sensor/<name> or run/<id>")
	}

	since := explainDefaultSince
	if f.since != "" {
		parsed, err := time.ParseDuration(f.since)
		if err != nil || parsed <= 0 {
			return usageError(fmt.Sprintf("--since %q is not a positive duration", f.since),
				"write 48h, 10m or 30s")
		}
		since = parsed
	}

	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	resolved, err := explain.Resolve(ctx, ro, ref)
	if err != nil {
		return explainResolveError(err)
	}

	daemonUp := false
	stateDir, dirErr := g.stateDir(env)
	if dirErr == nil {
		daemonUp = daemonResponds(daemonSocket(stateDir))
	}
	if !daemonUp {
		// A warning, never data: stdout stays clean for pipes either way.
		fmt.Fprintln(out.err, "paceq: (daemon down - answering from the state database, read-only)")
	}

	now := clockForEnv(env).Now().UTC()
	report, err := explain.Build(ctx, ro, resolved, explain.Options{
		Since:    now.Add(-since),
		Clock:    clockForEnv(env),
		DaemonUp: daemonUp,
	})
	if err != nil {
		return internalError("could not build the explanation", err)
	}
	_ = ro.Close()

	if out.mode == modeJSON {
		return out.json(report)
	}
	style := explain.StyleASCII()
	if unicodeOutput(env) {
		style = explain.StyleUnicode()
	}
	explain.RenderText(out.out, report, style)
	return nil
}

// refKindMismatch refuses the mixed spellings early ("explain run job/x"),
// because a command named one thing but asking for another is a typo, not a
// question about data.
func refKindMismatch(noun, ref string) string {
	if noun == "" {
		return "" // the bare form accepts every reference spelling
	}
	prefix, _, found := strings.Cut(ref, "/")
	if !found {
		return ""
	}
	switch prefix {
	case "job", "jobs":
		if noun != "job" {
			return fmt.Sprintf("%s takes a %s reference, got %q", noun, noun, ref)
		}
	case "run", "runs":
		if noun != "run" {
			return fmt.Sprintf("%s takes a %s reference, got %q", noun, noun, ref)
		}
	case "sensor", "sensors":
		if noun != "sensor" {
			return fmt.Sprintf("%s takes a %s reference, got %q", noun, noun, ref)
		}
	case "schedule", "schedules":
		if noun != "schedule" && noun != "job" {
			return fmt.Sprintf("%s takes a %s reference, got %q", noun, noun, ref)
		}
	}
	return ""
}

// explainResolveError maps the resolver's typed refusals onto the pinned exit
// codes, carrying the candidate lists into the message.
func explainResolveError(err error) error {
	var syntax *explain.Syntax
	if errors.As(err, &syntax) {
		return usageError(syntax.What,
			"write job/<name>, schedule/<job>.<name>, sensor/<name> or run/<id-or-prefix>")
	}
	var ambiguous *explain.Ambiguous
	if errors.As(err, &ambiguous) {
		next := append([]string{"give more characters, or name one exactly:"}, ambiguous.Candidates...)
		return notFoundError(ambiguous.What, "", next...)
	}
	var missing *explain.NotFound
	if errors.As(err, &missing) {
		next := []string{"paceq ls  shows what this project knows"}
		if len(missing.Candidates) > 0 {
			next = append(next, missing.Candidates...)
		} else {
			next = append(next, "write job/<name>, schedule/<job>.<name>, sensor/<name> or run/<id-or-prefix>")
		}
		return notFoundError(missing.What, "", next...)
	}
	return internalError("could not resolve the reference", err)
}

// daemonResponds reports whether a daemon answers on the socket right now.
// Half a second is generous for a local unix socket; a dead socket file or a
// refused dial both read as "down".
func daemonResponds(socketPath string) bool {
	if socketPath == "" {
		return false
	}
	conn, err := net.DialTimeout("unix", socketPath, 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
