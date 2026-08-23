package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/store"
)

// followInterval is how often -f looks for new output. New lines appear
// within about twice this interval; reading the end of a file needs no
// filesystem events, just Stat and Read.
const followInterval = 100 * time.Millisecond

// terminalStepStates are the states after which nothing new can happen: a
// step stops being followed once it reaches one, and so does the run when all
// of its steps have.
var terminalStepStates = map[string]bool{
	"succeeded": true,
	"failed":    true,
	"cancelled": true,
	"skipped":   true,
}

type logsFlags struct {
	step        string
	attempt     int
	allAttempts bool
	follow      bool
}

func newLogsCmd(env Env, g *globals) *cobra.Command {
	var f logsFlags
	cmd := &cobra.Command{
		Use:   "logs <run>",
		Short: "Show the log files of one run",
		Long: `Show what the steps of a run wrote.

Logs are plain NDJSON files under the state directory; this command reads
them and adds nothing to them. A run id may be shortened to any prefix that
still names exactly one run.

Without --step every step is shown in spec order, with the step name as a
separator. Without --attempt the newest attempt of each step is shown;
--all-attempts shows every attempt there is a file for, oldest first.

-f follows the logs until the run ends. Interrupting it is not an error.`,
		Args: exactArgs(1, "one run id or prefix"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			if f.attempt > 0 && f.allAttempts {
				return usageError("--attempt and --all-attempts cannot be combined",
					"pick one attempt with --attempt N, or all of them with --all-attempts")
			}
			return runLogs(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().StringVar(&f.step, "step", "", "show only one step")
	cmd.Flags().IntVar(&f.attempt, "attempt", 0, "show one attempt (default: the newest)")
	cmd.Flags().BoolVar(&f.allAttempts, "all-attempts", false, "show every attempt of each step")
	cmd.Flags().BoolVarP(&f.follow, "follow", "f", false, "follow the logs until the run ends")
	return cmd
}

// logSource is one file the command will read: which step and attempt it
// belongs to and where the file sits relative to the log root.
type logSource struct {
	step    string
	attempt int
	relPath string
}

func runLogs(ctx context.Context, env Env, g *globals, out *ui, runArg string, f logsFlags) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if _, err := os.Stat(dbPath); err != nil {
		return notFoundError(
			fmt.Sprintf("there is no paceq state at %s", stateDir),
			stateDir,
			"paceq init  creates a project with its state directory",
			"run the command inside the project directory, or pass --db")
	}

	// Files and the read only database carry everything this command needs.
	// It works while the daemon is down because nothing here asks it. An
	// explicitly named socket that nobody answers still earns its line on
	// stderr, so a human reading -f output knows why nothing new appears.
	noteDaemon(resolveSocket(g, env, stateDir), env, out)
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

	root := logsink.NewRoot(stateDir)
	sources, missing, err := selectLogSources(root, detail, f)
	if err != nil {
		return err
	}
	// A plain read needs something to read. A follow can wait instead: a
	// live step writes its first line soon, and -f exists for exactly that
	// wait, so an empty list now only means "not yet".
	if len(sources) == 0 && !f.follow {
		where := fmt.Sprintf("steps of run %s", detail.Run.ID)
		if f.step != "" {
			where = fmt.Sprintf("step %s of run %s", f.step, detail.Run.ID)
		}
		return notFoundError(
			"no log files for this selection yet",
			where,
			"a log exists from the first line a running step writes",
			strings.Join(missing, "\n"),
		)
	}

	renderer := newLogRenderer(out, detail.Run.ID, len(sources) > 1 || f.allAttempts)
	if !f.follow {
		for _, src := range sources {
			if renderer.headerNeeded {
				out.print("-- %s, attempt %d --", src.step, src.attempt)
			}
			path, err := root.Abs(src.relPath)
			if err != nil {
				return err
			}
			if _, err := logsink.ReadFrom(path, 0, true, func(l logsink.Line) error {
				return renderer.emit(src.step, src.attempt, l)
			}); err != nil {
				return err
			}
		}
		return nil
	}
	return followLogs(ctx, clkOf(env), ro, root, renderer, detail.Run.ID, sources, f, out)
}

// selectLogSources turns the flags into the concrete list of files to read,
// in the order they will be shown: spec order of steps, attempts oldest
// first. Missing files are reported back rather than invented.
func selectLogSources(root logsink.Root, detail store.RunDetail, f logsFlags) ([]logSource, []string, error) {
	var sources, missing []logSource
	var missingText []string

	for _, step := range detail.Steps {
		if f.step != "" && step.Name != f.step {
			continue
		}
		files, err := root.AttemptFiles(detail.Run.ID, step.Name)
		if err != nil {
			return nil, nil, err
		}
		present := map[int]string{}
		for _, file := range files {
			present[file.Attempt] = file.RelPath
		}

		want := make([]int, 0, 4)
		switch {
		case f.allAttempts:
			for _, file := range files {
				want = append(want, file.Attempt)
			}
		case f.attempt > 0:
			want = append(want, f.attempt)
		case step.Attempt > 0 && present[step.Attempt] != "":
			want = append(want, step.Attempt)
		case step.Attempt > 0 && step.LogPath != "":
			// The row points at the attempt even if the glob above came
			// up empty: trust the database over a directory scan.
			want = append(want, step.Attempt)
			present[step.Attempt] = step.LogPath
		default:
			want = append(want, 1)
		}

		for _, n := range want {
			rel, ok := present[n]
			if !ok && step.LogPath != "" && n == step.Attempt {
				rel, ok = step.LogPath, true
			}
			if !ok {
				missing = append(missing, logSource{step: step.Name, attempt: n})
				missingText = append(missingText,
					fmt.Sprintf("no log for %s attempt %d", step.Name, n))
				continue
			}
			sources = append(sources, logSource{step: step.Name, attempt: n, relPath: rel})
		}
	}

	if f.step != "" && len(sources) == 0 && len(missing) == 0 {
		names := make([]string, 0, len(detail.Steps))
		for _, step := range detail.Steps {
			names = append(names, step.Name)
		}
		return nil, nil, notFoundError(
			fmt.Sprintf("run %s has no step %q", detail.Run.ID, f.step),
			strings.Join(names, ", "),
			"the step names are the ones the job defines",
		)
	}
	return sources, missingText, nil
}

// ambiguousHint pulls the two conflicting ids into one line for the where
// part of the refusal. GetRun writes them in parentheses at the end.
func ambiguousHint(err error) string {
	text := err.Error()
	if i := strings.Index(text, "("); i >= 0 {
		return strings.TrimSuffix(text[i:], ")")
	}
	return ""
}

// followLogs drains every selected file as it grows and stops once the run
// reached a terminal state and nothing new arrived. Cancelling the context
// ends it with exit 0: interrupting a follow is using it, not breaking it.
func followLogs(ctx context.Context, clk clock.Clock, ro *store.Store, root logsink.Root,
	renderer *logRenderer, runID string, sources []logSource, f logsFlags, out *ui,
) error {
	offsets := map[string]int64{}
	drain := func(list []logSource, final bool) error {
		for _, src := range list {
			path, err := root.Abs(src.relPath)
			if err != nil {
				return err
			}
			key := src.relPath
			off := offsets[key]
			next, err := logsink.ReadFrom(path, off, final, func(l logsink.Line) error {
				return renderer.emit(src.step, src.attempt, l)
			})
			if err != nil {
				return err
			}
			offsets[key] = next
		}
		return nil
	}

	// Show what is already on disk before waiting for anything.
	if err := drain(sources, false); err != nil {
		return err
	}

	ticker := clk.NewTicker(followInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		live, err := ro.GetRun(ctx, runID)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}

		list := sources
		// An empty list re-resolves too: with a fixed --step the file can
		// appear after the follow began, and only a fresh look finds it.
		if len(list) == 0 || f.allAttempts || f.step == "" {
			// Attempts and steps can appear while following: re-resolve.
			found, _, err := selectLogSources(root, live, f)
			if err != nil {
				return err
			}
			list = found
		}
		terminal := allStepsTerminal(live.Steps)
		if err := drain(list, terminal); err != nil {
			return err
		}
		if terminal {
			return nil
		}
	}
}

// clkOf picks the clock a command runs on: the environment may bring one,
// otherwise the process clock. Commands own no time state worth keeping.
func clkOf(env Env) clock.Clock {
	if env.Clk != nil {
		return env.Clk
	}
	return clock.System()
}

// allStepsTerminal reports whether every step of the run has reached a
// terminal state. Following ends on this rather than on the run's own state,
// because in M1 nothing moves the run row while steps are still writing: the
// last evidence of life is the steps.
func allStepsTerminal(steps []store.Step) bool {
	if len(steps) == 0 {
		return false
	}
	for _, step := range steps {
		if !terminalStepStates[step.State] {
			return false
		}
	}
	return true
}

// logRenderer renders decoded log lines in either output mode and keeps the
// per file seq walkers, so a gap is noticed exactly once, where it happens.
type logRenderer struct {
	u            *ui
	text         bool
	run          string
	headerNeeded bool

	enc   *json.Encoder
	walks map[string]*logsink.SeqCheck
}

func newLogRenderer(u *ui, run string, headerNeeded bool) *logRenderer {
	// Headers are a text convenience. A pipe of JSON lines carries step and
	// attempt on every record, and a stray non-JSON line would break jq.
	text := u.mode == modeText
	return &logRenderer{
		u: u, text: text, run: run,
		headerNeeded: headerNeeded && text,
		walks:        map[string]*logsink.SeqCheck{},
	}
}

// streamColumn is the width of the stream column: stderr and pulseq both
// need six characters.
const streamColumn = 6

func (w *logRenderer) emit(step string, attempt int, l logsink.Line) error {
	key := fmt.Sprintf("%s.%d", step, attempt)
	if w.walks == nil {
		w.walks = map[string]*logsink.SeqCheck{}
	}
	if !w.text && w.enc == nil {
		w.enc = json.NewEncoder(w.u.out)
		w.enc.SetEscapeHTML(false)
	}
	walker := w.walks[key]
	if walker == nil {
		walker = &logsink.SeqCheck{}
		w.walks[key] = walker
	}
	gap, missed := walker.Next(l.Seq)
	if missed && l.Event != "truncated" {
		// The truncated marker explains its own hole. Any other gap is a
		// loss nobody announced, and the reader hears about it here.
		if err := w.notice(step, attempt, l.TS,
			fmt.Sprintf("%d lines missing before seq %d", gap, l.Seq)); err != nil {
			return err
		}
	}

	if !w.text {
		return w.enc.Encode(logLineOut{
			Run: w.run, Step: step, Attempt: attempt,
			TS: l.TS, Stream: l.Stream, Seq: l.Seq,
			Line: l.Line, Event: l.Event, Split: l.Split, DroppedBytes: l.DroppedBytes,
		})
	}

	ts := time.UnixMilli(l.TS).UTC().Format("2006-01-02 15:04:05.000")
	switch {
	case l.Event == "truncated":
		_, err := fmt.Fprintf(w.u.out, "%s  %-*s  log truncated: %s dropped\n",
			ts, streamColumn, l.Stream, humanBytes(l.DroppedBytes))
		return err
	case l.Event == "undecodable":
		_, err := fmt.Fprintf(w.u.out, "%s  %-*s  undecodable fragment: %s\n",
			ts, streamColumn, l.Stream, l.Line)
		return err
	case l.Stream == logsink.StreamPulseq:
		_, err := fmt.Fprintf(w.u.out, "%s  %-*s  %s\n", ts, streamColumn, l.Stream, l.Event)
		return err
	default:
		_, err := fmt.Fprintf(w.u.out, "%s  %-*s  %s\n", ts, streamColumn, l.Stream, l.Line)
		return err
	}
}

// notice renders the seq gap warning in both modes. Text gets a pulseq styled
// line; JSON gets data a script can select on.
func (w *logRenderer) notice(step string, attempt int, ts int64, message string) error {
	if !w.text {
		return w.enc.Encode(map[string]any{
			"run": w.run, "step": step, "attempt": attempt,
			"ts": ts, "stream": logsink.StreamPulseq, "event": "seq_gap", "note": message,
		})
	}
	tsText := time.UnixMilli(ts).UTC().Format("2006-01-02 15:04:05.000")
	_, err := fmt.Fprintf(w.u.out, "%s  %-*s  %s\n", tsText, streamColumn, logsink.StreamPulseq, message)
	return err
}

// logLineOut is the JSON form of one rendered line: the file's fields plus
// where it came from.
type logLineOut struct {
	Run          string `json:"run,omitempty"`
	Step         string `json:"step,omitempty"`
	Attempt      int    `json:"attempt,omitempty"`
	TS           int64  `json:"ts"`
	Stream       string `json:"stream"`
	Seq          int64  `json:"seq"`
	Line         string `json:"line,omitempty"`
	Event        string `json:"event,omitempty"`
	Split        bool   `json:"split,omitempty"`
	DroppedBytes int64  `json:"dropped_bytes,omitempty"`
}

// humanBytes sizes a byte count the way an operator reads it at 03:00.
func humanBytes(n int64) string {
	const unit = 1024
	switch {
	case n < unit:
		return fmt.Sprintf("%d B", n)
	case n < unit*unit:
		return fmt.Sprintf("%.1f KiB", float64(n)/unit)
	case n < unit*unit*unit:
		return fmt.Sprintf("%.1f MiB", float64(n)/(unit*unit))
	default:
		return fmt.Sprintf("%.1f GiB", float64(n)/(unit*unit*unit))
	}
}
