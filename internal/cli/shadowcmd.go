package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/explain"
	"github.com/a-holm/paceq/internal/store"
)

// shadow (#32) is the migrator's trust surface: report what Pulseq WOULD have
// done next to a still-running cron, and say plainly that nothing executed.
// Every text path in this group repeats the shadow marker, because the one
// dangerous misunderstanding - "these jobs are running now" - must never be
// even one screen away from the truth.

type shadowFlags struct {
	since string
	job   string
	json  bool
}

func newShadowCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shadow",
		Short: "Shadow mode reporting: what would have run, compared to cron",
		Long: `The migration's trust mechanism (M6-02).

Shadow mode runs while ` + "`paceq serve --shadow`" + ` is up: every schedule is
planned, evaluated and recorded exactly as normally - fire-times, skip
reasons, catch-up policy - but no run is ever materialised and nothing
executes. Observed cron starts can be captured from journald or a syslog
file for comparison.

This group reads that history and never writes. It works with the daemon
down, through the read-only database pool.

Nothing you see here ran. That sentence exists because it matters.`,
		Example: `  paceq shadow report --since 7d
    The weekly view: matches, deviations and overlaps per job.

  paceq shadow status
    How long shadow mode has been up and whether there is enough data yet.`,
	}
	report := newShadowReportCmd(env, g)
	status := newShadowStatusCmd(env, g)
	cmd.AddCommand(report, status)
	return cmd
}

func newShadowReportCmd(env Env, g *globals) *cobra.Command {
	var f shadowFlags
	cmd := &cobra.Command{
		Use:   "report [--since <duration>] [--job <name>] [--json]",
		Short: "Per-job diff between Pulseq's shadow decisions and observed cron",
		Long: `Compare what the shadow scheduler decided against what cron actually did.

Every job gets: how many fires Pulseq would have started, the stand-downs its
own overlap protection would have taken (the ones cron cannot do), and -
when observations were captured - which starts matched, drifted, or only one
side saw. Timezone drift shows as one steady offset with a concrete fix.
Jobs without enough history are marked unknown instead of guessed about.

--since takes 30m/2h/7d/3w style durations (default 7d). --job narrows to one
job name. Without any log source the report degrades to an analytic diff of
the expressions themselves and says so; it does not fail.

Exit codes: 0 ok, 3 unknown job, 4 bad duration.`,
		Args:              noArgs,
		RunE:              runE(env, g, func(ctx context.Context, out *ui) error { return runShadowReport(ctx, env, g, out, f) }),
		DisableAutoGenTag: true,
	}
	cmd.Flags().StringVar(&f.since, "since", "7d", "how far back to look (30m, 12h, 3d, 1w)")
	cmd.Flags().StringVar(&f.job, "job", "", "limit the diff to one job")
	cmd.Flags().BoolVar(&f.json, "json", false,
		"print the stable JSON document instead of prose")
	return cmd
}

func newShadowStatusCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Short shadow overview: uptime, tick count, enough-data verdict",
		Long: `One glance at the shadow itself: since when it has been recording,
how many evaluations were marked, whether the ground is thick enough for a
meaningful report yet, and - always, first - that nothing is executing.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runShadowStatus(ctx, env, g, out)
		}),
	}
}

// parseSince accepts Go durations plus day and week units cron users write.
func parseSince(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var total time.Duration
	i := 0
	for i < len(s) {
		start := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		num, convErr := strconv.Atoi(s[start:i])
		if convErr != nil || num < 0 {
			return 0, fmt.Errorf("unreadable number in duration %q", s)
		}
		start = i
		for i < len(s) && !(s[i] >= '0' && s[i] <= '9') {
			i++
		}
		unit := s[start:i]
		switch unit {
		case "s", "sec":
			total += time.Duration(num) * time.Second
		case "m", "min":
			total += time.Duration(num) * time.Minute
		case "h", "hr":
			total += time.Duration(num) * time.Hour
		case "d", "day", "days":
			total += time.Duration(num) * 24 * time.Hour
		case "w", "week", "weeks":
			total += time.Duration(num) * 7 * 24 * time.Hour
		case "":
			return 0, fmt.Errorf("missing time unit at end of %q", s)
		default:
			return 0, fmt.Errorf("unknown time unit %q in %q (want s/m/h/d/w)", unit, s)
		}
		if num == 0 && start > 0 {
			return 0, fmt.Errorf("no number before %q in %q", unit, s)
		}
	}
	if neg {
		total = -total
	}
	return total, nil
}

func runShadowReport(ctx context.Context, env Env, g *globals, out *ui, f shadowFlags) error {
	window := 7 * 24 * time.Hour
	if f.since != "" && f.since != "7d" {
		d, err := parseSince(f.since)
		if err != nil {
			return validationError("--since "+f.since+" is not a duration I understand",
				fmt.Errorf("%v", err),
				"Write it like 6h, 3d or 2w.")
		}
		if d <= 0 {
			return validationError("--since needs a positive window",
				fmt.Errorf("--since %s", f.since),
				"The report looks back into the past, so give it 30m or more.")
		}
		window = d
	}
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	now := clkOf(env).Now().UTC()
	info, err := ro.ShadowRuntime(ctx)
	if err != nil {
		return internalError("could not read shadow state", err)
	}
	in := explain.ShadowInput{
		SinceMs:       now.Add(-window).UnixMilli(),
		Now:           now,
		JobFilter:     f.job,
		LocalZoneName: localZoneName(),
	}
	rep, err := explain.BuildShadowReport(ctx, ro, in)
	if err != nil {
		return internalError("could not build the shadow report", err)
	}
	ev, err := ro.ShadowEvidence(ctx)
	if err != nil {
		return internalError("could not read shadow evidence", err)
	}

	if out.mode == modeJSON || f.json {
		type doc struct {
			SchemaVersion int                   `json:"schema_version"`
			Shadow        bool                  `json:"shadow_report"`
			Runtime       runtimeInfoJSON       `json:"runtime"`
			Evidence      evidenceJSON          `json:"evidence"`
			Report        *explain.ShadowReport `json:"report"`
		}
		d := doc{
			SchemaVersion: explain.ShadowReportSchemaVersion, Shadow: true,
			Runtime: runtimeInfoFrom(info), Evidence: evidenceFrom(ev), Report: rep,
		}
		return out.json(d)
	}

	renderShadowReport(out.out, rep, info, ev, window)
	return nil
}

type runtimeInfoJSON struct {
	Running bool       `json:"running"`
	Since   *time.Time `json:"since,omitempty"`
	Observe string     `json:"observe,omitempty"`
}

type evidenceJSON struct {
	Ticks int64      `json:"ticks"`
	First *time.Time `json:"first_evidence,omitempty"`
	Last  *time.Time `json:"last_evidence,omitempty"`
}

func runtimeInfoFrom(info store.ShadowRuntimeInfo) runtimeInfoJSON {
	d := runtimeInfoJSON{Running: info.Running, Observe: info.Observe}
	if !info.Since.IsZero() {
		t := info.Since.UTC()
		d.Since = &t
	}
	return d
}

func evidenceFrom(ev store.ShadowEvidenceSummary) evidenceJSON {
	e := evidenceJSON{Ticks: ev.TickCount}
	if ev.FirstEvidence != nil {
		f := ev.FirstEvidence.UTC()
		e.First = &f
	}
	if ev.LastEvidence != nil {
		l := ev.LastEvidence.UTC()
		e.Last = &l
	}
	return e
}

const shadowBanner = "SHADOW MODE - nothing is executing"

func renderShadowReport(w io.Writer, rep *explain.ShadowReport, info store.ShadowRuntimeInfo,
	ev store.ShadowEvidenceSummary, window time.Duration,
) {
	fprintf(w, "== %s ==\n", shadowBanner)
	spans := spanWord(rep.Until.Sub(rep.Since))
	first := rep.Since.Format(time.RFC3339)
	last := rep.Until.Format(time.RFC3339)
	fprintf(w, "Window: %s (%s -> %s)\n", spans, first, last)
	if info.Running {
		fprintf(w, "Instance: shadow mode active since %s (observation source: %s)\n",
			info.Since.Format(time.RFC3339), observeWord(info.Observe))
	} else {
		fprintf(w, "Instance: no live shadow marker - showing the last recorded history\n")
	}
	fprintf(w, "Comparison source: %s\n", comparisonWord(rep.Comparison))
	if len(rep.Sources) == 0 {
		fprintf(w, "  (analytic diff: the schedules were re-read the way cron reads them,\n")
		fprintf(w, "   in this machine's zone; capture real starts by running serve\n")
		fprintf(w, "   with --observe journald or --observe file=/var/log/syslog)\n")
	}
	fprintf(w, "\n%d job(s), %d planned ticks, %d would have run\n",
		len(rep.Jobs), rep.TotalTicks, rep.TotalWouldRun)

	var okJobs, warnLines int
	for _, f := range rep.Jobs {
		switch f.Verdict() {
		case explain.ShadowVerdictMatch:
			okJobs++
		default:
			warnLines++
		}
	}
	if warnLines > 0 {
		fprintf(w, "  %d job(s): exact agreement, %d need attention below\n", okJobs, warnLines)
	} else if len(rep.Jobs) > 0 {
		fprintf(w, "  %d job(s): Pulseq would have run exactly when cron did%s\n",
			okJobs, sourceWord(rep.Comparison))
	} else {
		fprintf(w, "  no shadowed evaluations in this window yet\n")
	}

	sortDeviations(rep.Jobs)
	for _, f := range rep.Jobs {
		switch f.Verdict() {
		case explain.ShadowVerdictMatch:
			continue
		default:
		}
		fprintf(w, "  [%s] %s\n", f.Verdict(), f.JobName)
		fprintf(w, "      would-run %d | skipped overlap %d | skipped other %d | observed %d\n",
			f.WouldRun, f.SkippedOverlap, f.SkippedOther, f.Observed)
		switch {
		case f.OffsetMS != nil:
			fprintf(w, "      steady shift %s across %d pairs\n", humanShiftMS(*f.OffsetMS), f.Pairs)
		case f.PulseqOnly > 0:
			fprintf(w, "      %d planned fire(s) cron never started\n", f.PulseqOnly)
		case f.CronOnly > 0:
			fprintf(w, "      %d cron start(s) Pulseq would not have made\n", f.CronOnly)
		default:
			fprintf(w, "      thin data - verdict waits for more runs\n")
		}
		if f.Hint != "" {
			fprintf(w, "      -> %s\n", f.Hint)
		}
	}
	if rep.UnmatchedObservations > 0 {
		fprintf(w, "\n%d observed cron start(s) matched no imported job and stayed out of the diff.\n",
			rep.UnmatchedObservations)
	}
	if !rep.EnoughData {
		fprintf(w, "\nNot enough ground for final conclusions yet%s.\n",
			runLongerHint(rep.Until, ev))
	}
	fprintf(w, "\nNext step: paceq shadow status  |  nothing here has executed anything\n")
}

// runLongerHint names the weakest link when the verdict stays modest.
func runLongerHint(now time.Time, ev store.ShadowEvidenceSummary) string {
	if ev.FirstEvidence == nil {
		return " - shadow mode has recorded no evaluations yet"
	}
	spans := spanWord(now.Sub(*ev.FirstEvidence))
	return fmt.Sprintf(" - shadowing for %s so far; weekly jobs need weeks, not days", spans)
}

func sortDeviations(jobs []explain.ShadowJobFinding) {
	rank := func(v string) int {
		switch v {
		case explain.ShadowVerdictCronOnly, explain.ShadowVerdictPulseqOnly:
			return 0
		case explain.ShadowVerdictOffset:
			return 1
		case explain.ShadowVerdictUnknown:
			return 2
		default:
			return 3
		}
	}
	sort.SliceStable(jobs, func(i, j int) bool {
		a, b := rank(jobs[i].Verdict()), rank(jobs[j].Verdict())
		if a != b {
			return a < b
		}
		return jobs[i].JobName < jobs[j].JobName
	})
}

func spanWord(d time.Duration) string {
	days := int64(d.Hours()) / 24
	hours := (int64(d.Hours()) % 24)
	minutes := int64(d.Minutes()) % 60
	switch {
	case days > 0 && hours > 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	case days > 0:
		return fmt.Sprintf("%dd", days)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, minutes)
	default:
		return fmt.Sprintf("%dm", minutes)
	}
}

func humanShiftMS(ms int64) string {
	d := time.Duration(ms) * time.Millisecond
	sign := "-"
	if d >= 0 {
		sign = "+"
	}
	a := absDuration(d)
	switch {
	case a%time.Hour == 0:
		return fmt.Sprintf("%s%dh", sign, a/time.Hour)
	case a%time.Minute == 0:
		return fmt.Sprintf("%s%dmin", sign, a/time.Minute)
	default:
		return fmt.Sprintf("%s%.1fmin", sign, a.Minutes())
	}
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func observeWord(o string) string {
	switch o {
	case store.ObsSourceJournald:
		return "journald"
	case store.ObsSourceFile:
		return "log file"
	case "", "none":
		return "none"
	default:
		return o
	}
}

func comparisonWord(c string) string {
	if c == "analytic" {
		return "none recorded - analytic expression diff"
	}
	if c == "" {
		return "none"
	}
	return c
}

func sourceWord(comparison string) string {
	if comparison == "analytic" || comparison == "" {
		return ""
	}
	return " in the recorded observations"
}

func fprintf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func runShadowStatus(ctx context.Context, env Env, g *globals, out *ui) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	info, err := ro.ShadowRuntime(ctx)
	if err != nil {
		return internalError("could not read shadow state", err)
	}
	ev, err := ro.ShadowEvidence(ctx)
	if err != nil {
		return internalError("could not read shadow evidence", err)
	}
	rows, err := ro.ListAllSchedules(ctx)
	if err != nil {
		return internalError("could not list schedules", err)
	}
	var shadowRows int
	names := []string{}
	for _, r := range rows {
		if r.Shadow {
			shadowRows++
			names = append(names, r.JobName+"/"+r.Name)
		}
	}

	if out.mode == modeJSON {
		type schedRow struct {
			Name string `json:"name"`
		}
		type doc struct {
			SchemaVersion   int             `json:"schema_version"`
			Banner          string          `json:"banner"`
			Runtime         runtimeInfoJSON `json:"runtime"`
			Evidence        evidenceJSON    `json:"evidence"`
			ShadowSchedules int             `json:"shadow_schedules"`
			ScheduleNames   []schedRow      `json:"schedule_names,omitempty"`
		}
		d := doc{
			SchemaVersion: explain.ShadowReportSchemaVersion, Banner: shadowBanner,
			Runtime: runtimeInfoFrom(info), Evidence: evidenceFrom(ev),
			ShadowSchedules: shadowRows,
		}
		for _, n := range names {
			d.ScheduleNames = append(d.ScheduleNames, schedRow{Name: n})
		}
		return out.json(d)
	}

	fprintf(out.out, "== %s ==\n", shadowBanner)
	if info.Running {
		fprintf(out.out, "A serve instance is running in shadow mode since %s.\n",
			info.Since.Format(time.RFC3339))
	} else {
		fprintf(out.out, "No live shadow marker in this state directory.\n")
		if ev.TickCount == 0 {
			fprintf(out.out, "Recorded shadow history: none.\n")
			fprintf(out.out, "\nStart one: paceq serve --shadow [--observe journald|file=<path>]\n")
			return nil
		}
	}
	fprintf(out.out, "Evaluations marked shadow_triggered: %d.\n", ev.TickCount)
	if ev.FirstEvidence != nil {
		fprintf(out.out, "Recording since: %s (%s ago).\n",
			ev.FirstEvidence.Format(time.RFC3339),
			spanWord(clkOf(env).Now().UTC().Sub(*ev.FirstEvidence)))
	}
	if shadowRows > 0 {
		fprintf(out.out, "Schedules carrying shadow:true in the database: %d.\n", shadowRows)
		if len(names) <= 8 {
			fprintf(out.out, "  %s\n", strings.Join(names, ", "))
		}
	}
	verdict := "enough"
	extra := ""
	if ev.TickCount == 0 {
		verdict = "none yet"
	} else if ev.FirstEvidence == nil || clkOf(env).Now().UTC().Sub(*ev.FirstEvidence) < 48*time.Hour {
		verdict = "thin"
		extra = " - let it shadow at least two days before trusting conclusions"
	}
	fprintf(out.out, "Data verdict: %s%s\n", verdict, extra)
	fprintf(out.out, "\nReminder: shadow records decisions; it has never started a process.\n")
	return nil
}

// localZoneName names the machine zone for concrete suggestions, reading the
// canonical sources operators know (/etc/localtime target or $TZ).
func localZoneName() string {
	if tz := os.Getenv("TZ"); tz != "" {
		return tz
	}
	if b, err := os.ReadFile("/etc/timezone"); err == nil { // #nosec G304 - fixed well-known system file, never user input
		s := strings.TrimSpace(string(b))
		if strings.Contains(s, "/") {
			return s
		}
	}
	return ""
}
