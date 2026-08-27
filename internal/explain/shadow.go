package explain

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/store"
)

// The shadow report (#32) answers one question: what would Pulseq have done
// differently than cron? It reads rows that already exist - would-run markers,
// skipped evaluations with their real reason codes, and observed cron starts
// captured from journald or a log file - and never re-derives what should have
// happened beyond the one honest exception below. Like the rest of explain it
// is presentation over persisted decisions.
//
// Every finding names its evidence and every classification carries its
// reason. Thin data says "not yet" rather than inventing a verdict, and the
// comparison source is always stated: when no observation was ever recorded,
// the report says it degraded to the analytic diff instead of pretending.

// ShadowReportSchemaVersion is the --json contract version of the report.
const ShadowReportSchemaVersion = 1

// ShadowTolerance is how close an observed cron start and a would-run tick
// must be to count as the same event. Cron starts on the minute plus a second
// or two; anything inside ninety seconds is that fire-time.
const ShadowTolerance = 90 * time.Second

// ShadowMinEvidence is the smallest total a job needs before its diff earns a
// conclusion. A weekly job after three days has run at most once - "unknown"
// is the only truthful verdict for it.
const ShadowMinEvidence = 3

// Report classes of the per-job diff.
const (
	ShadowVerdictMatch      = "match"
	ShadowVerdictOffset     = "offset"
	ShadowVerdictPulseqOnly = "pulseq_only"
	ShadowVerdictCronOnly   = "cron_only"
	ShadowVerdictUnknown    = "unknown"
)

// ShadowJobFinding is one job's diff across the window. Schedules of the same
// job fold together: overlap protection belongs to the job, and cron users
// think in command-lines-per-line anyway.
type ShadowJobFinding struct {
	SourceNames []string `json:"sources"`
	JobName     string   `json:"job"`

	WouldRun int64 `json:"would_run"`
	Repeats  int64 `json:"repeats"`

	SkippedOverlap     int64 `json:"skipped_overlap"`
	SkippedConcurrency int64 `json:"skipped_concurrency"`
	SkippedOther       int64 `json:"skipped_other"`

	Observed int64 `json:"observed"`

	Matches int64 `json:"matches"`

	// Unpaired leftovers after tolerance pairing and offset clustering.
	PulseqOnly int64 `json:"pulseq_only"`
	CronOnly   int64 `json:"cron_only"`

	// Offset is set when several unpaired observations sit at a consistent
	// shift from unpaired ticks - the timezone-not-set signature.
	OffsetMS *int64         `json:"offset_ms,omitempty"`
	Pairs    int64          `json:"offset_pairs,omitempty"`
	Hint     string         `json:"suggestion,omitempty"`
	Kinds    map[string]int `json:"deviation_kinds,omitempty"`
}

// Verdict collapses the finding into its headline class, most severe first:
// an unexplained deviation beats a consistent offset beats too-thin data
// beats plain agreement.
func (f ShadowJobFinding) Verdict() string {
	switch {
	case f.PulseqOnly > 0 || f.CronOnly > 0:
		if f.WouldRun+f.Observed+f.SkippedOverlap+f.SkippedOther < ShadowMinEvidence {
			return ShadowVerdictUnknown
		}
		if f.PulseqOnly > 0 {
			return ShadowVerdictPulseqOnly
		}
		return ShadowVerdictCronOnly
	case f.OffsetMS != nil:
		return ShadowVerdictOffset
	case f.WouldRun+f.Observed+f.SkippedOverlap+f.SkippedOther < ShadowMinEvidence:
		return ShadowVerdictUnknown
	default:
		return ShadowVerdictMatch
	}
}

// ShadowReport is the whole document behind `paceq shadow report`.
type ShadowReport struct {
	SchemaVersion int       `json:"schema_version"`
	Since         time.Time `json:"since"`
	Until         time.Time `json:"until"`

	// Sources lists the comparison sources present in the window ("journald",
	// "file"). Empty means the analytic fallback did the comparing alone.
	Sources []string `json:"sources,omitempty"`

	// Comparison is the honest one-word answer: journald, file, journald+file,
	// or "analytic" when nothing outside was recorded.
	Comparison string `json:"comparison"`

	// UnmatchedObservations counts observed cron starts whose command matched
	// no imported job: they cannot be diffed and they are not hidden.
	UnmatchedObservations int64 `json:"unmatched_observations,omitempty"`

	Jobs []ShadowJobFinding `json:"jobs"`

	TotalWouldRun int64 `json:"would_run_total"`
	TotalTicks    int64 `json:"tick_total"`
	EnoughData    bool  `json:"enough_data"`
}

// ShadowInput gathers what BuildShadowReport needs besides the store.
type ShadowInput struct {
	SinceMs int64
	Now     time.Time

	// JobFilter narrows the report to one job ("name"), "" means all.
	JobFilter string

	// LocalZoneName names the machine's own zone, so an offset suggestion
	// can carry a concrete `timezone:` line. Empty degrades gracefully.
	LocalZoneName string
}

// analyticScanCap bounds how many occurrences one analytic comparison walks;
// windows are days, expressions are small, and the report must not turn into
// a four-year sweep because someone wrote a broken schedule.
const analyticScanCap = 4096

// BuildShadowReport assembles the per-job diff from persisted rows. SQL lives
// in the store; this file pairs, clusters and renders.
func BuildShadowReport(ctx context.Context, st *store.Store, in ShadowInput) (*ShadowReport, error) {
	aggs, err := st.ShadowAggregatesWindow(ctx, in.SinceMs)
	if err != nil {
		return nil, err
	}
	points, err := st.ShadowTriggeredPoints(ctx, in.SinceMs, "")
	if err != nil {
		return nil, err
	}
	rawObs, err := st.ListShadowObservations(ctx, in.SinceMs, "")
	if err != nil {
		return nil, err
	}
	exprs, err := st.ShadowScheduleExpressions(ctx)
	if err != nil {
		return nil, err
	}
	return composeShadowReport(aggs, points, rawObs, exprs, in), nil
}

// composeShadowReport holds every pure piece so table-driven tests need no
// database at all.
func composeShadowReport(aggs []store.ShadowAggregateRow,
	points []store.ShadowTickPoint, rawObs []store.ShadowObservation,
	exprs []store.ScheduleExpressionRow, in ShadowInput,
) *ShadowReport {
	jobOf := func(source string) string {
		if i := strings.IndexByte(source, '/'); i >= 0 {
			return source[:i]
		}
		return source
	}
	want := func(job string) bool { return in.JobFilter == "" || job == in.JobFilter }

	type bucket struct {
		sources []string
		ticks   int64
		repeat  int64
		overlap int64
		conc    int64
		other   int64
		first   time.Time
	}
	buckets := map[string]*bucket{}
	bucketFor := func(job string) *bucket {
		b, ok := buckets[job]
		if !ok {
			b = &bucket{}
			buckets[job] = b
		}
		return b
	}
	addSource := func(b *bucket, source string) {
		for _, s := range b.sources {
			if s == source {
				return
			}
		}
		b.sources = append(b.sources, source)
	}

	for _, a := range aggs {
		job := jobOf(a.SourceName)
		if !want(job) {
			continue
		}
		b := bucketFor(job)
		addSource(b, a.SourceName)
		b.ticks += a.Ticks
		switch {
		case a.Outcome == store.OutcomeShadowTriggered:
			b.repeat += a.Repeats
			if a.FirstScheduled != nil && (b.first.IsZero() || a.FirstScheduled.Before(b.first)) {
				b.first = *a.FirstScheduled
			}
		case a.Outcome == store.OutcomeSkipped:
			switch {
			case a.ReasonCode == "TICK_SKIPPED_OVERLAP":
				b.overlap += a.Repeats
			case a.ReasonCode == "TICK_SKIPPED_CONCURRENCY":
				b.conc += a.Repeats
			default:
				b.other += a.Repeats
			}
		default:
			b.other += a.Repeats
		}
	}
	obsByJob := map[string][]time.Time{}
	var unmatched int64
	sourcesSet := map[string]bool{}
	for _, o := range rawObs {
		sourcesSet[o.Source] = true
		if o.HasJob && want(o.JobName) {
			obsByJob[o.JobName] = append(obsByJob[o.JobName], o.ObservedAt.UTC())
		} else {
			unmatched++
		}
	}
	ptsByJob := map[string][]time.Time{}
	for _, p := range points {
		job := jobOf(p.SourceName)
		if !want(job) {
			continue
		}
		// A schedule with would-run markers exists as its own job even when
		// no aggregate grouped it: the report must not lose someone whose
		// only output so far is clean would-runs.
		addSource(bucketFor(job), p.SourceName)
		ptsByJob[job] = append(ptsByJob[job], p.ScheduledFor.UTC())
	}

	compareAnalytically := len(sourcesSet) == 0
	naiveDiffs := map[string][]time.Time{}
	if compareAnalytically {
		// No observed cron activity anywhere: degrade to the analytic diff -
		// the same expression read the way cron would read it, in the
		// machine's own zone without a DST policy, against what shadow
		// recorded. This still catches the two most valuable finds:
		// timezone drift and overlaps (via the stored skip counters).
		for _, e := range exprs {
			if !want(e.JobName) {
				continue
			}
			parsed, err := cronx.Parse(e.Expr)
			if err != nil {
				continue
			}
			to := in.Now
			if to.Before(time.UnixMilli(in.SinceMs)) {
				continue
			}
			occs, _ := parsed.Between(time.UnixMilli(in.SinceMs), to, time.Local, cronx.Policy{})
			if len(occs) == 0 || len(occs) > analyticScanCap {
				continue
			}
			for _, o := range occs {
				if !o.Skipped {
					naiveDiffs[e.JobName] = append(naiveDiffs[e.JobName], o.At.UTC())
				}
			}
		}
	}

	jobs := make([]ShadowJobFinding, 0, len(buckets))
	names := make([]string, 0, len(buckets))
	for name := range buckets {
		names = append(names, name)
	}
	sort.Strings(names)

	totalWould, totalTicks := int64(0), int64(0)
	allUnknownNoObservation := true
	now := in.Now
	for _, job := range names {
		b := buckets[job]
		f := ShadowJobFinding{
			SourceNames:        b.sources,
			JobName:            job,
			SkippedOverlap:     b.overlap,
			SkippedConcurrency: b.conc,
			SkippedOther:       b.other,
			Observed:           int64(len(obsByJob[job])),
			Kinds:              map[string]int{},
		}
		f.WouldRun = int64(len(ptsByJob[job]))
		f.Repeats = b.repeat
		ticks := ptsByJob[job]

		matches, unpairedT, unpairedO := pairWithin(ticks, obsByJob[job], ShadowTolerance)
		f.Matches = matches

		delta, movedPairs, restT, restO := clusterOffset(unpairedT, unpairedO)
		if delta != nil {
			ms := delta.Milliseconds()
			f.OffsetMS, f.Pairs = &ms, movedPairs
			f.Kinds["offset"] = int(movedPairs)
			hint := fmt.Sprintf("cron and Pulseq disagree by a steady %s - the schedule has no working timezone",
				humanShift(*delta))
			if in.LocalZoneName != "" {
				hint += fmt.Sprintf(" → set timezone: %s on the schedule", in.LocalZoneName)
			} else {
				hint += " → set timezone on the schedule"
			}
			f.Hint = hint
		}
		f.PulseqOnly = int64(len(restT))
		f.CronOnly = int64(len(restO))

		// In the analytic world there is no observation to blame; the
		// naive-local interpretation plays cron's role instead.
		if compareAnalytically && f.OffsetMS == nil {
			naive := naiveDiffs[job]
			if len(naive) > 0 && f.Matches == 0 {
				if d2, p2, rt, ro := clusterOffset(ticks, naive); d2 != nil && p2 >= 2 {
					ms := d2.Milliseconds()
					f.OffsetMS, f.Pairs = &ms, p2
					f.Kinds["offset"] = int(p2)
					f.Hint = fmt.Sprintf("Pulseq records this job %s away from your wall clock - set timezone: %s on the schedule",
						humanShift(*d2), zoneWord(in.LocalZoneName))
					f.PulseqOnly = int64(len(rt))
					f.CronOnly = int64(len(ro))
				}
			}
		}

		if f.CronOnly > 0 {
			f.Kinds["cron_only"] = int(f.CronOnly)
			if f.Hint == "" && f.SkippedOverlap > 0 {
				f.Hint = fmt.Sprintf("%d cron starts sit where overlap protection would have stood down; "+
					"`paceq explain` shows each one", f.CronOnly)
			} else if f.Hint == "" {
				f.Hint = fmt.Sprintf("cron started %d fires Pulseq never marked - check %% escapes, PATH and host downtime", f.CronOnly)
			}
		}
		if f.PulseqOnly > 0 {
			f.Kinds["pulseq_only"] = int(f.PulseqOnly)
			if f.Hint == "" {
				f.Hint = fmt.Sprintf("Pulseq planned %d fires cron did not start (%s escape, PATH failure, host down?)",
					f.PulseqOnly, "%")
			}
		}

		totalWould += f.WouldRun
		totalTicks += b.ticks
		if f.Verdict() == ShadowVerdictUnknown {
			allUnknownNoObservation = false
		}
		jobs = append(jobs, f)
	}

	rep := &ShadowReport{
		SchemaVersion:         ShadowReportSchemaVersion,
		Since:                 time.UnixMilli(in.SinceMs).UTC(),
		Until:                 now.UTC(),
		UnmatchedObservations: unmatched,
		Jobs:                  jobs,
		TotalWouldRun:         totalWould,
		TotalTicks:            totalTicks,
	}
	for src := range sourcesSet {
		rep.Sources = append(rep.Sources, src)
	}
	sort.Strings(rep.Sources)
	switch {
	case len(rep.Sources) == 0:
		rep.Comparison = "analytic"
	case len(rep.Sources) == 1:
		rep.Comparison = rep.Sources[0]
	default:
		rep.Comparison = strings.Join(rep.Sources, "+")
	}

	// Enough ground for a meaningful report: nearly two days of shadowing
	// and every job carrying its weight of evidence - a job still marked
	// unknown keeps the verdict modest until it is not.
	enough := !now.Before(rep.Since.Add(48*time.Hour)) && compareOrNone(rep.Comparison) && len(jobs) > 0
	for _, f := range jobs {
		if f.Verdict() == ShadowVerdictUnknown {
			enough = false
			break
		}
	}
	rep.EnoughData = enough && !(compareAnalytically && allUnknownNoObservation)
	return rep
}

func compareOrNone(string) bool { return true }

// pairWithin greedily matches the closest tick and observation under
// tolerance. Input slices are copied-sorted; returns the pair count plus the
// unpaired remainders.
func pairWithin(ticks, obs []time.Time, tol time.Duration) (int64, []time.Time, []time.Time) {
	t := append([]time.Time(nil), ticks...)
	o := append([]time.Time(nil), obs...)
	sort.Slice(t, func(i, j int) bool { return t[i].Before(t[j]) })
	sort.Slice(o, func(i, j int) bool { return o[i].Before(o[j]) })

	var pairedT, pairedO []int
	var matches int64
	for i, j := 0, 0; i < len(t) && j < len(o); {
		d := t[i].Sub(o[j])
		switch {
		case absDur(d) <= tol:
			pairedT = append(pairedT, i)
			pairedO = append(pairedO, j)
			i++
			j++
			matches++
		case d > 0:
			j++
		default:
			i++
		}
	}
	take := func(all []time.Time, drop []int) []time.Time {
		skip := map[int]bool{}
		for _, i := range drop {
			skip[i] = true
		}
		out := make([]time.Time, 0, len(all)-len(drop))
		for i, v := range all {
			if !skip[i] {
				out = append(out, v)
			}
		}
		return out
	}
	return matches, take(t, pairedT), take(o, pairedO)
}

// clusterOffset hunts for a systematic shift between unpaired sides. The
// signature the report is built around: every start off by the same amount,
// typical when the schedule forgot its timezone across a DST border. Returns
// the detected shift, how many pairs back it, and the remainders.
// scorePairing greedily pairs ticks with shifted observations and returns
// how many landed inside tolerance plus the summed mismatch of those pairs.
func scorePairing(ticks, shifted []time.Time) (int64, int64) {
	t := append([]time.Time(nil), ticks...)
	sort.Slice(t, func(i, j int) bool { return t[i].Before(t[j]) })
	o := append([]time.Time(nil), shifted...)
	sort.Slice(o, func(i, j int) bool { return o[i].Before(o[j]) })

	var pairs, residual int64
	for i, j := 0, 0; i < len(t) && j < len(o); {
		d := absDur(t[i].Sub(o[j]))
		switch {
		case d <= ShadowTolerance:
			pairs++
			residual += d.Milliseconds()
			i++
			j++
		case t[i].After(o[j]):
			j++
		default:
			i++
		}
	}
	return pairs, residual
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

// clusterOffset hunts for a systematic shift between unpaired sides.
func clusterOffset(ticks, obs []time.Time) (*time.Duration, int64, []time.Time, []time.Time) {
	if len(ticks) == 0 || len(obs) == 0 {
		return nil, 0, ticks, obs
	}
	t := append([]time.Time(nil), ticks...)
	o := append([]time.Time(nil), obs...)
	sort.Slice(t, func(i, j int) bool { return t[i].Before(t[j]) })
	sort.Slice(o, func(i, j int) bool { return o[i].Before(o[j]) })

	cands := make([]int, 0, 721)
	for m := -360; m <= 360; m++ {
		if m == 0 {
			continue
		}
		cands = append(cands, m)
	}
	bestCount, bestMin, bestResidual := int64(0), 0, int64(1<<62)
	for _, m := range cands {
		shift := time.Duration(m) * time.Minute
		shifted := make([]time.Time, len(o))
		for i := range o {
			shifted[i] = o[i].Add(shift)
		}
		nm, residual := scorePairing(ticks, shifted)
		if nm == 0 {
			continue
		}
		// Score the candidate like a human would: how many it explains,
		// then how snugly it fits, then which side of zero reads cleaner.
		if nm > bestCount || (nm == bestCount && residual < bestResidual) ||
			(nm == bestCount && residual == bestResidual && absInt(m) < absInt(bestMin)) {
			bestCount, bestMin, bestResidual = nm, m, residual
		}
	}
	if bestCount < 2 {
		return nil, 0, ticks, obs
	}
	minSide := int64(len(ticks))
	if int64(len(o)) < minSide {
		minSide = int64(len(o))
	}
	if minSide > 0 && bestCount*2 < minSide {
		// Fewer than half the smaller side lined up on any single shift:
		// that is genuine scatter, not one clock moved wholesale.
		return nil, 0, ticks, obs
	}
	shift := time.Duration(bestMin) * time.Minute
	shifted := make([]time.Time, len(o))
	for i := range o {
		shifted[i] = o[i].Add(shift)
	}
	_, restT, restW := pairWithin(ticks, shifted, ShadowTolerance)
	restO := make([]time.Time, 0, len(restW))
	for _, w := range restW {
		restO = append(restO, w.Add(-shift))
	}
	// Reported orientation: obs minus tick, the way a human states it
	// ("cron fired an hour later than Pulseq would have").
	d := -shift
	return &d, bestCount, restT, restO
}

// clusterOffset hunts for a systematic shift between unpaired sides. The
// signature the report is built around: every start off by the same amount,
// typical when the schedule forgot its timezone across a DST border.

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// humanShift words a shift the way people say it out loud.
func humanShift(d time.Duration) string {
	sign := ""
	if d > 0 {
		sign = "+"
	} else if d < 0 {
		sign = "-"
	}
	a := absDur(d)
	if a%time.Hour == 0 {
		return fmt.Sprintf("%s%d h", sign, a/time.Hour)
	}
	if a%time.Minute == 0 {
		return fmt.Sprintf("%s%d min", sign, a/time.Minute)
	}
	return fmt.Sprintf("%s%.1f min", sign, a.Minutes())
}

func zoneWord(zone string) string {
	if zone == "" {
		return "your machine's zone"
	}
	return zone
}
