package explain

import (
	"context"
	"fmt"
	"strings"

	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/store"
)

// Reference syntax per 03 section 5.3: job/<name>, run/<ulid-or-prefix>,
// schedule/<job>.<name> (job/name accepted too), sensor/<name>. A bare name
// is resolved heuristically, and when the heuristic finds more than one kind
// the refusal lists the exact forms to type instead of guessing.

// RefKind is what a reference names.
type RefKind string

const (
	KindJob      RefKind = "job"
	KindSchedule RefKind = "schedule"
	KindSensor   RefKind = "sensor"
	KindRun      RefKind = "run"
)

// NotFound says nothing matched. Candidates carries full references that do
// exist, for the message; it is advice, never a guess.
type NotFound struct {
	What       string
	Candidates []string
}

func (e *NotFound) Error() string { return e.What }

// Ambiguous says the reference matches more than one thing: an id prefix
// shared by several runs, or a bare name that exists as two kinds. The
// candidates are the full references, so the next command can be exact.
type Ambiguous struct {
	What       string
	Candidates []string
}

func (e *Ambiguous) Error() string { return e.What }

// Syntax is a malformed reference: exit-2 material, not a search miss.
type Syntax struct{ What string }

func (e *Syntax) Error() string { return e.What }

// Resolved is a reference tied to rows that exist, carrying everything the
// report builder needs: the tick producers whose timeline answers for this
// subject.
type Resolved struct {
	Kind     RefKind
	Raw      string
	Job      string
	Schedule string
	Sensor   string
	RunID    string

	Sources []store.ExplainSource
}

// ParseRef splits a reference at the syntax level only: which kind does it
// claim to be, and what is the payload. Nothing here touches the database.
func ParseRef(raw string) (RefKind, string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", "", &Syntax{"the reference is empty"}
	}
	prefix, rest, found := strings.Cut(s, "/")
	if !found {
		return "", s, nil // bare name: resolved heuristically
	}
	switch prefix {
	case "job", "jobs":
		if rest == "" {
			return "", "", &Syntax{"job/ needs a job name"}
		}
		return KindJob, strings.TrimPrefix(rest, "job/"), nil
	case "schedule", "schedules":
		if rest == "" {
			return "", "", &Syntax{"schedule/ needs a reference like schedule/job.name"}
		}
		return KindSchedule, rest, nil
	case "sensor", "sensors":
		if rest == "" {
			return "", "", &Syntax{"sensor/ needs a sensor name"}
		}
		return KindSensor, rest, nil
	case "run", "runs":
		if rest == "" {
			return "", "", &Syntax{"run/ needs a run id or prefix"}
		}
		return KindRun, rest, nil
	default:
		// One slash, unknown namespace: treat the whole thing as a bare
		// name, which is how schedule refs job/name read anyway.
		return "", s, nil
	}
}

// Resolve ties a reference to existing rows. Every miss comes back typed, so
// the command layer can pick the exit code and print the candidates.
func Resolve(ctx context.Context, st *store.Store, raw string) (Resolved, error) {
	kind, payload, err := ParseRef(raw)
	if err != nil {
		return Resolved{}, err
	}
	switch kind {
	case KindJob:
		return resolveExplicitJob(ctx, st, raw, payload)
	case KindSchedule:
		return resolveExplicitSchedule(ctx, st, raw, payload)
	case KindSensor:
		return resolveExplicitSensor(ctx, st, raw, payload)
	case KindRun:
		return resolveRunRef(ctx, st, raw, payload)
	default:
		return resolveBareName(ctx, st, raw)
	}
}

func resolveExplicitJob(ctx context.Context, st *store.Store, raw, name string) (Resolved, error) {
	facts, err := st.ExplainJobFacts(ctx, name)
	if err != nil {
		return Resolved{}, err
	}
	if !facts.Found {
		others, _ := st.ListAllSchedules(ctx)
		var cands []string
		for _, row := range others {
			if row.JobName == name {
				cands = append(cands, "schedule/"+row.JobName+"."+row.Name)
			}
		}
		return Resolved{}, &NotFound{
			What:       fmt.Sprintf("no job named %q", name),
			Candidates: cands,
		}
	}
	res := Resolved{Kind: KindJob, Raw: raw, Job: name}
	res.Sources, err = sourcesForJob(ctx, st, name)
	if err != nil {
		return Resolved{}, err
	}
	return res, nil
}

func jobResolved(ctx context.Context, st *store.Store, raw, name string) (Resolved, error) {
	res := Resolved{Kind: KindJob, Raw: raw, Job: name}
	sources, err := sourcesForJob(ctx, st, name)
	if err != nil {
		return Resolved{}, err
	}
	res.Sources = sources
	return res, nil
}

// sourcesForJob reads the tick producers of one job: its schedules (whose
// ticks carry "job/name" as their source name) and its sensors (whose ticks
// carry the bare sensor name).
func sourcesForJob(ctx context.Context, st *store.Store, job string) ([]store.ExplainSource, error) {
	var out []store.ExplainSource
	schedules, err := st.ListAllSchedules(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the schedules of job %s: %w", job, err)
	}
	for _, row := range schedules {
		if row.JobName == job {
			out = append(out, store.ExplainSource{Kind: "schedule", Name: row.JobName + "/" + row.Name})
		}
	}
	sensors, err := st.ListSensors(ctx)
	if err != nil {
		return nil, fmt.Errorf("list the sensors of job %s: %w", job, err)
	}
	for _, sum := range sensors {
		if sum.JobName == job {
			out = append(out, store.ExplainSource{Kind: "sensor", Name: sum.Name})
		}
	}
	return out, nil
}

func resolveExplicitSchedule(ctx context.Context, st *store.Store, raw, payload string) (Resolved, error) {
	job, name, err := splitSchedulePayload(payload)
	if err != nil {
		return Resolved{}, err
	}
	row, err := st.GetSchedule(ctx, job, name)
	if err != nil {
		return Resolved{}, &NotFound{
			What: fmt.Sprintf("no schedule %q in job %q", name, job),
		}
	}
	res := Resolved{Kind: KindSchedule, Raw: raw, Job: row.JobName, Schedule: row.Name}
	res.Sources = []store.ExplainSource{{Kind: "schedule", Name: row.JobName + "/" + row.Name}}
	return res, nil
}

// splitSchedulePayload takes job.name or job/name.
func splitSchedulePayload(payload string) (string, string, error) {
	for _, sep := range []string{".", "/"} {
		if job, name, found := strings.Cut(payload, sep); found && job != "" && name != "" {
			return job, name, nil
		}
	}
	return "", "", &Syntax{
		fmt.Sprintf("schedule/%q does not say which job: write schedule/job.name", payload),
	}
}

func resolveExplicitSensor(ctx context.Context, st *store.Store, raw, name string) (Resolved, error) {
	sum, err := st.GetSensor(ctx, name)
	if err != nil {
		return Resolved{}, &NotFound{What: fmt.Sprintf("no sensor named %q", name)}
	}
	res := Resolved{Kind: KindSensor, Raw: raw, Job: sum.JobName, Sensor: sum.Name}
	res.Sources = []store.ExplainSource{{Kind: "sensor", Name: sum.Name}}
	return res, nil
}

// resolveRunRef resolves a whole id or any prefix, git style. A prefix
// matching several runs refuses with every candidate, not just the first two,
// so the operator can lengthen the prefix by looking once.
func resolveRunRef(ctx context.Context, st *store.Store, raw, payload string) (Resolved, error) {
	if _, err := id.PrefixRange(payload); err != nil {
		return Resolved{}, &Syntax{fmt.Sprintf("run/%q is not a run id or prefix: ids are 26 characters from %s", payload, id.Alphabet)}
	}
	matches, err := st.ExplainRunsByPrefix(ctx, payload, 10)
	if err != nil {
		return Resolved{}, err
	}
	switch len(matches) {
	case 0:
		return Resolved{}, &NotFound{
			What: fmt.Sprintf("no run matches %q", payload),
		}
	case 1:
		res := Resolved{Kind: KindRun, Raw: raw, Job: matches[0].JobName, RunID: matches[0].ID}
		res.Sources = nil // a run's timeline is its own events, not its producer's ticks
		return res, nil
	default:
		cands := make([]string, len(matches))
		for i, m := range matches {
			cands[i] = describeRunCandidate(m)
		}
		return Resolved{}, &Ambiguous{
			What:       fmt.Sprintf("%q matches %d runs", payload, len(matches)),
			Candidates: cands,
		}
	}
}

// describeRunCandidate names one matching run in a candidate list: enough to
// pick it out and lengthen a prefix without a second lookup.
func describeRunCandidate(m store.RunSummary) string {
	return fmt.Sprintf("run/%s (%s, %s)", m.ID, m.JobName, m.State)
}

// resolveBareName tries every reading of a bare word: a job, then a unique
// schedule name, then a sensor, then a run id. More than one reading refuses
// with the exact forms; zero readings refuse with close spellings.
func resolveBareName(ctx context.Context, st *store.Store, raw string) (Resolved, error) {
	type reading struct {
		form string
		res  Resolved
	}
	var readings []reading

	if facts, err := st.ExplainJobFacts(ctx, raw); err == nil && facts.Found {
		res, err := jobResolved(ctx, st, raw, raw)
		if err != nil {
			return Resolved{}, err
		}
		readings = append(readings, reading{form: "job/" + raw, res: res})
	}

	schedules, err := st.ListAllSchedules(ctx)
	if err != nil {
		return Resolved{}, err
	}
	var scheduleHits []string
	for _, row := range schedules {
		if row.Name == raw || row.JobName+"/"+row.Name == raw {
			scheduleHits = append(scheduleHits, row.JobName+"/"+row.Name)
		}
	}
	if len(scheduleHits) == 1 {
		job, name, _ := strings.Cut(scheduleHits[0], "/")
		res := Resolved{Kind: KindSchedule, Raw: raw, Job: job, Schedule: name}
		res.Sources = []store.ExplainSource{{Kind: "schedule", Name: scheduleHits[0]}}
		readings = append(readings, reading{form: "schedule/" + job + "." + name, res: res})
	} else if len(scheduleHits) > 1 {
		var cands []string
		for _, hit := range scheduleHits {
			job, name, _ := strings.Cut(hit, "/")
			cands = append(cands, "schedule/"+job+"."+name)
		}
		return Resolved{}, &Ambiguous{
			What:       fmt.Sprintf("%q names %d schedules", raw, len(scheduleHits)),
			Candidates: cands,
		}
	}

	if sum, err := st.GetSensor(ctx, raw); err == nil {
		res := Resolved{Kind: KindSensor, Raw: raw, Job: sum.JobName, Sensor: sum.Name}
		res.Sources = []store.ExplainSource{{Kind: "sensor", Name: sum.Name}}
		readings = append(readings, reading{form: "sensor/" + sum.Name, res: res})
	}

	if _, err := id.PrefixRange(raw); err == nil {
		matches, err := st.ExplainRunsByPrefix(ctx, raw, 10)
		if err != nil {
			return Resolved{}, err
		}
		if len(matches) == 1 {
			res := Resolved{Kind: KindRun, Raw: raw, Job: matches[0].JobName, RunID: matches[0].ID}
			readings = append(readings, reading{form: "run/" + matches[0].ID, res: res})
		} else if len(matches) > 1 {
			cands := make([]string, len(matches))
			for i, m := range matches {
				cands[i] = describeRunCandidate(m)
			}
			return Resolved{}, &Ambiguous{
				What:       fmt.Sprintf("%q matches %d runs", raw, len(matches)),
				Candidates: cands,
			}
		}
	}

	switch len(readings) {
	case 0:
		return Resolved{}, &NotFound{
			What:       fmt.Sprintf("nothing named %q: no job, unique schedule, sensor or run id matches", raw),
			Candidates: knownNames(st, raw),
		}
	case 1:
		return readings[0].res, nil
	default:
		forms := make([]string, len(readings))
		for i, r := range readings {
			forms[i] = r.form
		}
		return Resolved{}, &Ambiguous{
			What:       fmt.Sprintf("%q could mean %d different things", raw, len(readings)),
			Candidates: forms,
		}
	}
}

// knownNames collects a few real references for a not-found message.
func knownNames(st *store.Store, want string) []string {
	var out []string
	schedules, _ := st.ListAllSchedules(context.Background())
	for _, row := range schedules {
		out = append(out, "job/"+row.JobName, "schedule/"+row.JobName+"."+row.Name)
	}
	sensors, _ := st.ListSensors(context.Background())
	for _, s := range sensors {
		out = append(out, "sensor/"+s.Name)
	}
	_ = want
	if len(out) > 8 {
		out = out[:8]
	}
	return out
}
