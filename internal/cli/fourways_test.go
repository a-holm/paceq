package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/store"
)

// Four code paths judge one job file: validate, apply, schedules preview and
// the scheduler loop when it materialises a tick. They answer different
// questions, so they are allowed to differ in what they report and where;
// they are not allowed to disagree about the file. A file validate takes must
// be a file apply takes, and a row apply writes must be a row the other two
// can read without an internal error.

// scheduleJob is a job with one cron schedule, spelled by the caller.
func scheduleJob(name, cron, timezone string) string {
	return `name: ` + name + `
steps:
  - name: only
    run: ["/bin/true"]
schedules:
  - name: nightly
    cron: "` + cron + `"
    timezone: ` + timezone + `
`
}

// schedulerTicks runs one real scheduler wake over the state database apply
// wrote and returns the reason codes it recorded. This is the fourth judge
// itself, not a restatement of what it is believed to do.
func schedulerTicks(t *testing.T, dir, job, name string) []string {
	t.Helper()

	ctx := context.Background()
	s, err := store.OpenState(ctx, filepath.Join(dir, stateDirName), store.Options{})
	if err != nil {
		t.Fatalf("open the state store: %v", err)
	}
	defer func() { _ = s.Close() }()

	src, err := scheduler.New(scheduler.Config{Store: s, Clock: clock.System(), Holder: "fourways"})
	if err != nil {
		t.Fatalf("wire the scheduler: %v", err)
	}
	if err := src.Tick(ctx); err != nil {
		t.Fatalf("one scheduler wake: %v", err)
	}

	ticks, err := s.ScheduleTicks(ctx, job, name)
	if err != nil {
		t.Fatalf("read the ticks of %s/%s: %v", job, name, err)
	}
	codes := make([]string, 0, len(ticks))
	for _, tick := range ticks {
		codes = append(codes, tick.ReasonCode)
	}
	return codes
}

// scheduleRowCount says how many schedule rows one job left behind, which is
// what makes "apply refused the file" a fact about the database rather than
// about the exit code alone.
func scheduleRowCount(t *testing.T, dir, job string) int {
	t.Helper()

	ctx := context.Background()
	s, err := store.OpenState(ctx, filepath.Join(dir, stateDirName), store.Options{})
	if err != nil {
		t.Fatalf("open the state store: %v", err)
	}
	defer func() { _ = s.Close() }()

	rows, err := s.ListAllSchedules(ctx)
	if err != nil {
		t.Fatalf("list the schedules: %v", err)
	}
	count := 0
	for _, row := range rows {
		if row.JobName == job {
			count++
		}
	}
	return count
}

// previewOccurrences runs schedules preview and reports the series it printed.
func previewOccurrences(t *testing.T, dir, ref string) (result, int) {
	t.Helper()

	got := runCLI(t, dir, nil, "schedules", "preview", ref, "-o", "json")
	if got.code != ExitOK {
		return got, 0
	}
	var doc struct {
		Occurrences []struct {
			Index int `json:"index"`
		} `json:"occurrences"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &doc); err != nil {
		t.Fatalf("preview wrote no readable JSON: %v\n%s", err, got.stdout)
	}
	return got, len(doc.Occurrences)
}

// TestFourJudgementsAgreeOnOneJobFile is the round trip. Each fixture is put
// to validate, to apply, to the scheduler and to preview, and the four answers
// are compared against each other rather than against a remembered constant.
func TestFourJudgementsAgreeOnOneJobFile(t *testing.T) {
	cases := []struct {
		name string
		// job is the file all four judges read.
		job string
		// accepted is whether the file is a definition at all: validate and
		// apply must give the same answer, and a refused file leaves no
		// schedule row for the other two to misread.
		accepted bool
		// occurrences is the series preview must print for an accepted file.
		occurrences int
		// tickCodes is what one scheduler wake records. The scheduler reports
		// configuration rot through a tick row where preview prints an empty
		// series: different channels for one fact, which is the design.
		tickCodes []string
	}{
		{
			name:     "a zone that depends on the host",
			job:      scheduleJob("hostzone", "0 3 * * *", "Local"),
			accepted: false,
		},
		{
			name:        "an expression whose matches have run out",
			job:         scheduleJob("exhausted", "0 0 30 2 *", "UTC"),
			accepted:    true,
			occurrences: 0,
			tickCodes:   []string{string(reason.TICKErrorConfig)},
		},
		{
			name:        "a schedule that fires",
			job:         scheduleJob("healthy", "0 3 * * *", "Europe/Oslo"),
			accepted:    true,
			occurrences: 10,
			tickCodes:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := applyProject(t, map[string]string{"jobs/job.yaml": tc.job})
			job := jobNameOf(tc.job)

			validated := runCLI(t, dir, nil, "validate")
			applied := runCLI(t, dir, nil, "apply")
			if validated.code != applied.code {
				t.Fatalf("validate exited %d and apply exited %d over one file\nvalidate:\n%s%s\napply:\n%s%s",
					validated.code, applied.code, validated.stdout, validated.stderr,
					applied.stdout, applied.stderr)
			}

			if !tc.accepted {
				if validated.code != ExitValidation {
					t.Fatalf("both judges exited %d, want %d, so a file neither the scheduler "+
						"nor preview can use was taken\n%s%s",
						validated.code, ExitValidation, validated.stdout, applied.stdout)
				}
				if got := scheduleRowCount(t, dir, job); got != 0 {
					t.Fatalf("the refused file left %d schedule rows behind", got)
				}
				return
			}

			if validated.code != ExitOK {
				t.Fatalf("both judges exited %d, want %d\n%s%s",
					validated.code, ExitOK, validated.stdout, applied.stdout)
			}

			codes := schedulerTicks(t, dir, job, "nightly")
			if strings.Join(codes, ",") != strings.Join(tc.tickCodes, ",") {
				t.Errorf("one scheduler wake recorded %v, want %v", codes, tc.tickCodes)
			}

			got, occurrences := previewOccurrences(t, dir, job+"/nightly")
			if got.code != ExitOK {
				t.Fatalf("preview exited %d over a row apply wrote and the scheduler read\n%s%s",
					got.code, got.stdout, got.stderr)
			}
			if occurrences != tc.occurrences {
				t.Errorf("preview printed %d occurrences, want %d", occurrences, tc.occurrences)
			}
		})
	}
}

// jobNameOf reads the name line out of a fixture.
func jobNameOf(source string) string {
	for _, line := range strings.Split(source, "\n") {
		if rest, ok := strings.CutPrefix(line, "name: "); ok {
			return rest
		}
	}
	return ""
}

// TestValidateAndApplyRefuseOneDuplicatedSensorName holds the rule the two
// commands share: a sensor name is a primary key across every job, so two
// files claiming one name cannot both materialise. validate is the gate a
// pipeline runs; a gate that passes what the deploy step refuses is worse than
// no gate at all.
func TestValidateAndApplyRefuseOneDuplicatedSensorName(t *testing.T) {
	sensorJob := func(name string) string {
		return `name: ` + name + `
steps:
  - name: build
    run: ["/bin/true"]
sensors:
  - name: dropzone
    kind: exec
    run: ["/srv/etl/probe.sh"]
    interval: 15s
`
	}
	dir := applyProject(t, map[string]string{
		"jobs/first.yaml":  sensorJob("first"),
		"jobs/second.yaml": sensorJob("second"),
	})

	validated := runCLI(t, dir, nil, "validate", "-o", "text")
	applied := runCLI(t, dir, nil, "apply", "-o", "text")

	if validated.code != applied.code {
		t.Fatalf("validate exited %d and apply exited %d over one directory\nvalidate:\n%s\napply:\n%s",
			validated.code, applied.code, validated.stdout, applied.stdout)
	}
	if validated.code != ExitValidation {
		t.Fatalf("both exited %d, want %d", validated.code, ExitValidation)
	}
	for _, want := range []string{"dropzone", "first.yaml", "second.yaml"} {
		if !strings.Contains(validated.stdout, want) {
			t.Errorf("the validate report never names %s:\n%s", want, validated.stdout)
		}
	}
}

// TestValidateAndApplyReadOneCatalog: PACEQ_JOBS_DIR names the catalog, and
// two commands that read different directories cannot be checking the same
// thing. The file lists are compared as lists, not as counts, so a resolver
// that reads the right number of the wrong files still fails.
func TestValidateAndApplyReadOneCatalog(t *testing.T) {
	cases := []struct {
		name string
		// files are written before init, which adds jobs/hello.yaml of its own.
		files map[string]string
		// dropJobsDir removes the jobs directory after init, for the setup that
		// keeps every spec outside the project.
		dropJobsDir bool
		env         map[string]string
		args        []string
		want        []string
	}{
		{
			name:        "the environment names the catalog and there is no jobs directory",
			files:       map[string]string{"specs/nightly.yaml": goodJob},
			dropJobsDir: true,
			env:         map[string]string{"PACEQ_JOBS_DIR": "specs"},
			want:        []string{"specs/nightly.yaml"},
		},
		{
			name: "the environment names a catalog beside a stale jobs directory",
			files: map[string]string{
				"jobs/stale.yaml":    goodJob,
				"specs/nightly.yaml": goodJob,
			},
			env:  map[string]string{"PACEQ_JOBS_DIR": "specs"},
			want: []string{"specs/nightly.yaml"},
		},
		{
			name:  "nothing set, the jobs directory answers",
			files: map[string]string{"jobs/nightly.yaml": goodJob},
			want:  []string{"jobs/hello.yaml", "jobs/nightly.yaml"},
		},
		{
			name:  "an explicit path wins over both",
			files: map[string]string{"jobs/nightly.yaml": goodJob, "specs/other.yaml": goodJob},
			env:   map[string]string{"PACEQ_JOBS_DIR": "specs"},
			args:  []string{"jobs/nightly.yaml"},
			want:  []string{"jobs/nightly.yaml"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := applyProject(t, tc.files)
			if tc.dropJobsDir {
				if err := os.RemoveAll(filepath.Join(dir, jobsDir)); err != nil {
					t.Fatalf("remove the jobs directory: %v", err)
				}
			}

			validated := runCLI(t, dir, tc.env, append([]string{"-vv", "validate"}, tc.args...)...)
			applied := runCLI(t, dir, tc.env, append([]string{"-vv", "apply"}, tc.args...)...)

			byValidate := filesNamedOn(validated.stderr)
			byApply := filesNamedOn(applied.stderr)
			if strings.Join(byValidate, " ") != strings.Join(byApply, " ") {
				t.Fatalf("validate read %v and apply read %v\nvalidate:\n%s\napply:\n%s",
					byValidate, byApply, validated.stderr, applied.stderr)
			}
			if strings.Join(byValidate, " ") != strings.Join(tc.want, " ") {
				t.Fatalf("both commands read %v, want %v", byValidate, tc.want)
			}
		})
	}
}

// filesNamedOn picks the job files out of the progress notes both commands
// write at -vv, which is where each one says what it opened. Only the notes
// count: a hint that spells out an example command names a file nobody read.
func filesNamedOn(notes string) []string {
	var files []string
	for _, line := range strings.Split(notes, "\n") {
		for _, prefix := range []string{"paceq: reading ", "paceq: parsing "} {
			rest, ok := strings.CutPrefix(line, prefix)
			if !ok {
				continue
			}
			if strings.HasSuffix(rest, ".yaml") || strings.HasSuffix(rest, ".yml") {
				files = append(files, rest)
			}
		}
	}
	sort.Strings(files)
	return files
}
