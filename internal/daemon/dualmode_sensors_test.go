package daemon

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// Issue #207: a sensor write has to land the same way whether it travelled
// over the daemon socket or straight into the database. It did not. The socket
// carried the sensor name and nothing else, so with a daemon up a cursor move
// stored the empty string, a --cursor reset became a full replay, and a
// --forget-run-keys deleted nothing after the operator typed the sensor name
// to confirm it.
//
// This drives the shipped binary twice over identical state, once against a
// listening daemon and once with nothing in between, and demands the same row
// both times. It is the guard the earlier tests could not be, because they
// covered one of the two paths by design.

// sensorState is the part of a sensors row a write has to land the same way on
// either transport. The timestamps are left out: the two runs happen at
// different moments, and that difference is not the question.
type sensorState struct {
	Paused        bool
	PausedReason  string
	Cursor        string
	CursorVersion int64
	DedupEpoch    int64
	RunKeys       int
}

func (s sensorState) String() string {
	return "paused=" + strconv.FormatBool(s.Paused) +
		" reason=" + strconv.Quote(s.PausedReason) +
		" cursor=" + s.Cursor +
		" cursor_version=" + strconv.FormatInt(s.CursorVersion, 10) +
		" dedup_epoch=" + strconv.FormatInt(s.DedupEpoch, 10) +
		" run_keys=" + strconv.Itoa(s.RunKeys)
}

// seedSensorProject records a job, a sensor and one dedup key belonging to
// that sensor, so a reset that claims to forget run keys has something to
// forget.
func seedSensorProject(t *testing.T, ctx context.Context, s *store.Store) {
	t.Helper()

	const spec = `{"schema":"paceq.job.v1","name":"polling-job","max_concurrent":1,` +
		`"timeout_ms":3600000,"steps":[{"name":"c","run":["/bin/true"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "polling-job", SpecHash: "sha256:polling", SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	if err := s.UpsertSensor(ctx, store.SensorSeedInput{
		Name: "finder", JobName: "polling-job", ExecJSON: `["/bin/true"]`,
	}); err != nil {
		t.Fatalf("seed the sensor: %v", err)
	}
	queued, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "polling-job"})
	if err != nil {
		t.Fatalf("queue a run for the dedup key: %v", err)
	}
	if err := s.InjectRunKey(ctx, queued.Run.ID, "finder", "already-seen"); err != nil {
		t.Fatalf("plant the dedup key: %v", err)
	}
}

// readSensorState reads back what the write did.
func readSensorState(t *testing.T, ctx context.Context, s *store.Store) sensorState {
	t.Helper()

	row, err := s.GetSensor(ctx, "finder")
	if err != nil {
		t.Fatalf("read the sensor: %v", err)
	}
	keys, err := s.RunKeysSnapshot(ctx)
	if err != nil {
		t.Fatalf("read the dedup keys: %v", err)
	}
	out := sensorState{
		Paused:        row.Paused,
		PausedReason:  row.PausedReason,
		Cursor:        "(none)",
		CursorVersion: row.CursorVersion,
		DedupEpoch:    row.DedupEpoch,
	}
	if row.Cursor != nil {
		out.Cursor = strconv.Quote(*row.Cursor)
	}
	for _, key := range keys {
		if strings.HasPrefix(key, "finder/") {
			out.RunKeys++
		}
	}
	return out
}

// runSensorWrite drives one command of the shipped binary against a fresh
// project, with the daemon listening or with nothing there, and reports the
// row it left behind.
func runSensorWrite(t *testing.T, bin string, withDaemon bool, argv ...string) sensorState {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()

	paceqRun(t, bin, dir, "init")

	stateDir := filepath.Join(dir, ".paceq")
	s, err := store.Open(ctx, filepath.Join(stateDir, store.DatabaseFileName), store.Options{})
	if err != nil {
		t.Fatalf("open the state: %v", err)
	}
	defer func() { _ = s.Close() }()
	seedSensorProject(t, ctx, s)

	socket := "none"
	if withDaemon {
		// The socket does not live in the state directory: a unix socket
		// path is capped at 107 bytes and a path under t.TempDir() spends
		// most of them on the test's own name.
		socket = testutil.SocketPath(t)
		rec, logger := newRecLog()
		cfg := Config{
			StateDir:   stateDir,
			Version:    "sensorparity",
			SocketPath: socket,
			Logger:     logger,
		}
		sts := newStatuses(func() time.Time { return time.Unix(0, 0).UTC() })
		stop := startHealthEndpoint(cfg, sts, cfg.Logger, s, nil, nil)
		if stop == nil {
			t.Fatalf("the endpoints did not start on %s (%d bytes); the daemon logged:\n%s",
				socket, len(socket), rec.text())
		}
		defer func() { stop(context.Background()) }()
	}

	paceqRun(t, bin, dir, append([]string{"--socket", socket}, argv...)...)
	return readSensorState(t, ctx, s)
}

// TestSensorWritesLandTheSameWayOverBothTransports is the equivalence proof.
// Every sensor write command that changes a row is run on both paths against
// identical state, and the rows have to match.
//
// tick is not here: with a daemon up it makes the sensor due and the daemon's
// evaluator does the work later, and without one the evaluation runs in the
// command itself. The two paths deliberately do different work, so the row is
// not the thing that compares. Its request body is covered in internal/cli.
func TestSensorWritesLandTheSameWayOverBothTransports(t *testing.T) {
	bin := buildPaceq(t)

	cases := map[string][]string{
		"pause with a reason":   {"sensors", "pause", "finder", "--reason", "deploying"},
		"resume":                {"sensors", "resume", "finder"},
		"reset from a cursor":   {"sensors", "reset", "finder", "--cursor", "2026-08-21/09-20-03.csv"},
		"reset forgetting keys": {"sensors", "reset", "finder", "--forget-run-keys", "--yes"},
		"cursor set":            {"sensors", "cursor", "set", "finder", "2026-08-21/09-20-03.csv"},
	}

	for name, argv := range cases {
		t.Run(name, func(t *testing.T) {
			overSocket := runSensorWrite(t, bin, true, argv...)
			direct := runSensorWrite(t, bin, false, argv...)
			if overSocket != direct {
				t.Fatalf("paceq %s left different state on the two paths\nover the socket: %s\ndirect:         %s",
					strings.Join(argv, " "), overSocket, direct)
			}
		})
	}
}

// TestSensorResetOverTheSocketReallyForgetsTheRunKeys pins the worst of the
// three harms on its own, because equality alone would also be satisfied by
// both paths doing nothing. The operator typed the sensor name to confirm an
// irreversible delete; the delete has to have happened.
func TestSensorResetOverTheSocketReallyForgetsTheRunKeys(t *testing.T) {
	got := runSensorWrite(t, buildPaceq(t), true,
		"sensors", "reset", "finder", "--forget-run-keys", "--yes")

	if got.RunKeys != 0 {
		t.Errorf("%d dedup keys survived a confirmed --forget-run-keys", got.RunKeys)
	}
	if got.DedupEpoch != 1 {
		t.Errorf("dedup_epoch = %d, want 1", got.DedupEpoch)
	}
}

// TestSensorCursorSetOverTheSocketStoresTheValue is the reproduction: the
// command an operator runs to skip a bad batch, with a daemon up.
func TestSensorCursorSetOverTheSocketStoresTheValue(t *testing.T) {
	const value = "2026-08-21/09-20-03.csv"
	got := runSensorWrite(t, buildPaceq(t), true, "sensors", "cursor", "set", "finder", value)

	if got.Cursor != strconv.Quote(value) {
		t.Fatalf("the cursor reads %s, want %s", got.Cursor, strconv.Quote(value))
	}
}
