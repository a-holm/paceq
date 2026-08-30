//go:build !race || rapid

package store

// The property test for the sensor cursor guarantee (M3-08, issue #16). The
// store seam lands in this package, so the test lives here too: it is a model
// based sweep over a real SQLite file in t.TempDir(), not a unit test, and
// every action checks the invariants the guarantee is measured on.
//
// The model is exact, not a predictor. Because the driver is the only writer
// and runs one sensor at a time, the store must commit exactly the run keys
// and cursor the driver hands it, nothing more and nothing less. So the
// strongest possible invariant is model-versus-database equality: the run_keys
// set in the database equals the set of keys the model says were committed,
// the cursor and epoch in the database equal the model's, and no key the
// cursor has passed sits under the cursor without a committed run (plan 02
// section 4.3, invariant I7, plus the complement: no trigger is lost).
//
// The crash a property test can run in-process is the "cut the connection
// before commit" kind: an evaluation is begun and abandoned, or begun and
// overtaken by a newer evaluation whose commit fences the older one. Both are
// the durable-state equivalent of a mid-commit SIGKILL, because the atomic
// transaction rolls the whole window back; the honest SIGKILL variant lives in
// the M3-03 crash harness and the concurrency proof in dedup_epoch_test.go.

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
)

// Test tuning. The sweep is a loop over seeds, each seed a fresh store and a
// fresh pseudorandom action sequence. Both knobs default to something fast
// enough for every PR and can be raised on the command line, with no new
// dependency:
//
//	go test ./internal/store -run TestSensorCursorProperties -prop.seeds 10000
var (
	propSeeds   = flag.Int("prop.seeds", 100, "sensor cursor property seeds to run")
	propActions = flag.Int("prop.actions", 15, "sensor cursor actions per seed")
	propSeed    = flag.Int64("prop.seed", 1, "base prng seed for the property test")
)

// propName is the one sensor the whole property drives.
const propName = "cursor-property"

// maxTriggers is the seeded sensor's per-tick ceiling; a truncated tick overruns
// it and must stop at this many triggers.
const maxTriggers = 100

// keyStr turns a key index into the cursor string the world speaks. The world
// is an infinite tape of monotone files; a cursor of k means every file with
// index <= k has been delivered. Index 0 is the empty cursor.
func keyStr(k int) string {
	if k <= 0 {
		return ""
	}
	return fmt.Sprintf("file:%d", k)
}

// keyOf parses a cursor string back to an index. "" parses to 0, and the
// driver only ever writes cursors it generated, so a bad parse is a bug in the
// test itself and a panic names it immediately.
func keyOf(s string) int {
	if s == "" {
		return 0
	}
	var k int
	if _, err := fmt.Sscanf(s, "file:%d", &k); err != nil {
		panic("cursor property: unparseable cursor " + s)
	}
	return k
}

// propModel is what the guarantee promises, tracked key by key and epoch by
// epoch. emitted[e] is the set of keys that got a committed run in epoch e;
// exempt[e] is the set of keys an operator moved the cursor past without a run
// (a cursor_set or a reset that jumps the cursor). A key under the cursor must
// be in one or the other, which is exactly the "cursor never advances without
// its committed triggers" sentence.
type propModel struct {
	epoch     int64
	cursorKey int
	manCursor bool // the cursor was last moved by an operator, not a tick

	emitted map[int64]map[int]struct{}
	exempt  map[int64]map[int]struct{}

	// worldMax is the highest key index the world has reached, bounded by
	// worldCap so each seed's database stays small and the sweep stays fast.
	// The cursor guarantee is about monotonicity and coverage, not about how
	// many keys exist, so a bounded world exercises it just as hard.
	worldMax int
}

// propWorldCap bounds how many distinct keys the world ever reaches. It keeps
// a seed's commit and invariant work small no matter how many actions run.
const propWorldCap = 150

func newPropModel() *propModel {
	m := &propModel{
		emitted: map[int64]map[int]struct{}{},
		exempt:  map[int64]map[int]struct{}{},
	}
	m.emitted[0] = map[int]struct{}{}
	m.exempt[0] = map[int]struct{}{}
	return m
}

// commit records one committed triggered tick in the model and reports how
// many keys were accepted (first delivery in the epoch) versus deduped
// (already seen). Because the driver is the only writer, the commit result
// must match these counts exactly.
func (m *propModel) commit(keys []int, after int) (newKeys, dups int) {
	if m.emitted[m.epoch] == nil {
		m.emitted[m.epoch] = map[int]struct{}{}
	}
	for _, k := range keys {
		if _, seen := m.emitted[m.epoch][k]; !seen {
			newKeys++
		} else {
			dups++
		}
		m.emitted[m.epoch][k] = struct{}{}
	}
	m.cursorKey = after
	m.manCursor = false
	return newKeys, dups
}

// skip records a skipped tick: the cursor does not move, whatever watermark the
// sensor reported along with its skip, and the cursor is still owned by the last
// writer of the sensor, which was a committed tick or an operator, so the manual
// flag is left as it was.
func (m *propModel) skip() {}

// advance grows the world by n monotone keys, stopping at the cap. A sensor
// that finds the world already fully delivered simply skips, exactly as a real
// sensor idles when nothing is new.
func (m *propModel) advance(n int) {
	m.worldMax += n
	if m.worldMax > propWorldCap {
		m.worldMax = propWorldCap
	}
}

// cursorSet records an operator cursor move. Keys the operator stepped past
// (between the old cursor and the new one) that were never delivered become
// exempt: the operator declared them done, so no run is required of them.
func (m *propModel) cursorSet(newKey int) {
	prev := m.cursorKey
	m.cursorKey = newKey
	m.manCursor = true
	if m.exempt[m.epoch] == nil {
		m.exempt[m.epoch] = map[int]struct{}{}
	}
	for k := prev + 1; k <= newKey; k++ {
		m.exempt[m.epoch][k] = struct{}{}
	}
}

// reset records a reset: the epoch bumps, the new epoch starts empty, and an
// operator cursor (if any) exempts every key at or below it in the new epoch,
// because those are declared done and will not be re-run.
func (m *propModel) reset(setKey int, hasSet bool) {
	m.epoch++
	m.emitted[m.epoch] = map[int]struct{}{}
	m.exempt[m.epoch] = map[int]struct{}{}
	m.cursorKey = 0
	m.manCursor = true
	if hasSet && setKey > 0 {
		for k := 1; k <= setKey; k++ {
			m.exempt[m.epoch][k] = struct{}{}
		}
		m.cursorKey = setKey
	}
}

// propSUT is the store under test, opened on a real SQLite file with a fake
// clock the test can jump, exactly like the guarantee demands of real storage.
type propSUT struct {
	s   *Store
	clk *clock.Fake
}

func newPropSUT(t *testing.T) *propSUT {
	t.Helper()
	s := migratedStore(t)
	seedSensorJob(t, s)
	clk := clock.NewFake(time.Unix(1_720_000_000, 0))
	s.clk = clk
	seedSensor(t, s, propName, "", 0)
	return &propSUT{s: s, clk: clk}
}

// now is the current fake wall time, advanced once per action so timestamps
// are deterministic under a fixed seed.
func (p *propSUT) now() time.Time { return p.clk.Now() }

// TestSensorCursorProperties is the model based sweep. Each seed is a fresh
// store and a fresh action sequence; after every action the invariants are
// checked against the live database. Under a fixed seed the whole run is
// deterministic, so any failure names the seed that reproduces it.
func TestSensorCursorProperties(t *testing.T) {
	for s := int64(0); s < int64(*propSeeds); s++ {
		seed := *propSeed + s
		t.Run(fmt.Sprintf("seed_%012d", seed), func(t *testing.T) {
			r := rand.New(rand.NewSource(seed))
			sut := newPropSUT(t)
			m := newPropModel()
			ctx := context.Background()
			crashes := int64(0)

			for a := 0; a < *propActions; a++ {
				// Every action advances wall time a little first, so
				// clock jumps and ordinary passage both land in the
				// timestamps the store writes.
				sut.clk.Advance(time.Duration(5+r.Intn(300)) * time.Millisecond)

				step := r.Intn(100)
				switch {
				case step < 35:
					sensorPropTick(t, ctx, sut, m, false)
				case step < 45:
					sensorPropTick(t, ctx, sut, m, true)
				case step < 52:
					sensorPropSkip(t, ctx, sut, m)
				case step < 60:
					sensorPropCrash(t, ctx, sut, m)
					crashes++
				case step < 68:
					sensorPropFence(t, ctx, sut, m, r)
					crashes++
				case step < 76:
					sensorPropReset(t, ctx, sut, m, r)
				case step < 84:
					sensorPropCursorSet(t, ctx, sut, m, r)
				case step < 90:
					sensorPropRestart(t, ctx, sut)
				case step < 97:
					// clock_jump: move wall time only, the NTP shape.
					sut.clk.JumpWall(time.Duration(r.Intn(90000)-45000) * time.Millisecond)
				default:
					// pause / resume: no database effect on this
					// path, so the step just verifies the
					// invariants hold with nothing changing.
				}

				checkSensorInvariants(t, ctx, sut, m, crashes, seed, a)
			}
		})
	}
}

// emitRange computes, for a tick starting at the current cursor, which keys
// this evaluation delivers and where the cursor lands. A truncated tick stops
// at the sensor's max_triggers ceiling and leaves the rest for a later pass.
func emitRange(m *propModel, truncated bool) (keys []int, after int) {
	lo := m.cursorKey
	hi := m.worldMax
	if hi <= lo {
		return nil, lo
	}
	if truncated && hi-lo > maxTriggers {
		after = lo + maxTriggers
	} else {
		after = hi
	}
	for k := lo + 1; k <= after; k++ {
		keys = append(keys, k)
	}
	return keys, after
}

// sensorPropTick runs one triggered evaluation: the world advances, the sensor
// delivers the keys its cursor now covers, and the atomic commit records them.
func sensorPropTick(t *testing.T, ctx context.Context, sut *propSUT, m *propModel, truncated bool) {
	t.Helper()
	if truncated {
		m.advance(101 + mRandN(190))
	} else {
		m.advance(mRandN(5))
	}
	keys, after := emitRange(m, truncated)
	if len(keys) == 0 {
		sensorPropSkip(t, ctx, sut, m)
		return
	}

	begin, err := sut.s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName:   propName,
		CursorBefore: keyStr(m.cursorKey),
		Now:          sut.now(),
	})
	if err != nil {
		t.Fatalf("begin sensor tick: %v", err)
	}

	out, err := sut.s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    propName,
		JobName:       sensorJob,
		CursorVersion: begin.CursorVersion,
		CursorAfter:   keyStr(after),
		DedupEpoch:    m.epoch,
		Outcome:       OutcomeTriggered,
		NextEvalAt:    sut.now().UnixMilli() + 60000,
		DurationMs:    12,
		Now:           sut.now(),
		Triggers:      triggerList(keys),
	})
	if err != nil {
		t.Fatalf("commit sensor tick: %v", err)
	}
	if out.Fenced {
		t.Fatalf("a fresh evaluation was fenced; there is no concurrent writer")
	}
	wantNew, wantDup := m.commit(keys, after)
	if out.Accepted != wantNew || out.Deduped != wantDup {
		t.Fatalf("commit accepted %d deduped %d, want %d/%d for %d keys",
			out.Accepted, out.Deduped, wantNew, wantDup, len(keys))
	}
}

// mRandN is a tiny wrapper so the world advance is driven by a deterministic
// source shared across the whole test run without threading a *rand.Rand into
// every helper. It is only used for sizes, not for which action runs.
var propRand = rand.New(rand.NewSource(0x5EED))

func mRandN(n int) int { return propRand.Intn(n) }

// triggerList builds the store's trigger slice from key indices.
func triggerList(keys []int) []SensorTrigger {
	out := make([]SensorTrigger, 0, len(keys))
	for _, k := range keys {
		out = append(out, SensorTrigger{RunKey: keyStr(k)})
	}
	return out
}

// sensorPropSkip records a skipped evaluation: the cursor does not move and no
// run is produced, but the tick closes with the sensor's own reason and the
// cursor_version is bumped by the commit path.
func sensorPropSkip(t *testing.T, ctx context.Context, sut *propSUT, m *propModel) {
	t.Helper()
	begin, err := sut.s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName:   propName,
		CursorBefore: keyStr(m.cursorKey),
		Now:          sut.now(),
	})
	if err != nil {
		t.Fatalf("begin sensor skip: %v", err)
	}
	out, err := sut.s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    propName,
		JobName:       sensorJob,
		CursorVersion: begin.CursorVersion,
		// The sensor reports how far it looked. It elected nothing there, so
		// the cursor may not follow it: every key between the cursor and the
		// watermark would be stepped over with no run behind it.
		CursorAfter: keyStr(m.worldMax),
		DedupEpoch:  m.epoch,
		Outcome:     OutcomeSkipped,
		ReasonCode:  reason.TICKSkippedSensor,
		ReasonText:  "nothing new in the property world",
		NextEvalAt:  sut.now().UnixMilli() + 60000,
		Now:         sut.now(),
	})
	if err != nil {
		t.Fatalf("commit sensor skip: %v", err)
	}
	if out.Fenced {
		t.Fatalf("a fresh skip was fenced; there is no concurrent writer")
	}
	m.skip()
}

// sensorPropCrash cuts the connection before commit: an evaluation is begun
// (the intention row survives) and then abandoned. This is the in-process
// equivalent of dying between BEGIN and COMMIT, which the atomic transaction
// rolls back whole. The abandoned running tick is left for restart to
// reconcile; it never advances the cursor.
func sensorPropCrash(t *testing.T, ctx context.Context, sut *propSUT, m *propModel) {
	t.Helper()
	if _, err := sut.s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName:   propName,
		CursorBefore: keyStr(m.cursorKey),
		Now:          sut.now(),
	}); err != nil {
		t.Fatalf("begin the doomed evaluation: %v", err)
	}
	// No commit follows. The running intention row is durable; a restart
	// reconciles it, and the cursor is exactly where the crash left it.
}

// sensorPropFence races two evaluations and lets the newer one win. The older
// evaluation's commit is refused by the cursor CAS, proving an overtaken
// evaluation never writes an old result over a new one. The lost evaluation
// counts as a crash for the duplicate bound.
func sensorPropFence(t *testing.T, ctx context.Context, sut *propSUT, m *propModel, r *rand.Rand) {
	t.Helper()
	m.advance(1 + r.Intn(3))
	keys, after := emitRange(m, false)

	// The evaluation that will lose reads the current version...
	losing, err := sut.s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName:   propName,
		CursorBefore: keyStr(m.cursorKey),
		Now:          sut.now(),
	})
	if err != nil {
		t.Fatalf("begin the losing evaluation: %v", err)
	}

	// ...and a second evaluation starts from the same version and commits
	// first, winning the cursor.
	winning, err := sut.s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName:   propName,
		CursorBefore: keyStr(m.cursorKey),
		Now:          sut.now(),
	})
	if err != nil {
		t.Fatalf("begin the winning evaluation: %v", err)
	}
	wOut, err := sut.s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        winning.TickID,
		SensorName:    propName,
		JobName:       sensorJob,
		CursorVersion: winning.CursorVersion,
		CursorAfter:   keyStr(after),
		DedupEpoch:    m.epoch,
		Outcome:       OutcomeTriggered,
		NextEvalAt:    sut.now().UnixMilli() + 60000,
		Now:           sut.now(),
		Triggers:      triggerList(keys),
	})
	if err != nil {
		t.Fatalf("commit the winning evaluation: %v", err)
	}
	wantNew, wantDup := m.commit(keys, after)
	if wOut.Accepted != wantNew || wOut.Deduped != wantDup {
		t.Fatalf("winner accepted %d deduped %d, want %d/%d",
			wOut.Accepted, wOut.Deduped, wantNew, wantDup)
	}

	// The losing evaluation commits with a stale cursor_version and must be
	// refused: nothing it decided may land.
	lOut, err := sut.s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        losing.TickID,
		SensorName:    propName,
		JobName:       sensorJob,
		CursorVersion: losing.CursorVersion,
		CursorAfter:   keyStr(after),
		DedupEpoch:    m.epoch,
		Outcome:       OutcomeTriggered,
		NextEvalAt:    sut.now().UnixMilli() + 60000,
		Now:           sut.now(),
		Triggers:      triggerList(keys),
	})
	if err != nil {
		t.Fatalf("commit the losing evaluation: %v", err)
	}
	if !lOut.Fenced {
		t.Fatalf("a stale evaluation was not fenced by the cursor CAS")
	}
	if lOut.Accepted != 0 || lOut.Deduped != 0 || len(lOut.RunIDs) != 0 {
		t.Fatalf("fenced commit wrote runs: accepted %d deduped %d runs %d",
			lOut.Accepted, lOut.Deduped, len(lOut.RunIDs))
	}
}

// sensorPropReset bumps the dedup epoch. It may or may not carry an operator
// cursor; either way the model and the store must agree on the new epoch and
// on which keys the new epoch is exempt from re-running.
func sensorPropReset(t *testing.T, ctx context.Context, sut *propSUT, m *propModel, r *rand.Rand) {
	t.Helper()
	hasSet := r.Intn(2) == 0
	var setKey int
	if hasSet {
		setKey = r.Intn(m.worldMax + 1)
	}
	var ptr *string
	if hasSet {
		v := keyStr(setKey)
		ptr = &v
	}
	res, err := sut.s.ResetSensor(ctx, ResetSensorInput{Name: propName, SetCursor: ptr})
	if err != nil {
		t.Fatalf("reset sensor: %v", err)
	}
	if res.NewEpoch != m.epoch+1 {
		t.Fatalf("reset epoch = %d, want %d (one bump)", res.NewEpoch, m.epoch+1)
	}
	m.reset(setKey, hasSet)
}

// sensorPropCursorSet moves the cursor without touching the epoch, the
// operator's "spool without replay" move. Moving it forward exempts the keys
// it steps over; moving it backward means the next tick re-delivers them,
// which the run_keys gate dedups.
func sensorPropCursorSet(t *testing.T, ctx context.Context, sut *propSUT, m *propModel, r *rand.Rand) {
	t.Helper()
	target := r.Intn(m.worldMax + 1)
	if err := sut.s.SetSensorCursor(ctx, CursorInput{Name: propName, Cursor: keyStr(target)}); err != nil {
		t.Fatalf("set sensor cursor to %d: %v", target, err)
	}
	m.cursorSet(target)
}

// sensorPropRestart reconciles any running intention rows a crash left behind,
// exactly as a daemon restart does. It is a real state change the invariants
// then re-check, not a no-op.
func sensorPropRestart(t *testing.T, ctx context.Context, sut *propSUT) {
	t.Helper()
	if _, err := sut.s.w.ExecContext(ctx, `UPDATE ticks
SET outcome = 'error', finished_at = ?, reason_code = ?
WHERE source_kind = 'sensor' AND source_name = ? AND outcome = 'running'`,
		sut.now().UnixMilli(), string(reason.TICKErrorSensorFailed), propName); err != nil {
		t.Fatalf("reconcile running sensor ticks: %v", err)
	}
}

// checkSensorInvariants is the "" action of the property: it runs after every
// step and refuses any drift between what the guarantee promises and what the
// database actually holds. Every fatal here is a real violation of the cursor
// guarantee and names the seed and action that reproduced it.
func checkSensorInvariants(t *testing.T, ctx context.Context, sut *propSUT, m *propModel, crashes, seed int64, action int) {
	t.Helper()
	s := sut.s

	// 1. The database cursor and epoch equal the model's. A mismatch means a
	// write happened that the model did not see (or a step the model expected
	// never landed), and there is no point reading any further invariant.
	var dbEpoch int64
	var dbCursor sql.NullString
	if err := s.r.QueryRowContext(ctx,
		`SELECT dedup_epoch, cursor FROM sensors WHERE name = ?`, propName).Scan(&dbEpoch, &dbCursor); err != nil {
		t.Fatalf("read sensor row: %v", err)
	}
	if dbEpoch != m.epoch {
		t.Fatalf("epoch = %d, model says %d (reset did not bump?)", dbEpoch, m.epoch)
	}
	if dbCursor.String != keyStr(m.cursorKey) {
		t.Fatalf("cursor = %q, model says %q (a write was lost or an old one won)",
			dbCursor.String, keyStr(m.cursorKey))
	}

	// 2. Invariant I7 as SQL: a non-empty cursor that was not moved by an
	// operator must be the cursor_after of a committed tick. The operator
	// exception is the cursor_set and reset paths, which legitimately move the
	// cursor without a tick.
	if dbCursor.String != "" && !m.manCursor {
		var found int
		err := s.r.QueryRowContext(ctx, `SELECT 1 FROM ticks t
WHERE t.source_kind = 'sensor' AND t.source_name = ?
  AND t.outcome IN ('triggered','skipped') AND t.finished_at IS NOT NULL
  AND t.cursor_after = ? LIMIT 1`, propName, dbCursor.String).Scan(&found)
		if err != nil {
			t.Fatalf("I7: cursor %q advanced with no committed tick carrying it (seed %d action %d)",
				dbCursor.String, seed, action)
		}
	}

	// 3. The run_keys set in the database equals the model's committed set,
	// across every epoch. This is the whole "no lost trigger, no duplicate
	// run" proof in one equality: a key the model committed must be present
	// with its run, and a key the model never committed must not be present at
	// all. The primary key on (source_id, epoch, run_key) makes the set unique
	// by construction, so duplicates cannot hide.
	dbKeys := map[int64]map[int]struct{}{}
	rows, err := s.r.QueryContext(ctx, `SELECT epoch, run_key FROM run_keys WHERE source_id = ?`, propName)
	if err != nil {
		t.Fatalf("read run_keys: %v", err)
	}
	for rows.Next() {
		var e int64
		var rk string
		if err := rows.Scan(&e, &rk); err != nil {
			_ = rows.Close()
			t.Fatalf("scan run_key: %v", err)
		}
		if dbKeys[e] == nil {
			dbKeys[e] = map[int]struct{}{}
		}
		dbKeys[e][keyOf(rk)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("run_keys rows: %v", err)
	}
	_ = rows.Close()

	for e, keys := range m.emitted {
		if len(keys) == 0 {
			continue
		}
		if dbKeys[e] == nil {
			t.Fatalf("epoch %d has %d model keys but no run_keys rows (a whole commit vanished)",
				e, len(keys))
		}
		for k := range keys {
			if _, ok := dbKeys[e][k]; !ok {
				t.Fatalf("key %s at epoch %d was committed but has no run_keys row (lost trigger)",
					keyStr(k), e)
			}
		}
	}
	for e, keys := range dbKeys {
		model := m.emitted[e]
		for k := range keys {
			if _, ok := model[k]; !ok {
				t.Fatalf("run_keys holds %s at epoch %d the model never committed (stray trigger)",
					keyStr(k), e)
			}
		}
	}

	// 4. Every run_keys row names a real run. A dangling run_id is a run that
	// was half written, which the atomic transaction must forbid.
	var dangling int
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_keys rk
LEFT JOIN runs r ON r.id = rk.run_id
WHERE rk.source_id = ? AND (rk.run_id IS NULL OR r.id IS NULL)`, propName).Scan(&dangling); err != nil {
		t.Fatalf("count dangling run_keys: %v", err)
	}
	if dangling != 0 {
		t.Fatalf("%d run_keys rows point at no run", dangling)
	}

	// 5. No sensor run is orphaned: every sensor run the store created must be
	// named by a run_keys row, otherwise a duplicate run slipped in past the
	// dedup gate.
	var orphan int
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs r
WHERE r.origin = 'sensor' AND NOT EXISTS (
  SELECT 1 FROM run_keys rk WHERE rk.run_id = r.id)`).Scan(&orphan); err != nil {
		t.Fatalf("count orphan runs: %v", err)
	}
	if orphan != 0 {
		t.Fatalf("%d sensor runs have no run_keys row (duplicate created past the gate)", orphan)
	}

	// 6. Every accepted trigger has a run, and every deduped trigger names the
	// run it folded into. This is the trigger-level half of "no lost trigger".
	var badTriggers int
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM triggers tr
WHERE tr.job_name = ? AND (
  (tr.outcome = 'accepted' AND NOT EXISTS (
     SELECT 1 FROM runs r WHERE r.trigger_id = tr.id))
  OR (tr.outcome = 'deduped' AND (tr.run_id IS NULL OR NOT EXISTS (
     SELECT 1 FROM runs r WHERE r.id = tr.run_id))))`, sensorJob).Scan(&badTriggers); err != nil {
		t.Fatalf("count broken triggers: %v", err)
	}
	if badTriggers != 0 {
		t.Fatalf("%d triggers lost their run link", badTriggers)
	}

	// 7. The coverage sentence: a non-empty cursor must not sit over a key in
	// the current epoch that was neither delivered nor declared done. Every
	// key at or below the cursor either has a committed run this epoch or was
	// exempted by an operator move. This is the last line of the guarantee and
	// the reason the crash harness cannot cover: it holds across every
	// interleaving the PRNG throws at it.
	if m.cursorKey > 0 {
		emitted := m.emitted[m.epoch]
		exempt := m.exempt[m.epoch]
		var missing []int
		for k := 1; k <= m.cursorKey; k++ {
			_, had := emitted[k]
			_, skipped := exempt[k]
			if !had && !skipped {
				missing = append(missing, k)
			}
		}
		if len(missing) > 0 {
			sort.Ints(missing)
			t.Fatalf("cursor covers %d keys with no committed run and no operator exemption, first %v (seed %d action %d)",
				len(missing), missing, seed, action)
		}
	}

	// 8. The duplicate bound, exactly. A crash rolls its whole evaluation
	// back, so it can never mint a second run; a reset gives each re-delivered
	// key a fresh run in the new epoch, which the model counts. So the total
	// sensor run count must equal the number of (epoch, key) deliveries the
	// model recorded, and each logical trigger therefore holds exactly one run
	// within the looser guarantee 1 <= n <= 1 + crashes the design promises,
	// which this sweep pins more tightly to equality).
	var totalRuns int
	if err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs r
WHERE r.origin = 'sensor'`).Scan(&totalRuns); err != nil {
		t.Fatalf("count sensor runs: %v", err)
	}
	emittedCount := 0
	for _, keys := range m.emitted {
		emittedCount += len(keys)
	}
	if totalRuns != emittedCount {
		t.Fatalf("sensor runs = %d, want %d (a delivered key lost its only run, or a duplicate run exists)",
			totalRuns, emittedCount)
	}
	if totalRuns > emittedCount*(1+int(crashes)) {
		t.Fatalf("sensor runs = %d exceeds the 1+crashes duplicate bound", totalRuns)
	}
}
