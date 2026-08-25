package obs

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// scrapeTimeout is the whole scrape's deadline (06 section 6.4 design rule
// 6): monitoring that hangs is worse than monitoring that misses a field, so
// every database read shares this budget and whatever answered in time is
// what the answer contains.
const scrapeTimeout = 2 * time.Second

// Source is the read surface the collector needs, satisfied by *store.Store.
// It exists so tests can feed fixed datasets without a database, which is
// what keeps the golden test byte-stable and the cardinality test instant.
type Source interface {
	MetricsRunsByStates(ctx context.Context) ([]store.MetricsJobStateCount, error)
	MetricsLastSuccesses(ctx context.Context) ([]store.MetricsJobStamp, error)
	MetricsJobSLAs(ctx context.Context) ([]store.MetricsJobSLA, error)
	MetricsScheduleStates(ctx context.Context) ([]store.MetricsInstigatorState, error)
	MetricsSensorStates(ctx context.Context) ([]store.MetricsInstigatorState, error)
	MetricsTickLags(ctx context.Context) ([]store.MetricsSourceLag, error)
	MetricsLastTicks(ctx context.Context) ([]store.MetricsSourceStamp, error)
	MetricsOutageSeconds(ctx context.Context) (float64, error)
	MetricsMetaValues(ctx context.Context) (map[string]string, error)
	MetricsDBBytes(ctx context.Context) (int64, int64, error)

	// The writer-health pair lives in memory inside the store, not in its
	// database: the max is taken (reset) by the scrape, the busy total is
	// a plain load.
	TakeWriteWaitMax() float64
	BusyTotal() uint64
}

var _ Source = (*store.Store)(nil)

// Identity is the build stamp pulseq_build_info carries. It comes from
// buildinfo.Get() in production and from fixed strings in tests.
type Identity struct {
	Version   string
	Commit    string
	GoVersion string
}

// Collector renders one /metrics document per Scrape. It holds the two
// sources and nothing else: counters for events, a Source for state, plus
// the identity and stamps that belong to neither.
type Collector struct {
	src       Source
	counters  *Counters
	clk       clock.Clock
	startedAt time.Time
	identity  Identity
	logDir    string
}

// NewCollector builds the scraper. logDir is walked for
// pulseq_log_dir_bytes; empty disables that family.
func NewCollector(src Source, counters *Counters, clk clock.Clock, id Identity, logDir string) *Collector {
	return &Collector{
		src:       src,
		counters:  counters,
		clk:       clk,
		startedAt: clk.Now().UTC(),
		identity:  id,
		logDir:    logDir,
	}
}

// Scrape renders one exposition document. Database families fail soft: a
// family whose read did not answer within the budget is simply absent, and
// pulseq_metrics_db_error 1 says so once. Build info, the in-memory counters
// and the writer-health gauges can always be written - they touch no
// database - so a scrape of a broken store still proves the process lives.
func (c *Collector) Scrape(ctx context.Context) []byte {
	ctx, cancel := context.WithTimeout(ctx, scrapeTimeout)
	defer cancel()

	now := c.clk.Now().UTC()
	w := NewWriter()
	c.writeIdentity(w)
	c.writeMemory(w)

	dbErr := false
	dbErr = c.writeJobFamilies(ctx, w) || dbErr
	dbErr = c.writeInstigatorFamilies(ctx, w, now) || dbErr
	dbErr = c.writeRunFamilies(ctx, w) || dbErr
	if size, wal, err := c.src.MetricsDBBytes(ctx); err != nil {
		dbErr = true
	} else {
		w.Help("pulseq_db_size_bytes", "Main database file size in bytes.", "gauge")
		w.Metric("pulseq_db_size_bytes", nil, float64(size))
		w.Help("pulseq_wal_size_bytes", "Size of the database's WAL companion file; growth means checkpointing is blocked.", "gauge")
		w.Metric("pulseq_wal_size_bytes", nil, float64(wal))
	}
	if seconds, err := c.src.MetricsOutageSeconds(ctx); err != nil {
		dbErr = true
	} else {
		w.Help("pulseq_outage_seconds_total", "Summed provable downtime across recorded outages.", "counter")
		w.Metric("pulseq_outage_seconds_total", nil, seconds)
	}
	c.writeMetaFamilies(ctx, w, &dbErr)
	if bytes, ok := logDirBytes(c.logDir); ok {
		w.Help("pulseq_log_dir_bytes", "Bytes of run logs currently kept on disk.", "gauge")
		w.Metric("pulseq_log_dir_bytes", nil, float64(bytes))
	}

	if dbErr {
		w.Help("pulseq_metrics_db_error", "1 when at least one state family could not be read at this scrape.", "gauge")
		w.Metric("pulseq_metrics_db_error", nil, 1)
	}
	return w.Bytes()
}

// writeIdentity emits the two series that are true of the process itself.
func (c *Collector) writeIdentity(w *Writer) {
	w.Help("pulseq_build_info", "Build information of the running daemon.", "gauge")
	w.Metric("pulseq_build_info", []L{
		Label("version", c.identity.Version),
		Label("commit", c.identity.Commit),
		Label("go_version", c.identity.GoVersion),
	}, 1)
	w.Help("pulseq_daemon_start_timestamp_seconds", "When the running daemon started, in seconds since the epoch.", "gauge")
	w.Metric("pulseq_daemon_start_timestamp_seconds", nil, float64(c.startedAt.Unix()))
}

// writeMemory emits everything the in-memory half owns: the event counters
// and the writer-health pair. None of this can fail.
func (c *Collector) writeMemory(w *Writer) {
	ticks := c.counters.snapshotTicks()
	reclaims := c.counters.snapshotReclaims()

	w.Help("pulseq_tick_total", "Decided evaluations per instigator, status and reason.", "counter")
	writeSortedTicks(w, ticks)

	w.Help("pulseq_lease_reclaims_total", "Runs whose lease was taken away from a dead holder.", "counter")
	w.Metric("pulseq_lease_reclaims_total", nil, float64(reclaims))

	w.Help("pulseq_db_write_wait_seconds_max", "Slowest write transaction since the previous scrape, in seconds.", "gauge")
	w.Metric("pulseq_db_write_wait_seconds_max", nil, c.src.TakeWriteWaitMax())
	w.Help("pulseq_db_busy_total", "SQLITE_BUSY outcomes the write pool has seen.", "counter")
	w.Metric("pulseq_db_busy_total", nil, float64(c.src.BusyTotal()))
}

// writeJobFamilies emits the per-job pair the generic freshness alert reads:
// last success against declared SLA. A job with no success has no
// last-success series, and a job that declares no expectation has no SLA
// series - absence is the truth; a fabricated zero would alarm on the
// healthy (#40).
func (c *Collector) writeJobFamilies(ctx context.Context, w *Writer) bool {
	ok := true
	successes := map[string]time.Time{}
	if rows, err := c.src.MetricsLastSuccesses(ctx); err != nil {
		ok = false
	} else {
		for _, r := range rows {
			successes[r.Job] = r.At
		}
	}
	slas := map[string]time.Duration{}
	if rows, err := c.src.MetricsJobSLAs(ctx); err != nil {
		ok = false
	} else {
		for _, r := range rows {
			slas[r.Job] = r.Within
		}
	}

	w.Help("pulseq_last_success_timestamp_seconds", "Unix time of the job's newest succeeded run; absent while there has never been one.", "gauge")
	for _, job := range sortedKeys(successes) {
		w.Metric("pulseq_last_success_timestamp_seconds",
			[]L{Label("job", job)}, float64(successes[job].Unix()))
	}
	w.Help("pulseq_job_freshness_sla_seconds", "How long the job may go without a success before monitoring speaks up; jobs that declare nothing get no series.", "gauge")
	for _, job := range sortedDurationKeys(slas) {
		w.Metric("pulseq_job_freshness_sla_seconds",
			[]L{Label("job", job)}, slas[job].Seconds())
	}
	return !ok
}

// writeInstigatorFamilies emits the schedule/sensor state families. Each has
// one source: lag and last-tick come from the ticks ledger, cadence and
// pause and next fire from the instigator rows themselves.
func (c *Collector) writeInstigatorFamilies(ctx context.Context, w *Writer, now time.Time) bool {
	ok := true
	lags := map[srcKey]float64{}
	if rows, err := c.src.MetricsTickLags(ctx); err != nil {
		ok = false
	} else {
		for _, r := range rows {
			lags[srcKey{r.Kind, r.Name}] = float64(r.LagMillis) / 1000
		}
	}
	lastTicks := map[srcKey]time.Time{}
	if rows, err := c.src.MetricsLastTicks(ctx); err != nil {
		ok = false
	} else {
		for _, r := range rows {
			lastTicks[srcKey{r.Kind, r.Name}] = r.At
		}
	}
	var schedules, sensors []store.MetricsInstigatorState
	if rows, err := c.src.MetricsScheduleStates(ctx); err != nil {
		ok = false
	} else {
		schedules = rows
	}
	if rows, err := c.src.MetricsSensorStates(ctx); err != nil {
		ok = false
	} else {
		sensors = rows
	}

	w.Help("pulseq_tick_lag_seconds", "How late the newest decided evaluation fired relative to the moment it was scheduled for.", "gauge")
	for _, s := range schedules {
		if lag, found := lags[srcKey{"schedule", s.Name}]; found {
			w.Metric("pulseq_tick_lag_seconds", schedLabels(s.Name), lag)
		}
	}
	for _, s := range sensors {
		if lag, found := lags[srcKey{"sensor", s.Name}]; found {
			w.Metric("pulseq_tick_lag_seconds", sensorLabels(s.Name), lag)
		}
	}
	w.Help("pulseq_last_tick_timestamp_seconds", "Unix time of the newest evaluation per instigator.", "gauge")
	for _, s := range schedules {
		if at, found := lastTicks[srcKey{"schedule", s.Name}]; found && !at.IsZero() {
			w.Metric("pulseq_last_tick_timestamp_seconds", schedLabels(s.Name), float64(at.Unix()))
		}
	}
	for _, s := range sensors {
		if at, found := lastTicks[srcKey{"sensor", s.Name}]; found && !at.IsZero() {
			w.Metric("pulseq_last_tick_timestamp_seconds", sensorLabels(s.Name), float64(at.Unix()))
		}
	}
	w.Help("pulseq_next_tick_timestamp_seconds", "Unix time of the instigator's next scheduled evaluation.", "gauge")
	for _, s := range schedules {
		if !s.NextTick.IsZero() {
			w.Metric("pulseq_next_tick_timestamp_seconds", schedLabels(s.Name), float64(s.NextTick.Unix()))
		}
	}
	for _, s := range sensors {
		if !s.NextTick.IsZero() {
			w.Metric("pulseq_next_tick_timestamp_seconds", sensorLabels(s.Name), float64(s.NextTick.Unix()))
		}
	}

	// Cadence: sensors carry theirs as a column. Schedules have none, and
	// parsing cron expressions to invent one would be a second opinion
	// about the truth. The gap between their newest tick and their next
	// fire is what the stalled-tick alert actually compares against, so
	// that gap is what is exposed - and only when both ends exist.
	w.Help("pulseq_tick_interval_seconds", "Seconds between one evaluation and the next; for schedules, the observed gap between the newest tick and the next fire.", "gauge")
	for _, s := range schedules {
		last := lastTicks[srcKey{"schedule", s.Name}]
		if !last.IsZero() && !s.NextTick.IsZero() {
			if gap := s.NextTick.Sub(last).Seconds(); gap > 0 {
				w.Metric("pulseq_tick_interval_seconds", schedLabels(s.Name), gap)
			}
		}
	}
	for _, s := range sensors {
		if s.CadenceKnown && s.Cadence > 0 {
			w.Metric("pulseq_tick_interval_seconds", sensorLabels(s.Name), s.Cadence.Seconds())
		}
	}

	w.Help("pulseq_instigator_paused", "1 when the instigator is paused and will not evaluate.", "gauge")
	for _, s := range schedules {
		w.Metric("pulseq_instigator_paused", schedLabels(s.Name), boolGauge(s.Paused))
	}
	for _, s := range sensors {
		w.Metric("pulseq_instigator_paused", sensorLabels(s.Name), boolGauge(s.Paused))
	}

	// Sensor-only health: how stale the cursor is and whether evaluations
	// keep failing.
	w.Help("pulseq_sensor_cursor_age_seconds", "Age of the sensor's newest cursor movement, in seconds; absent while the cursor never moved.", "gauge")
	for _, s := range sensors {
		if s.CursorUpdatedAt.IsZero() {
			continue
		}
		age := now.Sub(s.CursorUpdatedAt).Seconds()
		if age < 0 {
			age = 0
		}
		w.Metric("pulseq_sensor_cursor_age_seconds", sensorLabels(s.Name), age)
	}
	w.Help("pulseq_sensor_consecutive_failures", "Failed evaluations in a row for one sensor; the breaker's warning light.", "gauge")
	for _, s := range sensors {
		w.Metric("pulseq_sensor_consecutive_failures", sensorLabels(s.Name), float64(s.ConsecutiveFailures))
	}
	return !ok
}

// runCell is one cell of the runs grid.
type runCell struct {
	job, state string
	count      int64
}

// writeRunFamilies emits the runs grid: live states as gauges, terminal
// outcomes as totals. One grouped scan feeds both (#40).
func (c *Collector) writeRunFamilies(ctx context.Context, w *Writer) bool {
	rows, err := c.src.MetricsRunsByStates(ctx)
	if err != nil {
		return true
	}
	live := make([]runCell, 0, len(rows))
	done := make([]runCell, 0, len(rows))
	for _, r := range rows {
		cell := runCell{r.Job, r.State, r.Count}
		switch r.State {
		case "queued", "running":
			live = append(live, cell)
		default: // succeeded, failed, cancelled: closed outcomes
			done = append(done, cell)
		}
	}
	sortRunCells(live)
	sortRunCells(done)
	w.Help("pulseq_runs_by_state", "Runs currently sitting in one live state, per job.", "gauge")
	for _, c := range live {
		w.Metric("pulseq_runs_by_state",
			[]L{Label("job", c.job), Label("state", c.state)}, float64(c.count))
	}
	w.Help("pulseq_run_total", "Runs that reached each terminal outcome, per job.", "counter")
	for _, c := range done {
		w.Metric("pulseq_run_total",
			[]L{Label("job", c.job), Label("status", c.state)}, float64(c.count))
	}
	return false
}

// writeMetaFamilies emits the backup/GC stamps from one meta read. A key
// that does not exist yields no series: the machinery that would fill it has
// not run yet (M6 work), and pretending otherwise would silence exactly the
// alerts these families feed.
func (c *Collector) writeMetaFamilies(ctx context.Context, w *Writer, dbErr *bool) {
	meta, err := c.src.MetricsMetaValues(ctx)
	if err != nil {
		*dbErr = true
		return
	}
	w.Help("pulseq_backup_last_success_timestamp_seconds", "Unix time of the newest successful backup.", "gauge")
	if raw, ok := meta[store.MetaBackupLastSuccessAtMs]; ok {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
			w.Metric("pulseq_backup_last_success_timestamp_seconds", nil, float64(ms)/1000)
		}
	}
	w.Help("pulseq_backup_last_verified", "1 when the newest backup passed verification, 0 when it failed.", "gauge")
	if raw, ok := meta[store.MetaBackupVerifiedOk]; ok {
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			w.Metric("pulseq_backup_last_verified", nil, v)
		}
	}
	w.Help("pulseq_gc_last_success_timestamp_seconds", "Unix time of the newest completed retention sweep.", "gauge")
	if raw, ok := meta[store.MetaGCLastSuccessAtMs]; ok {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
			w.Metric("pulseq_gc_last_success_timestamp_seconds", nil, float64(ms)/1000)
		}
	}
}

// srcKey is one instigator series identity.
type srcKey struct{ kind, name string }

func schedLabels(name string) []L {
	return []L{Label("instigator", "schedule"), Label("name", name)}
}

func sensorLabels(name string) []L {
	return []L{Label("instigator", "sensor"), Label("name", name)}
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// sortedKeys/sortedDurationKeys keep per-entity families deterministic: same
// state, same bytes, whatever order the scan answered in.
func sortedKeys(m map[string]time.Time) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedDurationKeys(m map[string]time.Duration) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortRunCells(cells []runCell) {
	sort.Slice(cells, func(i, j int) bool {
		return cells[i].job < cells[j].job ||
			(cells[i].job == cells[j].job && cells[i].state < cells[j].state)
	})
}

// logDirBytes sums the regular files under dir. A missing directory is not
// an error - nothing has logged yet - and reports as absent rather than as
// zero, so a misconfigured path stays visible as what it is. An I/O failure
// mid-walk skips the family entirely instead of reporting a partial number.
func logDirBytes(dir string) (int64, bool) {
	if dir == "" {
		return 0, false
	}
	if _, err := os.Stat(dir); err != nil {
		return 0, false
	}
	var total int64
	broken := false
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			broken = true
			return fs.SkipAll
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	if err != nil || broken {
		return 0, false
	}
	return total, true
}
