# Pulseq — prosjektplan sett fra persistenslaget

> Perspektiv: databasespesialisten. Premisset er at Pulseq **er** sin database. Scheduler, sensorer,
> DAG-motor og CLI er tynne lag over én relasjonell tilstandsmaskin. Hvis skjemaet og
> skrivestrategien er riktig, blir resten enkelt. Hvis de er feil, drukner prosjektet i
> `SQLITE_BUSY`, tapte runs og en database som vokser til den ikke kan sikkerhetskopieres.

---

## 0. Beslutninger i kortform

| Spørsmål | Beslutning | Begrunnelse (kort) |
|---|---|---|
| Motor | SQLite, én fil `state.db` | Single-node er målgruppen; null drift |
| Driver | `modernc.org/sqlite` (ren Go) | Statisk binær, ingen cgo, krysskompilering. ~25–30 % lavere skrivegjennomstrømning enn cgo — irrelevant på vårt volum |
| Skrivestrategi | **To pooler**: writer `*sql.DB` med `SetMaxOpenConns(1)` + `_txlock=immediate`; reader `*sql.DB` med N forbindelser, `mode=ro` | Serialiserer skriving i Go-laget. `busy_timeout` blir sikkerhetsnett, ikke strategi |
| Journal | WAL, `synchronous=NORMAL`, `journal_size_limit=64MiB` | Lesere blokkerer ikke skriver. NORMAL er korrupsjonssikkert i WAL |
| Flere filer | **Nei** i MVP. Valgfri `logs.db` i fase 6 | Transaksjoner over ATTACH er *ikke* atomiske i WAL |
| Køsemantikk | `BEGIN IMMEDIATE` + `UPDATE … WHERE id IN (SELECT … LIMIT n) RETURNING` | SKIP LOCKED finnes ikke fordi det ikke finnes noe å skippe |
| Leases | Innebygde kolonner på `step_runs` for arbeid; egen `leases`-tabell for singleton-roller | Ingen join på varm sti |
| Fencing | `lease_token` sjekkes i WHERE på alle resultatskrivinger | Hindrer at en worker med utløpt lease skriver |
| Idempotens | `run_keys (job_name, run_key)` `WITHOUT ROWID`, `INSERT … ON CONFLICT DO NOTHING RETURNING` | Overlever retention av `triggers` |
| Kodegenerering | `sqlc` for ~80 % av spørringene, håndskrevet for claim/reaper | Typesikkerhet uten ORM; escape hatch der sqlc's SQLite-parser svikter |
| Migrasjoner | Egen ~150-linjers migrator, `//go:embed migrations/*.sql`, forward-only, checksum-verifisert | Ingen avhengighet, goose-kompatible filnavn |
| Retention | Batchet `DELETE … LIMIT 500` i egne transaksjoner + `auto_vacuum=INCREMENTAL` | Aldri hold skrivelåsen lenge |
| Backup | `VACUUM INTO` nattlig (MVP), Litestream valgfri sidecar (fase 6) | Én setning, konsistent snapshot, komprimert, null avhengigheter |

---

## 1. Skrivestrategi: analyse av løsningsrommet

### 1.1 Begrensningen presist formulert

SQLite tillater vilkårlig mange samtidige lesere, men **én skriver om gangen på tvers av hele
databasefilen**. WAL endrer ikke dette; WAL fjerner bare at *lesere* blokkerer *skriveren*.

Den underkommuniserte fellen er ikke låsen i seg selv, men **oppgraderingsdødlåsen**: en
transaksjon som starter som `BEGIN DEFERRED` (Go's `database/sql` standard), leser noe, og deretter
skriver, må oppgradere til skrivelås. Hvis en annen skriver har committet i mellomtiden, får du
`SQLITE_BUSY_SNAPSHOT` — og **`busy_timeout` retryer ikke denne**. Snapshotet er utdatert; å vente
hjelper ikke. Applikasjonen må rulle tilbake og kjøre hele transaksjonen på nytt.

Dette er årsaken til at "vi setter bare `busy_timeout=5000`" er en ikke-strategi.

### 1.2 Alternativene

**A. WAL + `busy_timeout` alene.**
Sett `journal_mode=WAL`, `busy_timeout=5000`, la Go's pool ha 10 forbindelser og håp.
*Forkastet.* Løser ikke oppgraderingsdødlåsen. Feilraten er lav i utvikling og eksploderer under
last — nøyaktig feil feilkurve for en orchestrator som skal stå urørt i månedsvis.

**B. To pooler: én skriver, N lesere.** ← **anbefalt**
En `*sql.DB` for skriving med `SetMaxOpenConns(1)` og `_txlock=immediate` (så *hver* transaksjon
starter som `BEGIN IMMEDIATE` og tar skrivelåsen med én gang), og en separat `*sql.DB` for lesing
med `runtime.NumCPU()` forbindelser i `mode=ro`.

Poenget: `sql.DB` med maks 1 forbindelse **er allerede en serialisert skrivekø** — med
context-kansellering, timeout og mottrykk gratis. Go blokkerer goroutiner i `db.Conn()` til
forbindelsen er ledig. Ingen `SQLITE_BUSY` kan oppstå fra egne skrivere, fordi det aldri finnes to.
`busy_timeout` degraderes til det den bør være: et sikkerhetsnett mot *eksterne* skrivere
(`sqlite3`-CLI, backup-verktøy).

Sekundærgevinsten er stor og undervurdert: **inne i en `BEGIN IMMEDIATE`-transaksjon kan du lese,
regne i Go, og så skrive — uten CAS-akrobatikk.** Ingen andre kan ha skrevet i mellomtiden.
Admission control ("har denne jobben allerede N kjørende runs?") blir en vanlig `SELECT COUNT(*)`
etterfulgt av `UPDATE`, ikke en avansert atomisk spørring. Dette forenkler halve prosjektet.

Dette er også mønsteret River (den ledende Go-jobbkøen) dokumenterer for sin SQLite-driver.

**C. Dedikert skrive-goroutine med kommandokanal.**
En `chan writeOp`, én goroutine som eier `*sql.Conn`.
*Forkastet.* Reimplementerer B dårligere: du må selv bygge context-propagering, timeout,
feilretur-kanaler, panic-håndtering og mottrykk. Eneste reelle fordel er *batching* av mange små
skrivinger til én transaksjon — og det trenger vi kun for logglinjer, som løses lokalt med en
buffer foran ett `INSERT`-kall (se §6.4).

**D. Flere databasefiler.**
- *D1 — ATTACH.* **Forkastet.** I WAL-modus er en transaksjon over flere ATTACHed filer atomisk
  *per fil*, men ikke som helhet: super-journal-mekanismen som gir kryssfil-atomisitet virker ikke
  med WAL. Ved strømbrudd kan én fil rulle fram og en annen tilbake. Vi ville altså miste
  atomisitet nøyaktig der Pulseq trenger den (skriv resultat + oppdater status + dekrementér
  avhengighetstellere).
- *D2 — separate forbindelser per fil.* Fungerer, men hver fil trenger sin egen skriverserialisering,
  og du mister fremmednøkler og atomiske oppdateringer på tvers. Legitimt **kun** der atomisitet
  ikke har verdi.
- *Verdikt:* alt transaksjonelt i `state.db`. Logglinjer er den eneste tabellen med ubundet vekst
  og null transaksjonell verdi — en tapt logglinje korrumperer ikke tilstand. Derfor er `logs.db`
  det eneste legitime splitt-kandidatet, og først når måling viser at det trengs (fase 6).
  `archive-YYYY.db` for kaldt historikk er en tredje, valgfri fil produsert av retention-jobben.

**E. Bytt til Postgres.**
Riktig for multi-node, feil for produktets identitet ("systemd timers pluss sensorer"). Løsningen
er å legge all SQL bak `internal/store` slik at en Postgres-driver kan legges til senere uten at
domenelaget merker det — ikke å betale for Postgres nå.

### 1.3 Konkret oppsett

```go
// internal/store/open.go

const writerDSN = "file:%s?" +
    "_txlock=immediate" +                   // hver Tx = BEGIN IMMEDIATE
    "&_pragma=journal_mode(WAL)" +
    "&_pragma=synchronous(NORMAL)" +        // trygt i WAL; sync ved checkpoint
    "&_pragma=busy_timeout(10000)" +        // sikkerhetsnett mot eksterne skrivere
    "&_pragma=foreign_keys(ON)" +
    "&_pragma=temp_store(MEMORY)" +
    "&_pragma=cache_size(-16000)" +         // 16 MiB
    "&_pragma=wal_autocheckpoint(1000)" +
    "&_pragma=journal_size_limit(67108864)" // WAL krymper til 64 MiB etter checkpoint

const readerDSN = "file:%s?mode=ro" +
    "&_pragma=busy_timeout(5000)" +
    "&_pragma=query_only(ON)" +
    "&_pragma=cache_size(-32000)" +
    "&_pragma=temp_store(MEMORY)"

type Store struct {
    w *sql.DB // MaxOpenConns(1) — ALL skriving
    r *sql.DB // MaxOpenConns(NumCPU) — ALL lesing
}

func Open(path string) (*Store, error) {
    w, err := sql.Open("sqlite", fmt.Sprintf(writerDSN, path))
    // ...
    w.SetMaxOpenConns(1)
    w.SetMaxIdleConns(1)
    w.SetConnMaxLifetime(0) // aldri resirkulér skriveren

    r, err := sql.Open("sqlite", fmt.Sprintf(readerDSN, path))
    // ...
    r.SetMaxOpenConns(runtime.NumCPU())
    return &Store{w: w, r: r}, nil
}
```

**Arkitektonisk invariant (håndheves i review og med en liten `go vet`-lignende sjekk):**
> Ingen pakke utenfor `internal/store` har et skrivbart databasehåndtak. `Store.w` er privat.
> All mutasjon skjer gjennom metoder på `*Store` som internt kjører `WithTx(ctx, func(tx) error)`.

Merknader:
- `mmap_size` settes **ikke** som standard. Gevinsten er marginal for vårt tilgangsmønster, og
  mmap kan maskere I/O-feil som SIGBUS.
- `PRAGMA optimize` kjøres på en 4-timers timer fra skriveren; `ANALYZE` første gang etter at
  databasen har reelt datavolum.
- Nekt å starte hvis databasefilen ligger på NFS/CIFS (sjekk `statfs` `f_type`). SQLite-låsing er
  udefinert der, og feilene er umulige å diagnostisere.

---

## 2. Datamodell — de bærende skillene

Tre skiller avgjør om skjemaet holder over tid:

1. **Definisjon vs. kjøring.** `jobs`/`steps`/`schedules`/`sensors` er deklarasjoner.
   `runs`/`step_runs`/`ticks`/`triggers` er hendelser. Definisjoner endres; historikk må ikke
   endre betydning når de gjør det. Derfor er definisjonene **versjonert**
   (`job_versions`), og hver run peker på nøyaktig den versjonen den kjørte.
2. **Beslutning vs. utføring** (fra prosjektbeskrivelsen). `ticks` → `triggers` er
   beslutningssiden; `runs` → `step_runs` er utføringssiden. Skillet er en fremmednøkkel, ikke en
   konvensjon: `triggers.run_id` er nullbar fordi en trigger kan bli deduplisert bort.
3. **Tilstand vs. forklaring.** `runs.status` er *hva*; `run_events` er *hvorfor*.
   `pulseq explain` er en spørring over `run_events` + `ticks.skip_reason`, ikke en egen mekanisme.

Konvensjoner:
- **Tid**: `INTEGER` unix-millisekunder UTC, alltid. Sorterbart, indekserbart, ingen
  tekstparsing, ingen tidssoneambiguitet. Tidssone lagres separat som IANA-navn kun der
  *tolkning* trengs (`schedules.timezone`).
- **Nøkler**: `INTEGER PRIMARY KEY` (rowid-alias) på alle varme tabeller — SQLite's fysiske nøkkel,
  raskeste join. `runs` får i tillegg `pub_id TEXT UNIQUE` (ULID) for CLI/UI, så brukervendte
  ID-er er stabile og ikke-gjettbare. Definisjonstabeller nøkles på navn (`jobs.name`), som er den
  stabile menneskelige identiteten.
- **Status**: `TEXT` med `CHECK`-constraint, ikke heltall. Målet er at `sqlite3 state.db "select …"`
  skal være lesbart under feilsøking — det er hele produktets premiss.
- **JSON**: `TEXT` med kanonisk serialisering. `json_extract()` finnes hvis vi trenger det.
- **`STRICT`** på alle tabeller (SQLite ≥ 3.37) — reell typesjekk, ingen stille konvertering.

---

## 3. Komplett skjema

### 3.1 `0001_init.sql` — fundament

```sql
CREATE TABLE schema_migrations (
  version    INTEGER PRIMARY KEY,
  name       TEXT    NOT NULL,
  checksum   TEXT    NOT NULL,       -- sha256 av migrasjonsfilen
  applied_at INTEGER NOT NULL
) STRICT;

CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) STRICT, WITHOUT ROWID;
```

### 3.2 `0002_definitions.sql` — deklarativ side

```sql
CREATE TABLE jobs (
  name                TEXT PRIMARY KEY,
  current_version_id  INTEGER REFERENCES job_versions(id) DEFERRABLE INITIALLY DEFERRED,
  description         TEXT    NOT NULL DEFAULT '',
  queue               TEXT    NOT NULL DEFAULT 'default',
  max_concurrent_runs INTEGER NOT NULL DEFAULT 1 CHECK (max_concurrent_runs > 0),
  paused              INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1)),
  created_at          INTEGER NOT NULL,
  updated_at          INTEGER NOT NULL,
  deleted_at          INTEGER
) STRICT;

-- Uforanderlig. Ny spec => ny rad. Historikk beholder mening.
CREATE TABLE job_versions (
  id         INTEGER PRIMARY KEY,
  job_name   TEXT    NOT NULL REFERENCES jobs(name) ON DELETE CASCADE,
  version    INTEGER NOT NULL,
  spec_hash  TEXT    NOT NULL,          -- sha256 av kanonisk JSON
  spec       TEXT    NOT NULL,          -- hele definisjonen, kanonisk JSON
  source     TEXT,                      -- filsti spec kom fra
  created_at INTEGER NOT NULL,
  UNIQUE (job_name, version),
  UNIQUE (job_name, spec_hash)          -- idempotent reload: samme spec => samme versjon
) STRICT;

CREATE TABLE steps (
  id             INTEGER PRIMARY KEY,
  job_version_id INTEGER NOT NULL REFERENCES job_versions(id) ON DELETE CASCADE,
  name           TEXT    NOT NULL,
  topo_order     INTEGER NOT NULL,      -- forhåndsberegnet topologisk rekkefølge
  action         TEXT    NOT NULL,      -- JSON: {"type":"exec","argv":[...]}
  env            TEXT    NOT NULL DEFAULT '{}',
  working_dir    TEXT,
  timeout_ms     INTEGER CHECK (timeout_ms IS NULL OR timeout_ms > 0),
  max_attempts   INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
  backoff        TEXT    NOT NULL DEFAULT '{"kind":"exponential","base_ms":1000,"max_ms":300000,"jitter":0.2}',
  on_upstream_failure TEXT NOT NULL DEFAULT 'skip'
      CHECK (on_upstream_failure IN ('skip','run','fail_fast')),
  UNIQUE (job_version_id, name)
) STRICT;

CREATE TABLE step_deps (
  step_id       INTEGER NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
  depends_on_id INTEGER NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
  PRIMARY KEY (step_id, depends_on_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX idx_step_deps_upstream ON step_deps(depends_on_id);
```

> **Syklusdeteksjon kan ikke uttrykkes som SQLite-constraint.** DAG-validering (Kahn-topologisk
> sortering + syklusfeil) skjer i `internal/dag` ved innlasting, *før* `job_versions`-raden skrives.
> `topo_order` materialiseres samtidig, så kjøretidsstien slipper grafarbeid.

### 3.3 `0003_triggers.sql` — beslutningssiden

```sql
CREATE TABLE schedules (
  name          TEXT PRIMARY KEY,
  job_name      TEXT    NOT NULL REFERENCES jobs(name) ON DELETE CASCADE,
  kind          TEXT    NOT NULL CHECK (kind IN ('cron','interval','calendar')),
  expr          TEXT    NOT NULL,       -- "0 3 * * *" | "15m" | kalenderregel-JSON
  timezone      TEXT    NOT NULL DEFAULT 'UTC',  -- IANA
  params        TEXT    NOT NULL DEFAULT '{}',
  paused        INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1)),
  catchup       TEXT    NOT NULL DEFAULT 'skip'
                CHECK (catchup IN ('skip','last_only','all')),
  catchup_limit INTEGER NOT NULL DEFAULT 10,
  start_at      INTEGER,
  end_at        INTEGER,
  last_tick_at  INTEGER,                -- siste evaluerte logiske slot
  next_tick_at  INTEGER NOT NULL,       -- forhåndsberegnet; driver scheduler-spørringen
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
) STRICT;

-- Partielt indeks: kun aktive schedules. Scheduler-spørringen blir en range-scan
-- over noen få rader uansett hvor mange pausede schedules som finnes.
CREATE INDEX idx_schedules_due ON schedules(next_tick_at) WHERE paused = 0;

CREATE TABLE sensors (
  name             TEXT PRIMARY KEY,
  job_name         TEXT    NOT NULL REFERENCES jobs(name) ON DELETE CASCADE,
  kind             TEXT    NOT NULL CHECK (kind IN ('exec','http','sql','file')),
  spec             TEXT    NOT NULL,
  interval_ms      INTEGER NOT NULL DEFAULT 30000 CHECK (interval_ms >= 1000),
  timeout_ms       INTEGER NOT NULL DEFAULT 30000,
  paused           INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1)),
  error_backoff_ms INTEGER NOT NULL DEFAULT 60000,
  consecutive_errors INTEGER NOT NULL DEFAULT 0,
  next_tick_at     INTEGER NOT NULL,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
) STRICT;
CREATE INDEX idx_sensors_due ON sensors(next_tick_at) WHERE paused = 0;

CREATE TABLE cursors (
  sensor_name TEXT PRIMARY KEY REFERENCES sensors(name) ON DELETE CASCADE,
  value       TEXT    NOT NULL,        -- opak streng eid av sensoren
  revision    INTEGER NOT NULL DEFAULT 0,
  updated_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE cursor_history (
  id          INTEGER PRIMARY KEY,
  sensor_name TEXT    NOT NULL,
  value       TEXT    NOT NULL,
  tick_id     INTEGER REFERENCES ticks(id) ON DELETE SET NULL,
  created_at  INTEGER NOT NULL
) STRICT;
CREATE INDEX idx_cursor_history_sensor ON cursor_history(sensor_name, id DESC);

-- Én tabell for både schedule- og sensor-evalueringer: "explain" trenger én tidslinje.
CREATE TABLE ticks (
  id               INTEGER PRIMARY KEY,
  source_kind      TEXT    NOT NULL CHECK (source_kind IN ('schedule','sensor','manual','api')),
  source_name      TEXT    NOT NULL,
  scheduled_for    INTEGER,             -- logisk slot (UTC ms). NULL for sensor/manual.
  started_at       INTEGER NOT NULL,
  finished_at      INTEGER,
  duration_ms      INTEGER,
  outcome          TEXT    NOT NULL DEFAULT 'running'
                   CHECK (outcome IN ('running','triggered','skipped','error')),
  skip_reason      TEXT,
  error            TEXT,
  triggers_emitted INTEGER NOT NULL DEFAULT 0,
  cursor_before    TEXT,
  cursor_after     TEXT,
  UNIQUE (source_kind, source_name, scheduled_for)
) STRICT;
CREATE INDEX idx_ticks_source_time ON ticks(source_name, started_at DESC);
CREATE INDEX idx_ticks_retention   ON ticks(started_at);
```

> **Trikset i `UNIQUE (source_kind, source_name, scheduled_for)`:** SQLite behandler `NULL` som
> distinkt i unike indekser. Schedules har `scheduled_for` satt, og får dermed *nøyaktig én* tick
> per logisk slot — dobbeltfyring er strukturelt umulig, også hvis to prosesser evaluerer samtidig.
> Sensorer har `NULL` og får ubegrenset antall ticks. Én constraint, to semantikker, null kode.

```sql
CREATE TABLE triggers (
  id         INTEGER PRIMARY KEY,
  tick_id    INTEGER NOT NULL REFERENCES ticks(id) ON DELETE CASCADE,
  job_name   TEXT    NOT NULL REFERENCES jobs(name) ON DELETE CASCADE,
  run_key    TEXT    NOT NULL,
  params     TEXT    NOT NULL DEFAULT '{}',
  priority   INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  run_id     INTEGER REFERENCES runs(id) ON DELETE SET NULL,  -- NULL = deduplisert bort
  dedup_reason TEXT
) STRICT;
CREATE INDEX idx_triggers_tick ON triggers(tick_id);

-- Liten, langlevd dedup-tabell. Overlever retention av triggers/runs.
-- WITHOUT ROWID: ren nøkkeloppslagstabell med sammensatt tekstnøkkel — sparer et indeksnivå.
CREATE TABLE run_keys (
  job_name      TEXT    NOT NULL,
  run_key       TEXT    NOT NULL,
  first_seen_at INTEGER NOT NULL,
  run_id        INTEGER,
  PRIMARY KEY (job_name, run_key)
) STRICT, WITHOUT ROWID;
CREATE INDEX idx_run_keys_age ON run_keys(first_seen_at);
```

### 3.4 `0004_execution.sql` — utføringssiden

```sql
CREATE TABLE queues (
  name            TEXT PRIMARY KEY,
  max_concurrency INTEGER NOT NULL DEFAULT 4 CHECK (max_concurrency > 0),
  paused          INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1))
) STRICT;
INSERT INTO queues(name, max_concurrency) VALUES ('default', 4);

CREATE TABLE runs (
  id             INTEGER PRIMARY KEY,
  pub_id         TEXT    NOT NULL UNIQUE,     -- ULID
  job_name       TEXT    NOT NULL REFERENCES jobs(name) ON DELETE CASCADE,
  job_version_id INTEGER NOT NULL REFERENCES job_versions(id),
  trigger_id     INTEGER REFERENCES triggers(id) ON DELETE SET NULL,
  parent_run_id  INTEGER REFERENCES runs(id) ON DELETE SET NULL,  -- replay/re-run
  run_key        TEXT,
  origin         TEXT    NOT NULL
                 CHECK (origin IN ('schedule','sensor','manual','api','retry','backfill')),
  status         TEXT    NOT NULL
                 CHECK (status IN ('queued','running','succeeded','failed',
                                   'cancelled','deferred','timed_out')),
  queue          TEXT    NOT NULL DEFAULT 'default' REFERENCES queues(name),
  priority       INTEGER NOT NULL DEFAULT 0,
  params         TEXT    NOT NULL DEFAULT '{}',
  scheduled_for  INTEGER,                     -- logisk tid (cron-slot/backfill-partisjon)
  available_at   INTEGER NOT NULL,            -- for deferred/backoff
  enqueued_at    INTEGER NOT NULL,
  started_at     INTEGER,
  finished_at    INTEGER,
  duration_ms    INTEGER,
  error          TEXT,
  cancel_requested_at INTEGER,
  updated_at     INTEGER NOT NULL
) STRICT;

-- Varm sti: hva kan startes nå?
CREATE INDEX idx_runs_startable ON runs(queue, priority DESC, available_at, id)
  WHERE status IN ('queued','deferred');
-- Samtidighetstelling per job
CREATE INDEX idx_runs_active ON runs(job_name) WHERE status = 'running';
-- Historikk-listing (CLI/UI)
CREATE INDEX idx_runs_history ON runs(job_name, id DESC);
-- Retention
CREATE INDEX idx_runs_finished ON runs(finished_at) WHERE finished_at IS NOT NULL;
CREATE INDEX idx_runs_trigger  ON runs(trigger_id);

CREATE TABLE step_runs (
  id            INTEGER PRIMARY KEY,
  run_id        INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_id       INTEGER NOT NULL REFERENCES steps(id),
  step_name     TEXT    NOT NULL,       -- denormalisert: overlever spec-sletting
  topo_order    INTEGER NOT NULL,
  status        TEXT    NOT NULL
                CHECK (status IN ('pending','ready','running','succeeded','failed',
                                  'skipped','cancelled','upstream_failed','timed_out')),
  pending_deps  INTEGER NOT NULL DEFAULT 0,  -- teller; 0 => klar til å kjøre
  attempt       INTEGER NOT NULL DEFAULT 0,
  max_attempts  INTEGER NOT NULL DEFAULT 1,
  available_at  INTEGER NOT NULL DEFAULT 0,  -- retry-backoff
  -- lease inline: ingen join på varm sti
  lease_holder     TEXT,
  lease_expires_at INTEGER,
  lease_token      INTEGER NOT NULL DEFAULT 0,
  started_at    INTEGER,
  finished_at   INTEGER,
  duration_ms   INTEGER,
  exit_code     INTEGER,
  error         TEXT,
  skip_reason   TEXT,
  UNIQUE (run_id, step_name)
) STRICT;

CREATE INDEX idx_step_runs_claimable
  ON step_runs(available_at, id) WHERE status = 'ready';
CREATE INDEX idx_step_runs_reaper
  ON step_runs(lease_expires_at) WHERE status = 'running';
CREATE INDEX idx_step_runs_run
  ON step_runs(run_id, topo_order);

CREATE TABLE step_attempts (          -- fase 2; MVP bruker run_events
  id          INTEGER PRIMARY KEY,
  step_run_id INTEGER NOT NULL REFERENCES step_runs(id) ON DELETE CASCADE,
  attempt     INTEGER NOT NULL,
  worker_id   TEXT,
  started_at  INTEGER NOT NULL,
  finished_at INTEGER,
  exit_code   INTEGER,
  status      TEXT    NOT NULL,
  error       TEXT,
  UNIQUE (step_run_id, attempt)
) STRICT;

-- Singleton-roller: 'scheduler', 'sensor-evaluator', 'migrator', 'maintenance'
CREATE TABLE leases (
  name          TEXT PRIMARY KEY,
  holder        TEXT    NOT NULL,
  fencing_token INTEGER NOT NULL,
  acquired_at   INTEGER NOT NULL,
  expires_at    INTEGER NOT NULL,
  meta          TEXT    NOT NULL DEFAULT '{}'
) STRICT, WITHOUT ROWID;
CREATE INDEX idx_leases_expiry ON leases(expires_at);

CREATE TABLE artifacts (
  id          INTEGER PRIMARY KEY,
  run_id      INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_run_id INTEGER REFERENCES step_runs(id) ON DELETE CASCADE,
  name        TEXT    NOT NULL,
  uri         TEXT    NOT NULL,        -- file:// | s3:// | pg:// | …
  media_type  TEXT,
  size_bytes  INTEGER,
  checksum    TEXT,                    -- "sha256:…"
  meta        TEXT    NOT NULL DEFAULT '{}',
  created_at  INTEGER NOT NULL,
  UNIQUE (run_id, name)
) STRICT;
CREATE INDEX idx_artifacts_uri      ON artifacts(uri);
CREATE INDEX idx_artifacts_checksum ON artifacts(checksum) WHERE checksum IS NOT NULL;

CREATE TABLE artifact_inputs (        -- lineage, fase 6
  step_run_id INTEGER NOT NULL REFERENCES step_runs(id) ON DELETE CASCADE,
  artifact_id INTEGER NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
  PRIMARY KEY (step_run_id, artifact_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX idx_artifact_inputs_rev ON artifact_inputs(artifact_id);

-- Ryggraden i "explain": hver tilstandsovergang, med årsak.
CREATE TABLE run_events (
  id          INTEGER PRIMARY KEY,
  run_id      INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_run_id INTEGER REFERENCES step_runs(id) ON DELETE CASCADE,
  at          INTEGER NOT NULL,
  kind        TEXT    NOT NULL,   -- 'run.queued','step.started','step.retry_scheduled',…
  from_status TEXT,
  to_status   TEXT,
  detail      TEXT    NOT NULL DEFAULT '{}'
) STRICT;
CREATE INDEX idx_run_events_run ON run_events(run_id, id);
```

### 3.5 `0005_logs.sql`

```sql
CREATE TABLE log_lines (
  id          INTEGER PRIMARY KEY,
  run_id      INTEGER NOT NULL,     -- bevisst ingen FK: skal kunne flyttes til logs.db
  step_run_id INTEGER,
  seq         INTEGER NOT NULL,     -- monotont per step_run
  at          INTEGER NOT NULL,
  stream      TEXT NOT NULL CHECK (stream IN ('stdout','stderr','sys')),
  level       TEXT,
  msg         TEXT NOT NULL,
  fields      TEXT
) STRICT;
CREATE INDEX idx_log_lines_step ON log_lines(step_run_id, seq);
CREATE INDEX idx_log_lines_run  ON log_lines(run_id, id);
CREATE INDEX idx_log_lines_age  ON log_lines(at);
```

Ingen fremmednøkkel: hvis tabellen senere flyttes til `logs.db`, kan FK likevel ikke krysse filer.
Å designe den frittstående fra dag én gjør flyttingen til en ren datamigrasjon.

### 3.6 `0006_outbox.sql` (fase 6)

```sql
CREATE TABLE outbox (
  id           INTEGER PRIMARY KEY,
  topic        TEXT    NOT NULL,
  payload      TEXT    NOT NULL,
  created_at   INTEGER NOT NULL,
  available_at INTEGER NOT NULL,
  attempts     INTEGER NOT NULL DEFAULT 0,
  delivered_at INTEGER,
  last_error   TEXT
) STRICT;
CREATE INDEX idx_outbox_pending ON outbox(available_at, id) WHERE delivered_at IS NULL;
```

Notifikasjoner skrives i **samme transaksjon** som tilstandsendringen som utløste dem. Da kan
Pulseq ikke havne i "run feilet, men ingen varsel gikk ut" — eller motsatt.

---

## 4. Køsemantikk

### 4.1 Claim uten `SKIP LOCKED`

`SKIP LOCKED` finnes ikke i SQLite fordi **det ikke finnes noe å skippe**: det er kun én skriver, og
den skriveren holder skrivelåsen alene. Mønsteret er:

```sql
-- Kjøres på writer-poolen. _txlock=immediate => BEGIN IMMEDIATE er allerede aktiv.
UPDATE step_runs
   SET status           = 'running',
       attempt          = attempt + 1,
       lease_holder     = :worker_id,
       lease_token      = lease_token + 1,
       lease_expires_at = :now + :lease_ms,
       started_at       = COALESCE(started_at, :now)
 WHERE id IN (
   SELECT sr.id
     FROM step_runs sr
     JOIN runs r ON r.id = sr.run_id
    WHERE sr.status = 'ready'
      AND sr.available_at <= :now
      AND r.status = 'running'
      AND r.queue  = :queue
    ORDER BY r.priority DESC, sr.available_at, sr.id
    LIMIT :n
 )
RETURNING id, run_id, step_id, step_name, attempt, lease_token;
```

Tre ting å vite:

1. **`BEGIN IMMEDIATE` tar skrivelåsen før subspørringen evalueres.** Kandidatvalg, `UPDATE` og
   `RETURNING` er én atomisk enhet. Ingen annen skriver kan starte i mellomtiden. Det er *sterkere*
   enn `FOR UPDATE SKIP LOCKED`, ikke svakere.
2. **`RETURNING` garanterer ikke rekkefølge**, og radene emitteres etter hvert som de beregnes.
   Konsumér hele resultatsettet inn i en slice *før* du kjører noe annet på forbindelsen — med
   `MaxOpenConns(1)` vil et forsøk på å bruke forbindelsen midt i en åpen `Rows` ellers låse seg
   selv.
3. **Ingen `SELECT` først.** Ett tur/retur, ett låsevindu, målt i mikrosekunder.

### 4.2 Admission control — enkelheten single-writer gir

Å starte runs krever samtidighetsgrenser per job og per queue. Med én skriver trengs ingen
atomisk mega-spørring:

```go
func (s *Store) StartDueRuns(ctx context.Context, now int64, max int) ([]Run, error) {
    return WithTx(ctx, s.w, func(tx *sql.Tx) ([]Run, error) {
        // 1. Les nåværende belegg. Ingen kan skrive nå — tallene er sanne.
        active, _ := queryActiveCounts(tx)          // map[job]int, map[queue]int
        // 2. Les kandidater i prioritetsrekkefølge.
        cands, _ := queryStartable(tx, now, max*4)
        // 3. Bestem i Go. Vilkårlig kompleks logikk, null SQL-akrobatikk.
        ids := admit(cands, active, limits)
        // 4. Én UPDATE … WHERE id IN (…) RETURNING.
        return markRunning(tx, ids, now)
    })
}
```

Dette er hovedgevinsten ved alternativ B, og grunnen til at Pulseq kan holde kjernen liten.

### 4.3 Lease, heartbeat og fencing

- **Lease** settes ved claim (`lease_expires_at = now + 60s` som standard).
- **Heartbeat**: workeren fornyer på 1/3 av leasetiden:
  ```sql
  UPDATE step_runs SET lease_expires_at = :now + :lease_ms
   WHERE id = :id AND lease_holder = :worker AND lease_token = :token AND status = 'running';
  ```
  Én transaksjon, batchet for alle steg workeren holder.
- **Reaper** (kjører hvert 15. sekund fra lease-holderen for `maintenance`):
  ```sql
  UPDATE step_runs
     SET status       = CASE WHEN attempt < max_attempts THEN 'ready' ELSE 'failed' END,
         available_at = CASE WHEN attempt < max_attempts THEN :now + :backoff_ms ELSE available_at END,
         error        = COALESCE(error, 'lease expired'),
         lease_holder = NULL,
         lease_expires_at = NULL
   WHERE status = 'running' AND lease_expires_at < :now
  RETURNING id, run_id, status, attempt;
  ```
- **Fencing** — alle resultatskrivinger må bevise at leasen fortsatt er deres:
  ```sql
  UPDATE step_runs
     SET status = 'succeeded', finished_at = :now, duration_ms = :now - started_at, exit_code = 0
   WHERE id = :id AND lease_holder = :worker AND lease_token = :token AND status = 'running';
  ```
  `RowsAffected() == 0` ⇒ leasen ble overtatt mens steget kjørte. Resultatet **forkastes** og
  logges som `step.result_discarded`. Uten dette kan en langsom worker overskrive et resultat en
  annen worker allerede har produsert — den mest lumske klassen feil i denne typen system.

### 4.4 DAG-progresjon via avhengighetsteller

Naiv løsning: etter hvert fullført steg, kjør en grafspørring for å finne nye klare steg. Det
skalerer dårlig og er vanskelig å indeksere.

I stedet materialiseres tellere ved run-opprettelse (`pending_deps` = antall oppstrømssteg), og
dekrementeres ved fullføring — alt i samme transaksjon som statusendringen:

```sql
-- Ved suksess på step_id = :step_id i run_id = :run_id
UPDATE step_runs
   SET pending_deps = pending_deps - 1,
       status = CASE WHEN pending_deps - 1 <= 0 THEN 'ready' ELSE status END
 WHERE run_id = :run_id
   AND status  = 'pending'
   AND step_id IN (SELECT step_id FROM step_deps WHERE depends_on_id = :step_id);
```

Ved feil (og `on_upstream_failure = 'skip'`) markeres hele det nedstrøms transitive lukket:

```sql
WITH RECURSIVE downstream(step_id) AS (
    SELECT step_id FROM step_deps WHERE depends_on_id = :step_id
  UNION
    SELECT d.step_id FROM step_deps d JOIN downstream ds ON d.depends_on_id = ds.step_id
)
UPDATE step_runs
   SET status = 'upstream_failed', skip_reason = 'oppstrøms steg ' || :step_name || ' feilet',
       finished_at = :now
 WHERE run_id = :run_id
   AND status IN ('pending','ready')
   AND step_id IN (SELECT step_id FROM downstream);
```

Run-fullføring blir da ett aggregat:

```sql
UPDATE runs
   SET status = CASE
         WHEN EXISTS (SELECT 1 FROM step_runs WHERE run_id = :run_id
                       AND status IN ('failed','timed_out')) THEN 'failed'
         WHEN EXISTS (SELECT 1 FROM step_runs WHERE run_id = :run_id
                       AND status IN ('pending','ready','running')) THEN 'running'
         ELSE 'succeeded' END,
       finished_at = :now, duration_ms = :now - started_at, updated_at = :now
 WHERE id = :run_id
RETURNING status;
```

### 4.5 Idempotens

`run_key` er dedup-nøkkelen. Sensoren definerer den (f.eks. objektlager-nøkkel + etag).

```sql
INSERT INTO run_keys (job_name, run_key, first_seen_at)
VALUES (:job, :key, :now)
ON CONFLICT (job_name, run_key) DO NOTHING
RETURNING job_name;
```

Null rader tilbake ⇒ nøkkelen er sett før ⇒ ingen run opprettes, `triggers.dedup_reason` settes.
Hele sensor-tick-håndteringen (skriv tick, dedup N triggere, opprett M runs, oppdater cursor)
skjer i **én** transaksjon. Enten skjer alt, eller ingenting — cursor kan aldri flyttes forbi
triggere som ikke ble persistert.

### 4.6 "LISTEN/NOTIFY"-erstatning

SQLite har ingen notifikasjonsmekanisme. Tre nivåer:

1. **Samme prosess (normaltilfellet)**: scheduler og workere er goroutiner i én binær. Et
   `chan struct{}` med kapasitet 1 pinges etter commit. Latens ≈ 0.
2. **Flere prosesser på samme host**: polling hvert 250 ms mot `idx_step_runs_claimable`. Med
   partielt indeks er dette et B-tree-oppslag som treffer 0 rader — mikrosekunder. 4 spørringer/s
   er ikke måleverdig belastning.
3. **Fase 6, hvis latens måles som problem**: `notifications`-tabell + adaptiv poll (rask etter
   aktivitet, treg når idle). Samme løsning River valgte for sin SQLite-driver.

### 4.7 Rekonsiliering ved oppstart

Én transaksjon ved `pulseq serve`-start, før noen løkke starter:

1. `step_runs` med `status='running'` og utløpt/manglende lease → reaper-logikk.
2. `runs` med `status='running'` uten noen aktive `step_runs` → reevaluer aggregatet (§4.4).
3. `ticks` med `outcome='running'` eldre enn 2× timeout → `outcome='error'`,
   `error='prosess terminerte under evaluering'`.
4. `schedules.next_tick_at` i fortiden → catchup-policy anvendes; alt logges som `ticks` med
   `skip_reason` slik at `pulseq explain` kan forklare hullet.
5. `leases` med `expires_at < now` → slettes.

---

## 5. Migrasjoner

**Egen migrator, ~150 linjer, ingen avhengighet.** Goose og golang-migrate er gode, men Pulseq
selger "veldig liten kjerne", og migratoren trenger tre ting ingen av dem gir gratis: kjøring på
*vår* skriverforbindelse, lease-beskyttelse mot samtidige prosesser, og checksum-verifisering av
allerede anvendte migrasjoner.

```go
//go:embed migrations/*.sql
var migrationFS embed.FS   // 0001_init.sql, 0002_definitions.sql, …
```

Kontrakt:
- **Forward-only.** Ingen `down`-migrasjoner. Nedmigrering av en produksjonsdatabase er i praksis
  en løgn; tilbakerulling gjøres ved å gjenopprette nattens `VACUUM INTO`-kopi.
- **Én migrasjon = én transaksjon.** Ved feil rulles hele tilbake. SQLite har transaksjonell DDL,
  i motsetning til MySQL — utnytt det.
- **Checksum.** Ved oppstart sammenlignes sha256 av hver anvendt fil med `schema_migrations.checksum`.
  Avvik ⇒ hard feil. Redigerte migrasjoner er en av de vanligste kildene til divergerende skjemaer.
- **Lease.** Migratoren tar `leases('migrator')` med `BEGIN IMMEDIATE` før den kjører. To
  samtidige `pulseq serve` kan ikke migrere samtidig.
- **Versjonsgjerde.** `PRAGMA user_version` settes til høyeste anvendte versjon. Enhver prosess
  sjekker ved oppstart at `user_version <= binærens maks`. En gammel binær som møter et nyere
  skjema **nekter å starte** i stedet for å skrive feil data.
- **Fremmednøkler ved tabellombygging.** SQLite's `ALTER TABLE` støtter kun `ADD COLUMN`,
  `RENAME`, `DROP COLUMN`. Alt annet krever 12-stegs-prosedyren, og
  `PRAGMA foreign_keys` **kan ikke endres inne i en transaksjon**. Migratoren må derfor kunne
  markere en migrasjon som "table rebuild":
  ```
  -- +pulseq rebuild
  ```
  som gir sekvensen `PRAGMA foreign_keys=OFF` → `BEGIN` → 12 steg → `PRAGMA foreign_key_check`
  (feil ⇒ rollback) → `COMMIT` → `PRAGMA foreign_keys=ON`.
- **Gyldne skjematester.** En test dumper `sqlite_schema` etter migrering og differ mot en
  innsjekket `schema.golden.sql`. Fanger utilsiktede skjemaendringer i review, og gir `sqlc` én
  autoritativ skjemafil.

**`sqlc`**: konfigureres mot `schema.golden.sql`. Generer for ~80 % av spørringene. Advarsel:
sqlc's SQLite-parser er mindre moden enn Postgres-varianten, og feiler på kompliserte
CTE/`RETURNING`-kombinasjoner. De 6–8 varme spørringene (claim, reaper, dep-dekrementering,
downstream-CTE) skrives derfor for hånd i `internal/store` med `database/sql` direkte. Ikke kjemp
mot verktøyet der det er svakest.

---

## 6. Retention, vakuum og backup

### 6.1 Prinsipp: aldri hold skrivelåsen lenge

Den eneste måten single-writer-modellen kan skade Pulseq på, er en skriving som holder låsen i
sekunder. Et `DELETE FROM runs WHERE finished_at < …` som kaskaderer til 200 000 `step_runs`,
`run_events` og `artifacts` er nøyaktig det. Derfor: **batchet sletting, én transaksjon per batch,
kort pause mellom.**

```sql
DELETE FROM runs
 WHERE id IN (
   SELECT id FROM runs
    WHERE finished_at IS NOT NULL
      AND finished_at < :cutoff
      AND status IN ('succeeded','cancelled','failed')
    ORDER BY id
    LIMIT 500
 );
```

Løkke i Go: kjør til `RowsAffected() == 0`, `time.Sleep(50ms)` mellom batchene. `ON DELETE CASCADE`
rydder `step_runs`, `step_attempts`, `run_events`, `artifacts` og `artifact_inputs`. Med 500 runs
per batch holdes hver transaksjon på titalls millisekunder.

### 6.2 Retention-policy

| Tabell | Standard horisont | Merknad |
|---|---|---|
| `log_lines` | 14 dager | Slettes først og hyppigst; dominerer volumet |
| `run_events` | følger `runs` (cascade) | |
| `runs` + barn | 90 dager, men minst 50 siste per job | Dobbelt kriterium: en sjelden job mister ikke all historikk |
| `ticks` (outcome='skipped') | 7 dager | Enorme mengder, lav verdi etter kort tid |
| `ticks` (øvrige) | 90 dager | |
| `cursor_history` | 200 siste per sensor | |
| `run_keys` | 365 dager | **Lengst.** Sletting her betyr at gamle triggere kan re-fyre |
| `job_versions` | aldri auto-slettes | Bittesmå; historikk må beholde mening |
| `outbox` (levert) | 7 dager | |

`run_keys` er bevisst den langlevde tabellen. Den er derfor `WITHOUT ROWID` og inneholder kun
fire kolonner — 1 million nøkler koster i størrelsesorden 60 MB.

### 6.3 Vakuum uten smerte

- Sett `PRAGMA auto_vacuum = INCREMENTAL` **ved opprettelse av databasen** (`pulseq db init`).
  Innstillingen kan bare endres på en tom database eller etter full `VACUUM` — det er derfor den
  må være riktig fra dag én. Dette er en beslutning man ikke får ta om igjen billig.
- Nattlig, i vedlikeholdsjobben: `PRAGMA incremental_vacuum(2000);` — frigjør maks ~8 MB per
  kjøring, holder låsevinduet kort og forutsigbart.
- **Full `VACUUM` kjøres aldri automatisk.** Den krever eksklusiv lås og 2× diskplass.
  Eksponeres kun som `pulseq db compact --i-know-this-blocks`.
- Overvåk `page_count * page_size` mot `freelist_count * page_size`; eksponer som metrikk
  `pulseq_db_freelist_bytes`.

### 6.4 WAL-hygiene

Den mest sannsynlige produksjonsfeilen i dette designet er **checkpoint-svelt**: en langlevd
lesetransaksjon (typisk et web-UI som holder en `Tx` gjennom en paginert listing) hindrer
checkpointing, og WAL-filen vokser ubegrenset — 20 GB WAL på en 200 MB database er et dokumentert
utfall.

Mottiltak, alle obligatoriske:
1. **UI/CLI-lesing bruker aldri en eksplisitt `Tx`.** Enkeltspørringer, paginering med
   `WHERE id < :cursor LIMIT 50`, aldri `OFFSET`.
2. Alle lesekontekster har deadline (5 s standard).
3. `journal_size_limit = 64 MiB` så WAL faktisk krymper etter checkpoint.
4. Vedlikeholdsjobben kjører `PRAGMA wal_checkpoint(TRUNCATE)` når `runs` uten aktive kjøringer.
5. Metrikk `pulseq_wal_bytes` + advarsel over 64 MB, feil over 256 MB. Det er kanarifuglen for at
   noen har introdusert en langlevd leser.

### 6.5 Backup

**MVP — `VACUUM INTO`:**
```sql
VACUUM INTO '/var/backups/pulseq/state-2026-08-21T03-00-00Z.db';
```
Én setning. Konsistent snapshot fra én transaksjon. Blokkerer ikke andre skrivere. Utdata er
defragmentert og mindre enn originalen. Ingen ekstern avhengighet. Kjøres 03:00 fra
vedlikeholdslease-holderen, med `retain=14` generasjoner.

**Verifisering, ellers er backupen bare håp:** hver natt, etter kopiering, åpne kopien og kjør
`PRAGMA quick_check` (og `PRAGMA integrity_check` ukentlig). Resultat rapporteres av
`pulseq doctor`.

**Fase 6 — Litestream** som valgfri sidecar for point-in-time recovery til S3/MinIO. Passer der
tap av siste minutt er uakseptabelt. Kombineres greit med `VACUUM INTO`; det som *ikke* er lov
sammen med Litestream er full `VACUUM` på live-databasen og endring av `journal_mode`.

**Aldri**: `cp state.db backup.db` på en kjørende database. Dokumentér dette i `pulseq doctor`-output.

### 6.6 `synchronous=NORMAL`: den bevisste avveiningen

Med `NORMAL` i WAL synkroniseres først ved checkpoint. Databasen kan **ikke** korrumperes, men
de siste sekundene med committede transaksjoner kan gå tapt ved strømbrudd (ikke ved
applikasjonskrasj). For en orchestrator er dette akseptabelt fordi hele modellen allerede er
**at-least-once med rekonsiliering ved oppstart**: en run som "forsvinner" ved strømbrudd blir
enten re-trigget av schedule-catchup, eller står igjen som `queued` og plukkes opp. Vi betaler
med en teoretisk dobbeltkjøring for å få 10–100× skrivegjennomstrømning.

`synchronous=FULL` eksponeres som konfignøkkel for dem som vil ha det.

### 6.7 Logglinjer — den eneste reelle volumdriveren

Tilstandsskrivinger er små og få. Logger er verken. Regler:
- **Batching**: workeren buffrer stdout/stderr og skriver ett `INSERT` med opptil 200 linjer, eller
  hvert 200. ms — det som kommer først. Dette reduserer antall skrivetransaksjoner med ~2 størrelsesordener.
- **Tak per steg**: `max_log_bytes` (standard 8 MiB). Ved overskridelse skrives én
  `stream='sys'`-linje `"logg avkortet ved N bytes"` og resten forkastes. Ubegrenset logging fra
  et løpsk steg skal ikke kunne fylle disken.
- **Splitt til `logs.db`** vurderes først når `log_lines` overstiger ~60 % av databasestørrelsen.
  Da: egen fil, egen writer-pool, ingen FK (allerede designet slik), batchede appends. Kostnaden
  er tapt atomisitet mellom logglinje og stegstatus — bevisst akseptert.

---

## 7. Arkitektur

```
cmd/pulseq/                  CLI-entrypoint (stdlib flag; ingen cobra i MVP)
internal/store/              ENESTE eier av skrivehåndtak
  open.go                    to pooler, PRAGMA, NFS-sjekk
  tx.go                      WithTx / WithRead + retry på SQLITE_BUSY_SNAPSHOT
  migrate.go                 embedded migrator, checksum, lease, user_version
  migrations/*.sql
  schema.golden.sql          autoritativ for sqlc + gyldne tester
  queries/*.sql              sqlc-input
  gen/                       sqlc-output (innsjekket)
  hot.go                     håndskrevne varme spørringer (claim, reaper, dep-teller)
internal/model/              domenetyper + tilstandsmaskiner (ren Go, null SQL)
internal/dag/                topologisk sortering, syklusdeteksjon, transitiv lukning
internal/schedule/           cron/interval-parsing, next-tick, tidssone, catchup
internal/sensor/             evaluator, cursor-protokoll, exec/http/file-adaptere
internal/dispatch/           scheduler-løkke, admission control, leader-lease, reaper
internal/worker/             prosess-spawn, logginnsamling, heartbeat, fencing
internal/obs/                slog, metrikker, pulseq doctor
internal/api/                HTTP: /healthz, /metrics, web-UI (fase 6) — kun leserpoolen
```

**Prosessmodell.** Én binær. `pulseq serve` kjører scheduler, sensor-evaluator, reaper,
vedlikehold og N workere som goroutiner i samme prosess — normaltilfellet, og der latensen er null.
Flere `pulseq serve`-prosesser mot samme fil på samme host støttes: singleton-rollene koordineres
via `leases`, workere via `step_runs`-leases. Multi-node er utenfor SQLite-scope; `internal/store`
er grensesnittet som holder døren åpen for en Postgres-driver.

**Tilstandsmaskinene** ligger i `internal/model` som rene funksjoner
(`func (s RunStatus) CanTransitionTo(RunStatus) bool`), speiles av `CHECK`-constraints i skjemaet,
og verifiseres med en test som viser at hver lovlig overgang også er lovlig i databasen. To
uavhengige håndhevelser av samme regel er billig og fanger reelle feil.

**Testing av persistenslaget** (det viktigste testarbeidet i prosjektet):
- Hver test får egen DB i `t.TempDir()` — ikke `:memory:`, siden vi må teste WAL og reell låsing.
- **Konkurransetest i CI**: 16 goroutiner som skriver samtidig i 10 sekunder mot ekte fil.
  Assertion: **null** `SQLITE_BUSY`. Denne testen er hele skrivestrategiens eksistensberettigelse
  og skal skrives i fase 0, før noen domenekode finnes.
- Injisert klokke (`type Clock interface { Now() time.Time }`) overalt. Tidsavhengige tester uten
  `sleep`.
- Migrasjonstest: migrer fra tom → siste, diff mot golden; og fra hver historiske versjon → siste.
- `go test -race` obligatorisk.

---

## 8. MVP-kutt

Beholdes (fra prosjektbeskrivelsens MVP-liste):
cron/interval-schedules, sensorer med cursor, run-historikk, step-retries, CLI start/stop/list/replay,
tilstandslagring, strukturerte logger, DAG-avhengigheter.

**Kuttes fra MVP, med begrunnelse:**

| Kuttes | Hvorfor det er trygt |
|---|---|
| Web-UI | CLI over leserpoolen dekker alt; UI er den største kilden til langlevde lesetransaksjoner (§6.4) |
| `queues`-tabellen | Én implisitt kø. Kolonnen `runs.queue` finnes fra dag én med default `'default'` — å legge til grensene senere er en konfigendring, ikke en migrasjon |
| `step_attempts` | `run_events` gir samme retry-historikk. Tabellen legges inn i fase 6 når UI trenger strukturert visning |
| `artifact_inputs` / lineage | `artifacts` registreres, men grafen bygges ikke |
| `outbox` / notifikasjoner | Ingen mottakere ennå |
| Backfill | Krever `scheduled_for`-partisjonering som allerede finnes i skjemaet; funksjonen ventes |
| `kind='calendar'` schedules | Cron + interval dekker 95 % |
| Sensorer av typen `http`/`sql`/`file` | Kun `kind='exec'`: et subprosess som skriver JSON til stdout. Én adapter, uendelig fleksibilitet, minimal plugin-flate |
| `logs.db`-splitt | Én fil til det er målt at det trengs |
| Postgres-driver | Grensesnittet finnes; implementasjonen gjør ikke |

**Beholdes selv om det frister å kutte:**
- `job_versions` (~15 linjer) — uten det betyr "replay" ingenting.
- `lease_token`/fencing (3 kolonner) — hindrer stille resultatoverskriving, den verste feilklassen.
- `run_events` — det er `explain`-kommandoen, og den er et av produktets viktigste salgsargumenter.
- `auto_vacuum=INCREMENTAL` — kan ikke settes billig i ettertid.
- `run_keys` som egen tabell — å slå den sammen med `triggers` og skille dem senere er smertefullt.

---

## 9. Faseplan: tomt repo → ferdig produkt

**Fase 0 — Persistensfundamentet (uke 1).**
`go mod init`. `internal/store` med to pooler, PRAGMA-oppsett, NFS-deteksjon, `WithTx`/`WithRead`,
embedded migrator med checksum og lease, `schema_migrations`, `meta`.
`pulseq db init | check | info`.
**Utgangskriterium:** konkurransetesten (16 samtidige skrivere, 10 s, null `SQLITE_BUSY`) er grønn
i CI. Skrivestrategien er dermed bevist før noen domenekode skrives.

**Fase 1 — Definisjoner (uke 2).**
`0002_definitions.sql`. Spec-format (YAML) → kanonisk JSON → `spec_hash`. `internal/dag` med
Kahn-sortering og syklusdeteksjon. Idempotent `pulseq apply <dir>`: uendret spec ⇒ ingen ny
`job_versions`-rad. `pulseq job list | show`. Gyldne skjematester + `sqlc` i CI.

**Fase 2 — Utføring (uke 3–4). Kjernen.**
`0004_execution.sql` + `0005_logs.sql`. Worker med prosess-spawn, batchet logginnsamling,
heartbeat. Claim-spørringen, reaper, fencing, retry med eksponentiell backoff + jitter,
`pending_deps`-mekanikken, downstream-CTE ved feil, run-aggregering. `run_events` på hver overgang.
`pulseq run <job>`, `pulseq runs list|show|logs|cancel`, `pulseq replay <run> [--steps a,b]`.
**Utgangskriterium:** kill -9 på worker midt i et steg ⇒ reaper gjenoppretter, steget kjører
ferdig, og ingen dupliserte resultatskrivinger (fencing-test).

**Fase 3 — Schedules (uke 5).**
`0003_triggers.sql` (schedule-delen). Cron-parser med IANA-tidssone, `next_tick_at`-materialisering,
`ticks` med `UNIQUE(source,scheduled_for)`, catchup-policy, pause/resume.
`pulseq schedule list | preview | pause | resume`. Rekonsiliering ved oppstart.

**Fase 4 — Sensorer (uke 6).**
`sensors`, `cursors`, `cursor_history`, `triggers`, `run_keys`. `exec`-adapter med JSON-protokoll:
`{"triggers":[{"run_key":"…","params":{}}], "cursor":"…"}` eller `{"skip_reason":"…"}`.
Hele tick-håndteringen i én transaksjon (§4.5). `pulseq sensor test | preview | list`.

**Fase 5 — Drift (uke 7).**
Vedlikeholdslease. Batchet retention, `incremental_vacuum`, nattlig `VACUUM INTO` med verifisering,
`wal_checkpoint(TRUNCATE)`. `pulseq explain <run|schedule|sensor>` over `run_events` + `ticks`.
`pulseq doctor` (WAL-størrelse, freelist, integritetssjekk, backup-alder, skjemaversjon).
Prometheus-metrikker. systemd-unit + eksempelkonfig. **Første release: v0.1.**

**Fase 6 — Utvidelser (uke 8+), prioritert etter behov.**
Web-UI (kun leserpoolen, streng paginering, ingen `Tx`), backfill over `scheduled_for`,
`outbox` + notifikasjoner, `step_attempts`, artifact lineage, `queues` med grenser,
Litestream-sidecar, adaptiv notifikasjonstabell, valgfri `logs.db`-splitt, Postgres-driver.

---

## 10. Risikoer

| # | Risiko | Sannsynlighet | Konsekvens | Mottiltak |
|---|---|---|---|---|
| 1 | `SQLITE_BUSY` ved oppgradering fra deferred til write | Høy uten tiltak | Tapte/feilede operasjoner | `_txlock=immediate` + `MaxOpenConns(1)`; konkurransetest i CI fra fase 0 |
| 2 | Checkpoint-svelt fra langlevd leser ⇒ WAL vokser ubegrenset | Middels (UI-en er trusselen) | Full disk | Ingen `Tx` i lesestien, deadline på alle lesninger, `journal_size_limit`, metrikk + alarm |
| 3 | Retention holder skrivelåsen i sekunder | Høy uten tiltak | Scheduler stopper opp periodisk | Batchet `DELETE … LIMIT 500`, pause mellom batcher |
| 4 | Logglinjer dominerer databasestørrelsen | Høy | Backup og retention blir trege | Batching, `max_log_bytes` per steg, kortest horisont, evt. `logs.db` |
| 5 | Stille resultatoverskriving fra worker med utløpt lease | Lav, men alvorlig | Korrupt historikk | `lease_token`-fencing på alle resultatskrivinger; eksplisitt test |
| 6 | `modernc.org/sqlite`-regresjon eller ytelsestak | Lav | Blokkerer release | Pin versjon; `internal/store`-grensesnitt; byggetagg for `mattn/go-sqlite3` som rømningsvei |
| 7 | `sqlc` klarer ikke parse våre SQLite-spørringer | Middels | Friksjon i utvikling | De 6–8 varme spørringene håndskrives fra start; sqlc kun der det er komfortabelt |
| 8 | Cron + DST: hoppet/gjentatt time | Middels | Manglende eller doble kjøringer | `scheduled_for` lagres som UTC-instant; `UNIQUE` gjør dobbeltfyring umulig; hoppet time logges som `ticks` med `skip_reason`; dokumentert semantikk |
| 9 | Databasefil på NFS/CIFS ⇒ ødelagt låsing | Lav | Korrupsjon | Nekt oppstart ved `statfs`-deteksjon |
| 10 | To binærversjoner med ulikt skjema mot samme fil | Middels | Feil data | `user_version`-gjerde: gammel binær nekter å starte mot nyere skjema; migrator-lease |
| 11 | Backup finnes, men er ubrukelig | Middels | Totalt datatap | Verifiser hver kopi med `quick_check`; `pulseq doctor` rapporterer backup-alder og siste verifisering |
| 12 | Full `VACUUM` kjørt i produksjon | Lav | Flere minutters full stopp | Aldri automatisk; bak eksplisitt flagg med advarende navn |
| 13 | `run_keys` slettes for aggressivt | Lav | Gamle triggere re-fyrer | Lengste horisont (365 d), egen tabell, dokumentert som farlig knapp |
| 14 | `auto_vacuum` ikke satt ved opprettelse | Middels | Kan bare rettes med full `VACUUM` | Settes i `pulseq db init`; `pulseq doctor` advarer hvis `auto_vacuum=NONE` |

---

## 11. Kapasitetsbudsjett

Regnestykket som avgjør om SQLite holder:

- SQLite med `synchronous=NORMAL` i WAL leverer i størrelsesorden **10 000–100 000 små
  skrivetransaksjoner/s** på NVMe. Selv en beskjeden VM med nettverkslagring klarer flere tusen.
- Pulseqs tilstandsskrivinger: en run med 5 steg og 1 retry ≈ 25 skrivetransaksjoner
  (run + step-overganger + events). En installasjon med **1 000 runs/døgn** gir
  ~25 000 transaksjoner/døgn ≈ **0,3/s**.
- Logglinjer er den reelle driveren. 1 000 runs × 5 steg × 500 linjer = 2,5 M linjer/døgn ≈ 29/s.
  Med batching à 200 linjer: **~0,15 skrivetransaksjoner/s**.
- Retention og vedlikehold: batchede, korte.

**Konklusjon: designmålet er under ~200 skrivetransaksjoner/s, med mer enn 50× hodrom.**
Gjennomstrømning er ikke risikoen i dette prosjektet — **hvor lenge én enkelt skriving holder
låsen** er det. Derfor er alle designbeslutninger i §4 og §6 optimalisert for kort
lås-holdetid, ikke for transaksjoner per sekund.

Størrelsesestimat for en typisk installasjon etter ett år med standard retention:
`runs` + `step_runs` + `run_events` ≈ 150–400 MB, `log_lines` ≈ 1–3 GB (14 dager),
`run_keys` ≈ 50 MB. `VACUUM INTO`-backup tar sekunder på dette volumet.

---

## 12. Vedlegg: nøkkelspørringer i drift

```sql
-- Hvorfor kjørte ikke schedule X i går?
SELECT datetime(started_at/1000,'unixepoch') AS t, outcome, skip_reason, error, triggers_emitted
  FROM ticks
 WHERE source_kind='schedule' AND source_name=:name
   AND started_at > :since
 ORDER BY started_at DESC;

-- Hva står fast akkurat nå?
SELECT r.pub_id, r.job_name, sr.step_name, sr.status, sr.attempt,
       (:now - sr.started_at)/1000 AS sek, sr.lease_holder,
       CASE WHEN sr.lease_expires_at < :now THEN 'FORELDET' ELSE 'ok' END AS lease
  FROM step_runs sr JOIN runs r ON r.id = sr.run_id
 WHERE sr.status IN ('running','ready')
 ORDER BY sr.started_at;

-- Databasehelse
SELECT (SELECT * FROM pragma_page_count()) * (SELECT * FROM pragma_page_size()) AS db_bytes,
       (SELECT * FROM pragma_freelist_count()) * (SELECT * FROM pragma_page_size()) AS free_bytes,
       (SELECT * FROM pragma_journal_mode())  AS journal,
       (SELECT * FROM pragma_auto_vacuum())   AS auto_vacuum,
       (SELECT * FROM pragma_user_version())  AS schema_version;

-- Største tabeller (krever dbstat-modulen)
SELECT name, SUM(pgsize) AS bytes FROM dbstat GROUP BY name ORDER BY bytes DESC LIMIT 10;
```

---

## Kilder

- [SQLite: Write-Ahead Logging](https://www.sqlite.org/wal.html) — checkpoint-svelt, WAL-semantikk
- [SQLite: ATTACH DATABASE](https://sqlite.org/lang_attach.html) — manglende kryssfil-atomisitet i WAL
- [Bert Hubert: SQLITE_BUSY tross timeout](https://berthub.eu/articles/posts/a-brief-post-on-sqlite3-database-locked-despite-timeout/) — oppgraderingsdødlåsen
- [River: Using with SQLite](https://riverqueue.com/docs/sqlite) — to-pool-mønsteret i praksis
- [River-blogg: SQLite-driver](https://riverqueue.com/blog/sqlite-and-pro-dbsql-durable-periodic-jobs-performance-boosts) — pseudo listen/notify
- [modernc.org/sqlite: konfigurasjon](https://gitlab.com/cznic/sqlite/-/blob/master/_autodocs/configuration.md) — DSN-parametre, `_txlock`, `_pragma`
- [sqlc: Getting started with SQLite](https://docs.sqlc.dev/en/latest/tutorials/getting-started-sqlite.html)
- [Litestream: hvordan det virker](https://litestream.io/how-it-works/) og [cron-basert backup](https://litestream.io/alternatives/cron/)
- [Oldmoe: Backup-strategier for SQLite i produksjon](https://oldmoe.blog/2024/04/30/backup-strategies-for-sqlite-in-production/)
- [Loke.dev: SQLite checkpoint starvation](https://loke.dev/blog/sqlite-checkpoint-starvation-wal-growth)
- [PhotoStructure: hvordan VACUUM-e SQLite i WAL-modus](https://photostructure.com/coding/how-to-vacuum-sqlite/)
- [multiprocessio/sqlite-cgo-no-cgo](https://github.com/multiprocessio/sqlite-cgo-no-cgo) — driver-benchmark
- [pressly/goose: embedding migrations](https://pressly.github.io/goose/blog/2021/embed-sql-migrations/)
- [PowerSync: SQLite-optimaliseringer](https://powersync.com/blog/sqlite-optimizations-for-ultra-high-performance) — gjennomstrømningstall
