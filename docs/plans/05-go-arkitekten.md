# Pulseq — prosjektplan: Go-arkitekten

Perspektiv: idiomatisk Go, stdlib-first, minimal avhengighetsflate, én statisk binær.

---

## 1. Arkitektonisk tese

Pulseq er en **serialisert tilstandsmaskin over én SQLite-fil**. Scheduler, sensor-evaluator, worker-pool, CLI og API er kun produsenter og konsumenter av tilstandsoverganger i den maskinen. Ingen komponent har egen sannhet i minnet.

Tre konsekvenser som styrer hele designet:

1. **SQLites én-skriver-regel er ikke et problem som skal omgås — den er invarianten som gjør systemet debuggbart.** All skriving går gjennom én forbindelse. Beslutninger blir totalordnet, og «hvorfor skjedde dette» kan alltid rekonstrueres fra tabellene.
2. **DAG-en er et SQL-predikat, ikke en graf i minnet.** Et steg er kjørbart når ingen upstream-rad mangler `succeeded`. Da trenger vi ingen DAG-scheduler, ingen graf-serialisering og ingen gjenoppbygging etter krasj — bare en `claim`-spørring.
3. **Plugin-flaten er en prosess, ikke et Go-interface.** Sensorer og steg er eksterne kommandoer med en JSON-kontrakt. Ingen SDK, ingen `plugin.Open`, ingen kompilert utvidelsesmodell. Det er dette som gjør «sensorer lett å skrive» reelt.

Ikke-mål i MVP: distribuert konsensus, asset-graf, RBAC, container-isolering, dynamisk fan-out.

---

## 2. Bibliotekvalg

Måltall: **≤ 5 direkte avhengigheter**, `CGO_ENABLED=0`, `go build` uten verktøykjede utover Go.

| Behov | Valg | Begrunnelse | Forkastet |
|---|---|---|---|
| SQLite-driver | `modernc.org/sqlite` | Ren Go, transpilert fra SQLite-C. Gir `CGO_ENABLED=0`, kryss-kompilering fra én maskin, én statisk binær — som er selve produktløftet. Kjent ~2× tregere på INSERT enn C-versjonen; irrelevant når skrivevolumet er titalls tx/s. | `mattn/go-sqlite3` (CGO ⇒ toolchain per plattform, drar inn glibc-avhengighet); `ncruces/go-sqlite3` (wazero/WASM, raskere på INSERT, men større runtime-flate) — holdes som plan B bak driver-grensesnittet |
| Cron-parsing | `github.com/robfig/cron/v3` — **kun parseren** | `cron.ParseStandard` + `Schedule.Next(t)` er nøyaktig det vi trenger. Null transitive avhengigheter. `CRON_TZ=`-prefiks støttes. Runneren brukes ikke: vi eier tick-loopen selv. | `adhocore/gronx` (har `L/W/#`, men vi trenger dem ikke i MVP; kan legges til bak `cronx.Parse` senere); `gorhill/cronexpr` (mindre vedlikeholdt) |
| Interval/kalender | egen `internal/cronx` | Tynt lag over robfig: `@every 15m`, `every: 30s`, `@daily`, samt `Next`/`Prev`/`Between` for preview og catch-up. Egen kode fordi catch-up krever `Between(from, to)` som robfig ikke tilbyr. | — |
| Spørringslag | ren `database/sql` + generiske scan-hjelpere | ~25 spørringer totalt. `sqlc` gir liten gevinst når skjemaet endres hver uke i fase 1–4, og SQLite-analysen i sqlc er svakere enn Postgres-analysen. Vurderes på nytt i fase 5 når skjemaet er frosset. | `sqlc` (utsatt), `sqlx` (refleksjonsmagi vi ikke trenger), enhver ORM |
| Migrasjoner | egen, `embed.FS` + `PRAGMA user_version` | ~70 linjer. Migrasjonene ligger i binæren, kjøres ved oppstart i én `IMMEDIATE`-transaksjon, ingen ekstra tabell, ingen CLI-avhengighet. | `pressly/goose`, `golang-migrate` — begge drar inn drivere og et registry vi ikke trenger for en enkeltfil-database |
| CLI | stdlib `flag` + egen subkommando-ruter (~120 linjer) | Verb–substantiv-ruter (`pulseq run list`) er trivielt å skrive og gir full kontroll på hjelpetekst og JSON-output. | `cobra` (drar inn `pflag`, ofte `viper`, kodegenerering vi ikke bruker). Hvis shell-completion viser seg å være et reelt krav i fase 5: `urfave/cli/v3` (én modul, ingen viper) — aldri cobra |
| HTTP | stdlib `net/http` (Go 1.22+ `ServeMux`-mønstre) | `mux.HandleFunc("GET /v1/runs/{id}", …)` dekker hele API-flaten. Middleware = `func(http.Handler) http.Handler`-kjede på 20 linjer. | `chi`, `gin`, `echo` |
| Logging | stdlib `log/slog` | JSON-handler i produksjon, tekst-handler i terminal. Egen handler videresender run-/step-scoped records til `events`-tabellen. | `zerolog`, `zap` |
| YAML | `github.com/goccy/go-yaml` | `gopkg.in/yaml.v3` ble arkivert i april 2025. goccy er det økosystemet flyttet til, og gir presise feilposisjoner (linje/kolonne) — viktig for `pulseq job validate`. | `gopkg.in/yaml.v3` (uvedlikeholdt), `sigs.k8s.io/yaml` (drar inn JSON-omveien) |
| Samtidighet | `golang.org/x/sync/errgroup`, `.../semaphore` | Semi-stdlib, vedlikeholdt av Go-teamet. Strukturert oppstart/nedstenging. | egen WaitGroup-koreografi |
| ID-er | egen `internal/id`: `crypto/rand` + Crockford-base32, tidssortert (ULID-lignende) | 40 linjer, null avhengighet, leksikografisk sorterbar ⇒ `ORDER BY id` = kronologisk, og bra B-tree-lokalitet. | `google/uuid`, `oklog/ulid` |
| Metrikker | stdlib `expvar` i MVP; håndskrevet Prometheus-tekstformat i fase 5 | Prometheus-eksposisjonsformatet er ~30 linjer å skrive. `client_golang` er en av de tyngste avhengighetene i Go-økosystemet. | `prometheus/client_golang` |
| Test | stdlib `testing` + `testing/synctest` + `github.com/google/go-cmp` (kun test) | `testing/synctest` ble stabil i Go 1.25 og gir falsk klokke — avgjørende for en scheduler. go-cmp gir lesbare differ. | `testify` (assert-DSL vi ikke trenger), egen `Clock`-abstraksjon (unødvendig med synctest) |

**Go-versjon: 1.25+** (krav: stabil `testing/synctest`).

Direkte avhengigheter totalt: `modernc.org/sqlite`, `robfig/cron/v3`, `goccy/go-yaml`, `golang.org/x/sync`, `google/go-cmp` (test).

---

## 3. Prosess- og goroutine-modell

### 3.1 Prosessmodell: én binær, én daemon, roller som flagg

```
pulseq serve --roles=scheduler,sensors,workers,api   # default: alle
```

**Begrunnelse:** SQLite binder likevel alt til én maskin og ett filsystem. Å splitte i separate prosesser i MVP gir null gevinst og innfører en ny skriver mot samme fil — nøyaktig det vi skal unngå. Rollene er likevel separerbare bak `--roles` fra dag én, slik at en senere Postgres-backend (fase 7) gir multi-node uten omskriving.

**CLI snakker aldri direkte med databasen for skriving.** Alle kommandoer går mot daemonens HTTP-API over en Unix-domain-socket (`$PULSEQ_STATE/pulseq.sock`, mode 0660). Dette:

- fjerner enhver mulighet for en andre skriver,
- gir identisk oppførsel lokalt og over SSH/TCP,
- gir autorisasjon gratis via filrettigheter på socketen.

Kun rene lesekommandoer (`run list`, `explain`) faller tilbake til å åpne databasen `?_pragma=query_only(1)` hvis daemonen er nede — med tydelig `(daemon nede — skrivebeskyttet visning)` i output.

### 3.2 Goroutine-arkitektur i `pulseq serve`

```
main
└─ ctx, stop := signal.NotifyContext(ctx, SIGINT, SIGTERM)
   └─ g, gctx := errgroup.WithContext(ctx)
      ├─ scheduler   1 goroutine   ticker 1 s, låst av singleton-lease
      ├─ sensors     1 dispatcher + semaphore(maxParallelSensors, default 4)
      ├─ planner     1 goroutine   triggers → runs + steps   ← ENESTE materialiserer
      ├─ dispatcher  1 goroutine   claim-loop mot steps
      ├─ workers     N goroutiner  N = --workers (default runtime.NumCPU())
      ├─ reaper      1 goroutine   ticker 10 s: utløpte leases, foreldreløse runs
      ├─ janitor     1 goroutine   ticker 1 t: retention, loggrotasjon, wal_checkpoint
      ├─ jobwatcher  1 goroutine   ticker 5 s: mtime-scan av jobs-katalog → reload
      └─ api         1 goroutine   http.Server på UDS (+ valgfri TCP)
```

**Vekking uten polling-latens.** En intern `notify.Bus` (kanal-broadcast, ikke-blokkerende) lar planner vekke dispatcher og dispatcher vekke workers umiddelbart. Tickerne er kun sikkerhetsnett: systemet er korrekt med bare tickere, og rask med bussen.

**Nedstenging** — todelt kontekst, det klassiske fallgruven:

```go
// gctx er allerede kansellert ved SIGTERM — kan ikke brukes til drenering
shutCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), drainTimeout)
```

Sekvens ved SIGTERM:
1. `scheduler`, `sensors`, `planner`, `dispatcher` slutter å ta nytt arbeid (≤ 100 ms).
2. `api` kaller `srv.Shutdown(shutCtx)`.
3. Workers får `--drain-timeout` (default 30 s) på å fullføre pågående steg. Hvert steg får `SIGTERM` til prosessgruppen, deretter `SIGKILL` etter `step.kill_grace` (default 10 s).
4. Steg som ikke rakk å bli ferdige, settes tilbake til `pending` med `attempt` uendret (ikke forbrukt forsøk) og `lease` frigitt.
5. Skrive-poolen lukkes sist, etter `PRAGMA wal_checkpoint(TRUNCATE)`.

Andre SIGTERM innen drenering ⇒ umiddelbar `SIGKILL` til alle prosessgrupper og exit 130.

### 3.3 Skrivemodellen: to pooler, ingen egen kø

```go
// skriv: én forbindelse ⇒ database/sql serialiserer selv, med backpressure
w, _ := sql.Open("sqlite", dsn(path, "_txlock=immediate"))
w.SetMaxOpenConns(1); w.SetMaxIdleConns(1); w.SetConnMaxLifetime(0)

// les: N forbindelser, WAL gir samtidige lesere uten å blokkere skriveren
r, _ := sql.Open("sqlite", dsn(path, "_pragma=query_only(1)"))
r.SetMaxOpenConns(max(4, runtime.NumCPU()))
```

DSN (identisk på begge, bortsett fra `query_only`/`_txlock`):

```
file:/var/lib/pulseq/pulseq.db
  ?_pragma=journal_mode(WAL)
  &_pragma=busy_timeout(5000)
  &_pragma=synchronous(NORMAL)
  &_pragma=foreign_keys(ON)
```

**Hvorfor ikke en eksplisitt skrivekø med kanal?** `database/sql` med `MaxOpenConns(1)` *er* køen: den serialiserer, gir kø-venting via `ctx`, gir backpressure og respekterer timeouts. En egen kanalkø ville duplisert dette og lagt til en ny klasse av deadlocks. Enkel regel i stedet: **ingen nettverks- eller prosess-I/O inne i en skrivetransaksjon.**

`_txlock=immediate` er kritisk: uten det starter en transaksjon som `DEFERRED`, tar en leselås, og feiler med `SQLITE_BUSY` når den forsøker å oppgradere til skrivelås — og `busy_timeout` hjelper ikke mot låsoppgradering.

`query_only(1)` på lese-poolen håndhever invarianten i motoren, ikke bare ved konvensjon.

**Oppstartssjekk:** nekt å starte hvis databasefilen ligger på et nettverksfilsystem (`statfs`-magic mot NFS/SMB/FUSE-liste). SQLite-fillåsing er upålitelig der, og feilen viser seg som datakorrupsjon uker senere.

---

## 4. Pakkelayout

```
cmd/pulseq/main.go            # ~60 linjer: flagg-parse → cli.Run(ctx, args)

internal/
  cli/          subkommando-ruter, flaggsett per kommando, tabell-/JSON-render
  config/       XDG-/systemstier, defaults, konfigfil, env-overstyring
  model/        rene typer + tilstandsmaskiner. Null I/O, null imports utover stdlib
  jobspec/      YAML → model.JobSpec, validering, DAG-validering (sykler, ukjente needs)
  cronx/        Schedule-interface: cron | interval | descriptor. Next/Prev/Between
  store/        SQLite: pooler, migrations (embed.FS), alle SQL-spørringer, Tx-hjelpere
    migrations/ 0001_init.sql, 0002_….sql
  leases/       singleton-lease (scheduler/sensors) + steg-lease + fencing token
  scheduler/    tick-loop, catch-up-policy, pause, neste-tick-beregning, tick-historikk
  sensor/       evaluator, prosess-exec, JSON-kontrakt, cursor, feil-backoff
  planner/      trigger → run + steps + step_deps. Idempotens og concurrency-gating
  worker/       dispatcher (claim-loop), pool, retry-policy, steg-livssyklus
  runner/       prosess-kjøring: pgid, signalering, logg-streaming, artefakt-oppsamling
  events/       slog.Handler som dubler run-/step-scoped records til events-tabellen
  explain/      «hvorfor kjørte ikke X» — leser ticks, evals, triggers, runs
  httpapi/      net/http ServeMux, handlers, UDS-listener, versjonshandshake
  webui/        embed.FS + html/template (fase 6)
  notify/       intern ikke-blokkerende broadcast-buss
  id/           tidssorterte ID-er
  testutil/     temp-db, synctest-hjelpere, fakecmd-bygging

testdata/fakecmd/   # liten Go-binær: sov, feil, skriv artefakt, ignorer SIGTERM
docs/               # prosjektbeskrivelse, planer, ADR-er
```

**Avhengighetsretning** (håndheves med en `go vet`-lignende test som leser `go list -deps`):

```
cli, httpapi  →  scheduler, sensor, planner, worker, explain  →  store, leases  →  model
                                          ↘ runner, cronx, jobspec ↗
model importerer INGENTING fra internal/
```

`store` eksponerer ett `Store`-grensesnitt med små, oppgavespesifikke metoder (`ClaimStep`, `FinishStep`, `InsertTrigger`, `DueSchedules`) — ikke et generisk repository. Dette er koblingspunktet der Postgres kan tre inn i fase 7.

---

## 5. Datamodell

Alle tider lagres som **UTC epoch-millisekunder (INTEGER)**. Tidssone er data på `schedules`, ikke på lagringsformatet.

```sql
-- Definisjoner (avledet fra YAML på disk, men speilet for historikk)
CREATE TABLE jobs (
  name         TEXT PRIMARY KEY,
  spec_yaml    TEXT NOT NULL,
  spec_hash    TEXT NOT NULL,
  source_path  TEXT,
  enabled      INTEGER NOT NULL DEFAULT 1,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL
);

CREATE TABLE schedules (
  id                TEXT PRIMARY KEY,
  job_name          TEXT NOT NULL REFERENCES jobs(name) ON DELETE CASCADE,
  kind              TEXT NOT NULL,              -- cron | interval
  expr              TEXT NOT NULL,
  timezone          TEXT NOT NULL DEFAULT 'UTC',
  paused            INTEGER NOT NULL DEFAULT 0,
  catchup           TEXT NOT NULL DEFAULT 'latest',  -- none | latest | all
  max_catchup       INTEGER NOT NULL DEFAULT 1,
  concurrency_limit INTEGER,
  params_json       TEXT,
  last_tick_at      INTEGER,                    -- siste evaluerte scheduled_for
  next_tick_at      INTEGER
);

CREATE TABLE sensors (
  id                   TEXT PRIMARY KEY,
  name                 TEXT NOT NULL UNIQUE,
  job_name             TEXT NOT NULL REFERENCES jobs(name) ON DELETE CASCADE,
  exec_json            TEXT NOT NULL,           -- argv som JSON-array
  workdir              TEXT,
  env_json             TEXT,
  interval_ms          INTEGER NOT NULL DEFAULT 30000,
  timeout_ms           INTEGER NOT NULL DEFAULT 30000,
  paused               INTEGER NOT NULL DEFAULT 0,
  cursor               TEXT,
  cursor_updated_at    INTEGER,
  last_eval_at         INTEGER,
  next_eval_at         INTEGER,
  consecutive_failures INTEGER NOT NULL DEFAULT 0
);

-- Beslutningshistorikk: grunnlaget for `pulseq explain`
CREATE TABLE schedule_ticks (
  id            TEXT PRIMARY KEY,
  schedule_id   TEXT NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
  scheduled_for INTEGER NOT NULL,
  evaluated_at  INTEGER NOT NULL,
  outcome       TEXT NOT NULL,      -- triggered | skipped | error
  skip_reason   TEXT,
  trigger_id    TEXT,
  error         TEXT
);
CREATE UNIQUE INDEX ux_tick ON schedule_ticks(schedule_id, scheduled_for);

CREATE TABLE sensor_evals (
  id             TEXT PRIMARY KEY,
  sensor_id      TEXT NOT NULL REFERENCES sensors(id) ON DELETE CASCADE,
  started_at     INTEGER NOT NULL,
  finished_at    INTEGER,
  outcome        TEXT NOT NULL,     -- triggered | skipped | error | timeout
  skip_reason    TEXT,
  trigger_count  INTEGER NOT NULL DEFAULT 0,
  cursor_before  TEXT,
  cursor_after   TEXT,
  error          TEXT,
  stderr_excerpt TEXT               -- maks 4 KiB
);

-- Triggere: den eneste inngangen til å lage en run
CREATE TABLE triggers (
  id           TEXT PRIMARY KEY,
  source       TEXT NOT NULL,       -- schedule | sensor | manual | api
  source_id    TEXT,
  job_name     TEXT NOT NULL,
  run_key      TEXT,                -- idempotensnøkkel
  params_json  TEXT,
  created_at   INTEGER NOT NULL,
  state        TEXT NOT NULL,       -- pending | materialized | deduped | rejected
  reject_reason TEXT,
  run_id       TEXT
);
-- At-most-once materialisering. Dette ER idempotensgarantien.
CREATE UNIQUE INDEX ux_trigger_runkey
  ON triggers(job_name, run_key) WHERE run_key IS NOT NULL;
CREATE INDEX ix_trigger_pending ON triggers(state, created_at) WHERE state='pending';

CREATE TABLE runs (
  id               TEXT PRIMARY KEY,
  job_name         TEXT NOT NULL,
  trigger_id       TEXT REFERENCES triggers(id),
  run_key          TEXT,
  state            TEXT NOT NULL,   -- pending|running|succeeded|failed|deferred|cancelled
  scheduled_for    INTEGER,
  enqueued_at      INTEGER NOT NULL,
  started_at       INTEGER,
  finished_at      INTEGER,
  priority         INTEGER NOT NULL DEFAULT 0,
  concurrency_key  TEXT,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  timeout_ms       INTEGER,
  spec_snapshot    TEXT NOT NULL,   -- frosset jobbdefinisjon: gjør replay determinisk
  params_json      TEXT,
  error            TEXT,
  parent_run_id    TEXT             -- for retry/replay-kjeder
);
-- «Ikke mer enn én aktiv run per nøkkel» — håndhevet av motoren, ikke av kode
CREATE UNIQUE INDEX ux_run_active_key ON runs(concurrency_key)
  WHERE concurrency_key IS NOT NULL AND state IN ('pending','running','deferred');
CREATE INDEX ix_run_state ON runs(state, enqueued_at);

CREATE TABLE steps (
  id                TEXT PRIMARY KEY,
  run_id            TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  name              TEXT NOT NULL,
  idx               INTEGER NOT NULL,
  state             TEXT NOT NULL,  -- pending|running|succeeded|failed|skipped|cancelled
  attempt           INTEGER NOT NULL DEFAULT 0,
  max_attempts      INTEGER NOT NULL DEFAULT 1,
  next_attempt_at   INTEGER,        -- backoff-gate
  started_at        INTEGER,
  finished_at       INTEGER,
  exit_code         INTEGER,
  error             TEXT,
  log_path          TEXT,
  log_bytes         INTEGER NOT NULL DEFAULT 0,
  lease_owner       TEXT,
  lease_expires_at  INTEGER,
  claim_token       TEXT            -- fencing token
);
CREATE UNIQUE INDEX ux_step ON steps(run_id, name);
CREATE INDEX ix_step_claimable ON steps(state, next_attempt_at);
CREATE INDEX ix_step_lease ON steps(lease_expires_at) WHERE state='running';

CREATE TABLE step_deps (            -- frosne kanter per run
  run_id     TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_name  TEXT NOT NULL,
  depends_on TEXT NOT NULL,
  PRIMARY KEY (run_id, step_name, depends_on)
) WITHOUT ROWID;

CREATE TABLE artifacts (
  id         TEXT PRIMARY KEY,
  run_id     TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_id    TEXT REFERENCES steps(id) ON DELETE CASCADE,
  key        TEXT NOT NULL,
  uri        TEXT NOT NULL,
  media_type TEXT,
  size_bytes INTEGER,
  checksum   TEXT,
  meta_json  TEXT,
  created_at INTEGER NOT NULL
);
CREATE INDEX ix_artifact_key ON artifacts(key, created_at);

CREATE TABLE events (               -- strukturert hendelsesstrøm, IKKE rå stdout
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  ts       INTEGER NOT NULL,
  run_id   TEXT, step_id TEXT, schedule_id TEXT, sensor_id TEXT,
  level    TEXT NOT NULL,
  kind     TEXT NOT NULL,           -- state_change | claim | retry | skip | lease_lost | …
  msg      TEXT NOT NULL,
  attrs_json TEXT
);
CREATE INDEX ix_event_run ON events(run_id, id);

CREATE TABLE locks (                -- singleton-lease for scheduler/sensors
  name           TEXT PRIMARY KEY,
  owner          TEXT NOT NULL,
  expires_at     INTEGER NOT NULL,
  fencing_token  INTEGER NOT NULL
) WITHOUT ROWID;
```

Skjemaversjon i `PRAGMA user_version`. Ingen `schema_migrations`-tabell.

**Tilstandsmaskin (run)** — speiler prosjektbeskrivelsens seks tilstander direkte:

```
                 ┌──────────► deferred ──┐   (venter på concurrency-slot)
pending ─────────┤                       │
   │             └──────────► running ◄──┘
   │                            │
   └─► cancelled ◄──────────────┼──► succeeded
                                └──► failed
```

**Tilstandsmaskin (step):** `pending → running → {succeeded | failed | cancelled}`, samt `skipped` når en upstream har feilet. Retry er ikke en tilstand: den er `failed → pending` med `attempt++` og `next_attempt_at = now + backoff`. Da trengs ingen egen retry-scheduler.

---

## 6. Kjernemekanikker

### 6.1 Scheduler-tick

Loop hvert sekund, under singleton-lease (`locks.name = 'scheduler'`, TTL 30 s, fornyes hvert 10 s):

```
for hver ikke-pauset schedule med next_tick_at <= now:
    ticks := cronx.Between(last_tick_at, now, tz)      // alle savnede tidspunkter
    ticks = anvendCatchupPolicy(ticks)                  // none | latest | all(max_catchup)
    for hver t in ticks:
        i ÉN skrivetransaksjon:
          INSERT OR IGNORE i schedule_ticks(schedule_id, t)   -- eksakt-én-gang per tick
          hvis inserted:
            hvis concurrency_limit nådd → outcome='skipped', skip_reason='concurrency_limit(3/3)'
            ellers → INSERT trigger(run_key = sprintf("%s@%d", schedule_id, t))
    oppdater last_tick_at, next_tick_at
```

`UNIQUE(schedule_id, scheduled_for)` gjør ticket idempotent uansett hvor mange ganger loopen kjøres. En restart midt i en tick gir aldri dobbelt trigger.

**Tidssone og DST**, med `Europe/Oslo` som test-case:
- `scheduled_for` lagres alltid i UTC.
- Ikke-eksisterende lokaltid (vårovergang, 02:30 finnes ikke): policy `shift` (kjør 03:00) eller `skip`, konfigurerbar, default `shift`, alltid logget som `skip_reason`/`event`.
- Dobbel lokaltid (høstovergang, 02:30 finnes to ganger): kjør kun **første** forekomst. Unik-indeksen på `(schedule_id, scheduled_for)` (UTC) gjør dette til en ren konsekvens av at UTC-tidspunktene er forskjellige — så policyen må være eksplisitt i `cronx.Between`, ikke implisitt.

### 6.2 Sensor-evaluering

Kontrakt — **prosess inn/ut, ikke Go-interface**:

```
argv:   som konfigurert
stdin:  {"sensor":"new-files","cursor":"2026-08-20T10:00:00Z","last_eval_at":…,"now":…}
stdout: {"triggers":[{"run_key":"s3://b/f1.csv","params":{"path":"…"}}],
         "cursor":"2026-08-21T04:00:00Z"}
   ELLER {"skip_reason":"ingen nye filer siden 2026-08-20T10:00:00Z"}
stderr: fritekst → logg (4 KiB lagres på sensor_evals)
exit:   0 = ok, ≠0 = feil
```

Alternativ ved mange triggere: NDJSON på stdout (én trigger per linje, siste linje `{"cursor":…}`) — samme parser, strømmes.

Regler:
- Timeout via `context.WithTimeout` → `SIGTERM` til prosessgruppen → `SIGKILL` etter 5 s.
- Maks 1 MiB stdout; overskridelse ⇒ `outcome='error'`, ingen cursor-oppdatering.
- **Cursor og triggere skrives i samme transaksjon.** Feiler transaksjonen, går cursor tilbake, og kjøringen gjentas — at-least-once. `run_key`-unikindeksen gjør gjentakelsen ufarlig.
- `consecutive_failures` gir eksponentiell backoff: `interval * 2^min(failures,6)`, tak 1 time. Feiler den 10 ganger på rad, auto-pauses sensoren med tydelig grunn — den er da ødelagt, ikke treg.

### 6.3 Planner: trigger → run

Én goroutine, ett sted som materialiserer. Én skrivetransaksjon per trigger:

```sql
BEGIN IMMEDIATE;
  -- 1. dedup: finnes allerede aktiv/fullført run med samme run_key?
  -- 2. concurrency: INSERT i runs; unik-partial-index avviser hvis nøkkelen er opptatt
  --    ⇒ state='deferred' i stedet for 'pending'
  -- 3. frys spec_snapshot fra jobs.spec_yaml
  -- 4. INSERT steps (alle 'pending', attempt=0) + INSERT step_deps
  -- 5. UPDATE triggers SET state='materialized', run_id=…
COMMIT;
```

Å fryse `spec_snapshot` per run er det som gjør replay og «vis nøyaktig hva som kjørte» korrekt selv etter at YAML-filen er endret.

### 6.4 Claim-loop (dispatcher)

Én spørring gjør både DAG-oppløsning, prioritering og lås:

```sql
UPDATE steps
   SET state='running', lease_owner=?, claim_token=?, lease_expires_at=?,
       started_at=?, attempt = attempt + 1
 WHERE id = (
   SELECT s.id
     FROM steps s JOIN runs r ON r.id = s.run_id
    WHERE s.state = 'pending'
      AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= :now)
      AND r.state = 'running'
      AND r.cancel_requested = 0
      AND NOT EXISTS (
            SELECT 1 FROM step_deps d
              JOIN steps u ON u.run_id = d.run_id AND u.name = d.depends_on
             WHERE d.run_id = s.run_id AND d.step_name = s.name
               AND u.state <> 'succeeded')
    ORDER BY r.priority DESC, r.scheduled_for, r.enqueued_at, s.idx
    LIMIT 1)
RETURNING id, run_id, name, attempt, claim_token;
```

`NOT EXISTS`-leddet **er** DAG-utføringen. Parallelle steg faller ut av seg selv: to steg uten uoppfylte avhengigheter claim'es av hver sin worker. Ingen topologisk sortering i minnet, ingen graftilstand å gjenopprette etter krasj.

Fullføring bruker fencing token:

```sql
UPDATE steps SET state=?, finished_at=?, exit_code=?, error=?, lease_owner=NULL
 WHERE id=? AND claim_token=?;   -- 0 rader = leasen ble tatt fra oss; forkast resultatet
```

`RETURNING` støttes av SQLite (3.35+) og av `modernc.org/sqlite`.

### 6.5 Steg-utføring (`internal/runner`)

```go
cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}   // egen prosessgruppe
cmd.Cancel = func() error {                              // SIGTERM, ikke SIGKILL
    return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
}
cmd.WaitDelay = killGrace                                // deretter SIGKILL av Go selv
```

`Setpgid` er ikke valgfritt: uten det overlever barnebarn (skallet ditt starter `python`, som starter `curl`) og blir zombier som holder på filer og porter.

Miljø til steget:

```
PULSEQ_RUN_ID, PULSEQ_JOB, PULSEQ_STEP, PULSEQ_ATTEMPT, PULSEQ_RUN_KEY,
PULSEQ_TRIGGER_SOURCE, PULSEQ_SCHEDULED_FOR, PULSEQ_PARAMS  (JSON)
PULSEQ_OUTPUT         # fil steget kan skrive NDJSON til: {"artifact":{…}} / {"params":{…}}
PULSEQ_ARTIFACT_DIR   # arbeidskatalog for artefakter
```

Ingen ny protokoll: steget skriver linjer til en fil, Pulseq leser filen etter exit. Artefakter og videreførte parametre kommer «gratis».

**Logg går til fil, aldri til databasen.** `$PULSEQ_STATE/logs/<run_id>/<step>.<attempt>.log`, append-only, med tak (default 64 MiB, deretter avkorting med markør). Databasen holder kun `log_path` og `log_bytes`. Dette er det viktigste enkeltgrepet for å holde skrive-poolen ledig: strukturert hendelsesstrøm i `events` er små rader, rå stdout er megabyte.

`events`-skriving batches: opptil 200 rader eller 250 ms, én transaksjon.

### 6.6 Retry og backoff

Per steg: `max_attempts`, `backoff: fixed|exponential`, `initial`, `max_delay`, `jitter` (full jitter, default på). Ved feil:

```
attempt < max_attempts  → state='pending', next_attempt_at = now + backoff(attempt)
attempt >= max_attempts → state='failed'
                          nedstrøms steg → 'skipped'
                          run → 'failed'
```

Ingen egen retry-goroutine — `next_attempt_at`-gaten i claim-spørringen holder.

### 6.7 Lease, reaper og rekonsiliering

- Steg-lease: TTL 60 s, fornyes hvert 20 s av workeren mens steget kjører.
- Reaper (hvert 10 s): `state='running' AND lease_expires_at < now` ⇒ tilbake til `pending`, `attempt--` hvis prosessen aldri startet, `event(kind='lease_expired')`.
- **Oppstartsrekonsiliering** (før noen loop starter):
  1. Alle `steps` med `lease_owner = denne_instansens_forrige_id` → `pending`.
  2. Alle `runs` i `running` uten noe steg i `running`/`pending` → utled sluttilstand fra stegene.
  3. Alle `triggers` i `pending` → planner-køen.
  4. `deferred` runs revurderes mot concurrency-nøkler.

Dette gir **at-least-once start** og garantert konvergens etter `SIGKILL`, strømbrudd eller OOM.

### 6.8 `pulseq explain`

Ren lesekommando som svarer på «hvorfor kjørte ikke X»:

```
$ pulseq explain schedule nightly-report
schedule nightly-report  (cron "0 2 * * *", Europe/Oslo, aktiv)
  neste tick   2026-08-22 02:00:00 +02:00  (om 21t 4m)
  siste 5 tick:
    2026-08-21 02:00  triggered  → run 01K3… succeeded (4m12s)
    2026-08-20 02:00  skipped    → concurrency_limit(1/1): run 01K2… kjørte fortsatt
    2026-08-19 02:00  skipped    → schedule pausert av johan 2026-08-18 16:02
    2026-08-18 02:00  triggered  → run 01K1… failed  (step "transform" exit 1)
```

Alle data finnes allerede i `schedule_ticks`, `sensor_evals`, `triggers`, `runs`, `steps`. Kommandoen er ren presentasjon — derfor billig, og derfor må skip-grunner skrives *strukturert* fra dag én, ikke bare logges.

---

## 7. Jobbdefinisjon

```yaml
name: nightly-report
description: Genererer nattlig salgsrapport
concurrency_key: nightly-report      # maks én aktiv run med denne nøkkelen
timeout: 30m

schedules:
  - cron: "0 2 * * *"
    timezone: Europe/Oslo
    catchup: latest                  # none | latest | all
    concurrency_limit: 1

sensors:
  - name: new-sales-files
    exec: ["/opt/pulseq/sensors/s3-new-files", "--bucket", "sales-raw"]
    interval: 5m
    timeout: 60s

steps:
  - name: extract
    run: ["/usr/bin/python3", "/opt/etl/extract.py"]
    workdir: /opt/etl
    env: { PGHOST: db.internal }
    retries: { max: 3, backoff: exponential, initial: 10s, max_delay: 5m }

  - name: transform
    needs: [extract]
    run: ["/opt/etl/transform"]

  - name: load-warehouse
    needs: [transform]
    run: ["/opt/etl/load", "--target=dwh"]

  - name: load-cache
    needs: [transform]                # kjører parallelt med load-warehouse
    run: ["/opt/etl/load", "--target=cache"]
```

Validering ved `pulseq job validate` og ved reload: ukjent `needs`, syklus (Kahn), duplikat-stegnavn, ugyldig cron, ukjent tidssone, tom `run`. Feil rapporteres med linje/kolonne fra goccy-parseren.

Hot reload: `jobwatcher` sjekker mtime hvert 5. sekund. Ugyldig fil ⇒ **forrige gyldige definisjon beholdes**, feil eksponeres i `pulseq status` og `events`. Aldri delvis lasting.

---

## 8. CLI- og API-flate

```
pulseq serve      [--roles=…] [--workers N] [--db PATH] [--jobs-dir DIR] [--socket PATH]
pulseq status
pulseq job        list | show <navn> | validate [fil…] | enable <navn> | disable <navn>
pulseq run        start <job> [--param k=v]… [--run-key K] [--wait] [--follow]
                  list [--job] [--state] [--since] | show <id> | logs <id> [--step] [-f]
                  cancel <id> | retry <id> [--only-failed | --from-step <navn>]
pulseq schedule   list | show <id> | next <id> [-n 5] | pause <id> | resume <id> | fire <id>
pulseq sensor     list | show <navn> | eval <navn> [--dry-run] | pause | resume
                  cursor get <navn> | cursor set <navn> <verdi>
pulseq explain    schedule|sensor|job|run <id>
pulseq gc         [--older-than 30d] [--keep-runs 1000]
pulseq db         verify | vacuum | checkpoint
```

Alle kommandoer tar `--json` for maskinlesbar output (samme struktur som HTTP-API-et). `--dry-run` på `sensor eval` kjører sensoren, viser triggerne, men skriver hverken cursor eller triggere.

HTTP-API (samme handlers, UDS + valgfri `--listen`): `GET /v1/runs`, `GET /v1/runs/{id}`, `POST /v1/runs`, `POST /v1/runs/{id}/cancel`, `GET /v1/runs/{id}/steps/{name}/logs` (SSE ved `?follow=1`), `GET /v1/schedules`, `POST /v1/schedules/{id}/pause`, `GET /v1/explain/…`, `GET /v1/healthz`, `GET /debug/vars`.

Versjonshandshake: CLI sender `X-Pulseq-Client: <version>`; daemon avviser med 409 og tydelig melding ved inkompatibel major.

Systemd:

```ini
[Service]
Type=notify-reload
ExecStart=/usr/local/bin/pulseq serve
Restart=always
RuntimeDirectory=pulseq
StateDirectory=pulseq
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=/var/lib/pulseq
```

---

## 9. MVP-avgrensning

**Inne i MVP (fase 0–4):**
cron/interval-schedules med tidssone, pause/resume, catch-up-policy, concurrency-limit · sensorer med cursor, multi-trigger, skip_reason, `run_key`-dedup · runs og steg med DAG-avhengigheter, parallelle steg, retry per steg, timeout, cancel · run-historikk og per-steg-logg · CLI med start/stop/list/replay/explain · SQLite med WAL, leases, rekonsiliering · strukturert logging (slog JSON).

**Bevisst kuttet fra MVP:**

| Kuttet | Hvorfor | Kommer i |
|---|---|---|
| Web-UI | CLI + `--json` dekker behovet; UI er ren lesning over samme data | Fase 6 |
| Backfill | Krever parameterisert tidsvindu-modell; catch-up dekker 90 % | Fase 6 |
| Notifications | Trivielt som exec-hook på run-avslutning når events finnes | Fase 5 |
| Dynamisk fan-out | Krever steg som endrer DAG-en under kjøring — bryter «frosne kanter» | Fase 7 |
| Artefakt-lineage-graf | Tabellen finnes fra fase 2; grafvisning er UI-arbeid | Fase 6 |
| Postgres-backend | SQLite dekker single-node; grensesnittet er på plass | Fase 7 |
| Auth/RBAC | UDS-filrettigheter er riktig sikkerhetsmodell for single-node | Ved TCP-eksponering |
| Container-/cgroup-isolering | `systemd-run --scope` fra brukerens `run:`-kommando dekker behovet | — |
| Distribuert kjøring | Eksplisitt ikke-mål | — |

**Definisjon av ferdig MVP:** en operatør kan legge én YAML-fil i `/var/lib/pulseq/jobs/`, kjøre `systemctl start pulseq`, og få en nattlig 4-stegs DAG med retry, cron-tidsstyring i Europe/Oslo, en filsensor med cursor, og `pulseq explain` som forklarer hver eneste manglende kjøring — uten å ha lest annet enn `pulseq --help`.

---

## 10. Faseplan

| Fase | Innhold | Milepæl / akseptansekriterium | Estimat |
|---|---|---|---|
| **0. Fundament** | `go.mod` (Go 1.25), pakkeskjelett, `internal/model` med tilstandsmaskiner, `store` med begge pooler + migrasjonsmotor, `internal/id`, Makefile, CI (build, `vet`, `test -race`, `staticcheck`), `pulseq version` | `go build` gir én statisk binær < 15 MB, `CGO_ENABLED=0`. Benchmark: ≥ 500 skrive-tx/s mot temp-DB (går ikke dette, byttes driver nå, ikke senere) | 2 dager |
| **1. Kjøring** | `jobspec` (YAML + validering), planner, dispatcher, worker-pool, `runner` med pgid, logg til fil, run/step-tilstandsmaskin, `pulseq run start/list/show/logs`, `pulseq job validate` | Enkeltstegs jobb kjøres end-to-end fra CLI; logg strømmes; exit-kode gjenspeiles i run-state | 1 uke |
| **2. DAG og robusthet** | `needs` + `step_deps` + claim-predikat, parallelle steg, retry med backoff, run-/step-timeout, cancel med signalkjede, leases + reaper, oppstartsrekonsiliering, `run retry --only-failed/--from-step` | 4-stegs diamant-DAG kjører to steg parallelt. `SIGKILL -9` på daemon midt i run → restart → run konvergerer korrekt. Test: 50 samtidige steg, `-race`, hvert steg claim'et nøyaktig én gang | 1 uke |
| **3. Schedules** | `cronx` (cron/interval/descriptor, `Next`/`Prev`/`Between`), tick-loop under singleton-lease, tidssone og DST-policy, catch-up, pause/resume, concurrency-limit, `schedule_ticks`, `pulseq schedule …`, `pulseq explain` | `pulseq schedule next -n 5` viser korrekte tider over DST-overgangen i Europe/Oslo. Daemon nede i 6 timer → catch-up-policy `latest` gir nøyaktig én run | 1 uke |
| **4. Sensors** | JSON-kontrakt (objekt + NDJSON), cursor i samme tx som triggere, multi-trigger, `run_key`-dedup, skip_reason, feil-backoff og auto-pause, timeout og pgid-kill, `pulseq sensor eval --dry-run` | **MVP ferdig.** Filsensor oppdager 100 nye filer på ett tick, produserer 100 triggere, gjenkjøring gir null duplikater | 1 uke |
| **5. Observabilitet og API** | UDS HTTP-API, `--json` på alle kommandoer, `events`-tabell + slog-handler, SSE-loggstrøm, `expvar` + Prometheus-tekst, retention/`gc`, `wal_checkpoint`, notification-hooks | `pulseq status --json` gir full systemtilstand. Loggkatalog vokser ikke ubegrenset over 30 dagers syntetisk kjøring | 1 uke |
| **6. UI og fase 2-funksjoner** | Web-UI (`embed.FS`, `html/template`, server-rendret, auto-refresh — ingen JS-byggesteg), backfill, artefakt-lineage-visning, dry-run/explain-plan | UI viser run-liste, DAG-status og logger uten npm i repoet | 2 uker |
| **7. Skalering (valgfri)** | Postgres-driver bak `store.Store`, `--roles` som separate prosesser, `FOR UPDATE SKIP LOCKED` i claim-loopen | Samme testsuite grønn mot begge backends | 2 uker |

Rekkefølgen er valgt slik at **utføring kommer før trigging**: en scheduler uten fungerende worker er umulig å teste meningsfullt, mens en worker uten scheduler kan drives fra CLI fra dag tre.

---

## 11. Teststrategi

**Enhetstester (raske, ingen I/O)**
- `internal/model`: alle tillatte og forbudte tilstandsoverganger, tabelldrevet.
- `internal/cronx`: tabelldrevet mot kjente uttrykk; **DST-grensetilfeller i Europe/Oslo eksplisitt** (siste søndag i mars 02:30 finnes ikke; siste søndag i oktober 02:30 finnes to ganger).
- `internal/jobspec`: golden-filer for valideringsfeil, inkludert linje/kolonne i meldingen.
- Retry-backoff: monotoni, tak, jitter innenfor grenser.

**`testing/synctest` — falsk klokke**
Scheduler-loop, lease-utløp, reaper-intervaller og backoff testes inne i en synctest-boble: 30 simulerte døgn kjører på millisekunder.
Konsekvens for designet: **vi trenger ingen egen `Clock`-abstraksjon.** Koden kaller `time.Now`, `time.After`, `time.Ticker` direkte, og synctest gjør dem falske. Det sparer et helt lag med dependency injection.
Forbehold: goroutiner som blokkerer på fil-I/O mot SQLite er ikke «durably blocked», så synctest-tester av tidslogikk kjører mot en in-memory-implementasjon av `store.Store`. Alt som faktisk skal treffe disk, testes i store-lagets egne tester.

**Store-tester (ekte SQLite)**
- Mot `t.TempDir()`, **ikke `:memory:`** — WAL-oppførsel og fillåsing må testes ekte.
- Migrasjonstester: hver migrasjon opp fra tom DB, samt fra hver tidligere `user_version`.
- **Samtidighetstest:** 200 steg, 32 goroutiner, `-race`. Invariant: hvert steg claim'es nøyaktig én gang, ingen `SQLITE_BUSY` lekker ut til kalleren.
- Fencing-test: claim, la leasen utløpe, la reaperen re-claime, forsøk å fullføre med gammelt token ⇒ 0 rader, resultatet forkastes.

**Integrasjonstester**
- `testdata/fakecmd`: liten Go-binær som på kommando sover, feiler med gitt exit-kode, skriver artefakter, ignorerer `SIGTERM`, eller spawner barnebarn. Dekker alle `runner`-scenarier deterministisk uten skallskript.
- End-to-end: start ekte `pulseq serve` i egen prosess mot temp-state, snakk med den over UDS, kjør DAG-er, drep den med `SIGKILL`, start på nytt, verifiser konvergens.
- Zombie-test: steg starter barnebarn som ignorerer `SIGTERM`; verifiser at hele prosessgruppen er borte etter cancel.

**Golden-tester på CLI**
`--json`-output og tabell-output mot golden-filer, med `-update`-flagg. Fanger utilsiktede formatendringer i et CLI-first-produkt.

**Fuzzing**
`FuzzCronParse`, `FuzzSensorOutput`, `FuzzJobSpec` — alle tre parser upålitelig input og må aldri panikke.

**Krasj-/kaostest (nattlig CI)**
Kjør 500 runs mens daemonen `SIGKILL`-es på tilfeldige tidspunkter. Invarianter etterpå: ingen run i `running` uten levende lease; ingen `run_key` med to fullførte runs; sum av steg-tilstander konsistent med run-tilstand.

**Dekningsmål:** `model`, `planner`, `scheduler`, `cronx`, `worker` ≥ 85 %. Ingen måltall for `cli`, `httpapi`, `webui` — de dekkes av golden- og e2e-tester.

---

## 12. Risikoer

| # | Risiko | Sannsynlighet / konsekvens | Mottiltak |
|---|---|---|---|
| 1 | SQLite-skrivekontensjon blir taket for gjennomstrømning | Middels / høy | Logg til fil, ikke DB. Batchede `events`-skriv. `BEGIN IMMEDIATE`, `busy_timeout=5000`, korte transaksjoner, **null nettverks-/prosess-I/O i transaksjon**. Måltall verifiseres i fase 0; ~500 skrive-tx/s bærer ~100 samtidige steg |
| 2 | `modernc.org/sqlite` viser seg for treg eller har en blokkerende feil | Lav / høy | Driveren er isolert bak `store`; DSN og drivernavn er konfig. Bytte til `ncruces/go-sqlite3` (wasm) eller `mattn/go-sqlite3` (bak byggetag) skal koste under en dag. Benchmark og beslutning tas i fase 0, ikke i produksjon |
| 3 | DST/tidssone gir hoppede eller doble kjøringer | Middels / middels | UTC i lagring, tidssone som data, eksplisitt policy for begge overgangstyper, unik-indeks på `(schedule_id, scheduled_for)`. Egen testsuite for Europe/Oslo og America/New_York |
| 4 | Catch-up-storm etter lang nedetid starter hundrevis av runs | Middels / høy | Default `catchup: latest`, `max_catchup: 1`. `all` krever eksplisitt konfig og respekterer `concurrency_limit`. `pulseq status` varsler om utestående catch-up før den utføres |
| 5 | Sensor henger, lekker prosesser eller spyr ut stdout | Høy / middels | Timeout + pgid-kill (`SIGTERM`→`SIGKILL`), 1 MiB stdout-tak, `consecutive_failures`-backoff, auto-pause etter 10 feil |
| 6 | Zombie-runs etter krasj | Middels / høy | Lease + fencing token + reaper + oppstartsrekonsiliering. Dekkes av nattlig kaostest |
| 7 | Loggkatalog fyller disken | Høy / middels | Per-steg-tak (64 MiB), retention i dager og totale bytes, `pulseq gc`, disk-terskelvarsel i `status`. Nekt å starte nye runs under 5 % ledig disk |
| 8 | Funksjonskryp mot Dagster-paritet | Høy / høy | Eksplisitt ikke-mål-liste i denne planen. Regel: en ny funksjon må ikke øke direkte avhengigheter, og må kunne forklares i `pulseq --help` på én linje |
| 9 | Database på nettverksfilsystem ⇒ stille korrupsjon | Lav / kritisk | `statfs`-sjekk ved oppstart; nekt start på NFS/SMB/FUSE med mindre `--i-know-what-im-doing` |
| 10 | Skjevhet mellom CLI-versjon og daemon-versjon | Middels / lav | Versjonshandshake i HTTP-header, 409 med tydelig melding |
| 11 | `run_key` velges dårlig av brukeren ⇒ falsk dedup eller ingen dedup | Middels / middels | `pulseq sensor eval --dry-run` viser genererte nøkler. `explain` viser `deduped`-triggere med hvilken run de kolliderte mot |
| 12 | WAL-fil vokser fordi en langtlesende spørring holder snapshot | Lav / middels | `query_only` lese-pool med `SetConnMaxLifetime`, periodisk `wal_checkpoint(TRUNCATE)` i janitor, WAL-størrelse i `status` |

---

## 13. Åpne beslutninger

1. **Schedules/sensors i jobb-YAML eller egne filer?** Planen antar i jobb-filen (færre filer, alt om én jobb ett sted). Alternativ: separate `schedules.yaml`/`sensors.yaml` for gjenbruk på tvers av jobber. Avgjøres i fase 3.
2. **Parametre fra sensor til steg** — kun via `PULSEQ_PARAMS`-miljøvariabel, eller også templating i `run:`-argv? Miljøvariabel i MVP; templating er en hel klasse av injeksjonsproblemer.
3. **Skal `runs` beholde `spec_snapshot` som full YAML eller normalisert JSON?** JSON er billigere å spørre mot; YAML er lettere å lese i `run show`. Foreslår JSON + `--raw`-flagg som re-serialiserer.
4. **`sqlc` i fase 5** når skjemaet er frosset — reell gevinst eller unødvendig verktøykjede? Evalueres når spørringstallet passerer 40.

---

## Kilder

- [SQLite in Go, with and without cgo (DataStation)](https://datastation.multiprocess.io/blog/2022-05-12-sqlite-in-go-with-and-without-cgo.html) · [modernc.org/sqlite (pkg.go.dev)](https://pkg.go.dev/modernc.org/sqlite) · [ncruces/go-sqlite-bench](https://github.com/ncruces/go-sqlite-bench)
- [SQLite concurrent writes and "database is locked" errors](https://tenthousandmeters.com/blog/sqlite-concurrent-writes-and-database-is-locked-errors/) · [File Locking And Concurrency In SQLite](https://sqlite.org/lockingv3.html)
- [robfig/cron/v3 (pkg.go.dev)](https://pkg.go.dev/github.com/robfig/cron/v3) · [adhocore/gronx](https://github.com/adhocore/gronx)
- [Routing Enhancements for Go 1.22](https://go.dev/blog/routing-enhancements) · [Go's 1.22+ ServeMux vs Chi (Calhoun)](https://www.calhoun.io/go-servemux-vs-chi/)
- [Testing Time (and other asynchronicities) — testing/synctest](https://go.dev/blog/testing-time) · [The Synctest Package, new in Go 1.25](https://appliedgo.net/spotlight/go-1.25-the-synctest-package/)
- [Structured Logging with slog](https://go.dev/blog/slog) · [Graceful Shutdown in Go: Practical Patterns (VictoriaMetrics)](https://victoriametrics.com/blog/go-graceful-shutdown/)
- [Killing a child process and all of its children in Go](https://medium.com/@felixge/killing-a-child-process-and-all-of-its-children-in-go-54079af94773)
- [gopkg.in/yaml.v3 er uvedlikeholdt — migrering til goccy/go-yaml (cli/cli#10784)](https://github.com/cli/cli/issues/10784)
- [sqlc: Getting started with SQLite](https://docs.sqlc.dev/en/latest/tutorials/getting-started-sqlite.html) · [Embedding migrations (goose)](https://pressly.github.io/goose/blog/2021/embed-sql-migrations/)
- [River: SQLite support, durable periodic jobs](https://riverqueue.com/blog/sqlite-and-pro-dbsql-durable-periodic-jobs-performance-boosts) (vurdert som ferdig kø-bibliotek; forkastet — claim-loopen er ~40 linjer SQL og må uansett integreres med DAG-predikatet)
