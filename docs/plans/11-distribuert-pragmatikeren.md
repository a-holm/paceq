# Pulseq — prosjektplan: den distribuerte pragmatikeren

**Perspektiv:** single-node er hovedscenarioet. Cluster er en opsjon vi ikke bygger nå, men aldri blokkerer.
**Styrende regel:** all koordinering uttrykkes som transaksjonelle tilstandsoverganger i lagringslaget — aldri som prosess-lokalt minne.

Konsekvensen av regelen: å legge til node nummer to blir en *drivervalg*-endring, ikke en arkitekturendring. Alt som holdes i RAM (en in-memory kø, en `sync.Map` av aktive runs, en cron-ticker som eier sannheten) er per definisjon usynlig for node to og må derfor skrives om når clusteret kommer. Vi betaler den prisen på dag én — den er lav — i stedet for på dag 500.

---

## 1. Hva som eksplisitt IKKE trenger konsensus

Dette er planens viktigste avsnitt. Alt annet følger av det.

| Beslutning | Mekanisme | Hvorfor ikke konsensus |
|---|---|---|
| Hvilken worker som tar en run | Atomisk CAS på run-raden (`WHERE state='queued'`) | Taperen får «ingen jobb» og prøver igjen. Ingen skade. |
| Hvem som evaluerer schedules/sensors | Navngitt lease med TTL | To samtidige ledere gir duplikate *forslag*, ikke duplikate runs — unik idempotency-nøkkel absorberer dem. |
| Om en worker er død | Tidsutløp på `lease_expires_at` | Feilaktig «død» er trygt fordi zombien blir gjerdet ut (fencing) og ikke kan skrive resultat. |
| Cursor-fremdrift for en sensor | CAS på `cursor_version` i samme transaksjon som trigger-innsetting | Tapt løp gir re-evaluering fra gammel cursor → samme `run_key` → dedup. |
| Kansellering | Flagg i DB, plukkes opp av worker ved neste renew | Kansellering er best-effort per definisjon. |
| Konfigurasjon (jobbdefinisjoner) | Fil på disk, lastet ved oppstart/SIGHUP | Ikke replikert tilstand. Deployes som kode. |

Ting som *ville* krevd konsensus, og som vi derfor bevisst ikke lover:

- **Exactly-once utføring.** Vi lover at-least-once. Brukerens steg må være idempotente; vi gir dem verktøyet (se §5.4).
- **Global totalordning på tvers av noder.** Vi gir ordning per `concurrency_key`, ikke globalt.
- **Automatisk failover av selve datalageret.** Delegeres oppover: SQLite = ingen failover; Postgres = brukerens HA-Postgres. Pulseq bygger aldri egen replikering.

Resultat: **null Raft, null etcd, null Consul, null Redis.** Dkron trenger Raft fordi det ikke har en delt transaksjonell butikk. Vi har én — det er hele poenget med å velge en database som koordineringsprimitiv (samme valg som River gjør mot Postgres).

---

## 2. Arkitektur

Ett statisk binary, `pulseq`. Tre logiske roller som i MVP kjører i samme prosess og senere kan splittes uten kodeendring i domenet:

```
                       pulseq (ett binary)
  ┌─────────────────────────────────────────────────────────────┐
  │  KONTROLLPLAN                                               │
  │   ├── scheduler-loop   ─┐                                   │
  │   ├── sensor-loop      ─┼─ krever lease "scheduler"/"sensor"│
  │   ├── reaper-loop      ─┘                                   │
  │   └── api (net/http)    — fase 6                            │
  ├─────────────────────────────────────────────────────────────┤
  │  DISPATCHER (port)                                          │
  │   ├── LocalDispatcher   — MVP: claim rett mot Store         │
  │   └── HTTPDispatcher    — fase 6: worker claimer over HTTP  │
  ├─────────────────────────────────────────────────────────────┤
  │  EXECUTOR (worker)                                          │
  │   claim → lease → kjør DAG → heartbeat → complete           │
  ├─────────────────────────────────────────────────────────────┤
  │  STORE (port)         │  LOGSINK (port)                     │
  │   └── sqlite (MVP)    │   └── file (MVP)                    │
  │   └── postgres (f.7)  │   └── s3 / pg (senere)              │
  └─────────────────────────────────────────────────────────────┘
```

Pakkestruktur:

```
cmd/pulseq/              main, subkommandoer
internal/core/           domenetyper + state machine. Ingen SQL, ingen HTTP, ingen time.Now.
internal/store/          Store-porten (interfaces) + Caps
internal/store/sqlite/   driver: SQL, migrasjoner, pooling
internal/store/postgres/ driver: fase 7
internal/store/storetest/ konformitetssuite — kjøres mot ENHVER driver
internal/scheduler/      tick-beregning, catch-up, leader-loop
internal/sensor/         evaluator-runtime, cursor-håndtering
internal/executor/       DAG-utføring, retry, step-prosesser
internal/dispatch/       Dispatcher-porten + local/http
internal/logsink/        LogSink-porten + file
internal/clock/          Clock-interface + fake
internal/api/            HTTP-handlere (fase 6)
```

**Avhengighetsregel (håndheves i CI med `go list`):** `internal/core` importerer ingenting fra `store`, `api` eller `executor`. Alle andre pakker importerer `core`. Ingen SQL-streng finnes utenfor `internal/store/<driver>`.

---

## 3. Køsemantikk: run-claiming

### 3.1 Pull, ikke push — fra dag én

Workeren *henter* arbeid. Kontrollplanet dytter aldri. Grunner:

1. **Backpressure gratis.** En travel worker spør bare ikke om mer.
2. **Ingen ruting-tilstand.** Kontrollplanet trenger ikke vite hvilke workere som finnes eller hvor de er.
3. **Krysser NAT/brannmur.** Fase 2-workere på en annen maskin trenger utgående HTTP, ikke innkommende port.
4. **Samme kode single- og multi-node.** `LocalDispatcher` og `HTTPDispatcher` implementerer samme tre verb.

MVP-workeren kaller `Store.ClaimRuns(...)` direkte i prosessen. Det er *fortsatt* pull — bare med null nettverk.

### 3.2 Claim er én setning

Run-tabellen *er* køen. Ingen sidevogn-kø, ingen Redis, ingen in-memory heap.

```sql
-- SQLite-varianten. Kjøres i BEGIN IMMEDIATE.
UPDATE runs
   SET state           = 'running',
       worker_id       = :worker,
       lease_epoch     = lease_epoch + 1,
       lease_expires_at= :now + :ttl,
       heartbeat_at    = :now,
       started_at      = COALESCE(started_at, :now),
       attempt         = attempt + 1
 WHERE id IN (
       SELECT r.id FROM runs r
        WHERE r.state = 'queued'
          AND r.scheduled_for <= :now
          AND r.queue IN (:queues)
          AND NOT EXISTS (               -- concurrency limit per job
              SELECT 1 FROM runs a
               WHERE a.job_id = r.job_id AND a.state = 'running'
              HAVING COUNT(*) >= r.max_concurrency)
        ORDER BY r.priority DESC, r.scheduled_for ASC, r.id ASC
        LIMIT :n)
RETURNING *;
```

Postgres-varianten er identisk bortsett fra `FOR UPDATE SKIP LOCKED` i subselecten. **Signaturen i Go-porten er den samme.** Det er hele poenget.

Merk: SQLite trenger ikke `SKIP LOCKED` fordi det bare finnes én skriver — serialiseringen er allerede gitt. Det som i Postgres er en optimalisering, er i SQLite en naturlov.

### 3.3 Claim-enhet: run, ikke step (i MVP)

En claim gir workeren **hele DAG-en**. Begrunnelse:

- Steg deler ofte arbeidskatalog og mellomresultater. Run-affinitet gjør artefakt-håndtering trivielt i MVP.
- Færre DB-skriv per run — viktig når skriveren er én.
- DAG-utføring blir vanlig Go-kode med goroutines, ikke en distribuert scheduler.

Prisen: heterogene workere (GPU-steg vs. lettvektssteg) er ikke mulig. Derfor:

- `job.execution_unit` finnes i skjemaet fra dag én med verdiene `run` og `step`.
- `step_runs` har **allerede** `worker_id`, `lease_epoch`, `lease_expires_at`, `queue`.
- MVP validerer `execution_unit == "run"` og feiler tydelig på `step`.

Fase 3+ implementerer `step`-varianten ved å gjenbruke nøyaktig samme claim-setning mot `step_runs`. Ingen migrasjon, ingen API-brudd.

### 3.4 Vekking uten polling-støy

`Notifier`-port med to implementasjoner:

- **SQLite:** in-process Go-kanal (`chan struct{}`, coalescing). Fungerer perfekt fordi alle rollene er i samme prosess.
- **Postgres (fase 7):** `LISTEN/NOTIFY`.
- **Fallback alltid aktiv:** poll med jitter — 250 ms når køen nettopp hadde arbeid, eksponentiell backoff til 5 s ved tomhet. Notify er en *optimalisering*, aldri en korrekthetsforutsetning. Et tapt signal koster latens, ikke arbeid.

### 3.5 Køer og prioritet

- `runs.queue` (tekst, default `"default"`). Worker konfigureres med liste av køer den betjener.
- `runs.priority` (int, default 0).
- Concurrency-tak på fire nivåer, alle håndhevet i claim-setningen: global, per kø, per job, per schedule.
- Rettferdighet: MVP bruker `ORDER BY priority DESC, scheduled_for ASC`. Dokumentert svakhet: en høyprioritetskø kan sulte de andre. Fase 6 legger til round-robin over køer per claim-runde. Ikke et MVP-problem ved lav trafikk.

---

## 4. Lease- og heartbeat-design

### 4.1 To slags leases

**A. Run-lease (arbeids-eierskap).** Eies av en worker, per run.
**B. Rolle-lease (singleton-eierskap).** Navngitt, per rolle: `scheduler`, `sensor`, `reaper`.

Samme underliggende idé: `(holder, epoch, expires_at)` + CAS på fornyelse.

### 4.2 Rolle-lease

```sql
CREATE TABLE leases (
  name        TEXT PRIMARY KEY,        -- 'scheduler', 'sensor', 'reaper'
  holder_id   TEXT NOT NULL,           -- node-id (ULID, generert ved oppstart)
  epoch       INTEGER NOT NULL,        -- fencing token, monotont
  acquired_at INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL
);
```

Anskaffelse/fornyelse er én idempotent setning:

```sql
INSERT INTO leases (name, holder_id, epoch, acquired_at, expires_at)
VALUES (:name, :me, 1, :now, :now + :ttl)
ON CONFLICT(name) DO UPDATE SET
    holder_id   = :me,
    epoch       = CASE WHEN leases.holder_id = :me
                       THEN leases.epoch ELSE leases.epoch + 1 END,
    acquired_at = CASE WHEN leases.holder_id = :me
                       THEN leases.acquired_at ELSE :now END,
    expires_at  = :now + :ttl
WHERE leases.holder_id = :me OR leases.expires_at <= :now
RETURNING epoch, expires_at;
```

Tom retur = noen andre eier leasen og den lever ennå → gå tilbake til follower-modus.

Parametere (River bruker 5 s TTL; vi er mindre latensfølsomme og velger romsligere):

- TTL 15 s, fornyelse hvert 5 s. Tåler to tapte fornyelser.
- Ved ren avslutning: `DELETE FROM leases WHERE name=? AND holder_id=?` → umiddelbar overtakelse.
- Ved kræsj: opptil 15 s uten scheduler. For en cron-orchestrator er det irrelevant.

**Hvorfor rolle-lease på single-node i det hele tatt?** Fordi det løser et reelt single-node-problem: overlappende restart (systemd `Restart=` før gammel prosess er ute), en glemt daemon i et annet terminalvindu, eller en operatør som kjører `pulseq serve` mens tjenesten går. Uten lease får du dobbelt schedule-tick. Leasen betaler for seg selv med én node.

### 4.3 Run-lease, heartbeat og fencing

- Worker eier en run så lenge `lease_expires_at > now`. TTL 60 s, fornyelse hvert 20 s.
- **Hver skriveoperasjon fra workeren bærer `(run_id, worker_id, lease_epoch)`** og er en CAS:

```sql
UPDATE runs SET heartbeat_at = :now, lease_expires_at = :now + :ttl
 WHERE id = :run AND worker_id = :me AND lease_epoch = :epoch
RETURNING cancel_requested_at, lease_expires_at;
```

Null rader tilbake = workeren har mistet leasen (reaperen har bumpet epoch). Da skal workeren **avbryte lokalt arbeid umiddelbart** og aldri skrive resultatet. Det er fencing: den gamle eieren kan ikke lenger korrumpere tilstand, selv om den lever i beste velgående (klassisk «GC-pause i 90 sekunder»-scenarioet).

Uten epoch-CAS ville en reclaimet run kunne fullføres to ganger med motstridende sluttstatus. Med den er verste utfall duplikat *arbeid*, aldri duplikat *tilstand*.

- **Renew er toveis.** Svaret bærer `cancel_requested_at`. Kansellering trenger dermed ingen egen kanal, ingen push, ingen ekstra polling. Maks kanselleringslatens = fornyelsesintervallet.

### 4.4 Reaper (orphan-deteksjon)

Kjører hvert 10. sekund under `reaper`-leasen:

```sql
UPDATE runs
   SET state = CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'queued' END,
       lease_epoch      = lease_epoch + 1,       -- gjerder ut den gamle eieren
       worker_id        = NULL,
       lease_expires_at = NULL,
       scheduled_for    = :now + :backoff,
       last_error       = 'lease expired (worker ' || worker_id || ')'
 WHERE state = 'running' AND lease_expires_at <= :now;
```

Alltid `epoch + 1`, også når runen går til `failed` — så en sen zombie ikke kan overskrive sluttstatusen.

Reaperen rydder også: utløpte rolle-leases (ingenting å gjøre, `expires_at` er selvforklarende), `workers`-rader uten heartbeat > 10× TTL, og fullførte runs eldre enn retensjonsvinduet.

**Falske positiver er forventet, ikke en feil.** En worker som fryser i 61 s får runen tatt fra seg. Design-svaret er ikke lengre TTL (det forsinker ekte gjenoppretting), men:

- Steg med kjent lang varighet setter `job.lease_ttl` eksplisitt.
- Langtløpende steg fornyer leasen fra en egen goroutine som *ikke* blokkeres av arbeidet.
- At-least-once-kontrakten (§5.4) gjør duplikatet håndterbart.

### 4.5 Klokke — den subtile fellen

To ulike tidsregimer, og de må ikke blandes:

1. **Domenetid** (når skal neste tick være?) → injisert `clock.Clock`. Aldri `time.Now()` i `internal/core`. Gjør scheduler-logikk testbar uten `time.Sleep`.
2. **Lease-tid** → **alltid databasens klokke, aldri applikasjonens.** I SQLite betyr det prosessens klokke (samme maskin — trivielt konsistent). I Postgres betyr det `now()` beregnet i SQL. Derfor tar `Store`-portens lease-metoder **ingen `now`-parameter**; driveren fyller den inn.

Dette er den eneste grunnen til at planen ikke trenger å bry seg om klokkeskew i et fremtidig cluster: ingen node sammenligner noen gang sin egen klokke med en annen nodes.

---

## 5. Idempotens og at-least-once

### 5.1 Tre lag med nøkler

| Nøkkel | Formål | Håndhevelse |
|---|---|---|
| `idempotency_key` | Hindre at *samme logiske trigger* skaper to runs | `UNIQUE INDEX ... WHERE idempotency_key IS NOT NULL` |
| `concurrency_key` | Maks én *aktiv* run per nøkkel | `UNIQUE INDEX ... WHERE state IN ('queued','running')` |
| `run_key` (sensor) | Idempotens per trigget enhet, som beskrevet i prosjektbeskrivelsen | Inngår i `idempotency_key` |

Nøkkelkonstruksjon:

- Schedule: `sched:<schedule_id>:<tick_unix>` — samme tick gir alltid samme nøkkel, uansett hvor mange ledere som forsøker.
- Sensor: `sensor:<sensor_id>:<run_key>` — `run_key` kommer fra evaluatoren (f.eks. objekt-etag, filnavn+mtime, maks-id).
- Manuell: `manual:<ulid>` — aldri deduplisert.

### 5.2 Insert-first, aldri check-first

```sql
INSERT INTO runs (...) VALUES (...)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id;
```

Tom retur = duplikat. Vi logger en `trigger_deduped`-hendelse med peker til den eksisterende runen. Ingen `SELECT` først — check-then-insert er et TOCTOU-hull i akkurat den situasjonen vi bygger for.

### 5.3 Sensor-transaksjonen

Kjernekravet: **cursor-fremdrift og trigger-innsetting må committe atomisk.** Ellers gir kræsj enten tapte triggere (cursor først) eller uendelig duplisering (triggere først).

```go
type SensorTick struct {
    SensorID   string
    FromVersion int64      // CAS-vakt
    ToCursor   []byte
    Triggers   []Trigger   // hver med run_key
    SkipReason string      // sett hvis ingen triggere
}
// Atomisk: CAS på cursor_version + N insert-or-ignore + 1 evalueringslogg.
func (s Store) CommitSensorTick(ctx, SensorTick) (Result, error)
```

Selve `check()`-kallet (nettverk, filsystem, API) skjer **utenfor** transaksjonen — vi holder aldri en SQLite-skrivelås over I/O. Kræsj mellom check og commit gir re-evaluering fra gammel cursor, som produserer samme `run_key`-er, som dedupliseres. At-least-once hele veien ned.

### 5.4 Kontrakten mot brukerens steg

Vi lover at-least-once, og gjør det derfor mulig å oppfylle. Hvert stegprosess får:

```
PULSEQ_RUN_ID, PULSEQ_STEP_ID, PULSEQ_ATTEMPT,
PULSEQ_IDEMPOTENCY_KEY,   # stabil på tvers av retries og duplikater
PULSEQ_LEASE_EPOCH
```

`PULSEQ_IDEMPOTENCY_KEY` er ment å sendes videre som idempotency-nøkkel til Stripe, til en `INSERT ... ON CONFLICT` i brukerens egen database, til et objektnavn i S3. Dette dokumenteres som **kontrakt, ikke som tips.** Et system som lover at-least-once uten å gi brukeren en stabil nøkkel, flytter bare problemet.

---

## 6. Storage-abstraksjon

Målet er ikke «databaseuavhengighet» i abstrakt forstand. Målet er konkret: **kunne skrive `internal/store/postgres` senere uten å røre en linje i `core`, `scheduler`, `sensor` eller `executor`.**

### 6.1 Seks regler

1. **Porten uttrykker forretningsoperasjoner med atomisitetskrav — ikke CRUD.** `ClaimRuns`, `RenewRunLease`, `CommitSensorTick`, `CompleteStep`, `AcquireLease`. Aldri `UpdateRun(fields)`. Det som varierer mellom SQLite og Postgres er nettopp *hvordan* atomisiteten oppnås; hvis porten er CRUD, lekker den variasjonen opp i domenet og hele øvelsen er bortkastet.
2. **Ingen driverspesifikke typer i signaturer.** Ingen `*sql.Tx`, ingen `pgx.Conn`, ingen `sql.NullString`. Kun `core`-typer og stdlib.
3. **Ingen multi-kall-invarianter.** Hvis to skriv må skje sammen, er de én metode. Domenet får aldri holde en transaksjon åpen over egen logikk.
4. **Én konformitetssuite for alle drivere.** `storetest.Run(t, factory)` — samme testfil kjøres mot SQLite i dag og Postgres i fase 7. Inkluderer samtidighetstester: 32 goroutines claimer 100 runs → nøyaktig 100 claims, null dobbeltclaim; lease-utløp under last; epoch-fencing avviser sen skriving.
5. **Migrasjoner per driver, samme logiske versjonsnummer.** `sqlite/migrations/0007_*.sql` og `postgres/migrations/0007_*.sql` betyr samme skjematilstand.
6. **`Capabilities()` for det som ikke kan poly-fylles.** `SupportsNotify`, `MaxWriters`, `SupportsSkipLocked`. Domenet leser kun `MaxWriters` — og bruker den til én ting: å velge batch-størrelse og poll-intervall.

### 6.2 Porten

```go
package store

type Store interface {
    Runs()      RunStore
    Jobs()      JobStore
    Schedules() ScheduleStore
    Sensors()   SensorStore
    Leases()    LeaseStore
    Events()    EventStore
    Notifier()  Notifier
    Caps()      Capabilities
    Migrate(ctx context.Context) error
    Close() error
}

type RunStore interface {
    Enqueue(ctx, core.RunRequest) (core.Run, bool, error)   // bool = ny (false = dedup)
    ClaimRuns(ctx, ClaimSpec) ([]core.Run, error)
    RenewRunLease(ctx, RunLease) (LeaseStatus, error)       // status bærer cancel-flagg
    CompleteRun(ctx, RunLease, core.RunOutcome) error       // CAS på epoch
    StartStep(ctx, RunLease, stepID string) (core.StepRun, error)
    CompleteStep(ctx, RunLease, core.StepOutcome) error
    ReclaimExpired(ctx) (int, error)
    RequestCancel(ctx, runID string, reason string) error
    List(ctx, RunFilter) ([]core.Run, error)
    Get(ctx, runID string) (core.RunDetail, error)
}

type LeaseStore interface {
    Acquire(ctx, name, holder string, ttl time.Duration) (core.Lease, bool, error)
    Release(ctx, name, holder string) error
    Peek(ctx, name string) (core.Lease, error)
}
```

Merk fraværet av `now time.Time` i alle lease-relaterte metoder — se §4.5.

### 6.3 Det som ikke abstraheres

- **Logglinjer.** Egen `LogSink`-port (§7.3). Høyfrekvent append hører ikke hjemme i metadata-databasen.
- **Tid på disk.** SQLite: `INTEGER` unix-mikrosekunder. Postgres: `timestamptz`. Konvertering skjer i driveren; `core` ser bare `time.Time`.
- **JSON.** Begge databaser har det. Lagres som `TEXT`/`JSONB`, marshales i driveren.

---

## 7. SQLite: én skriver, håndtert

### 7.1 To pooler

```go
writer, _ := sql.Open("sqlite", dsn+"?_txlock=immediate&_pragma=busy_timeout(5000)")
writer.SetMaxOpenConns(1)      // ← selve løsningen
writer.SetMaxIdleConns(1)
writer.SetConnMaxLifetime(0)

reader, _ := sql.Open("sqlite", dsn+"?_pragma=busy_timeout(5000)")
reader.SetMaxOpenConns(runtime.NumCPU())
```

`SetMaxOpenConns(1)` gjør Go-ens connection pool til vår skrivekø. Skrivere venter i `database/sql` på en mutex — deterministisk, rettferdig, uten `SQLITE_BUSY`-retry-løkker. `_txlock=immediate` tar skrivelåsen ved `BEGIN`, ikke ved første skriv, som eliminerer klassen av oppgraderingsdeadlocks som ellers gir `database is locked` selv med `busy_timeout` satt.

Pragmaer ved oppstart: `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`, `wal_autocheckpoint=1000`, `temp_store=MEMORY`.

**Invariant, testet i CI:** enhver kodevei som skriver bruker writer-poolen. Håndheves ved at SQLite-driveren eksponerer `w.tx(ctx, fn)` og `r.query(...)` internt, og at `writer` aldri er tilgjengelig fra en lesesti.

### 7.2 Skrivebudsjett

Per run av en 5-stegs jobb: ~1 enqueue + 1 claim + 5 stegstart + 5 stegslutt + ~4 heartbeats + 1 complete ≈ **17 skriv**. Ved 100 runs/minutt ≈ 28 skriv/s. SQLite i WAL-modus med `synchronous=NORMAL` gjør tusenvis. **Vi har to størrelsesordener margin.** Skriveren er ikke flaskehalsen — så lenge logger holdes ute (§7.3).

### 7.3 Logger ut av databasen

Logglinjer er høyfrekvent append og ville alene sprengt skrivebudsjettet. `LogSink`-port:

```go
type LogSink interface {
    Writer(ctx, runID, stepID string, attempt int) (io.WriteCloser, error)
    Open(ctx, ref core.LogRef) (io.ReadCloser, error)
}
```

MVP: `FileSink` skriver JSONL til `<datadir>/logs/<yyyy-mm-dd>/<run_id>/<step_id>.<attempt>.jsonl`. Metadata-DB lagrer kun `LogRef` (sti, størrelse, linjeantall). Gevinster: skrivekøen holdes kort, `grep` og `tail -f` virker uten verktøy, log-shipping til Loki/journald er en cron-jobb, og Postgres-porten arver løsningen uendret. Fase 7+ legger til `S3Sink` for remote workers.

### 7.4 Én fil, ikke flere

Prosjektbeskrivelsen nevner flere SQLite-filer som mulig løsning på skriverbegrensningen. **Vi avviser det.** Flere filer betyr ingen transaksjonell atomisitet på tvers — nøyaktig den garantien hele designet hviler på (§5.3). Vi løser skriverbegrensningen med én skriveforbindelse (§7.1) og ved å flytte det høyfrekvente volumet ut av databasen helt (§7.3). Én metadatafil, `pulseq.db`.

---

## 8. Datamodell

Sentrale tabeller. Alle IDer er ULID (`TEXT`, leksikografisk sorterbar, tidsordnet — gir gratis `ORDER BY id` som «nyeste først» og unngår UUIDv4-indeksfragmentering).

```sql
CREATE TABLE jobs (
  id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
  spec_hash TEXT NOT NULL,          -- endring => ny job_version
  execution_unit TEXT NOT NULL DEFAULT 'run'   -- 'run' | 'step' (step = fase 3+)
    CHECK (execution_unit IN ('run','step')),
  max_concurrency INTEGER NOT NULL DEFAULT 1,
  default_queue TEXT NOT NULL DEFAULT 'default',
  lease_ttl_ms INTEGER NOT NULL DEFAULT 60000,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);

CREATE TABLE job_versions (
  id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
  version INTEGER NOT NULL, spec_json TEXT NOT NULL, created_at INTEGER NOT NULL,
  UNIQUE(job_id, version)
);

CREATE TABLE step_defs (          -- statisk DAG per job_version
  job_version_id TEXT NOT NULL REFERENCES job_versions(id),
  step_id TEXT NOT NULL,          -- navn, stabilt på tvers av versjoner
  depends_on TEXT NOT NULL DEFAULT '[]',   -- JSON-array av step_id
  max_attempts INTEGER NOT NULL DEFAULT 1,
  retry_backoff_ms INTEGER NOT NULL DEFAULT 5000,
  queue TEXT,                     -- brukes først ved execution_unit='step'
  PRIMARY KEY (job_version_id, step_id)
);

CREATE TABLE runs (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id),
  job_version_id TEXT NOT NULL REFERENCES job_versions(id),
  trigger_id TEXT REFERENCES triggers(id),
  state TEXT NOT NULL CHECK (state IN
    ('queued','running','succeeded','failed','cancelled','skipped','blocked')),
  queue TEXT NOT NULL DEFAULT 'default',
  priority INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT,
  concurrency_key TEXT,
  params_json TEXT NOT NULL DEFAULT '{}',
  attempt INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 1,
  max_concurrency INTEGER NOT NULL DEFAULT 1,   -- denormalisert for claim-setningen
  scheduled_for INTEGER NOT NULL,
  -- lease
  worker_id TEXT, lease_epoch INTEGER NOT NULL DEFAULT 0,
  lease_expires_at INTEGER, heartbeat_at INTEGER,
  -- livssyklus
  created_at INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER,
  cancel_requested_at INTEGER, skip_reason TEXT, last_error TEXT
);

CREATE UNIQUE INDEX runs_idem ON runs(idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX runs_conc_active ON runs(concurrency_key)
  WHERE concurrency_key IS NOT NULL AND state IN ('queued','running');
CREATE INDEX runs_claim  ON runs(queue, state, priority DESC, scheduled_for)
  WHERE state = 'queued';
CREATE INDEX runs_reaper ON runs(lease_expires_at) WHERE state = 'running';

CREATE TABLE step_runs (
  id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_id TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL CHECK (state IN
    ('pending','queued','running','succeeded','failed','skipped','cancelled')),
  -- lease-kolonner: ubrukte i MVP, bærebjelken for execution_unit='step'
  queue TEXT, worker_id TEXT, lease_epoch INTEGER NOT NULL DEFAULT 0,
  lease_expires_at INTEGER, heartbeat_at INTEGER,
  started_at INTEGER, finished_at INTEGER, exit_code INTEGER,
  log_ref TEXT, error TEXT,
  UNIQUE(run_id, step_id, attempt)
);

CREATE TABLE schedules (
  id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
  job_id TEXT NOT NULL REFERENCES jobs(id),
  kind TEXT NOT NULL CHECK (kind IN ('cron','interval')),
  expr TEXT NOT NULL, timezone TEXT NOT NULL DEFAULT 'UTC',
  catchup_policy TEXT NOT NULL DEFAULT 'skip'    -- 'skip'|'last'|'all'
    CHECK (catchup_policy IN ('skip','last','all')),
  catchup_window_ms INTEGER NOT NULL DEFAULT 3600000,
  max_concurrency INTEGER NOT NULL DEFAULT 1,
  paused INTEGER NOT NULL DEFAULT 0,
  last_tick_at INTEGER,           -- fremdriftsmarkør, alltid tick-tid (ikke veggklokke)
  next_tick_at INTEGER,           -- materialisert for "explain"/preview
  created_at INTEGER NOT NULL
);

CREATE TABLE schedule_ticks (     -- historikk + tick-idempotens
  schedule_id TEXT NOT NULL REFERENCES schedules(id),
  tick_at INTEGER NOT NULL,
  decided_at INTEGER NOT NULL,
  outcome TEXT NOT NULL,          -- 'enqueued'|'deduped'|'skipped'|'error'
  reason TEXT, run_id TEXT REFERENCES runs(id),
  PRIMARY KEY (schedule_id, tick_at)
);

CREATE TABLE sensors (
  id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
  job_id TEXT NOT NULL REFERENCES jobs(id),
  kind TEXT NOT NULL,             -- 'exec'|'http'|'sql'|'fs'
  config_json TEXT NOT NULL,
  interval_ms INTEGER NOT NULL DEFAULT 30000,
  timeout_ms INTEGER NOT NULL DEFAULT 30000,
  max_triggers_per_tick INTEGER NOT NULL DEFAULT 100,
  cursor BLOB, cursor_version INTEGER NOT NULL DEFAULT 0,   -- CAS-vakt
  paused INTEGER NOT NULL DEFAULT 0,
  last_eval_at INTEGER, next_eval_at INTEGER,
  consecutive_failures INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE sensor_evaluations (
  id TEXT PRIMARY KEY, sensor_id TEXT NOT NULL REFERENCES sensors(id),
  started_at INTEGER NOT NULL, duration_ms INTEGER NOT NULL,
  outcome TEXT NOT NULL,          -- 'triggered'|'skipped'|'error'
  trigger_count INTEGER NOT NULL DEFAULT 0,
  skip_reason TEXT, error TEXT,
  cursor_before BLOB, cursor_after BLOB
);

CREATE TABLE triggers (           -- felles opphavssporing for enhver run
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,           -- 'schedule'|'sensor'|'manual'|'api'|'retry'
  source_id TEXT, run_key TEXT, payload_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL
);

CREATE TABLE run_events (         -- state machine-revisjonsspor
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_id TEXT, at INTEGER NOT NULL,
  kind TEXT NOT NULL,             -- 'enqueued','claimed','lease_lost','reclaimed',...
  from_state TEXT, to_state TEXT,
  actor TEXT,                     -- worker_id / node_id
  detail_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE artifacts (
  id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_id TEXT NOT NULL, name TEXT NOT NULL, uri TEXT NOT NULL,
  size_bytes INTEGER, checksum TEXT, created_at INTEGER NOT NULL
);

CREATE TABLE workers (            -- registry; ren observabilitet i MVP, ruting i fase 6
  id TEXT PRIMARY KEY, hostname TEXT, pid INTEGER,
  queues TEXT NOT NULL DEFAULT '[]', capacity INTEGER NOT NULL DEFAULT 1,
  version TEXT, started_at INTEGER NOT NULL, heartbeat_at INTEGER NOT NULL
);

CREATE TABLE leases (...);        -- se §4.2
CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL);
```

**Tilstandsmaskin for run:**

```
                   ┌──────────── cancel ────────────┐
                   ▼                                │
 (ny) ──> queued ──claim──> running ──ok──> succeeded
            ▲                  │
            │                  ├──feil, attempt<max──> queued  (backoff)
            │                  ├──feil, attempt=max──> failed
            └──reclaim─────────┘  (lease utløpt, epoch++)

 blocked   : concurrency_key opptatt, policy=queue
 skipped   : schedule/sensor besluttet å ikke kjøre (skip_reason satt)
 cancelled : cancel_requested_at observert av worker, eller avbrutt mens queued
```

Kun `queued → running` er en claim. Kun `running → {succeeded, failed, queued}` krever gyldig `lease_epoch`. Alle overganger skriver `run_events` — det er «explain»-kommandoens datagrunnlag.

---

## 9. Teknologivalg

| Valg | Alternativ vurdert | Begrunnelse |
|---|---|---|
| Go 1.24+, kun stdlib i `core` | — | `log/slog` gir strukturert logging uten avhengighet; `net/http` sin router (1.22+) fjerner behovet for chi/gin. |
| **modernc.org/sqlite** (cgo-fri) | mattn/go-sqlite3 | Statisk binary, triviell krysskompilering, ingen C-verktøykjede — avgjørende for «last ned én fil og kjør». Koster ~2× på råytelse, men §7.2 viser to størrelsesordener margin. Driveren ligger bak byggflagg slik at `mattn` kan velges der ytelse trumfer distribusjon. |
| **adhocore/gronx** for cron | robfig/cron | `robfig/cron` er en *in-process runner* som eier tidsstyringen i RAM — feil form for en DB-drevet scheduler, og uvedlikeholdt siden 2020 (kjente DST-panics). `gronx` er avhengighetsfri og eksponerer `NextTick(from)`/`PrevTick(from)` fra vilkårlig tidspunkt, som er nøyaktig primitivet catch-up og «next run»-preview trenger. |
| Håndskrevet SQL i driverpakken | sqlc, GORM, ent | De kritiske spørringene er dialektspesifikke (`RETURNING`, `SKIP LOCKED`, partial indexes). Kodegenerering hjelper ikke der, og en ORM skjuler nettopp den atomisiteten designet hviler på. Ca. 50 spørringer totalt — håndterbart. Porten er uansett et Go-interface. |
| Migrasjoner: `embed.FS` + versjonstabell | goose, atlas, golang-migrate | ~80 linjer kode, null avhengighet, og vi trenger uansett *per-driver* migrasjonssett med delt logisk versjonsnummer. |
| `spf13/cobra` for CLI | stdlib `flag`, urfave/cli | CLI-first er et produktkrav. Cobra gir nøstede subkommandoer og shell-completion gratis. Eneste tunge avhengighet — akseptert, isolert til `cmd/`. |
| Jobbspec i YAML | TOML, HCL, Starlark | Kjent form for målgruppen (Dagster/Argo/Compose). Parses til intern struct ved oppstart; YAML-typer lekker aldri inn i `core`. Starlark ville gitt dynamikk vi ikke vil ha i en «veldig liten kjerne». |
| Sensorer som eksterne prosesser | Innebygd plugin-API, Go-plugins, WASM | «Minimal plugin-flate» er et produktmål. Kontrakten er: prosess leser JSON på stdin (`{cursor, last_run_at}`), skriver JSON på stdout (`{triggers:[{run_key,payload}], cursor, skip_reason}`), exit 0. Fungerer fra ethvert språk, testbart med `echo`, ingen ABI-forpliktelse. Innebygde `http`/`sql`/`fs`-sensorer dekker de vanligste tilfellene uten skripting. |
| JSON over HTTP for remote workers (fase 6) | gRPC fra start | Worker-protokollen er *tre verb* (Claim/Renew/Complete) med samme signatur som `Store`-porten. HTTP + long-poll krysser brannmurer og proxyer der gRPC-streaming feiler, og debugges med `curl`. gRPC blir en ren transport-swap hvis throughput noen gang krever det. |
| Prometheus-tekstformat på `/metrics` | OpenTelemetry SDK | Håndskrevet eksposisjon er ~100 linjer og null avhengighet. OTel kan legges til bak et interface hvis noen ber om det. |

---

## 10. MVP-avgrensning

**Med i MVP** (fase 0–5):

Cron/interval-schedules med tidssone og catch-up · sensorer med cursor og multi-trigger · run- og steghistorikk · retry per steg · DAG med avhengigheter og parallelle steg · claim/lease/heartbeat/reaper · rolle-lease · idempotency- og concurrency-nøkler · strukturerte logger på fil · CLI (`run`, `list`, `show`, `logs`, `pause`, `resume`, `cancel`, `replay`, `explain`, `preview`) · `/metrics` og `/healthz`.

**Bevisst utelatt fra MVP — men ikke blokkert:**

| Utelatt | Hva som holder døren åpen |
|---|---|
| Postgres-driver | Porten + konformitetssuiten finnes fra fase 0. Fase 7 er å skrive én pakke. |
| Remote workers | `Dispatcher`-porten finnes fra fase 1 med `LocalDispatcher`. Claim-protokollen er allerede pull. |
| gRPC | HTTP-protokollen har samme tre verb. Transport-swap. |
| Per-steg-claiming | `step_runs` har lease-kolonner fra fase 0. `execution_unit` finnes i `jobs`. |
| Web UI | Alt UI-et trenger finnes i `run_events`, `schedule_ticks`, `sensor_evaluations`. Fase 6 eksponerer det som JSON. |
| Backfill | `schedule_ticks` med `(schedule_id, tick_at)` som PK er nettopp backfill-journalen. Backfill = å sette inn historiske ticks. |
| Dynamisk fan-out | `step_runs` er allerede rader, ikke definisjoner. Fan-out = sette inn flere rader for samme `step_id`. |
| Varsling | `run_events` er hendelsesstrømmen en varsler ville abonnert på. |
| Artefakt-lineage | `artifacts`-tabellen finnes med `run_id`+`step_id` fra fase 4; lineage er en spørring over den. |

**Utelatt og bevisst blokkert** (avvist scope, ikke utsatt): asset-graf med materialisering, innebygd sekret-håndtering (bruk `systemd` `LoadCredential` eller env), utførelse i container (steg er prosesser; bruk `podman run` som kommando), distribuert datalager-replikering.

---

## 11. Faser fra tomt repo til ferdig produkt

Hver fase avsluttes med et binary som kjører og et sett akseptansetester som består. Ingen fase etterlater halvferdig tilstand i `main`.

### Fase 0 — Fundament (≈1 uke)
Repo-skjelett, `Makefile`, CI (build, `go vet`, race-tester, avhengighetsregel-sjekk). `internal/core`-typer og tilstandsmaskin som ren funksjon. `clock.Clock` + fake. ULID-generering. `Store`-porten. `storetest`-konformitetssuiten (skrives **før** driveren). `sqlite`-driver med to-pool-oppsett, pragmaer, migrasjonsmotor, migrasjon 0001.
**Aksept:** konformitetssuiten kjører grønt mot SQLite. Tilstandsmaskinen har 100 % grenkdekning. Ingen `time.Now()` i `core` (håndhevet av linter).

### Fase 1 — Runs, claiming og lease (≈2 uker)
Enqueue med idempotency- og concurrency-nøkler. `ClaimRuns`. Run-lease, heartbeat, fencing via epoch. Reaper. `LocalDispatcher` + `Dispatcher`-porten. Executor som kjører ett steg (én kommando). `LogSink`/`FileSink`. `run_events`. CLI: `pulseq run`, `list`, `show`, `logs`, `cancel`, `serve`.
**Aksept:** 32 samtidige claimere gir null dobbeltclaim (i suiten). `SIGKILL` på worker → runen tas tilbake innen TTL og fullføres av neste claim. Sen skriving fra gjerdet worker avvises med epoch-feil.

### Fase 2 — Schedules og rolle-lease (≈1,5 uker)
`leases`-tabell og `AcquireLease`/`Release`. Leader-loop med TTL 15 s. Cron/interval via `gronx`, tidssone per schedule, DST-regler dokumentert og testet (hoppet time, dublert time). Catch-up-policyer `skip`/`last`/`all` med vindu. `schedule_ticks` som idempotensjournal. Pause/resume. `skip_reason`.
**Aksept:** to `pulseq serve`-prosesser mot samme fil produserer nøyaktig én run per tick. Stopp i 30 min med `catchup=all` gir riktig antall runs ved oppstart. DST-vårovergang taper ingen tick.

### Fase 3 — Sensorer (≈2 uker)
Sensor-runtime med prosesskontrakt (JSON stdin/stdout). Innebygde `http`, `fs`, `sql`. `CommitSensorTick` med cursor-CAS. Multi-trigger med `run_key`-dedup. `sensor_evaluations`-historikk. Backoff ved gjentatte feil. Timeout og drap av hengende evaluator.
**Aksept:** kræsj mellom `check()` og commit gir null tapte og null duplikate runs. 100 nye filer i ett tick gir 100 runs; samme tick gjentatt gir 0 nye.

### Fase 4 — DAG (≈2 uker)
`step_defs` med `depends_on`. Topologisk sortering med syklusdeteksjon ved spec-innlasting (ikke ved kjøretid). Parallell utføring av uavhengige steg med semafor. Retry per steg med backoff. `pulseq replay --from-step` og `--failed-only`. Artefaktregistrering.
**Aksept:** diamant-DAG kjører B og C parallelt. Feil i B gir `skipped` for D, ikke `failed`. Replay fra steg gjenbruker artefakter fra vellykkede steg.

### Fase 5 — Observabilitet, MVP ferdig (≈1,5 uker)
`pulseq explain <schedule|sensor|run>` — leser `run_events`/`schedule_ticks`/`sensor_evaluations` og svarer i klartekst på «hvorfor kjørte ikke dette?». `pulseq preview <schedule> --next 10`. `pulseq dry-run`. `/metrics` (kølengde, claim-latens, lease-reclaims, tick-avvik, sensorfeilrate). `/healthz` med lederstatus. Retensjon og opprydding. Dokumentasjon: at-least-once-kontrakten, driftsveiledning, `systemd`-enhet.
**Aksept:** `explain` gir korrekt årsak for hver av: pauset, concurrency-blokkert, dedupliktert, skip-reason fra sensor, ingen leder. **Produktet er brukbart i produksjon på én maskin.**

### Fase 6 — HTTP API og remote workers (≈2 uker)
Read/write-API over `net/http`. Worker-protokoll: `POST /v1/claim` (long-poll, 30 s), `POST /v1/leases/{run}/renew` (returnerer cancel-flagg), `POST /v1/runs/{id}/complete`. Token-auth. `workers`-registry med heartbeat og synlighet i CLI. Artefakt-opplasting fra remote worker. Round-robin over køer i claim.
**Aksept:** worker på annen maskin kjører jobber ende-til-ende. Nettverksbrudd midt i en run → reaper tar den tilbake, worker oppdager tapt lease ved neste renew og avbryter lokalt.

### Fase 7 — Postgres-driver, multi-node muliggjort (≈1,5 uker)
`internal/store/postgres` med pgx. Samme migrasjonsnumre. `SKIP LOCKED` i claim. `LISTEN/NOTIFY` i `Notifier`. Konformitetssuiten kjøres uendret mot Postgres i CI. Dokumentert oppsett: N `pulseq serve`-prosesser mot delt Postgres, hvorav én holder scheduler-leasen.
**Aksept:** **null linjer endret utenfor `internal/store/`.** Det er fasens egentlige akseptansekriterium. Konformitetssuiten grønn mot begge drivere. Kaos-test: drep lederen under last → ny leder innen 15 s, ingen tapte eller dupliserte runs.

### Fase 8 — Modning (løpende)
Web UI (les-først, server-rendret over API-et fra fase 6). Backfill via historiske `schedule_ticks`. Varsling (webhook/e-post) drevet av `run_events`. Dynamisk fan-out. Artefakt-lineage-visning. `execution_unit='step'` for heterogene workere. S3-`LogSink`.

**Sum til MVP (fase 0–5): ≈10 uker. Til fullt multi-node (fase 0–7): ≈14 uker.**

---

## 12. Risikoer

| # | Risiko | Sannsynlighet | Konsekvens | Tiltak |
|---|---|---|---|---|
| R1 | **Driver-divergens:** Postgres-driveren oppfører seg subtilt annerledes enn SQLite (låsing, isolasjon, `NULL`-ordning) | Høy | Høy | Konformitetssuiten skrives i fase 0 og eier definisjonen av korrekt oppførsel. Postgres-driveren er «ferdig» først når suiten er grønn — inkludert samtidighetstestene. Suiten kjøres mot begge i hver CI-kjøring fra fase 7. |
| R2 | **SQLite skrivemetning** ved uventet volum | Lav | Høy | §7.2-budsjettet gir to størrelsesordener margin, og logger ligger utenfor DB. Metrikk på skrivelås-ventetid med varsling. Rømningsvei: fase 7 finnes allerede. |
| R3 | **Lease-TTL vs. lange steg:** et 4-timers steg med 60 s TTL blir reclaimet | Middels | Høy | Fornyelse skjer i egen goroutine som aldri blokkeres av arbeidet. `job.lease_ttl_ms` er konfigurerbar. Advarsel i loggen når et steg overskrider 50 % av TTL uten fornyelse. Fencing gjør at falsk reclaim aldri korrumperer tilstand — bare kaster bort arbeid. |
| R4 | **At-least-once overrasker brukeren:** en jobb kjører to ganger og gjør noe dobbelt | Middels | Høy | Kontrakten dokumenteres først, ikke sist. `PULSEQ_IDEMPOTENCY_KEY` gis til hvert steg. `pulseq explain` viser eksplisitt når en run er et gjenopptak. Vurder `at_most_once: true` per job i fase 8 (ingen automatisk reclaim — operatøren må gripe inn) for jobber der duplikat er verre enn tap. |
| R5 | **Porten lekker:** noen legger `*sql.Tx` eller en `time.Time`-parameter i en lease-metode, og fase 7 blir en omskriving | Middels | Høy | Avhengighetsregel håndhevet i CI. Kodegjennomgang med §6.1 som sjekkliste. En bevisst «fake in-memory store» som *ikke* er SQL-basert holdes i live i testene — den bryter umiddelbart hvis porten begynner å anta SQL. |
| R6 | **Scope-glidning mot Dagster:** asset-graf, materialiseringer, plugin-SDK | Høy | Middels | §10 sin «blokkert»-liste er en beslutning, ikke en kø. Produktløftet er «veldig liten kjerne». Nytt scope krever at noe annet fjernes. |
| R7 | **DST og cron:** hoppet eller dublert time gir tapt eller dobbel kjøring | Middels | Middels | Eksplisitte tester for begge overganger i alle støttede tidssoner. Dokumentert semantikk: ved hoppet time kjører jobben ved første gyldige tidspunkt etter hoppet; ved dublert time kjører den én gang (dedup på `tick_at` i UTC). `schedule_ticks` lagrer alltid `tick_at` i UTC. |
| R8 | **Klokkeskew** mellom noder ødelegger leases i fase 7 | Lav | Høy | §4.5: lease-tid beregnes alltid i databasen, aldri i applikasjonen. Ingen node sammenligner egen klokke med en annens. Testes ved å kjøre en node med bevisst forskjøvet klokke i kaos-testen. |
| R9 | **Sensor-evaluator henger** og blokkerer alle andre sensorer | Middels | Middels | Timeout per evaluering, hard prosessdrap, evaluering i egen goroutine med semafor. `consecutive_failures` gir eksponentiell backoff og til slutt auto-pause med tydelig årsak. |
| R10 | **Loggfiler på disk går ut av synk med DB** ved remote workers (fase 6) | Middels | Lav | `LogRef` behandles som best-effort-peker; manglende fil gir «logg utilgjengelig», aldri feilet run. S3-`LogSink` i fase 8 fjerner problemet for remote-oppsett. |
| R11 | **Ingen leder** fordi alle noder er nede eller leasen står fast | Lav | Middels | `expires_at` er selvhelbredende — en fastlåst lease utløper alltid. `/healthz` rapporterer «ingen leder observert på N sekunder». Ingen manuell opprydding er noen gang nødvendig. |

---

## 13. Sammendrag av de bærende beslutningene

1. **Databasen er koordineringsprimitivet.** Ingen Raft, ingen etcd, ingen Redis. Én transaksjonell butikk gjør alt Dkron trenger et konsensuslag for.
2. **Pull fra dag én**, selv når workeren er in-process. Backpressure, ingen ruting-tilstand, brannmurvennlig, og identisk kode single- og multi-node.
3. **Lease + fencing token i stedet for lås.** Utløp er selvhelbredende; epoch-CAS gjør at en zombie kan kaste bort arbeid, men aldri korrumpere tilstand.
4. **Idempotens er billettprisen for at-least-once**, og nøkkelen gis videre til brukerens steg som en dokumentert kontrakt.
5. **Storage-porten uttrykker atomiske forretningsoperasjoner**, ikke CRUD — og en konformitetssuite skrevet før den første driveren holder den ærlig.
6. **Én SQLite-fil, én skriveforbindelse, logger på disk.** Skriverbegrensningen løses ved å eie køen, ikke ved å omgå den med flere filer.
7. **Fase 7 sitt akseptansekriterium — null linjer endret utenfor `internal/store/` — er hele planens eksistensberettigelse.** Alt over er i tjeneste for det.

---

## Kilder

- [River: Leader election](https://riverqueue.com/docs/leader-election) · [Maintenance services](https://riverqueue.com/docs/maintenance-services) · [Periodic jobs](https://riverqueue.com/docs/periodic-jobs) · [Migrations / table reference](https://riverqueue.com/docs/migrations)
- [River: a Fast, Robust Job Queue for Go + Postgres (brandur.org)](https://brandur.org/river)
- [hibiken/asynq](https://github.com/hibiken/asynq) · [Simple task queue with Redis Streams (lease/heartbeat-mønster)](https://cschleiden.dev/blog/2022-04-08-task-queue-with-redis/)
- [Dkron architecture](https://dkron.io/docs/architecture/) · [distribworks/dkron overview](https://deepwiki.com/distribworks/dkron/1-overview)
- [Nomad: heartbeat.go](https://github.com/hashicorp/nomad/blob/main/nomad/heartbeat.go) · [When Nomad misses a (heart)beat](https://blog.aleksic.dev/when-nomad-misses-a-heartbeat) · [Nomad server block (heartbeat TTL)](https://developer.hashicorp.com/nomad/docs/configuration/server)
- [Temporal: Task Queues](https://docs.temporal.io/task-queue) · [Matching service architecture](https://github.com/temporalio/temporal/blob/main/docs/architecture/matching-service.md) · [Tasks](https://docs.temporal.io/tasks)
- [Airflow: Tasks (heartbeat timeout / zombies)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html) · [«zombie task» → «task heartbeat timeout»](https://www.mail-archive.com/dev@airflow.apache.org/msg15177.html)
- [SQLite concurrent writes and "database is locked"](https://tenthousandmeters.com/blog/sqlite-concurrent-writes-and-database-is-locked-errors/) · [SQLITE_BUSY despite timeout (BEGIN IMMEDIATE)](https://berthub.eu/articles/posts/a-brief-post-on-sqlite3-database-locked-despite-timeout/) · [Concurrent write transactions in SQLite](https://oldmoe.blog/2024/07/08/the-write-stuff-concurrent-write-transactions-in-sqlite/)
- [goqite (SQLite-kø i Go)](https://github.com/maragudk/goqite) · [liteq](https://github.com/khepin/liteq)
- [mattn/go-sqlite3 vs modernc.org/sqlite benchmark](https://github.com/multiprocessio/sqlite-cgo-no-cgo) · [go-sqlite-bench](https://github.com/ncruces/go-sqlite-bench)
- [adhocore/gronx](https://github.com/adhocore/gronx) · [robfig/cron](https://github.com/robfig/cron) · [netresearch/go-cron (om robfig-vedlikehold)](https://github.com/netresearch/go-cron)
- [Repositories, transactions, and unit of work in Go](https://rednafi.com/go/repo-txn-uow/) · [Do you need a repository layer on top of sqlc?](https://rednafi.com/shards/2026/03/repository-layer-over-sqlc/)
