# Pulseq — prosjektplan (04: Orkestrator-veteranen)

Perspektiv: skrevet av noen som har driftet Airflow, Dagster, Prefect og Temporal i produksjon.
Planen er derfor organisert rundt **hva som faktisk går galt**, ikke rundt hva som ser pent ut i en README.

---

## 0. Tese

Verdien i Dagsters schedule/sensor-modell ligger i fire ting som må være sanne **samtidig**:

1. Korrekt tick-generering (tid, tidssone, catch-up).
2. Idempotent run-oppretting (`run_key`, cursor, dedup).
3. Holdbar utføring (lease, retry, reaping, ingen foreldreløse prosesser).
4. Forklarbarhet (hver beslutning har en lagret grunn).

Svikter én av dem, er produktet enten upålitelig eller uforståelig. Alt annet i Airflow/Dagster/Prefect —
asset-grafer, SDK-er, plugin-økosystem, executor-abstraksjoner — er **plattform-tyngde**, ikke kjerneverdi.

**Sentral observasjon:** plattform-tyngden i disse verktøyene kommer ikke fra schedule/sensor-ideen.
Den kommer fra at *brukerkode kjøres inne i orchestratorens egen prosess*. Airflows DAG-parsing i
scheduleren, Dagsters sensorfunksjoner i daemonen, Prefects flow-objekter i agenten. Derfra kommer
timeouts, OOM-krasj, versjonsskew, serialiseringsproblemer og "hvorfor forsvant DAG-en min".

> **Pulseqs arkitektoniske grunnregel #1: orchestratorprosessen kjører aldri brukerkode.
> Alt brukerdefinert er en subprosess med hard timeout og egen prosessgruppe.**

Dette gir språknøytralitet gratis (bash, python, go, hva som helst), fjerner hele SDK-flaten,
og gjør daemonen mulig å resonnere om.

---

## 1. Prior art — hva vi stjeler, hva vi unngår

### 1.1 Dagster (hovedreferansen)

| Stjel | Hvorfor |
|---|---|
| **Schedule = tidstrigger, sensor = hendelsestrigger** som separate førstegangsbegreper | Dette er delingen som gjør modellen forståelig. Prosjektbeskrivelsen bygger allerede på den. |
| **Tick som lagret objekt** med status `SUCCESS`/`SKIPPED`/`FAILURE`, run-IDer og skip-grunn (`dg api sensor get-ticks`) | Dette *er* observabiliteten. Uten tick-tabell finnes ikke "explain". |
| **`SkipReason` som førsteklasses returverdi** | "Ingenting skjedde" må være en forklart hendelse, ikke stillhet. Cron sin største synd. |
| **Cursor: ugjennomsiktig streng, eid av systemet, oppdatert etter at runs er opprettet** | Riktig transaksjonsrekkefølge. Sensoren skal ikke måtte skrive sin egen state. |
| **`run_key` for dedup** — én run per unik nøkkel per sensor | Gjør at-least-once-evaluering trygg. Multi-trigger per tick (`run_key = filnavn:mtime`) er akkurat riktig granularitet. |
| **`minimum_interval_seconds` per sensor** | Enkel, forståelig rate-kontroll uten cron-kompleksitet på sensorer. |
| **Chunking av tunge sensorer via cursor** (Dagsters eget råd mot timeout) | Kodifiseres som eksplisitt `max_triggers_per_tick`. |
| **Default status pause/running på definisjonsnivå** | Deploy uten å starte 40 schedules ved et uhell. |

| Unngå | Hvorfor |
|---|---|
| **60 s hard gRPC-timeout på sensorfunksjon som blokkerer daemonen** | Kjent produksjonsfeil: treg sensor stopper *hele* sensor-loopen. Pulseq: sensorer i subprosess, per-sensor timeout, evaluering i egen goroutine-pool, aldri på kritisk sti. |
| **`run_key` + cursor-reset-fella**: resetter du cursoren, men filene ikke endres, kjører ingenting — fordi run-nøklene er brukt. Dagsters eget råd er "ikke bruk run_key hvis du vil kunne resette". | Uakseptabel avveining. Pulseq løser dette med **dedup-epoch** (§4.3): `pulseq sensor reset` bumper epoken, og dedup-nøkkelen blir `(sensor, epoch, run_key)`. Både dedup *og* replay. |
| **Manglende dedup innenfor samme tick** (dagster#26753: to `RunRequest` med samme `run_key` i én evaluering gir to runs) | Pulseq: UNIQUE-indeks i DB gjør dette gratis og umulig å bomme på. |
| **To-stegs commit: launch runs, deretter skriv cursor** | Krasj imellom gir duplikater. Pulseq committer tick + cursor + dedup + køet run i **én** SQLite-transaksjon (§0/§4.4). |
| **Daemon som single point of failure uten replika** — daemon-krasj (OOM på stor asset-graf) stopper hele køprosesseringen | Pulseq: daemon er tynn og statsløs mellom ticks; all state i DB; restart rekonsilierer. Ingen in-memory kø. |
| **Asset-grafen og declarative automation** | Dagsters egen dokumentasjon anbefaler "bruk schedules og jobs så lenge de er nyttige, før du bytter til det mer komplekse systemet". Pulseq stopper der. Ingen asset keys, ingen partisjoner, ingen `AutomationCondition`-algebra. |
| **Catch-up som er inkonsistent** (partisjonerte schedules tar igjen, vanlige cron-schedules gjør det ikke) | Pulseq: én eksplisitt `catchup`-policy per schedule, samme semantikk overalt. |
| **DST/tidssone-avvik** (dagster#33448: tidssoner med minutt-offset gir avvikende tick-tider) | Pulseq: eksplisitt DST-policy per schedule + gullfil-tester (§8). |
| **Enterprise-låste basisfunksjoner (SSO m.m.)** | Pulseq er ett produkt, én lisens. Ingen open-core-splitt. |

### 1.2 Øvrige verktøy

| Verktøy | Stjel | Unngå |
|---|---|---|
| **Airflow** | Airflow 3s **Task Execution API**: tasks snakker aldri direkte med metadata-DB, bare via API. Riktig sikkerhets- og koblingsgrense. Airflow 3s **DAG-versjonering**: en run peker på den versjonen den faktisk kjørte. Eksplisitt, avgrenset `backfill`-kommando med egen concurrency. | **DAG-parsing i scheduleren** — tyngste operasjonen, gir import-timeouts, DAG-er som forsvinner og deretter dukker opp igjen, CPU-sult ved 1000+ DAG-er. Pulseq parser konfig **kun** ved `pulseq apply`, aldri per tick. **`catchup=True`-stormen**: 90 dagers historikk køes umiddelbart og flater workerne. Pulseq: `catchup: skip` er default. Top-level-kode med DB-kall ved parsing. |
| **Prefect** | Enkelheten i "flow = vanlig funksjon". `.serve()`-modellen for lav terskel. | Store bruddendringer mellom majorversjoner (agents → workers). Blokk-/work-pool-konfigurasjon som er vanskelig å forstå og dårlig dokumentert. Agenter som poller alle pooler og kjører feil deployments. Pulseq: konfigurasjonsmodellen skal få plass på én side. |
| **Temporal** | **Retry-policy som datastruktur** (initial interval, backoff-koeffisient, maks intervall, maks forsøk, ikke-retrybare feiltyper). **Heartbeat + heartbeat-timeout** for lange oppgaver. Grensen mellom "beslutning" og "sideeffekt". **Fencing** slik at en gammel worker ikke kan skrive resultat etter reclaim. Fullstendig hendelseshistorikk som primær datastruktur. | Determinisme/replay-modellen — krever et SDK, et sandkassemiljø og en helt egen mental modell. Operasjonell tyngde: fire tjenester + Cassandra/Postgres; ett rapportert tilfelle brukte åtte ingeniørmåneder i året bare på vedlikehold av selvhostet Temporal. Pulseqs steg er *ikke* deterministiske funksjoner; de er kommandoer med exit-koder. |
| **Windmill** | **Databasen som kø** — ingen Redis, ingen ekstern broker. Én binær, to roller (`MODE=server` / `MODE=worker`). Worker-grupper som lytter på tags. Rask kaldstart. | Overflaten: script-plattform + intern-app-bygger + PaaS. Pulseq gjør én ting. Ingen innebygd editor, ingen app-builder. |
| **Kestra** | Deklarativ YAML som primærgrensesnitt. Førsteklasses event-triggere. **Repository/queue som skiftbart grensesnitt** (memory/JDBC/Kafka) — gir en ryddig migrasjonsvei. | To helt ulike backend-arkitekturer (JDBC vs Kafka+Elasticsearch) som brukeren må velge mellom. 1200+ plugins å vedlikeholde. JVM-fotavtrykk. Pulseq: **ett** lagringsgrensesnitt, én implementasjon i MVP, ingen plugin-register. |
| **Cronicle** | Per-event: retry med delay, timeout, concurrency-limit, kø, **catch-up for tapte schedules**, tidssone per event. Chain-reaction (trigger neste ved suksess) som billig DAG. Live logg-visning. Per-jobb CPU/minne-grenser. | Primary/worker-topologi med UDP-autodiscovery og valgalgoritme — for mye maskineri for én maskin. Skiftbart storage-backend (S3/Couchbase/Redis/fil) som konfigvalg. Node.js-runtime. |
| **Dkron** | Fault tolerance uten ekstern koordinator: Raft + Serf innebygget, én binær. God påminnelse om at "distribuert" kan pakkes ganske stramt. | Raft/gossip er **ikke** MVP-materiale. Prosjektbeskrivelsen sier eksplisitt at distribuert konsensus ikke trengs. Å ta det inn tidlig kjøper kompleksitet mot en risiko som ikke finnes ennå. Dkron mangler dessuten steg/DAG og sensorer helt. |
| **ofelia** | Minimalisme: cron for Docker, ingen state, én binær, INI/label-konfig. Nyttig som nedre grense for "hvor lite kan dette være". | Ingen historikk, ingen retry, ingen DAG. Dette er *for* lite — det er nettopp gapet Pulseq fyller. |
| **GoCron (go-co-op/gocron v2)** | Rent API for in-process planlegging. Nyttig som *bibliotek* for interne periodiske oppgaver (vacuum, reaper), ikke som produktets scheduler. | Distributed locker (Redis/GORM) med dokumenterte designbegrensninger — best-effort låsing uten fencing. Pulseq eier sin egen tick-tabell; ikke deleger dette. |
| **River (Go + Postgres/SQLite)** | **Transaksjonell enqueue**: jobber committes sammen med applikasjonsdata, blir usynlige ved rollback. Nøyaktig samme grep som Pulseq trenger. Unike jobber (args/periode/kø/state). Durable periodic jobs. SQLite-driver finnes (`riverdriver/riversqlite`, fortsatt tidlig). | Ikke bruk River som Pulseqs kjerne — Pulseq trenger tick/cursor/lease-semantikk som ikke er River sin modell, og en avhengighet her binder datamodellen. **Stjel mønsteret, ikke biblioteket.** |
| **asynq (Redis)** | Polert CLI + web-UI over kø-state. God UX-referanse. | Redis-avhengighet og ingen transaksjonell garanti mot applikasjonsdata. Feil retning for et single-binary-produkt. |
| **systemd timers** | `Persistent=true` (ta igjen etter nedetid), journal-integrasjon, unit-nivå ressursgrenser (dette er gratis via cgroups). | Ingen varsling når en jobb *ikke* kjørte. Ingen historikk på tvers av maskiner. Ingen avhengigheter. Ingen sensorer. Bare Linux/systemd. — Pulseq bør posisjoneres som "systemd timers + sensors + små DAG-er + historikk", og bør *bruke* systemd (én service-unit) i stedet for å konkurrere med det. |

### 1.3 Tre lærdommer destillert

1. **Alt som kan kjøre bruker-definert logikk i orchestratorprosessen, vil før eller siden henge der.**
   (Airflow DAG-parsing, Dagster sensor-timeout, Prefect agent-OOM.)
2. **Alt som ikke committes transaksjonelt sammen med state, vil duplisere eller forsvinne ved krasj.**
   (Dagsters cursor-etter-launch, Cronicles filbaserte storage.)
3. **Alt som ikke har en lagret grunn, vil generere en supporthenvendelse.**
   ("Hvorfor kjørte den ikke?" er den vanligste orchestrator-spørsmålet i eksistens.)

---

## 2. Arkitektur

### 2.1 Prosesser og roller

Én binær, `pulseq`, tre roller:

```
pulseq serve      # daemon: scheduler-loop, sensor-loop, coordinator, reaper, HTTP API
pulseq worker     # executor: claimer runs, kjører steg som subprosesser
pulseq <cmd>      # CLI-klient mot HTTP API (unix socket default)
```

MVP kjører `pulseq serve --with-worker` — alt i én prosess, men **med samme interne grenser**
som i delt modus. Grensene er kode fra dag 1; utrullingen kan forenkles.

```
             ┌──────────────────────── pulseq serve ────────────────────────┐
             │                                                              │
  jobs.d/    │  ┌───────────┐   ┌────────────┐   ┌──────────────┐          │
  *.yaml ──► │  │ scheduler │   │  sensor    │   │ coordinator  │          │
  (apply)    │  │   loop    │   │   loop     │   │ (kø→claimbar)│          │
             │  └─────┬─────┘   └──────┬─────┘   └──────┬───────┘          │
             │        │ ticks          │ ticks          │ semaforer         │
             │        └────────┬───────┴────────────────┘                   │
             │                 ▼                                            │
             │        ┌──────────────────┐        ┌──────────┐             │
             │        │  store (SQLite)  │◄───────│  reaper  │             │
             │        │  1 skriver + N   │        │ (leases) │             │
             │        │  lesere, WAL     │        └──────────┘             │
             │        └────────┬─────────┘                                 │
             │                 │  HTTP API (unix socket / TCP)             │
             └─────────────────┼──────────────────────────────────────────┘
                               │
        ┌──────────────────────┼────────────────────────┐
        ▼                      ▼                        ▼
   pulseq worker          pulseq CLI              web-UI (fase 6)
        │
        ├─ step-subprosess (egen prosessgruppe, stdout/stderr → loggfil)
        └─ sensor-subprosess (JSON inn/ut, hard timeout)
```

### 2.2 Kontrollflyt

**Scheduler-loop** (hvert `tick_interval`, default 1 s):
1. Ta scheduler-lease (`BEGIN IMMEDIATE`, `lease` med `expires_at` + fencing token).
2. For hver aktiv schedule: beregn alle `scheduled_for` mellom `last_tick` og `now` fra cron/interval + IANA-sone.
3. Anvend `catchup`-policy → liste av ticks som skal materialiseres.
4. Én transaksjon per schedule: `INSERT INTO schedule_tick` (UNIQUE på `(schedule_id, scheduled_for)`),
   opprett `trigger` + `run` i status `queued`, oppdater `schedule.last_tick`.

**Sensor-loop** (per sensor, uavhengige goroutines, respekterer `min_interval`):
1. Ta sensor-lease.
2. Les cursor + epoch. Start subprosess med JSON på stdin, hard `timeout` (default 30 s), egen prosessgruppe.
3. Parse svar. Ved timeout/feil: tick = `FAILURE` med grunn, cursor **uendret**, backoff.
4. Én transaksjon: skriv `sensor_tick`, sett inn `trigger`-rader (dedup via UNIQUE `(sensor_id, epoch, run_key)`),
   opprett `run` for de som ikke ble deduplisert, oppdater cursor.

**Coordinator-loop:**
- Flytter `queued` → `claimable` når concurrency-betingelser er oppfylt (globalt, per job, per semafor, per `serial_key`).
- Avvisning skriver `event` med grunn (blir synlig i `explain`).

**Worker-loop:**
1. `BEGIN IMMEDIATE` → plukk eldste `claimable` run som matcher workerens `tags` → sett `running`,
   `claimed_by`, `lease_expires_at`, `fencing_token = fencing_token + 1`.
2. Kjør DAG topologisk, `max_parallel_steps` samtidig.
3. Hvert steg: subprosess i egen prosessgruppe (`Setpgid`), stdout/stderr → loggfil, timeout → SIGTERM til
   *hele gruppen* → grace → SIGKILL.
4. Heartbeat på run hvert `lease_ttl/3`. Alle skrivinger bærer fencing token; utdatert token ⇒ avvist.

**Reaper-loop:**
- Utløpte leases: run → `lost`, deretter `queued` (hvis attempts igjen) eller `failed`.
- Prosjektbeskrivelsens "rekonsiliering ved restart" er samme kodesti: ved oppstart er alt med utløpt lease foreldreløst.

### 2.3 Hva som bevisst *ikke* er i arkitekturen

- Ingen message broker (Redis/Kafka/NATS).
- Ingen Raft/gossip/leder-valg. Én daemon; lease-tabellen forhindrer dobbeltkjøring hvis to startes ved uhell.
- Ingen plugin-ABI, ingen Go `plugin`-pakke, ingen innebygd scripting-motor.
- Ingen executor-abstraksjon (Local/Celery/K8s). Steg er `argv`. Vil du ha container: `argv = ["docker","run",...]`.
- Ingen secrets-manager. Env fra fil med `0600`-rettigheter, maskering i logg via `secret: true`-felt.

---

## 3. Teknologivalg

| Valg | Begrunnelse | Vurdert alternativ |
|---|---|---|
| **Go 1.23+, én statisk binær** | Gitt av rammen. Cross-compile, ingen runtime å installere. | — |
| **SQLite, WAL** | Gitt av rammen. Én fil, transaksjonell, null drift. | Postgres — utsettes til multi-node faktisk trengs (§7, fase 8). |
| **`modernc.org/sqlite`** (ren Go) som default | Ingen cgo ⇒ enkel kryssbygging, ingen glibc-avhengighet, statisk binær. Ytelsestapet mot cgo er irrelevant for tick-volumet vårt (tusenvis av skrivinger/dag, ikke millioner/sek). | `mattn/go-sqlite3` bak build-tag `cgosqlite` for de som trenger maks skriveytelse. `ncruces/go-sqlite3` (wasm/wazero) er teknisk elegant, men høyere minnebruk per connection — hold som reserve. |
| **`database/sql` + `sqlc` for typede spørringer** | Genererer Go-kode fra ren SQL; ingen ORM-magi; SQL-en er lesbar og reviewbar. Bytte til Postgres senere = ny sqlc-dialekt, ikke omskriving. | GORM/ent — skjuler nettopp den SQL-en vi må ha kontroll på (`BEGIN IMMEDIATE`, `ON CONFLICT`, indeksbruk). |
| **`pressly/goose` for migrasjoner** | Enkle SQL-filer opp/ned, embeddes med `embed.FS`, kjøres automatisk ved oppstart. Kombineres rutinemessig med sqlc. | Atlas — mer maskineri enn et enfilprodukt trenger. |
| **Egen cron-parser (~300 linjer) + `time.LoadLocation`** | Cron+DST er der alle bommer: `robfig/cron` er i praksis uvedlikeholdt siden 2020 med kjente panic- og DST-problemer, og forkes derfor. Dette er kjernelogikk i produktet vårt — vi eier den, tester den med gullfiler, og støtter `@hourly`-aliaser + `L`/`#` bare hvis vi vil. | `adhocore/gronx` (null avhengigheter, rask) som referanse-implementasjon i differansetester. |
| **`log/slog` (stdlib), JSON-handler** | Strukturert logging uten avhengighet. Faste felt: `run_id`, `step`, `attempt`, `job`, `schedule_id`, `sensor`, `tick_id`, `cursor`. | zap/zerolog — unødvendig ytelse for denne loggmengden. |
| **`net/http` + `ServeMux` (Go 1.22-ruting)** | Stdlib holder for ~30 endepunkter. | chi/echo/gin — ingen av dem løser et problem vi har. |
| **`html/template` + htmx + `embed.FS` for UI** | Ingen node-byggesteg, UI ligger i binæren, fungerer uten JS-bundling. Ren lesevisning over data som allerede finnes. | React/SPA — legger til et helt byggesystem for et dashboard. |
| **YAML for jobbdefinisjoner (`gopkg.in/yaml.v3`), JSON Schema-validering** | Kestra viser at deklarativ YAML er en god primærflate. Validering ved `apply`, ikke ved kjøring. | HCL (færre kjenner det), CUE (kraftig, men bratt), Starlark (introduserer kodeutførelse — bryter grunnregel #1). |
| **Loggfiler på disk, ikke i DB** | §4.6. Høyvolum append må ikke dele skriveforbindelse med orkestreringsstate. | Egen `logs.db` — greit alternativ, men filer gir `tail -f`, `grep`, logrotate og zero-copy streaming gratis. |
| **Testing: `testing` + `testify/require` + gullfiler** | Deterministisk klokke er viktigere enn testrammeverk. | — |

---

## 4. Datamodell

Skjema-skisse (SQLite-dialekt, forenklet — indekser og sjekker vist der de bærer semantikk).

### 4.1 Definisjoner (skrives kun av `pulseq apply`)

```sql
CREATE TABLE job (
  id            TEXT PRIMARY KEY,          -- stabilt navn, f.eks. "nightly-report"
  current_version_id TEXT REFERENCES job_version(id),
  paused        INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL
);

-- Immutabel. Content-hash av normalisert spec. Airflow 3s DAG-versjonering, gratis fra dag 1.
CREATE TABLE job_version (
  id         TEXT PRIMARY KEY,             -- sha256 av spec_json
  job_id     TEXT NOT NULL REFERENCES job(id),
  spec_json  TEXT NOT NULL,                -- steg, avhengigheter, retry, timeouts, env
  applied_at INTEGER NOT NULL,
  applied_by TEXT
);

CREATE TABLE schedule (
  id             TEXT PRIMARY KEY,
  job_id         TEXT NOT NULL REFERENCES job(id),
  kind           TEXT NOT NULL,            -- 'cron' | 'interval' | 'calendar'
  expr           TEXT NOT NULL,            -- "0 3 * * *" | "15m"
  timezone       TEXT NOT NULL DEFAULT 'UTC',   -- IANA
  dst_policy     TEXT NOT NULL DEFAULT 'skip',  -- 'skip' | 'first' | 'both'
  catchup        TEXT NOT NULL DEFAULT 'skip',  -- 'skip' | 'last_only' | 'all'
  max_catchup    INTEGER NOT NULL DEFAULT 10,
  jitter_seconds INTEGER NOT NULL DEFAULT 0,
  params_json    TEXT,
  paused         INTEGER NOT NULL DEFAULT 0,
  last_tick_at   INTEGER
);

CREATE TABLE sensor (
  id                   TEXT PRIMARY KEY,
  job_id               TEXT NOT NULL REFERENCES job(id),
  kind                 TEXT NOT NULL,      -- 'exec' | 'file' | 'http' | 'sql' | 'webhook'
  config_json          TEXT NOT NULL,
  min_interval_seconds INTEGER NOT NULL DEFAULT 30,
  timeout_seconds      INTEGER NOT NULL DEFAULT 30,
  max_triggers_per_tick INTEGER NOT NULL DEFAULT 100,
  cursor               TEXT,               -- ugjennomsiktig for Pulseq
  dedup_epoch          INTEGER NOT NULL DEFAULT 0,   -- ⭐ løser Dagsters reset-felle
  dedup_retention_days INTEGER NOT NULL DEFAULT 30,
  paused               INTEGER NOT NULL DEFAULT 0,
  last_tick_at         INTEGER,
  consecutive_failures INTEGER NOT NULL DEFAULT 0
);
```

### 4.2 Ticks — beslutningshistorikken

```sql
CREATE TABLE tick (
  id           TEXT PRIMARY KEY,
  source_kind  TEXT NOT NULL,             -- 'schedule' | 'sensor' | 'manual' | 'backfill' | 'chain'
  source_id    TEXT NOT NULL,
  scheduled_for INTEGER,                  -- NULL for sensorer
  started_at   INTEGER NOT NULL,
  finished_at  INTEGER,
  status       TEXT NOT NULL,             -- 'success' | 'skipped' | 'failed'
  skip_reason  TEXT,                      -- ⭐ Dagsters SkipReason, alltid lagret
  error        TEXT,
  trigger_count INTEGER NOT NULL DEFAULT 0,
  cursor_before TEXT,
  cursor_after  TEXT
);
CREATE UNIQUE INDEX tick_schedule_slot
  ON tick(source_id, scheduled_for) WHERE source_kind = 'schedule';
```

`UNIQUE(source_id, scheduled_for)` er hele idempotens-garantien for schedules: en schedule kan ikke
produsere to ticks for samme sekund, uansett hvor mange ganger loopen kjører eller krasjer.

### 4.3 Triggere og dedup

```sql
CREATE TABLE trigger (
  id        TEXT PRIMARY KEY,
  tick_id   TEXT NOT NULL REFERENCES tick(id),
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  epoch     INTEGER NOT NULL DEFAULT 0,
  run_key   TEXT NOT NULL,                -- schedules: ISO-tid av scheduled_for
  params_json TEXT,
  run_id    TEXT REFERENCES run(id),      -- NULL = deduplisert bort
  dedup_of  TEXT REFERENCES trigger(id),
  created_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX trigger_dedup ON trigger(source_id, epoch, run_key);
```

**Dedup-epoch — Pulseqs forbedring over Dagster.** Dagster dokumenterer eksplisitt at hvis du både
bruker `run_key` og resetter cursoren, skjer ingenting, fordi run-nøklene allerede er brukt; rådet er
"ikke bruk run_key hvis du vil kunne resette". Det er en falsk avveining. I Pulseq:

- `pulseq sensor reset <navn>` → `dedup_epoch += 1`, cursor → NULL.
- `pulseq sensor reset <navn> --cursor <verdi>` → epoch += 1, cursor settes.
- `pulseq sensor replay <navn> --run-key <k>` → sletter én dedup-rad kirurgisk.

Du beholder dedup i normal drift *og* får ekte replay. Dedup-rader eldre enn
`dedup_retention_days` ryddes av vedlikeholdsjobben — med den dokumenterte konsekvensen at et
run_key som dukker opp igjen etter retention-vinduet vil trigge på nytt.

### 4.4 Runs, steg og forsøk

```sql
CREATE TABLE run (
  id             TEXT PRIMARY KEY,
  job_id         TEXT NOT NULL REFERENCES job(id),
  job_version_id TEXT NOT NULL REFERENCES job_version(id),  -- ⭐ runs er versjonsforankret
  trigger_id     TEXT REFERENCES trigger(id),
  status         TEXT NOT NULL,      -- queued|claimable|running|succeeded|failed|cancelled|lost|skipped
  params_json    TEXT,
  priority       INTEGER NOT NULL DEFAULT 0,
  serial_key     TEXT,               -- max én aktiv run per nøkkel
  concurrency_keys_json TEXT,        -- semaforer denne runen holder
  worker_tag     TEXT,
  claimed_by     TEXT,
  fencing_token  INTEGER NOT NULL DEFAULT 0,   -- ⭐ Temporal-mønster
  lease_expires_at INTEGER,
  queued_at INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER,
  attempt INTEGER NOT NULL DEFAULT 1,
  error TEXT
);
CREATE INDEX run_dispatch ON run(status, priority DESC, queued_at) WHERE status='claimable';
CREATE UNIQUE INDEX run_serial ON run(serial_key)
  WHERE serial_key IS NOT NULL AND status IN ('queued','claimable','running');

CREATE TABLE run_step (
  run_id TEXT NOT NULL REFERENCES run(id),
  name   TEXT NOT NULL,
  status TEXT NOT NULL,   -- pending|ready|running|succeeded|failed|skipped|upstream_failed|cached
  attempt INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 1,
  started_at INTEGER, finished_at INTEGER,
  exit_code INTEGER, error TEXT,
  PRIMARY KEY (run_id, name)
);

CREATE TABLE step_attempt (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL, step_name TEXT NOT NULL, attempt INTEGER NOT NULL,
  started_at INTEGER NOT NULL, finished_at INTEGER,
  exit_code INTEGER, signal TEXT, timed_out INTEGER NOT NULL DEFAULT 0,
  pid INTEGER,
  log_path TEXT, log_bytes INTEGER,        -- ⭐ referanse, ikke innhold
  next_retry_at INTEGER,
  UNIQUE (run_id, step_name, attempt)
);

CREATE TABLE artifact (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL, step_name TEXT NOT NULL,
  name TEXT NOT NULL, uri TEXT NOT NULL,   -- file://, s3://, postgres://tabell, http://
  media_type TEXT, size_bytes INTEGER, checksum TEXT,
  metadata_json TEXT, created_at INTEGER NOT NULL
);
```

### 4.5 Koordinering og hendelser

```sql
CREATE TABLE semaphore (
  key TEXT PRIMARY KEY, limit_n INTEGER NOT NULL, held INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE lease (             -- scheduler, sensor-loop, coordinator, reaper
  name TEXT PRIMARY KEY, holder TEXT NOT NULL,
  fencing_token INTEGER NOT NULL, expires_at INTEGER NOT NULL
);

-- Alle beslutninger. Dette er "explain"-datakilden.
CREATE TABLE event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at INTEGER NOT NULL,
  kind TEXT NOT NULL,            -- tick_created|trigger_deduped|run_queued|run_blocked|
                                 -- step_retry_scheduled|lease_expired|run_reaped|...
  run_id TEXT, tick_id TEXT, source_id TEXT, step_name TEXT,
  reason TEXT NOT NULL,          -- menneskelesbar, alltid utfylt
  detail_json TEXT
);
CREATE INDEX event_run ON event(run_id, at);
CREATE INDEX event_source ON event(source_id, at);
```

### 4.6 SQLite-strategien (den kritiske delen)

SQLite i WAL-modus: mange samtidige lesere, **nøyaktig én skriver**. Naiv bruk av `database/sql`
gir `SQLITE_BUSY` selv med `busy_timeout` satt, fordi en lesetransaksjon som oppgraderes til skriv
feiler umiddelbart hvis noen andre har skrevet i mellomtiden.

**Løsning — to pooler:**

```go
// Skriver: nøyaktig én forbindelse. Serialisering skjer i database/sql sin egen kø.
writeDB.SetMaxOpenConns(1)
writeDB.SetMaxIdleConns(1)
writeDB.SetConnMaxLifetime(0)

// Lesere: N forbindelser, WAL gjør at de aldri blokkerer skriveren.
readDB.SetMaxOpenConns(runtime.NumCPU() * 2)
```

DSN-pragmaer for begge:
`_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL&_foreign_keys=on&_txlock=immediate`

**Alle skrivetransaksjoner starter med `BEGIN IMMEDIATE`** (via `_txlock=immediate`). Med
`MaxOpenConns(1)` er dette strengt tatt overflødig, men det gjør invarianten eksplisitt og
overlever en fremtidig refaktorering.

**Filoppdeling — "flere SQLite-filer" i praksis:**

| Data | Hvor | Hvorfor |
|---|---|---|
| Definisjoner, ticks, triggere, runs, steg, leases, events | `pulseq.db` | Må være transaksjonelt konsistente med hverandre. |
| **Steg-logger** | `logs/<yyyy>/<mm>/<run_id>/<step>.<attempt>.log` på disk | Høyvolum append. Ville sultet den ene skriveren. Gir `tail -f`, `grep`, logrotate gratis. DB holder bare `log_path` + `log_bytes`. |
| Historikk eldre enn `retention` | `history.db` (fase 7) | Holder hoved-DB liten så indeksene får plass i page cache. |
| Metrics-tidsserier | ikke lagret; eksponeres via `/metrics` | Prometheus eier tidsserier, ikke oss. |

**Skrivebudsjett-regel:** ingen kodesti skriver mer enn O(1) rader per steg-attempt. Heartbeats er en
`UPDATE` av én kolonne hvert `lease_ttl/3`; ved 100 samtidige runs og 10 s intervall er det 10 skrivinger/s —
langt under det én SQLite-skriver takler i WAL. Logglinjer er per definisjon utenfor dette budsjettet.

**Postgres-veien holdes åpen billig:** all SQL bak et `Store`-interface, generert av sqlc.
Ingen SQLite-spesifikk syntaks i kjernen (bruk `ON CONFLICT ... DO NOTHING`, ikke `INSERT OR IGNORE`;
`strftime`/tidsberegninger skjer i Go, ikke i SQL). Prisen er lav nå; å rette det senere er dyrt.

---

## 5. Kontrakter

### 5.1 Jobbdefinisjon (YAML)

```yaml
job: nightly-report
description: Genererer og publiserer nattlig rapport
env_file: /etc/pulseq/secrets/report.env
defaults:
  timeout: 30m
  retry: { max_attempts: 3, initial_backoff: 10s, multiplier: 2, max_backoff: 5m, jitter: 0.2 }

steps:
  - name: extract
    run: ["/opt/etl/extract.sh", "{{.params.date}}"]
  - name: transform
    run: ["/opt/etl/transform.py"]
    needs: [extract]
    retry: { max_attempts: 5, retry_on_exit_codes: [75] }   # EX_TEMPFAIL
  - name: publish
    run: ["/opt/etl/publish.sh"]
    needs: [transform]
    timeout: 10m

concurrency:
  serial_key: "nightly-report"        # aldri to samtidige
  semaphores: ["warehouse"]           # deler en global grense
  on_conflict: skip                   # queue | skip | replace

schedules:
  - id: nightly
    cron: "0 3 * * *"
    timezone: Europe/Oslo
    catchup: last_only
    params: { date: "{{.tick.date}}" }

sensors:
  - id: new-source-file
    kind: file
    config: { path: /srv/incoming, glob: "*.csv", watch: mtime }
    min_interval: 60s
    params: { file: "{{.trigger.path}}" }
```

### 5.2 Sensor-protokoll (`kind: exec`)

Stdin (JSON):
```json
{"sensor":"new-source-file","tick_id":"...","cursor":"1732012800.0",
 "last_tick_at":1732012800,"deadline_unix":1732012830,"max_triggers":100}
```

Stdout (JSON, siste linje):
```json
{"cursor":"1732099200.0",
 "triggers":[{"run_key":"data-2024-11-20.csv:1732099200","params":{"file":"/srv/incoming/data-2024-11-20.csv"}}],
 "skip_reason":null}
```

Regler, med Dagsters arr innbakt:
- Exit ≠ 0 eller ugyldig JSON ⇒ tick = `failed`, **cursor uendret**, eksponentiell backoff på sensoren,
  varsel etter `consecutive_failures >= N`.
- Timeout ⇒ SIGTERM til hele prosessgruppen, deretter SIGKILL. Tick = `failed`. Aldri blokkering av
  andre sensorer (dette er Dagsters 60 s-daemon-blokkering, unngått ved konstruksjon).
- Tom `triggers` + `skip_reason` ⇒ tick = `skipped`, grunnen lagres og vises i `explain`.
- Cursor er **ugjennomsiktig** for Pulseq. Aldri tolket, aldri validert utover å være en streng.
- `> max_triggers` ⇒ ta de første N, sett cursor til N-te, skriv event `truncated`, kjør sensoren
  igjen umiddelbart (chunking, som Dagster anbefaler manuelt — vi automatiserer det).
- **Cursor skrives i samme transaksjon som triggerne og de køede runsene.** Ikke etterpå.

### 5.3 Innebygde sensorer (dekker ~80 % uten kode)

| kind | Config | Cursor |
|---|---|---|
| `file` | `path`, `glob`, `watch: mtime\|size\|checksum`, `stable_for` | høyeste sette mtime |
| `http` | `url`, `method`, `headers`, `jsonpath`, `mode: etag\|hash\|jsonpath_gt` | ETag / innholdshash / siste verdi |
| `sql` | `dsn`, `query` (med `:cursor`), `key_column`, `cursor_column` | siste `cursor_column`-verdi |
| `webhook` | `token`, `path` — `POST /hooks/<id>` fyller en kø-tabell | siste konsumerte hendelses-ID |

`stable_for` på `file` er et arr fra virkeligheten: uten det trigger du på halvopplastede filer.

### 5.4 Steg-kontrakt

Miljø gitt til hvert steg:
`PULSEQ_RUN_ID`, `PULSEQ_JOB`, `PULSEQ_JOB_VERSION`, `PULSEQ_STEP`, `PULSEQ_ATTEMPT`,
`PULSEQ_TRIGGER_KEY`, `PULSEQ_SCHEDULED_FOR`, `PULSEQ_PARAMS` (JSON),
`PULSEQ_ARTIFACTS` (skrivbar sti — JSON-linjer inn her blir `artifact`-rader),
`PULSEQ_INPUTS` (JSON: artefakter fra oppstrøms steg).

Exit-koder: `0` = suksess. `75` (EX_TEMPFAIL) = retrybar uansett policy. `> 128` = signal.
Konfigurerbar `retry_on_exit_codes` / `fail_fast_exit_codes`.

---

## 6. MVP-kutt — eksplisitt

**Med i MVP (v0.1):** cron/interval-schedules med tidssone og catchup-policy · sensorer (exec + file)
med cursor og run_key-dedup · run-historikk med tick- og skip-grunner · DAG av steg med `needs` ·
retry per steg med backoff · lease + reaper + rekonsiliering ved restart · `serial_key` og global
concurrency · CLI: `apply/list/show/run/pause/resume/cancel/logs/explain/preview/replay` ·
strukturert JSON-logging · HTTP API over unix socket.

**Kuttet, med begrunnelse:**

| Kuttet | Hvorfor |
|---|---|
| Web-UI | CLI + API dekker MVP-behovet. UI uten stabilt API blir omskrevet. Fase 6. |
| Postgres / multi-node | Prosjektbeskrivelsen sier eksplisitt at distribuert konsensus ikke trengs. Én maskin dekker målgruppen. `Store`-interfacet holder døren åpen. |
| Dynamic fan-out (`map` over sensor-output) | Krever dynamisk DAG-materialisering og endrer run_step fra statisk til dynamisk. Reelt nyttig, men fase 7. |
| Artifact **lineage** (graf) | Artefakt-*referanser* er med (billig). Grafen/spørrespråket er ikke. Dette er inngangsporten til Dagsters asset-modell — hold den lukket. |
| Varsling (e-post/Slack/webhook) | Fase 6. Inntil da: `on_failure`-steg i jobben selv. Ingen grunn til å bygge en notifikasjonsplattform. |
| Auth/RBAC | Unix socket + filrettigheter. TCP + token først i fase 6. |
| Container-executor | `argv = ["docker","run",...]` fungerer i dag. Vi bygger ikke om ofelia. |
| Secrets-backend (Vault/SOPS) | `env_file` med `0600`. Integrasjon = et wrapper-script. |
| Kalenderregler (helligdager, "siste virkedag") | Krever kalenderdata og fører rett til Airflows `Timetable`-plugin-abstraksjon. Fase 7, som ren datafil. |
| Backfill-UI/-planlegger | CLI `pulseq backfill --from --to --max-parallel` i fase 6. Med bevisst grense — Airflows catchup-storm skal ikke reproduseres. |
| Plugin-API | Bevisst permanent kutt. Utvidbarhet = subprosesser. |

---

## 7. Faseinndeling — fra tomt repo til ferdig produkt

Hver fase avsluttes med en demonstrerbar tilstand og eksplisitte akseptansekriterier.

### Fase 0 — Fundament (≈1 uke)
Repo-oppsett (`cmd/pulseq`, `internal/{store,scheduler,sensor,worker,api,cli,cron,clock}`).
goose-migrasjoner embeddet. sqlc-oppsett. **`clock.Clock`-interface med `RealClock` og `FakeClock` — dette
er det første som skrives.** Store-interface + SQLite-implementasjon med to pooler. CI: build, vet,
staticcheck, race-detector, cross-compile amd64/arm64.

*Akseptanse:* `pulseq version` bygger som statisk binær; migrasjoner kjører opp og ned; en test som
kjører 1000 samtidige skrivinger uten `SQLITE_BUSY`.

### Fase 1 — Jobber, steg, worker (≈2 uker)
YAML-parsing + validering + content-hash → `job_version`. `pulseq apply`. Manuell run:
`pulseq run <job> --param k=v`. Worker-loop med claim, sekvensielle steg, subprosess med prosessgruppe,
loggfil per attempt, timeout med SIGTERM→SIGKILL. `pulseq logs -f`.

*Akseptanse:* trestegsjobb kjører; `kill -9` på worker etterlater ingen foreldreløse barnebarn-prosesser;
logger er komplette på disk.

### Fase 2 — Schedules (≈2 uker)
Egen cron-parser. `time.LoadLocation`. Tick-materialisering med UNIQUE-constraint. Catchup-policyer
(`skip`/`last_only`/`all` + `max_catchup`). DST-policy. Jitter. Pause/resume.
`pulseq preview schedule <id> --for 30d`.

*Akseptanse:* gullfil-tester for DST-overgang i `Europe/Oslo` (både vårens manglende time og
høstens doble); daemon drept i 6 timer og restartet produserer nøyaktig det `catchup`-policyen sier;
differansetest mot `adhocore/gronx` over 10 000 tilfeldige uttrykk.

### Fase 3 — Sensorer (≈2 uker)
Sensor-loop med per-sensor goroutine og `min_interval`. Exec-protokoll (§5.2). Cursor + dedup-epoch
+ `trigger`-UNIQUE. Innebygd `file`-sensor. Backoff ved gjentatte feil. Chunking ved `max_triggers`.
`pulseq sensor reset|replay|test`.

*Akseptanse:* sensor som slippes 50 filer produserer nøyaktig 50 runs; kjøres den igjen: 0 nye runs;
`reset` gir 50 nye runs; SIGKILL av daemon midt i sensor-commit gir aldri dupliserte runs (chaos-test).

### Fase 4 — DAG, retry, robusthet (≈2 uker)
`needs`-avhengigheter, topologisk kjøring, `max_parallel_steps`, `upstream_failed`-propagering.
Retry-policy som datastruktur (Temporal-stil). Lease + fencing token + heartbeat + reaper.
Rekonsiliering ved oppstart. `pulseq run --resume <run_id>` (kjør bare feilede/ikke-kjørte steg) og
`--only <steg>`.

*Akseptanse:* diamant-DAG kjører B og C parallelt; `SIGKILL` av worker gir run i `lost` innen
`lease_ttl`, deretter re-kjøring; gammel worker som våkner opp igjen får skrivingen avvist på fencing token.

### Fase 5 — Concurrency og forklarbarhet (≈1,5 uker)
Semaforer, `serial_key`, `on_conflict`-policyer, prioritet. `event`-tabellen fylles fra alle
beslutningspunkter. **`pulseq explain`** for schedule/sensor/run/step. `pulseq why-not <job>`.
HTTP API ferdigstilles (CLI går utelukkende via API — Airflow 3s lærdom).

*Akseptanse:* for hvert mulig "kjørte ikke"-scenario finnes en lagret, menneskelesbar grunn som
`explain` viser. Dette testes som en sjekkliste, ikke som en påstand.

### Fase 6 — Drift og UI (≈2 uker)
Read-only web-UI (`html/template` + htmx + `embed.FS`): schedules med neste tick, sensorer med
siste ticks og skip-grunner, run-liste med filter, run-detalj med DAG og logg-streaming.
`/metrics` (Prometheus). Varsling (`on_failure`-hooks: exec/webhook). `pulseq backfill --from --to
--max-parallel`. Retention + `VACUUM`-vedlikeholdsjobb. systemd-unit, deb/rpm, Docker-image.

*Akseptanse:* fersk installasjon til første kjørende jobb på under 5 minutter med bare
`apt install` + én YAML-fil.

### Fase 7 — Utvidelser etter faktisk etterspørsel (åpen)
Innebygde `http`/`sql`/`webhook`-sensorer · dynamic fan-out · kalenderregler fra datafil ·
artefakt-lineage-visning · `history.db`-arkivering · chain-triggere (jobb A ferdig ⇒ jobb B, Cronicle-stil).

### Fase 8 — Multi-node (kun hvis reelt behov oppstår)
Postgres-driver via sqlc-dialekt. Fjern `MaxOpenConns(1)`-antakelsen bak `Store`. Flere workers på
tvers av maskiner over HTTP API (workere har allerede aldri DB-tilgang — det er fase 5-designet).
Scheduler forblir én aktiv instans via lease-tabellen. **Ingen Raft.**

---

## 8. Testing (arret fra alle tidligere scheduler-prosjekter)

1. **Deterministisk klokke fra dag 0.** Alle tidsavhengige komponenter tar `clock.Clock`.
   Ingen `time.Now()` utenfor `RealClock`. Uten dette blir schedule-testene flaky og deretter slettet.
2. **Gullfil-tester for cron × tidssone × DST.** Input: uttrykk, sone, starttid, N. Output: N neste tider.
   Fasit sjekkes inn. Endres den, må endringen begrunnes i commit-meldingen.
3. **Differansetest** mot en uavhengig cron-implementasjon over tilfeldige uttrykk.
4. **Chaos-tester:** `SIGKILL` av daemon i hvert commit-punkt (før/etter tick-insert, før/etter
   cursor-write, før/etter run-insert). Invariant: aldri dupliserte runs, aldri tapte cursor-fremskritt
   uten korresponderende tick-failure.
5. **Fencing-test:** to workers hevder samme run; den med lavere token må avvises på alle skrivinger.
6. **Prosesstre-test:** steg som starter barnebarn; verifiser at hele gruppen dør ved timeout og cancel.
7. **Skriveproptest:** 100 samtidige runs med heartbeats i 5 minutter; ingen `SQLITE_BUSY`,
   p99 på scheduler-tick under 50 ms.
8. **Explain-sjekkliste:** hvert "kjørte ikke"-scenario må ha en test som asserterer at grunnen er lagret.

---

## 9. Risikoer

| Risiko | Sannsynlighet | Konsekvens | Mottiltak |
|---|---|---|---|
| **SQLite-skrivekontensjon under last** | Middels | Trege beslutninger, `SQLITE_BUSY` | To pooler, `MaxOpenConns(1)`, `BEGIN IMMEDIATE`, logger utenfor DB, skrivebudsjett-regel (§4.6), lastetest i fase 0 og fase 5 |
| **Cron/DST-feil** | **Høy** — alle bommer her | Jobber kjører dobbelt eller aldri ved tidsomstilling | Egen parser (ikke uvedlikeholdt bibliotek), eksplisitt `dst_policy`, gullfiler, differansetest |
| **Sensor-subprosess-kostnad** (fork/exec per tick per sensor) | Middels | CPU-sult ved mange sensorer med lav `min_interval` | Innebygde sensorer for vanlige tilfeller (ingen fork), minimum `min_interval` håndhevet, samlet worker-pool med grense, dokumentert kapasitet |
| **Loggvolum sprenger disk** | Høy over tid | Full disk stopper alt | Per-attempt cap (`max_log_bytes`, truncate midt-på med markør), retention-jobb, `df`-sjekk i helsestatus |
| **Scope creep mot asset-graf** | **Høy** — det er dit alle slike prosjekter driver | Pulseq blir Dagster, bare dårligere | Artefakter er *referanser*, aldri en graf. Skriftlig ikke-mål. Enhver PR som introduserer "asset key" avvises. |
| **Foreldreløse prosesser overlever timeout** | Høy uten tiltak | Ressurslekkasje, dobbeltkjøring | `Setpgid` + drep prosessgruppe, verifisert i test fra fase 1 |
| **Fencing-hull: gammel worker skriver etter reclaim** | Middels | Korrupt run-state | Monotont fencing token på run, alle skrivinger betinget på token |
| **Deduplikasjons-oppblåsing** (millioner av run_keys) | Middels | Indeksen vokser ubegrenset | `dedup_retention_days` + dokumentert konsekvens av retention |
| **Cross-compile brekker på cgo** | Lav med `modernc.org/sqlite` | Kan ikke levere statisk binær | Ren Go som default; cgo-driver bak build-tag; CI bygger begge |
| **Holdbarhet ved strømbrudd** (`synchronous=NORMAL` kan miste siste transaksjoner ved OS-krasj) | Lav | Tapt tick eller run | Dokumentert avveining; `synchronous=FULL` som konfigvalg; ticks er uansett idempotente ved reprise |
| **Konfig-drift mellom YAML på disk og DB** | Middels | Forvirring om hva som faktisk kjører | `job_version`-hash vises i `pulseq status`; `pulseq diff` sammenligner disk mot DB; runs peker på versjonen de kjørte |
| **Bikeshedding rundt YAML-skjemaet** | Høy | Tapt tid | Skjemaet fryses etter fase 1; endringer krever migrasjonsnotat |

---

## 10. Ikke-mål (skriftlig, for å kunne avvise PR-er)

Asset-graf og materialiseringsstatus · deklarative automasjonsbetingelser · datakvalitetssjekker ·
innebygd transformasjonsmotor · plugin-marked · flerbruker-RBAC · innebygd secrets-manager ·
distribuert konsensus · innebygd container-runtime · en Python/Go-SDK for å definere jobber i kode.

Alt over har en billig utvei: **det er en subprosess.**

---

## 11. Definisjon av ferdig (v1.0)

- Én statisk binær, < 25 MB, ingen runtime-avhengigheter.
- Installasjon til første kjørende schedule: under 5 minutter.
- Ett kommando svarer alltid på "hvorfor kjørte ikke X": `pulseq explain`.
- Krasj i ethvert commit-punkt gir aldri dupliserte runs.
- 100 samtidige runs på én VM uten `SQLITE_BUSY`.
- Konfigurasjonsreferansen får plass på én skjermside.
- Ingen brukerkode har noensinne kjørt inne i daemon-prosessen.
