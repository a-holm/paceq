# Pulseq — prosjektplan sett fra drift og observabilitet

**Planleggerperspektiv:** SRE. Alt i denne planen er utledet av ett krav:

> **Kjerneløftet:** Spørsmålet "hvorfor kjørte ikke jobben min?" skal alltid kunne besvares på sekunder, av én person, med én kommando, uten å lese kildekode og uten at daemonen kjører.

Alt annet — schedules, sensorer, DAG-er — er implementasjonsdetaljer som må underordne seg dette. Planen er selvstendig: den beskriver hele produktet fra tom katalog til driftet tjeneste, men prioriterer der observabilitet står på spill.

---

## 1. Problemanalyse: de fem stille feilene

Mellom "det er 02:00" og "prosessen kjørte" er det fem ledd. Hvert ledd kan svikte **stille** — resultatet er identisk (ingenting skjedde), men årsaken og botemidlet er helt forskjellige:

| # | Ledd | Stille feil | Hva operatøren ser i cron/systemd i dag |
|---|------|-------------|------------------------------------------|
| 1 | Evaluering | Tidspunktet ble aldri evaluert (daemon nede, lease tapt, klokkehopp) | Ingenting |
| 2 | Beslutning | Evaluert, men hoppet over (pauset, overlap, concurrency, sensor fant ingenting) | Ingenting |
| 3 | Trigger | Trigger laget, men deduplisert eller avvist (`run_key` sett før, ukjent job) | Ingenting |
| 4 | Kø | Run køet, men aldri plukket (concurrency-tak, worker død, lease) | "Kjører" i evig tid |
| 5 | Utføring | Steg feilet, ble hoppet over, timet ut, eller kommandoen fantes ikke | Kanskje en exit-kode |

**Konklusjon som styrer hele designet:** *Fravær av en run er ikke data.* Systemet må skrive en rad hver gang det bestemmer seg for å **ikke** gjøre noe. Observabilitet i Pulseq er derfor ikke logging som legges på til slutt — det er datamodellen for beslutninger, og den bygges før utføringsmotoren.

### 1.1 Fire prinsipper

1. **Beslutninger persisteres, ikke bare utfall.** Hver evaluering av en schedule eller sensor gir en `tick`-rad med status og maskinlesbar årsakskode.
2. **Én beslutningsmotor for fortid og fremtid.** `Decide()` er en ren funksjon uten I/O. Daemonen kaller den og lagrer resultatet; `pulseq preview` kaller den med simulert tid og lagrer ingenting. Forklaring og virkelighet kan aldri divergere.
3. **Årsaker er data, ikke prosa.** `(reason_code, reason_data JSON, reason_text)`. Koden driver metrics og alarmer, teksten driver mennesker, dataen driver tiltaksforslag.
4. **Observabilitetsdata må ikke sulte kontrollplanet.** SQLite har én skriver. Høyfrekvent lavverdi-data (stdout fra steg) skal aldri gjennom samme skrivevei som beslutningsdata.

### 1.2 Definisjon: hva er en "tick"?

Kritisk avklaring som ellers sprenger historikken. Scheduler-loopen våkner hvert sekund — det er **ikke** ticks.

> **Tick = én evaluering som var forfalt (due).** For en schedule: ett cron-/intervalltidspunkt. For en sensor: ett polling-intervall.

Det finnes derfor ingen `TICK_SKIPPED_NOT_DUE`. Loopen som konstaterer "ingenting er forfalt" skriver ingenting.

### 1.3 Koalesering av repeterte skips

En sensor med 30 s intervall gir 2 880 ticks/døgn, de fleste identiske ("ingen nye filer"). Det er både volum og støy.

**Løsning:** hvis forrige tick for samme `(instigator, name)` har identisk `status` + `reason_code` og produserte null triggere, inkrementeres `repeat_count` og `last_started_at` oppdateres i stedet for INSERT. "Sensoren har hoppet over 2 400 ganger siden 03:12 med grunn X" blir én lesbar rad og én skriveoperasjon i minuttet i stedet for 120.

Konfigurerbart (`coalesce_skips`, default `true`). Feil, triggere og statusendringer koalesceres aldri.

---

## 2. Observabilitetsmodellen

Fem lag, hvert med en kommando som besvarer det:

| Lag | Spørsmål | Kommando | Lagring |
|-----|----------|----------|---------|
| Beslutning | Ble tidspunktet evaluert, og hva ble bestemt? | `pulseq explain schedule\|sensor` | `ticks` |
| Utløsning | Ble det laget en trigger, og overlevde den dedup? | `pulseq explain trigger` | `triggers` |
| Kjøring | Hvor står runen, og hvorfor? | `pulseq explain run` | `runs`, `step_attempts` |
| Utdata | Hva skrev prosessen? | `pulseq logs <run> [--step] [-f]` | NDJSON-filer + `error_tail` i DB |
| Aggregat | Er dette et mønster? | `/metrics`, `pulseq status` | Prometheus |

Og på tvers: **`pulseq explain job <navn>`** som setter sammen hele kjeden bakover i tid. Det er produktets signaturkommando.

### 2.1 Årsakskodekatalog

Stabilt strengenum. Koder gjenbrukes aldri med ny betydning. Hver kode har i kildekoden: kort tekst, lang forklaring, og **tiltaksforslag** — mappingen `reason_code → remediation hint` er det billigste enkelttiltaket i hele planen.

**Tick-nivå**
```
TICK_SKIPPED_PAUSED              schedule/sensor er pauset
TICK_SKIPPED_OVERLAP             forrige run kjører fortsatt
TICK_SKIPPED_CONCURRENCY         tak nådd (job / global / navngitt kø)
TICK_SKIPPED_WINDOW              utenfor tillatt kjørevindu
TICK_SKIPPED_CATCHUP_DISABLED    forfalt tick forkastet, catch_up=false
TICK_SKIPPED_SENSOR              sensor returnerte skip (sensorens egen tekst i reason_text)
TICK_ERROR_SENSOR_FAILED         sensor returnerte feil / panic
TICK_ERROR_SENSOR_TIMEOUT        sensor overskred evalueringsfrist
TICK_ERROR_CONFIG                ugyldig cron/tidssone/jobbreferanse
TICK_MISSED_DAEMON_DOWN          syntetisk, laget av gap-deteksjon ved oppstart
TICK_MISSED_LEASE_LOST           en annen instans holdt leasen
TICK_MISSED_CLOCK_JUMP           klokken hoppet forbi tidspunktet
```

**Trigger-nivå**
```
TRIGGER_ACCEPTED                 → run opprettet
TRIGGER_DEDUPED_RUN_KEY          run_key allerede sett (peker til original run)
TRIGGER_REJECTED_JOB_UNKNOWN
TRIGGER_REJECTED_JOB_DISABLED
TRIGGER_REJECTED_PAYLOAD         payload validerte ikke mot jobbens skjema
```

**Run-nivå**
```
RUN_QUEUED_CONCURRENCY_JOB / _GLOBAL / _QUEUE
RUN_CANCELLED_SUPERSEDED         nyere run for samme nøkkel tok over
RUN_CANCELLED_MANUAL
RUN_TIMED_OUT
RUN_FAILED_STEP                  (reason_data peker på step + attempt)
RUN_ORPHANED_RECONCILED          markert kjørende, men prosessen fantes ikke etter restart
```

**Step-nivå**
```
STEP_SKIPPED_UPSTREAM_FAILED
STEP_SKIPPED_UPSTREAM_SKIPPED
STEP_SKIPPED_CONDITION_FALSE
STEP_RETRY_SCHEDULED             (reason_data: next_retry_at, attempt, backoff)
STEP_RETRIES_EXHAUSTED
STEP_FAILED_NONZERO_EXIT
STEP_FAILED_TIMEOUT
STEP_FAILED_SPAWN                kommando finnes ikke / permission denied / cwd mangler
STEP_FAILED_SIGNAL               drept av signal (inkl. OOM-killer)
```

`STEP_FAILED_SPAWN` og `STEP_FAILED_SIGNAL` fortjener egne koder fordi de i praksis er de vanligste og mest forvirrende feilene, og fordi tiltaket er helt forskjellig fra en vanlig exit≠0.

**Regel håndhevet i CI:** ingen terminal tilstand (`skipped`, `error`, `failed`, `missed`, `deduped`, `rejected`) får lagres med `reason_code IS NULL` eller `= 'UNKNOWN'`. En test iterer over alle tilstandsoverganger i koden og feiler ellers. Uten dette degenererer katalogen til `UNKNOWN` i løpet av et halvår.

---

## 3. Datamodell

Alle tider er `INTEGER` = Unix-epoch millisekunder i UTC. Tidssone er en egenskap ved schedulen, aldri ved lagringen. ID-er er ULID (26 tegn, tidssortert, kopivennlig) — `oklog/ulid/v2`.

### 3.1 Kontrollplan (`pulseq.db`)

```sql
-- Én rad per forfalt evaluering av en schedule eller sensor.
CREATE TABLE ticks (
  id                TEXT PRIMARY KEY,          -- ULID
  instigator        TEXT NOT NULL,             -- 'schedule' | 'sensor'
  name              TEXT NOT NULL,
  scheduled_at      INTEGER,                   -- planlagt tid (NULL for sensor)
  started_at        INTEGER NOT NULL,
  last_started_at   INTEGER NOT NULL,          -- = started_at hvis ikke koalescert
  finished_at       INTEGER,
  repeat_count      INTEGER NOT NULL DEFAULT 1,
  status            TEXT NOT NULL,             -- started|triggered|skipped|error|missed
  reason_code       TEXT,
  reason_text       TEXT,
  reason_data       TEXT,                      -- JSON
  trigger_count     INTEGER NOT NULL DEFAULT 0,
  cursor_before     TEXT,
  cursor_after      TEXT,
  duration_ms       INTEGER,
  daemon_session_id TEXT NOT NULL
);
CREATE INDEX ticks_by_name  ON ticks(instigator, name, started_at DESC);
CREATE INDEX ticks_by_time  ON ticks(started_at DESC);
CREATE INDEX ticks_bad      ON ticks(started_at DESC) WHERE status IN ('error','missed');

-- Én rad per utløst enhet. Dedup er en DB-invariant, ikke kodelogikk.
CREATE TABLE triggers (
  id          TEXT PRIMARY KEY,
  tick_id     TEXT NOT NULL REFERENCES ticks(id) ON DELETE CASCADE,
  job         TEXT NOT NULL,
  run_key     TEXT,
  payload     TEXT,                            -- JSON
  created_at  INTEGER NOT NULL,
  outcome     TEXT NOT NULL,                   -- accepted|deduped|rejected
  reason_code TEXT,
  reason_text TEXT,
  run_id      TEXT,                            -- accepted: ny run. deduped: original run.
  FOREIGN KEY (run_id) REFERENCES runs(id)
);
CREATE UNIQUE INDEX triggers_run_key
  ON triggers(job, run_key)
  WHERE run_key IS NOT NULL AND outcome = 'accepted';
CREATE INDEX triggers_by_job ON triggers(job, created_at DESC);

CREATE TABLE runs (
  id           TEXT PRIMARY KEY,
  job          TEXT NOT NULL,
  trigger_id   TEXT REFERENCES triggers(id),
  origin       TEXT NOT NULL,                  -- schedule|sensor|manual|retry|backfill
  status       TEXT NOT NULL,                  -- queued|running|succeeded|failed|cancelled|timed_out
  queued_at    INTEGER NOT NULL,
  started_at   INTEGER,
  finished_at  INTEGER,
  replay_of    TEXT REFERENCES runs(id),
  reason_code  TEXT,
  reason_text  TEXT,
  reason_data  TEXT,
  worker_id    TEXT,
  lease_until  INTEGER,
  daemon_session_id TEXT
);
CREATE INDEX runs_by_job  ON runs(job, queued_at DESC);
CREATE INDEX runs_live    ON runs(lease_until) WHERE status IN ('queued','running');
CREATE INDEX runs_recent  ON runs(queued_at DESC);

CREATE TABLE step_attempts (
  id            TEXT PRIMARY KEY,
  run_id        TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step          TEXT NOT NULL,
  attempt       INTEGER NOT NULL,
  status        TEXT NOT NULL,                 -- pending|running|succeeded|failed|skipped|retrying
  started_at    INTEGER,
  finished_at   INTEGER,
  exit_code     INTEGER,
  signal        TEXT,
  reason_code   TEXT,
  reason_text   TEXT,
  reason_data   TEXT,
  next_retry_at INTEGER,
  log_path      TEXT,                          -- relativ sti under loggroten
  log_bytes     INTEGER NOT NULL DEFAULT 0,
  log_truncated INTEGER NOT NULL DEFAULT 0,
  error_tail    TEXT,                          -- siste ~4 KiB, for explain uten filaksess
  UNIQUE(run_id, step, attempt)
);
CREATE INDEX steps_by_run ON step_attempts(run_id, step, attempt);

-- Grunnlaget for gap-deteksjon.
CREATE TABLE daemon_sessions (
  id           TEXT PRIMARY KEY,
  version      TEXT NOT NULL,
  started_at   INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL,               -- heartbeat hvert 10 s
  stopped_at   INTEGER,
  stop_reason  TEXT                            -- 'clean' | NULL (= crash)
);

CREATE TABLE outages (
  id           INTEGER PRIMARY KEY,
  from_ts      INTEGER NOT NULL,
  to_ts        INTEGER NOT NULL,
  detected_at  INTEGER NOT NULL,
  kind         TEXT NOT NULL,                  -- crash|clean|clock_jump
  prev_session TEXT,
  missed_ticks INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE cursors (
  sensor     TEXT PRIMARY KEY,
  value      TEXT,
  updated_at INTEGER NOT NULL,
  tick_id    TEXT REFERENCES ticks(id)
);

CREATE TABLE leases (
  name       TEXT PRIMARY KEY,                 -- 'scheduler', 'sensors', 'gc'
  owner      TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  acquired_at INTEGER NOT NULL
);

-- Outbox: varsler overlever crash og er reviderbare.
CREATE TABLE notifications (
  id           TEXT PRIMARY KEY,
  event        TEXT NOT NULL,                  -- run.failed, tick.error, ...
  subject      TEXT NOT NULL,                  -- job/schedule/sensor-navn
  payload      TEXT NOT NULL,                  -- JSON
  created_at   INTEGER NOT NULL,
  target       TEXT NOT NULL,                  -- navngitt notifier fra config
  state        TEXT NOT NULL,                  -- pending|sent|failed|throttled
  attempts     INTEGER NOT NULL DEFAULT 0,
  next_try_at  INTEGER,
  last_error   TEXT,
  sent_at      INTEGER
);
CREATE INDEX notif_pending ON notifications(next_try_at) WHERE state = 'pending';

CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL);
```

### 3.2 Steglogger på disk

Filer, ikke rader. Begrunnelse: én skriver i SQLite, `tail -f` og `grep` skal virke uten Pulseq, og loggvolum skal ikke blåse opp backup-strømmen.

```
$STATE_DIRECTORY/logs/2026-08-21/01K3ZQ.../extract.1.ndjson
                      └ datoshard    └ run_id  └ step.attempt
```

Datoshard gjør retention til `rm -rf` på én katalog. NDJSON-linje:

```json
{"ts":1755740400123,"stream":"stdout","seq":1,"line":"connecting to warehouse"}
```

`seq` gjør trunkering og tap detekterbart. Per forsøk gjelder en hard kvote (default 16 MiB): ved overskridelse beholdes **hode og hale** (første 25 %, siste 75 %) og en markørlinje `{"stream":"pulseq","event":"truncated","dropped_bytes":N}` settes inn. Hodet gir oppstartskontekst, halen gir feilen.

Ved terminering kopieres siste ~4 KiB til `step_attempts.error_tail`, slik at `pulseq explain run` og et fremtidig web-UI kan vise feilen uten å røre filsystemet.

### 3.3 Hvorfor to lagringsmedier, ikke ett

| Alternativ | Vurdering |
|---|---|
| Alt i SQLite | Enkelt, søkbart med FTS5 — men konkurrerer om den ene skriveren, blåser opp DB-filen og dermed backup/replikering. **Forkastet.** |
| Alt i filer | Billigst, men `explain` må åpne filer for å vise noe som helst, og strukturerte spørringer blir umulige. **Forkastet.** |
| **Hybrid: filer som sannhet, `error_tail` + metadata i DB** | Beslutningsdata forblir små og spørrbare; utdata skalerer fritt; `explain` er rask uten filaksess. **Valgt.** |

Hvis eventvolumet senere sprenger kontrollplanet, er utvidelsesveien en egen `pulseq-events.db` med egen skriverkø — ikke en større `pulseq.db`.

---

## 4. `pulseq explain`

Kommandoen har én modell: **en omvendt kronologisk liste av beslutninger**, hver med tid, aktør, utfall, årsakskode, forklaring og tiltaksforslag.

```
pulseq explain job <navn> [--since 48h]     # hele kjeden — signaturkommandoen
pulseq explain schedule <navn>              # ticks, neste tick, pause, catch-up
pulseq explain sensor <navn>                # ticks, cursor-utvikling, skip-grunner
pulseq explain run <run-id> [--timeline]    # beslutningskjede + steg-gantt
pulseq explain trigger <trigger-id>
pulseq explain step <run-id> <steg>
```

`--json` gir samme struktur maskinlesbart; det er kontrakten et senere web-UI bygger på — UI-et får ingen egen datamodell.

Eksempel:

```
$ pulseq explain job nightly-report --since 48h

Job: nightly-report            siste suksess: 2026-08-19 02:04 (1d 22t siden)
Freshness-SLA: 26t             STATUS: BRUTT (overskredet med 20t 12m)

  2026-08-21 02:00:00  schedule/nightly   tick 01K3ZP…   SKIPPED
     TICK_SKIPPED_OVERLAP — run 01K2XQ… kjører fortsatt (startet 20t 3m siden)
     → `pulseq explain run 01K2XQ…`. Vurder run_timeout, eller
       overlap_policy = "cancel_previous" på schedulen.

  2026-08-20 02:00:00  schedule/nightly   tick 01K2ZP…   TRIGGERED (1)
     trigger 01K2XP… → run 01K2XQ…  RUNNING i 20t 3m
     steg: extract ✓ 4m12s │ transform ⣿ RUNNING 20t │ load ⋯ PENDING

  2026-08-19 13:58 – 14:07     daemon             outage         MISSED
     TICK_MISSED_DAEMON_DOWN — daemon nede 9m (crash, ingen ren avslutning)
     → 0 schedules var forfalt i vinduet. `journalctl -u pulseq --since …`
```

**Explain må virke når daemonen er nede.** CLI-en snakker over unix-socket når daemonen svarer; ellers åpner den databasen direkte i `mode=ro` og svarer likevel. Dette er ikke en detalj — det er nettopp under en hendelse at daemonen er nede og spørsmålet stilles.

`pulseq status` er komplementet uten argument: brutte freshness-SLA-er, feilede runs siste døgn, sensorer i feil, runs som har stått fast, køstørrelse.

---

## 5. Strukturert logging

**log/slog** fra standardbiblioteket. Ingen ekstern loggavhengighet — ytelse er irrelevant på dette volumet, og stdlib gir økosystemnøytralitet.

### 5.1 Korrelasjon via context

Faste felt som aldri skal måtte huskes manuelt:

```
ts level msg component run_id step_id attempt job trigger_id tick_id
schedule sensor run_key origin err dur_ms daemon_session
```

Mekanismen er en `ContextHandler` som pakker inn `slog.JSONHandler` og trekker korrelasjons-ID-er ut av `context.Context` på hver `Handle`. ID-ene legges i konteksten ett sted (der en tick/run/step starter) og følger automatisk med:

```go
ctx = obs.With(ctx, "run_id", run.ID, "job", run.Job)
obs.From(ctx).Info("step started", "step", s.Name, "attempt", n)
```

Uten dette mønsteret vil noen call sites glemme feltene, og korrelasjonen blir upålitelig akkurat der den trengs.

### 5.2 Utdata og nivå

- JSON til stdout som default under systemd (journald eier rotasjon og retention for daemonloggen — Pulseq roterer den ikke selv). `PULSEQ_LOG_FORMAT=text` for interaktiv bruk.
- `slog.LevelVar` + `pulseq log-level debug [--for 15m]` over unix-socket: nivå kan skrus opp midt i en hendelse uten restart, og skrus automatisk ned igjen. Svært billig, uforholdsmessig nyttig.
- `pulseq logs --daemon` er et tynt lag over `journalctl -u pulseq` med samme filtre som resten av CLI-en.

### 5.3 Støydisiplin

INFO er reservert for **beslutninger og tilstandsoverganger** — ikke for loop-aktivitet. Koalescerte skips logges én gang ved start og én gang når grunnen endrer seg, med `repeat_count`. En operatør skal kunne lese en hel dags INFO-logg.

---

## 6. Metrics

**prometheus/client_golang** med et eget registry (ingen implisitte default-collectors utover Go/process runtime).

### 6.1 To kilder, ingen dobbeltbokføring

- **Tellere og histogrammer** holdes i minnet og inkrementeres ved hendelser. Counter-reset ved restart er akseptabelt og håndteres av PromQL.
- **Tilstandsgauges** (siste suksess, køstørrelse, aktive runs, neste tick) leses fra databasen i en **custom `Collector`** ved scrape. Da finnes tallet ett sted, og verdiene er korrekte umiddelbart etter restart uten gjenoppbygging.

### 6.2 Metrikksettet

```
pulseq_build_info{version,commit,go_version}                       gauge=1
pulseq_daemon_start_timestamp_seconds                              gauge

pulseq_tick_total{instigator,name,status,reason_code}              counter
pulseq_tick_duration_seconds{instigator,name}                      histogram
pulseq_tick_lag_seconds{instigator,name}                           gauge
pulseq_last_tick_timestamp_seconds{instigator,name}                gauge
pulseq_next_tick_timestamp_seconds{instigator,name}                gauge
pulseq_tick_interval_seconds{instigator,name}                      gauge   ← fra config
pulseq_instigator_paused{instigator,name}                          gauge

pulseq_trigger_total{job,outcome,reason_code}                       counter
pulseq_run_total{job,status}                                        counter
pulseq_run_duration_seconds{job}                                    histogram
pulseq_run_queue_wait_seconds{job}                                  histogram
pulseq_runs_active{job}                                             gauge
pulseq_runs_queued{job}                                             gauge
pulseq_last_success_timestamp_seconds{job}                          gauge   ← nøkkelen
pulseq_job_freshness_sla_seconds{job}                               gauge   ← fra config

pulseq_step_attempt_total{job,step,status,reason_code}              counter
pulseq_step_duration_seconds{job,step}                              histogram
pulseq_sensor_cursor_age_seconds{name}                              gauge

pulseq_db_write_queue_depth                                         gauge
pulseq_db_write_wait_seconds                                        histogram
pulseq_db_busy_total                                                counter
pulseq_db_size_bytes / pulseq_wal_size_bytes                        gauge
pulseq_log_dir_bytes / pulseq_log_disk_free_bytes                   gauge
pulseq_backup_last_success_timestamp_seconds                        gauge
pulseq_gc_last_success_timestamp_seconds                            gauge
pulseq_outage_seconds_total                                         counter
```

### 6.3 SLA-en som metrikk — det viktigste grepet

Ved å eksponere jobbens forventede ferskhet **fra konfigurasjonen** som en gauge, blir alarmregelen generisk og trenger aldri vedlikehold når jobber legges til:

```yaml
- alert: PulseqJobStale
  expr: time() - pulseq_last_success_timestamp_seconds > pulseq_job_freshness_sla_seconds
  for: 5m
  annotations:
    summary: "{{ $labels.job }} har ikke lykkes innen sin SLA"
    runbook: "pulseq explain job {{ $labels.job }}"
```

Samme mønster for ticks: `time() - pulseq_last_tick_timestamp_seconds > 3 * pulseq_tick_interval_seconds`.

Ferdige regler leveres i repoet som `deploy/pulseq-alerts.yml`: `JobStale`, `TickStalled`, `SensorErrorRate`, `RunFailureRate`, `QueueBacklog`, `WALGrowth`, `BackupStale`, `LogDiskLow`, `DaemonFlapping`.

### 6.4 Kardinalitetsdisiplin

**Absolutt forbud mot `run_id`, `step_id`, `trigger_id`, `run_key` som labels.** De er ubegrensede og vil drepe Prometheus. Høykardinalitetsdata bor i SQLite og i loggen — det er hele poenget med å ha begge.

`reason_code` er tillatt fordi katalogen er lukket og eksplisitt. `job`/`name` er begrenset av konfigurasjonen.

CI-test: generer 1 000 jobber og 200 sensorer, scrape `/metrics`, feil hvis serieantallet overstiger en fastsatt grense.

**OpenTelemetry er ikke MVP.** Prometheus-pull krever ingen collector og ingen sidecar, som passer "én binær på én VM". OTLP-eksport og traces (run = trace, steg = span) er fase 3; Prometheus-registryet kan bridges til OTel uten å endre instrumenteringen.

---

## 7. Helse, oppstart og gap-deteksjon

### 7.1 Endepunkter

Default binding er en **unix-socket** i `$RUNTIME_DIRECTORY/pulseq.sock` — CLI-en bruker den, og ingen TCP-port åpnes med mindre operatøren ber om det. Metrics-porten er opt-in.

| Endepunkt | Semantikk |
|---|---|
| `/livez` | Prosessen lever og kontroll-loopen har tikket nylig. **Rører aldri databasen** — en låst DB skal ikke gi restart-løkke. |
| `/readyz` | Dyp: DB åpen, migrasjoner kjørt, lease-status kjent, worker-pool startet, diskplass over terskel. Degraderer ved <10 % ledig plass. |
| `/metrics` | Prometheus. |
| `/debug/pprof` | Kun over unix-socket, eller bak eksplisitt flagg. |

`/healthz` finnes som alias til `/readyz` for verktøy som forventer det, men dokumenteres ikke.

### 7.2 systemd

```ini
[Unit]
Description=Pulseq orchestrator
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
NotifyAccess=main
ExecStart=/usr/local/bin/pulseq serve --config ${CONFIGURATION_DIRECTORY}/config.toml
WatchdogSec=30s
Restart=always
RestartSec=2s
TimeoutStopSec=120s
KillSignal=SIGTERM

User=pulseq
Group=pulseq
StateDirectory=pulseq
RuntimeDirectory=pulseq
ConfigurationDirectory=pulseq
Environment=PULSEQ_LOG_FORMAT=json

NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
LockPersonality=yes
SystemCallArchitectures=native
SystemCallFilter=@system-service
```

`sd_notify` via `coreos/go-systemd/v22/daemon`: `READY=1` etter at migrasjoner er kjørt og loopene er startet, `STATUS=` med kort tilstandstekst, `WATCHDOG=1` fra en egen goroutine som **kun** pinger hvis kontroll-loopen har tikket. Da betyr watchdog-timeout faktisk "orchestratoren har sluttet å ta beslutninger", ikke "maskinen er treg".

**Felle som må dokumenteres:** Pulseq kjører brukerdefinerte kommandoer, og sandkassedirektivene arves av barneprosesser. `ProtectSystem=strict` vil bryte jobber som skriver utenfor `StateDirectory`. `pulseq doctor` leser `/proc/self/status` og viser den effektive sandkassen, og pakken leverer en kommentert `pulseq.service` med en avslappet variant. Isolering per steg (`systemd-run --scope`) er fase 3.

### 7.3 Gap-deteksjon — svaret på "var systemet i det hele tatt oppe?"

Uten dette må operatøren gjette fra fravær av ticks. Ved hver oppstart:

1. Finn forrige `daemon_sessions`-rad uten `stopped_at` → gapet er `[last_seen_at, now]`, `kind = crash`. Ren avslutning gir `kind = clean`.
2. Skriv en `outages`-rad.
3. Beregn hvilke schedule-tidspunkt som var forfalt i gapet og skriv **syntetiske ticks** med `TICK_MISSED_DAEMON_DOWN` (eller kjør dem hvis `catch_up = true`, med `origin = catchup`).
4. Rekonsiliér runs med utløpt lease → `RUN_ORPHANED_RECONCILED`.
5. Send `daemon.recovered`-event med gapets lengde og antall missede ticks.

Klokkehopp større enn en terskel gir tilsvarende `clock_jump`-outage. `pulseq doctor` sjekker NTP-synkronisering.

---

## 8. Alerting-kroker

Intern eventbuss der hver tilstandsovergang publiseres: `run.failed`, `run.stuck`, `run.succeeded`, `tick.error`, `sensor.stalled`, `job.sla_breached`, `daemon.recovered`, `backup.failed`, `gc.failed`, `disk.low`.

**Outbox-mønster:** varselet skrives til `notifications` i **samme transaksjon** som tilstandsendringen. En egen leveringsloop plukker `pending` med backoff. Konsekvenser: varsler går ikke tapt ved crash mellom "feilet" og "varslet", og `pulseq notifications list` viser reviderbart hva som faktisk ble sendt — "fikk vi varsel om dette?" blir et spørsmål med et svar.

Notifiers, i prioritert rekkefølge:

1. **`exec`** — kjør en kommando med hendelsen som JSON på stdin. Null avhengigheter, dekker alt (Slack, e-post, SMS, PagerDuty) via brukerens egne skript. Dette er den eneste som må finnes.
2. **`webhook`** — HTTP POST med JSON og HMAC-signatur.
3. **`stderr`** — for utvikling.

Regler i config med `on`, `match` (glob på jobbnavn), `throttle` og `group_by`, slik at ett systematisk problem gir ett varsel og ikke to hundre.

**Dead man's switch:** valgfri periodisk HTTP-pling til en ekstern tjeneste. Dekker det ingen intern mekanisme kan dekke — at hele maskinen er borte.

---

## 9. SQLite-drift

### 9.1 Én skriver, mange lesere

```
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = NORMAL;      -- trygt i WAL; FULL kun hvis strømtap-tap er uakseptabelt
PRAGMA foreign_keys = ON;
PRAGMA wal_autocheckpoint = 1000;
```

To `*sql.DB` mot samme fil:

- **writer**: `SetMaxOpenConns(1)`, alle transaksjoner som `BEGIN IMMEDIATE`. Med én forbindelse serialiseres skriving i Go, ikke i SQLite, og `SQLITE_BUSY` fra egne tråder blir umulig. `BEGIN IMMEDIATE` er nødvendig fordi `busy_timeout` ikke hjelper når en deferred lesetransaksjon oppgraderes til skriv.
- **reader**: `mode=ro`, `SetMaxOpenConns(N)`. `explain`, `/metrics` og UI leser aldri gjennom skriveren.

Køen instrumenteres (`pulseq_db_write_queue_depth`, `pulseq_db_write_wait_seconds`) slik at metning er synlig før den blir et problem. Transaksjoner holdes korte; ingen prosessutføring eller nettverkskall inne i en skrivetransaksjon — noensinne.

Driver: **`modernc.org/sqlite`** (CGO-fri). Begrunnelse er ren drift: statisk binær uten glibc-avhengighet, triviell kryssbygging, én fil å kopiere til serveren. Ytelsen er lavere enn `mattn/go-sqlite3`; derfor ligger all SQL bak et smalt `store`-interface med benchmark i CI, slik at bytte er en dags arbeid hvis profilering krever det.

### 9.2 Migrasjoner

Nummererte, forover-only `.sql`-filer embeddet med `go:embed`, kjørt i én transaksjon ved oppstart før `READY=1`. Før migrering: automatisk `VACUUM INTO` av en pre-migrasjons-kopi. `pulseq migrate --dry-run` viser hva som ville kjørt. Nedgradering støttes ikke — gjenoppretting fra kopien er veien tilbake, og det er ærligere enn halvtestede down-migrasjoner.

### 9.3 Backup

**MVP: `VACUUM INTO`.** SQLite-nativt, konsistent snapshot uten å låse skrivere, ingen ekstra programvare:

```
pulseq backup --to /var/backups/pulseq/pulseq-$(date -Is).db
```

Kjøres av en **egen systemd timer**, ikke bare som en Pulseq-jobb. Sirkulær avhengighet — "backupen tas av det systemet som er nede" — er en klassisk feil. En intern jobb kan gjerne finnes i tillegg, for da blir backup-historikk synlig i `explain`.

**Fase 2: Litestream** som separat prosess for point-in-time recovery, med S3/lokal replika. Nødvendige forbehold i runbooken:

- Litestream er katastrofegjenoppretting, ikke høy tilgjengelighet — replikering er asynkron, og siste sekund kan gå tapt.
- Litestream overtar checkpointing; Pulseq skal ikke tvinge fram checkpoints.
- Manuell `VACUUM` skriver hele databasen om og utløser full snapshot. Bruk `PRAGMA incremental_vacuum` i stedet.
- Det har vært rapportert stille replikeringsfeil i enkelte versjoner. **Derfor: månedlig restore-drill som en Pulseq-jobb** — restore til temp, `PRAGMA integrity_check`, sammenlign radantall, oppdater `pulseq_backup_last_success_timestamp_seconds`. En backup ingen har gjenopprettet fra er en hypotese.

### 9.4 Retention og opprydding

Kjøres av en intern systemjobb `__pulseq.gc` — den får egne ticks og runs, så GC-feil blir synlige i samme grensesnitt som alt annet.

| Data | Default | Regel |
|---|---|---|
| `ticks` | 30 dager | **og** alltid minst 200 siste per instigator |
| `runs` + `step_attempts` | 90 dager | **og** alltid minst 50 siste per job |
| Steglogger på disk | 14 dager | eller til `log_max_bytes` (default 10 GiB) nås → eldste dato-shard slettes først |
| `notifications` | 30 dager | sendte; `failed` beholdes til de kvitteres |
| `outages` | beholdes | små og verdifulle |

**"Minst N siste per objekt" er ikke pynt.** En jobb som kjører kvartalsvis mister ellers all historikk mellom kjøringene, og da svikter kjerneløftet nettopp for de jobbene som er vanskeligst å feilsøke.

Sletting skjer i porsjoner (10 000 rader per transaksjon) med `PRAGMA incremental_vacuum` etterpå — aldri full `VACUUM` i en driftende tjeneste, siden den krever eksklusiv tilgang.

`pulseq export run <id>` pakker alle DB-rader og logger for én run til en tar.gz. Da kan man bevare et konkret bevis før retention tar det, uten å slå av retention.

### 9.5 Oppgradering

1. Binærene er selvstendige; oppgradering er å bytte fil og `systemctl restart`.
2. `pulseq migrate --dry-run` mot en kopi før produksjon.
3. `SIGTERM` → nye triggere avvises (`TRIGGER_REJECTED_SHUTDOWN`), pågående steg får `drain_timeout` (default 60 s), deretter `SIGTERM` til barneprosesser. Ikke-drenerte runs beholder leasen og plukkes opp av neste instans.
4. CLI-en sjekker versjonsavvik mot daemonen og advarer.
5. `pulseq doctor` etter oppgradering; `pulseq_build_info` gjør versjonen synlig i Prometheus.

---

## 10. Arkitektur

Én binær, tre roller:

- **`pulseq serve`** — daemon: scheduler-loop, sensor-loop, dispatcher, executor-pool, GC, notifier, unix-socket + valgfri HTTP.
- **`pulseq <kommando>`** — CLI: unix-socket når daemonen svarer, ellers direkte lesetilgang til DB.
- **steg-prosess** — ett barn per step-attempt, med egen prosessgruppe slik at timeout dreper hele treet.

```
cmd/pulseq/
internal/
  config/     lasting, validering, defaults, freshness-SLA
  store/      SQLite, migrasjoner, writer/reader-pool, retention-spørringer
  model/      Tick, Trigger, Run, StepAttempt, Reason
  reason/     årsakskatalog + tiltaksforslag (én kilde, genererer docs)
  schedule/   cron/interval, Decide(), next/prev, tidssone, catch-up
  sensor/     kontrakt, cursor, timeout-innpakning
  dispatch/   trigger → run, dedup, concurrency, overlap-policy
  executor/   DAG, parallellitet, retries, prosesshåndtering
  logstore/   NDJSON-skriver, kvote, head/tail-trunkering, retention
  obs/        slog-oppsett, ContextHandler, metrics-collectors, health
  explain/    bygger beslutningstidslinjen — delt av CLI, JSON og fremtidig UI
  notify/     outbox, exec/webhook-notifiers, throttling
  gc/
  api/        unix-socket + HTTP
deploy/       pulseq.service, pulseq-backup.timer, pulseq-alerts.yml, grafana.json
```

To arkitekturvalg bærer hele planen:

**`internal/explain` er den eneste veien til historikk.** CLI, `--json` og et senere web-UI kaller samme kode. Ingen alternativ spørringsvei betyr ingen mulighet for at grensesnittene forteller ulike historier.

**`Decide()` er ren.** Signaturen `schedule.Decide(state, now) → Decision{Action, ReasonCode, ReasonData}` uten I/O. Daemonen persisterer resultatet; `pulseq preview --at "2026-12-24T02:00"` kaller samme funksjon og persisterer ingenting. Preview kan derfor ikke lyve om hva som ville skjedd.

---

## 11. Teknologivalg

| Valg | Begrunnelse | Vurdert alternativ |
|---|---|---|
| `log/slog` | Stdlib, JSON + text, `LevelVar`, custom handler for kontekstkorrelasjon | zerolog/zap — raskere, men hastighet er irrelevant her og gir en avhengighet uten gevinst |
| `prometheus/client_golang` | Pull-modell uten collector eller sidecar; custom `Collector` leser tilstand fra DB ved scrape | OpenTelemetry — mer maskineri for MVP; utsatt til fase 3 |
| `modernc.org/sqlite` | CGO-fri → statisk binær, triviell distribusjon og kryssbygging | `mattn/go-sqlite3` raskere; bytte er mulig bak `store`-interfacet |
| To `*sql.DB` (writer 1 conn / reader N) | Serialiserer skriving i Go; `SQLITE_BUSY` fra egne tråder blir umulig | Én pool med `busy_timeout` — feiler ved deferred→write-oppgradering |
| `VACUUM INTO` (MVP) → Litestream (fase 2) | Null avhengigheter først; PITR når behovet er reelt og drillet | Kun filkopiering — ikke konsistent under skriv |
| `coreos/go-systemd/v22/daemon` | `sd_notify` + watchdog, nesten ingen avhengigheter | Egen implementasjon av protokollen — unødvendig |
| `oklog/ulid/v2` | Tidssorterte, kopivennlige ID-er; gir gratis kronologi i indekser | UUIDv7 likeverdig, men lengre å lese høyt over telefon |
| Cron bak eget `schedule.Spec`-interface | Catch-up krever **forrige** tidspunkt, ikke bare neste; `gronx` har `PrevTick` | `robfig/cron/v3` er mest utbredt, men gir kun `Next()` |
| `net/http` | Fire endepunkter trenger ikke rammeverk | chi/gin — unødvendig flate |
| NDJSON-filer for steglogger | `tail -f`/`grep` virker uten Pulseq; skalerer uten å belaste skriveren eller backup | Logger i SQLite — konkurrerer om skriveren, blåser opp replikering |

---

## 12. MVP-kutt

Skillelinjen er skarp: **alt som kreves for at kjerneløftet holder, er MVP. Alt annet venter.**

### MVP
- `ticks` med status, `reason_code` og koalesering — for schedules *og* sensorer.
- `triggers` med `run_key`-dedup som unik indeks.
- `runs` + `step_attempts` med `reason_code` og `error_tail`.
- Steglogger til NDJSON med kvote og head/tail-trunkering.
- slog JSON med `ContextHandler` og full korrelasjon.
- `pulseq explain job|run|schedule|sensor|trigger|step`, `pulseq status`, `pulseq logs [-f]`, `pulseq ticks`, `pulseq runs`.
- Explain virker med daemonen nede (read-only DB-vei).
- `daemon_sessions` + `outages` + gap-deteksjon + syntetiske missede ticks.
- `/livez`, `/readyz`, `/metrics` med kjernesettet, inkludert `last_success_timestamp` og `freshness_sla`.
- systemd-unit med `Type=notify` og watchdog; `pulseq doctor`.
- GC/retention for ticks, runs og loggfiler.
- `VACUUM INTO`-backup + backup-timer.
- `deploy/pulseq-alerts.yml`.

### Fase 2
Notification-outbox og `exec`/`webhook`-notifiers · Litestream/PITR + restore-drill-jobb · rollup-aggregater som overlever retention · `pulseq preview` over tidsvindu · FTS5-søk i `error_tail` · cursor-alder-metrikker · backfill med egen historikk · `explain --json` stabilisert som UI-kontrakt · `pulseq export run`.

### Fase 3
Web-UI (tick-tidslinje, run-graf) som ren renderer over `explain --json` · OTLP-eksport og traces (run = trace, steg = span) · steg-isolering via `systemd-run --scope` · Postgres for multi-node · fjernlagring av logger.

### Aldri
Innebygd TSDB, loggsøkemotor eller alertmanager. Pulseq eksponerer data og hendelser; Prometheus, journald og brukerens egne skript gjør resten. Distribuert konsensus — lease i SQLite er nok for single-node, og multi-node er et Postgres-problem.

---

## 13. Faser fra tom katalog

Rekkefølgen er bevisst: **historikkmodellen bygges før utføringsmotoren.** Bygger man motoren først, blir observabilitet påklistret, og årsakskodene blir etterrasjonaliseringer.

**F0 — Skjelett (uke 1).** Repo, `cmd/pulseq`, config-lasting og -validering, `internal/store` med migrasjoner og pragma-oppsett, writer/reader-pooler, `obs`-pakken med `ContextHandler`, CI (`go vet`, `staticcheck`, `-race`).
*Ferdig når:* `pulseq doctor` rapporterer DB-sti, `journal_mode=wal`, migrasjonsversjon, ledig diskplass og klokkeskjevhet.

**F1 — Beslutningslogg (uke 2).** Skjema for `ticks`/`triggers`/`runs`/`step_attempts`, `reason`-katalogen med tiltaksforslag, `internal/explain`, CLI-kommandoene mot read-only DB. Ingen scheduler ennå — mates fra fixtures.
*Ferdig når:* `pulseq explain run <id>` gir en lesbar tidslinje fra testdata, og CI-testen som krever `reason_code` på alle terminale tilstander er grønn.

**F2 — Scheduler (uke 3–4).** Ren `Decide()`, cron/interval, tidssone, pause/resume, catch-up, overlap-policy, lease. Hver forfalt evaluering skriver en tick — også de som ikke gjør noe. Koalesering.
*Ferdig når:* `pulseq explain schedule X` viser 100 % av forfalte evalueringer siste døgn, og en pauset schedule produserer `TICK_SKIPPED_PAUSED` framfor stillhet.

**F3 — Executor og steglogger (uke 5–6).** DAG-topologi, parallellitet, retry med backoff, prosessgrupper og timeout, NDJSON-logger med kvote, `error_tail`, `pulseq logs -f`, run-lease og rekonsiliering.
*Ferdig når:* `SIGKILL` av daemonen midt i en run gir `RUN_ORPHANED_RECONCILED` ved restart, og runen dukker opp i `explain` med korrekt begrunnelse.

**F4 — Sensorer (uke 7).** Sensorkontrakt (`check(ctx) → []Trigger | SkipReason`), cursor-persistering med `cursor_before`/`cursor_after` på ticken, multi-trigger, evalueringstimeout, `run_key`-dedup.
*Ferdig når:* samme `run_key` to ganger gir én run og én `TRIGGER_DEDUPED_RUN_KEY`-rad som lenker til den opprinnelige runen, og cursor-utviklingen er lesbar tick for tick.

**F5 — Drift (uke 8).** Metrics og custom collector, `/livez` og `/readyz`, `sd_notify` + watchdog, `daemon_sessions` + gap-deteksjon, GC/retention, backup, systemd-unit, pakking (tarball + deb), alertregler, Grafana-dashboard, runbook.
*Ferdig når:* en øvelse der daemonen stoppes i 30 minutter gir en `outages`-rad og korrekte `TICK_MISSED_DAEMON_DOWN`-ticks, **og** en restore-drill fra backup lykkes med `integrity_check` grønn.

**F6 — Varsling og polering (uke 9–10).** Notification-outbox, `exec`/`webhook`, throttling og gruppering, `pulseq preview`, `pulseq export run`, generert dokumentasjon av hele årsakskatalogen.
*Ferdig når:* en indusert jobbfeil gir nøyaktig ett varsel, én `notifications`-rad med `state=sent`, og `pulseq explain job` peker rett på årsaken med et konkret tiltak.

**F7 — Fase 2/3** etter behov og faktisk bruk.

---

## 14. SLO-er for verktøyet selv

Målbare krav, testet i CI der det lar seg gjøre:

1. **Dekning:** 100 % av forfalte schedule-/sensor-evalueringer har en persistert tick.
2. **Attribusjon:** 100 % av terminale run- og steg-tilstander har `reason_code IS NOT NULL` og `<> 'UNKNOWN'`.
3. **Svartid:** `pulseq explain <objekt>` under 1 s p99 med 90 dagers historikk (10⁶ ticks, 10⁵ runs). Testes med generert datasett og `EXPLAIN QUERY PLAN`-assertions.
4. **Holdbarhet:** ved `SIGKILL` tapes maksimalt én evalueringssyklus av beslutningsdata.
5. **Synlighet av hull:** enhver periode uten kjørende daemon lengre enn 60 s har en tilsvarende `outages`-rad innen 10 s etter oppstart.
6. **Alarmerbarhet:** `pulseq doctor` lister jobber uten definert freshness-SLA — altså de som ikke kan alarmeres på.

Punkt 6 er den stille favoritten: verktøyet forteller deg selv hvor overvåkingen din har hull.

---

## 15. Risikoer

| # | Risiko | Konsekvens | Mottiltak |
|---|---|---|---|
| 1 | Skrivekontensjon på SQLite fra tick- og loggvolum | Beslutninger forsinkes; køen vokser | Koalesering av skips; steglogger til fil; én writer-pool; `BEGIN IMMEDIATE`; korte transaksjoner; kø-metrics med alarm. Rømningsvei: egen `events.db` |
| 2 | Kardinalitetseksplosjon i Prometheus | Overvåkingen dør før tjenesten | Forbud mot ID-labels; lukket `reason_code`-katalog; CI-test som teller serier ved 1 000 jobber |
| 3 | Loggdisken fylles | Steg feiler av grunner som ikke er jobbenes | Hard kvote per forsøk; global soft cap; GC kjører før executor får skrive; `readyz` degraderer under 10 % ledig; alarm på `log_disk_free_bytes` |
| 4 | Watchdog dreper daemonen under legitim treg operasjon | Restart-løkke midt i en hendelse | Watchdog-ping avhenger kun av kontroll-loopen; backup, GC og migrasjoner i egne goroutines med egne frister; `WatchdogSec` romslig satt |
| 5 | Litestream replikerer stille feil, eller `VACUUM` utløser full snapshot | Backup finnes ikke når den trengs | Månedlig restore-drill som Pulseq-jobb med `integrity_check`; `backup_last_success_timestamp` med alarm; `incremental_vacuum` framfor `VACUUM`; MVP bruker `VACUUM INTO` uten Litestream |
| 6 | Klokkehopp og sommertid | Doble eller tapte kjøringer | Alt lagres i UTC-millis; tidssone er schedulens egenskap; `clock_jump`-outage; `doctor` sjekker NTP; DST-testsuite for cron |
| 7 | Retention sletter bevis rett før noen trenger det | Kjerneløftet svikter for sjeldne jobber | "Minst N siste per objekt" i tillegg til tidsgrense; `pulseq export run` for å bevare et konkret tilfelle |
| 8 | Årsakskoder degenererer til `UNKNOWN` | Explain blir ubrukelig over tid | CI-test på alle terminale overganger; katalogen er én kilde som også genererer dokumentasjonen |
| 9 | Explain blir treg når historikken vokser | Kjerneløftet brytes stille | Indekser definert opp front (delvise indekser på feiltilstander); ytelsestest på 10⁶ ticks i CI |
| 10 | systemd-hardening bryter brukerjobber | Adopsjon stopper på første jobb | Daemon hardnes, exec-miljøet dokumenteres eksplisitt; `doctor` viser effektiv sandkasse; kommentert avslappet unit i pakken |
| 11 | Sirkulær avhengighet backup ↔ scheduler | Ingen backup nettopp når det trengs | Backup som selvstendig systemd timer; Pulseq-jobben er kun for synlighet |
| 12 | CGO-fri driver for treg ved høy last | Må bytte driver sent i løpet | `store`-interface fra dag én; benchmark i CI; bytteveien dokumentert |
| 13 | Sensor-ticks druknes i støy | Ingen leser tick-historikken | Koalesering med `repeat_count`; INFO kun ved endring i grunn; `pulseq status` viser aggregat, ikke strøm |

---

## 16. Kontrakter mot de andre delplanene

Denne planen eier fire ting som resten av systemet må forholde seg til:

1. **`reason`-katalogen.** Ingen modul lager sine egne feilstrenger. Ny kode som kan avslå eller hoppe over arbeid må registrere en kode med tekst og tiltaksforslag.
2. **`ticks`-kontrakten.** Enhver ny utløsermekanisme (backfill, manuell, webhook) skriver ticks med samme skjema. Da virker `explain` for den uten videre arbeid.
3. **`obs`-pakken.** Alle komponenter henter loggeren fra `context` og setter korrelasjons-ID-er ved inngangen til en enhet av arbeid.
4. **`explain --json`.** Eneste lesevei til historikk for eksterne grensesnitt. Web-UI får ikke egne spørringer.

Konsekvensen er at observabilitet ikke er en fase som kan skyves — det er en invariant som håndheves ved kodegjennomgang og i CI.
