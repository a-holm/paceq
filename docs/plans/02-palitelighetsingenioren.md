# Pulseq — prosjektplan fra pålitelighetsperspektivet

Forfatterrolle: pålitelighetsingeniør. Hele planen er skrevet ut fra ett spørsmål, stilt på hvert eneste punkt i koden: **hva skjer hvis prosessen dør akkurat her?**

---

## 0. Designfilosofi

Fem påstander som all resten følger av:

1. **Intensjon før effekt.** Enhver observerbar sideeffekt (prosess-spawn, HTTP-kall, filskriving) skal ha en committet DB-rad som beskriver intensjonen *før* effekten utføres.
2. **Effekt er aldri exactly-once.** Vinduet mellom «commit intensjon» og «effekt skjer» kan ikke lukkes uten distribuert transaksjon mot sideeffekten. Derfor er den ærlige garantien at-least-once, og hvert steg må deklarere sin idempotensmodell.
3. **Eierskap er tidsbegrenset.** Ingen aktør eier en run permanent. Eierskap = lease med utløp + monotont økende `epoch` (fencing token) som validerer hver skriving.
4. **Restart rekonsilierer.** Tilstand som bare kan oppstå på grunn av krasj, må ha en deterministisk og idempotent opprydningsregel som kjøres ved oppstart og periodisk.
5. **To klokker.** Veggklokke brukes kun til *planlagt tid*. Monoton klokke brukes til lease, timeout og backoff. Blanding av de to er den vanligste kilden til feil ved DST og NTP-korreksjon.

Alt som ikke tjener disse fem påstandene er utsatt til etter MVP.

---

## 1. Arkitektur

### 1.1 Prosessmodell

Én statisk binær, `pulseq`, med roller:

```
pulseq daemon          # den eneste langtidskjørende prosessen (single-node)
  ├── writer           # goroutine: eneste skriver mot state.db (serialisert kø)
  ├── scheduler        # goroutine: cron/interval → schedule_ticks → runs
  ├── sensorrunner     # goroutine: sensor-evaluering → sensor_ticks → triggers → runs
  ├── executor         # goroutine-pool: claim runs, kjør steg som barneprosesser
  ├── reconciler       # goroutine: lease-reaper, spool-konsument, /proc-sweep, fsck
  └── logwriter        # goroutine: eneste skriver mot logs.db

pulseq exec            # intern shim; wrapper rundt hvert steg-forsøk (aldri kalt manuelt)
pulseq <cli-kommando>  # kortlivet klient; leser via read-pool, skriver via daemon-socket
```

`daemon` holder en `flock` på `$PULSEQ_HOME/pulseq.lock`. To daemoner mot samme katalog er dermed umulig på én maskin. Leases beskytter da mot *restart-vinduet*, ikke mot samtidighet — men de er designet for å også holde ved multi-node (fase 6, Postgres).

CLI-skriveoperasjoner går over en unix-socket til daemonen, ikke direkte mot DB. Det bevarer invarianten «én skriver» uten å måtte koordinere mellom prosesser. Hvis daemonen er nede, faller CLI tilbake til å ta `flock` selv og gjøre skrivingen direkte — samme kodesti, samme transaksjoner.

### 1.2 Lagdeling

```
cmd/pulseq            CLI + daemon-oppstart
internal/clock        Wall/Mono/fake — eneste sted time.Now() finnes
internal/store        migrasjoner, connection-pooler, Writer.Tx, alle SQL-spørringer
internal/model        state-maskiner som rene funksjoner (ingen I/O)
internal/schedule     cron/interval-iterator, tidssoner, DST-policy, catch-up
internal/sensor       sensor-protokoll, cursor-håndtering
internal/exec         prosessoppstart, prosessgruppe, watchdog-pipe, spool
internal/reconcile    oppstartsrekonsiliering, reaper, fsck, invarianter
internal/obs          slog-handler, events, metrikker
internal/faults       crash-injection-punkter (no-op i produksjonsbygg)
```

`internal/model` er ren: `func NextRunState(cur RunState, ev Event, guards Guards) (RunState, error)`. Den kan property-testes uten database, og `internal/store` er tvunget til å bruke den.

### 1.3 SQLite-oppsett: flere filer, én skriver per fil

| Fil | Innhold | `synchronous` | Begrunnelse |
|---|---|---|---|
| `state.db` | jobs, runs, steps, attempts, ticks, triggers, events, leases | `FULL` | Beslutningskritisk. `NORMAL` i WAL kan rulle tilbake committede transaksjoner ved strømbrudd — det bryter påstand 1 direkte. |
| `logs.db` | step_logs (stdout/stderr, høyt volum) | `NORMAL` | Tap av siste millisekunder logg er akseptabelt. Isolerer loggflom fra beslutningstransaksjoner. |
| `spool/` | attempt-resultatfiler skrevet av `pulseq exec` | fsync+rename | Se §5.6 — lukker det verste krasjvinduet. |

DSN for `state.db`:

```
file:state.db?_journal=WAL&_txlock=immediate&_timeout=5000
   &_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_pragma=wal_autocheckpoint(1000)
```

Pooler:

- `writeDB`: `SetMaxOpenConns(1)`, `_txlock=immediate`. Alle skriv går gjennom `Writer.Tx(ctx, fn)`. Fordi det finnes nøyaktig én skriveforbindelse og transaksjonen starter som IMMEDIATE, kan «les-så-skriv» gjøres trygt uten optimistisk låsing — det forenkler concurrency-håndhevingen betydelig (§5.4).
- `readDB`: `SetMaxOpenConns(N)`, WAL gir samtidige lesere uten å blokkere skriveren.

Ved oppstart verifiseres PRAGMA-verdiene med `PRAGMA journal_mode` / `PRAGMA synchronous` og daemonen nekter å starte ved avvik. Feilkonfigurert durabilitet skal være en oppstartsfeil, ikke en stille degradering.

**Arkitekturtest:** en `go test` som parser AST-en og feiler hvis `writeDB`/`sql.Tx` refereres utenfor `internal/store`, eller hvis `time.Now` brukes utenfor `internal/clock`.

---

## 2. Datamodell

Alle tidsstempler er `INTEGER` = UTC millisekunder siden epoch. Ingen lokal tid i databasen noensinne.

```sql
-- Identitet og koordinering ------------------------------------------------
CREATE TABLE instances (
  id            TEXT PRIMARY KEY,          -- uuidv7 generert ved oppstart
  hostname      TEXT NOT NULL,
  pid           INTEGER NOT NULL,
  boot_id       TEXT NOT NULL,             -- /proc/sys/kernel/random/boot_id
  version       TEXT NOT NULL,
  started_at    INTEGER NOT NULL,
  heartbeat_at  INTEGER NOT NULL,
  stopped_at    INTEGER,                   -- satt ved ren avslutning
  stop_reason   TEXT
);

CREATE TABLE sequences (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
-- rad 'epoch': global monotont økende fencing-teller

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
-- schema_version, tzdata_version, current_boot_id

-- Jobbdefinisjon -----------------------------------------------------------
CREATE TABLE jobs (
  id                 INTEGER PRIMARY KEY,
  name               TEXT NOT NULL UNIQUE,
  current_version    INTEGER NOT NULL,
  max_concurrent     INTEGER NOT NULL DEFAULT 1,
  concurrency_key_tpl TEXT,                -- template, f.eks. "{{.payload.tenant}}"
  overflow_policy    TEXT NOT NULL DEFAULT 'queue',  -- queue|skip|replace
  enabled            INTEGER NOT NULL DEFAULT 1,
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL
);

CREATE TABLE job_versions (
  job_id     INTEGER NOT NULL REFERENCES jobs(id),
  version    INTEGER NOT NULL,
  spec_json  TEXT NOT NULL,                -- steg, avhengigheter, retry-policy
  spec_hash  TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (job_id, version)
);
-- Runs peker på en versjon. Endring av jobbspec mens runs er aktive kan
-- dermed aldri endre semantikken til en pågående run.

-- Triggere -----------------------------------------------------------------
CREATE TABLE schedules (
  id            INTEGER PRIMARY KEY,
  job_id        INTEGER NOT NULL REFERENCES jobs(id),
  name          TEXT NOT NULL UNIQUE,
  kind          TEXT NOT NULL,             -- cron|interval|calendar
  expr          TEXT NOT NULL,
  timezone      TEXT NOT NULL DEFAULT 'UTC',
  start_at      INTEGER,
  end_at        INTEGER,
  enabled       INTEGER NOT NULL DEFAULT 1,
  paused_reason TEXT,
  jitter_ms     INTEGER NOT NULL DEFAULT 0,
  catchup       TEXT NOT NULL DEFAULT 'last',   -- none|last|all
  catchup_window_ms INTEGER NOT NULL DEFAULT 86400000,
  catchup_max_runs  INTEGER NOT NULL DEFAULT 1,
  spring_forward TEXT NOT NULL DEFAULT 'skip',  -- skip|shift
  fall_back      TEXT NOT NULL DEFAULT 'first', -- first|both
  next_tick_at  INTEGER,
  last_tick_at  INTEGER,
  lease_owner   TEXT, lease_expires_at INTEGER, lease_epoch INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE schedule_ticks (
  id            INTEGER PRIMARY KEY,
  schedule_id   INTEGER NOT NULL REFERENCES schedules(id),
  scheduled_for INTEGER NOT NULL,          -- UTC, deterministisk
  state         TEXT NOT NULL,             -- planlagt|ferdig|hoppet_over|feilet
  decided_at    INTEGER,
  run_id        INTEGER REFERENCES runs(id),
  skip_reason   TEXT,
  error         TEXT,
  UNIQUE (schedule_id, scheduled_for)      -- kjernen i tick-idempotens
);

CREATE TABLE sensors (
  id            INTEGER PRIMARY KEY,
  job_id        INTEGER NOT NULL REFERENCES jobs(id),
  name          TEXT NOT NULL UNIQUE,
  cmd_json      TEXT NOT NULL,
  interval_ms   INTEGER NOT NULL DEFAULT 30000,
  min_interval_ms INTEGER NOT NULL DEFAULT 5000,
  timeout_ms    INTEGER NOT NULL DEFAULT 60000,
  max_triggers_per_tick INTEGER NOT NULL DEFAULT 100,
  enabled       INTEGER NOT NULL DEFAULT 1,
  paused_reason TEXT,
  cursor        TEXT,                      -- ugjennomsiktig for Pulseq
  cursor_updated_at INTEGER,
  next_tick_at  INTEGER,
  seq           INTEGER NOT NULL DEFAULT 0,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  lease_owner   TEXT, lease_expires_at INTEGER, lease_epoch INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE sensor_ticks (
  id           INTEGER PRIMARY KEY,
  sensor_id    INTEGER NOT NULL REFERENCES sensors(id),
  seq          INTEGER NOT NULL,
  state        TEXT NOT NULL,              -- aktiv|ferdig|hoppet_over|feilet
  started_at   INTEGER NOT NULL,
  ended_at     INTEGER,
  cursor_before TEXT,
  cursor_after  TEXT,
  trigger_count INTEGER NOT NULL DEFAULT 0,
  truncated     INTEGER NOT NULL DEFAULT 0,
  skip_reason  TEXT,
  error        TEXT,
  owner        TEXT, owner_epoch INTEGER,
  UNIQUE (sensor_id, seq)
);

CREATE TABLE triggers (
  id          INTEGER PRIMARY KEY,
  source_kind TEXT NOT NULL,               -- schedule|sensor|manual|replay
  source_id   INTEGER,
  tick_id     INTEGER,
  job_id      INTEGER NOT NULL REFERENCES jobs(id),
  run_key     TEXT NOT NULL,
  payload_json TEXT,
  created_at  INTEGER NOT NULL,
  run_id      INTEGER REFERENCES runs(id),
  dedup_of    INTEGER REFERENCES triggers(id)  -- satt hvis run_key allerede fantes
);

-- Utføring -----------------------------------------------------------------
CREATE TABLE runs (
  id            INTEGER PRIMARY KEY,
  job_id        INTEGER NOT NULL REFERENCES jobs(id),
  job_version   INTEGER NOT NULL,
  run_key       TEXT NOT NULL,
  concurrency_key TEXT,
  state         TEXT NOT NULL,             -- planlagt|aktiv|utsatt|ferdig|feilet|kansellert
  generation    INTEGER NOT NULL DEFAULT 1,-- økes ved operatør-reopen
  priority      INTEGER NOT NULL DEFAULT 0,
  trigger_id    INTEGER REFERENCES triggers(id),
  scheduled_for INTEGER,                   -- logisk tid (fra schedule)
  logical_from  INTEGER, logical_to INTEGER,  -- datavindu
  payload_json  TEXT,
  created_at    INTEGER NOT NULL,
  started_at    INTEGER, ended_at INTEGER,
  resume_at     INTEGER, defer_reason TEXT,
  cancel_requested_at INTEGER, cancel_reason TEXT, cancel_requested_by TEXT,
  crash_count   INTEGER NOT NULL DEFAULT 0,-- antall ganger rekonsiliert etter krasj
  lease_owner   TEXT, lease_expires_at INTEGER,
  claim_epoch   INTEGER NOT NULL DEFAULT 0,-- fencing token
  replay_of     INTEGER REFERENCES runs(id),
  UNIQUE (job_id, run_key)                 -- kjernen i run-idempotens
);
CREATE INDEX runs_claimable ON runs(state, resume_at, priority)
  WHERE state IN ('planlagt','utsatt');
CREATE INDEX runs_leased ON runs(lease_expires_at) WHERE state = 'aktiv';

CREATE TABLE run_steps (
  run_id        INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  name          TEXT NOT NULL,
  state         TEXT NOT NULL,             -- venter|klar|aktiv|utsatt|ferdig|feilet|hoppet_over|kansellert
  attempts_used INTEGER NOT NULL DEFAULT 0,
  max_attempts  INTEGER NOT NULL DEFAULT 1,
  next_attempt_at INTEGER,
  outcome_unknown INTEGER NOT NULL DEFAULT 0,
  skip_reason   TEXT,
  last_error    TEXT,
  started_at    INTEGER, ended_at INTEGER,
  PRIMARY KEY (run_id, name)
);

CREATE TABLE step_attempts (
  id            TEXT PRIMARY KEY,          -- uuidv7; brukt som PULSEQ_ATTEMPT_ID
  run_id        INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_name     TEXT NOT NULL,
  attempt_no    INTEGER NOT NULL,
  state         TEXT NOT NULL,             -- aktiv|ferdig|feilet|avbrutt|kansellert
  claim_epoch   INTEGER NOT NULL,          -- eierens epoch da forsøket startet
  owner         TEXT NOT NULL,
  started_at    INTEGER NOT NULL,
  ended_at      INTEGER,
  pid           INTEGER, pid_start_ticks INTEGER,   -- /proc/<pid>/stat felt 22
  exit_code     INTEGER, signal INTEGER,
  error_class   TEXT,                      -- exit|timeout|avbrutt|drept|spawn_feil|giftig
  error         TEXT,
  outcome_source TEXT,                     -- direkte|spool|utledet
  UNIQUE (run_id, step_name, attempt_no)
);
CREATE INDEX attempts_live ON step_attempts(state) WHERE state = 'aktiv';

CREATE TABLE artifacts (
  id         INTEGER PRIMARY KEY,
  run_id     INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_name  TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  kind       TEXT NOT NULL,                -- file|object|dataset|url
  uri        TEXT NOT NULL,
  size_bytes INTEGER, checksum TEXT,
  created_at INTEGER NOT NULL
);

-- Revisjonsspor ------------------------------------------------------------
CREATE TABLE events (
  id          INTEGER PRIMARY KEY,
  at          INTEGER NOT NULL,
  level       TEXT NOT NULL,               -- debug|info|warn|error
  kind        TEXT NOT NULL,               -- state_change|skip|lease_lost|clock_jump|...
  instance_id TEXT,
  run_id INTEGER, step_name TEXT, attempt_id TEXT,
  schedule_id INTEGER, sensor_id INTEGER, tick_id INTEGER,
  from_state TEXT, to_state TEXT, reason TEXT,
  payload_json TEXT
);
CREATE INDEX events_run ON events(run_id, id);
```

**Regel:** hver state-overgang skriver nøyaktig én `events`-rad i **samme transaksjon** som overgangen. `events` er append-only. Dette gjør `pulseq explain` til en ren spørring, og gir en krasj-konsistent revisjonslogg — det finnes ingen state-endring uten tilhørende forklaring.

---

## 3. State machines

### 3.1 Run

Tilstander (som i prosjektbeskrivelsen): `planlagt`, `aktiv`, `utsatt`, `ferdig`, `feilet`, `kansellert`.

Semantikk:

- `planlagt` — raden finnes og er durabel, ingen eier, ingen sideeffekt har skjedd.
- `aktiv` — nøyaktig én eier holder gyldig lease; null eller flere steg-prosesser kjører.
- `utsatt` — ikke kjørbar akkurat nå. Alltid ledsaget av `defer_reason` og som regel `resume_at`. Årsaker: retry-backoff, ingen ledig concurrency-slot, rekonsiliert etter krasj, venter på manuell avklaring.
- `ferdig` / `feilet` / `kansellert` — terminale for automatikken.

| # | Fra | Til | Utløser | Vakt | Atomisk skriving |
|---|---|---|---|---|---|
| T1 | — | planlagt | trigger materialiseres | — | `INSERT INTO runs ... ON CONFLICT(job_id,run_key) DO NOTHING` + trigger-rad + event |
| T2 | planlagt | aktiv | executor claim | `resume_at<=now`, `cancel_requested_at IS NULL`, ledig slot | `epoch++`, lease, `started_at=COALESCE(started_at,now)` |
| T3 | utsatt | aktiv | executor claim | som T2 | som T2 |
| T4 | planlagt/utsatt | kansellert | cancel-forespørsel | — | direkte, ingen prosess å drepe |
| T5 | planlagt | utsatt | ingen ledig slot | overflow=queue | `defer_reason='concurrency'`, `resume_at=now+backoff` |
| T6 | utsatt | planlagt | `resume_at` nådd | — | reconciler-sweep |
| T7 | aktiv | aktiv | heartbeat | `claim_epoch` matcher | `lease_expires_at=now+ttl` |
| T8 | aktiv | ferdig | alle steg terminale, ingen `feilet` | epoch matcher | `ended_at`, lease frigis |
| T9 | aktiv | feilet | steg endelig feilet + `on_step_failure=fail_run` | epoch matcher | `ended_at`, lease frigis |
| T10 | aktiv | utsatt | alle kjørbare steg i backoff | epoch matcher | `resume_at=min(next_attempt_at)`, lease frigis |
| T11 | aktiv | kansellert | eier observerer `cancel_requested_at` | epoch matcher | drep prosessgruppe → commit |
| T12 | aktiv | utsatt | **reaper**: lease utløpt | `now > lease_expires_at + skew` | `epoch++`, `defer_reason='rekonsiliert_etter_krasj'`, `crash_count++` |
| T13 | aktiv | kansellert | reaper: lease utløpt **og** cancel bedt om | som T12 | `epoch++` |
| T14 | feilet | planlagt | operatør `run retry` | eksplisitt kommando | `generation++`, `epoch++`, event `operator_reopen` |
| T15 | ferdig/feilet | (ny run) | operatør `run replay` | — | ny run med `run_key='<orig>#r<n>'`, `replay_of` satt |
| T16 | planlagt/utsatt | feilet | `crash_count > max_crash_count` | — | `error_class='giftig'`, karantene |

T14 er den eneste overgangen ut av en terminal tilstand. Den krever eksplisitt operatørkommando, logges som egen event-type, og øker `generation` slik at `generation` blir del av fencing. Automatikken kan aldri gjøre T14.

**Krasj-analyse per overgang:** samtlige overganger er én SQLite-transaksjon. Krasj før commit ⇒ ingen effekt. Krasj etter commit ⇒ tilstanden er den nye, og eventuell etterfølgende handling (spawn, kill) gjenopptas av rekonsilieringen. Den eneste overgangen med sideeffekt *før* commit er T11 (drep før commit) — bevisst valgt, fordi dobbelt drap er ufarlig mens dobbel kjøring ikke er.

### 3.2 Step

| Fra | Til | Utløser |
|---|---|---|
| — | venter | run opprettes, steget har uoppfylte avhengigheter |
| — | klar | run opprettes, steget er rot |
| venter | klar | alle upstream `ferdig` |
| venter | hoppet_over | minst én upstream `feilet`/`hoppet_over`/`kansellert` |
| klar | aktiv | attempt-rad committet (§5.5) |
| aktiv | ferdig | exit 0, resultat committet |
| aktiv | utsatt | exit≠0, `attempts_used < max_attempts`, `retry_on` matcher |
| aktiv | feilet | exit≠0 og retry uttømt/ikke tillatt |
| aktiv | feilet (`outcome_unknown=1`) | eier mistet lease / krasj — se §5.7 |
| aktiv | kansellert | cancel observert |
| utsatt | klar | `next_attempt_at` nådd |
| feilet | klar | operatør `run retry --step X` |
| ferdig | klar | operatør `run replay --step X` (nedstrøms steg settes til `venter`) |

### 3.3 Attempt

`aktiv → ferdig | feilet | avbrutt | kansellert`. Attempt-rader er **append-only**; et forsøk gjenbrukes aldri. `attempt_no` er tett fra 1 og oppover per `(run_id, step_name)` på tvers av generasjoner.

### 3.4 Ticks

- Schedule-tick: `planlagt → ferdig | hoppet_over | feilet`. `hoppet_over` krever `skip_reason`.
- Sensor-tick: `aktiv → ferdig | hoppet_over | feilet`.

Alle ticks materialiseres, også de som hoppes over. Det er dette som gjør `explain` mulig: spørsmålet «hvorfor kjørte ikke jobben 03:00?» besvares av en rad, ikke av gjetning.

---

## 4. Garantier og invarianter

### 4.1 Garantier (G)

| Id | Garanti |
|---|---|
| **G1** | **Durabel intensjon.** Når en kommando returnerer OK, er raden committet med `synchronous=FULL` i WAL. Ingen bekreftet beslutning går tapt ved krasj eller strømbrudd. |
| **G2** | **Høyst én run per trigger-identitet.** For enhver `(job_id, run_key)` finnes maksimalt én run-rad, håndhevet av en UNIQUE-indeks — ikke av applikasjonslogikk. Kombinert med at triggeren retryes til suksess gir det *eksakt én* run. |
| **G3** | **At-least-once start av steg.** Et steg som når `ferdig` har blitt utført minst én gang. Ved krasj i vinduet mellom spawn og resultat-commit kan et steg utføres flere ganger. |
| **G4** | **Ingen tapte sensortriggere.** Sensor-cursor rykker aldri frem uten at alle triggere avledet fra det intervallet er committet i samme transaksjon. En krasj gir replay, aldri tap. |
| **G5** | **Fremdrift.** En run i ikke-terminal tilstand vil, gitt at daemonen restarter, i endelig tid enten gjøre fremdrift eller nå terminal tilstand. Ingen run blir permanent «hengende». |
| **G6** | **Høyst én samtidig utføring per steg.** Håndhevet av `flock` (single-node, absolutt) og av lease+fencing (generelt, gitt `lease_ttl > maks stall`). |
| **G7** | **Monotont fencing.** En skriving fra en eier med utdatert `claim_epoch` avvises (0 rader oppdatert), og eieren selv-fencer: dreper prosessgruppen og slipper alt. |
| **G8** | **Kansellering er monoton.** `cancel_requested_at` kan settes, aldri fjernes. En kansellert run starter aldri nye forsøk. |
| **G9** | **Deterministisk planlegging.** Settet av `scheduled_for` for et gitt `(uttrykk, tidssone, intervall)` er uavhengig av når scheduleren faktisk kjørte. To daemoner med samme konfigurasjon og forskjellig oppetid produserer identiske tick-mengder. |
| **G10** | **Fullstendig revisjonsspor.** Hver state-overgang har nøyaktig én `events`-rad committet i samme transaksjon. |

### 4.2 Eksplisitte ikke-garantier

Like viktig, og dokumentert i produktet (ikke bare i planen):

- **Ingen exactly-once sideeffekter.** Steg må selv være idempotente, eller deklarere `on_crash: manual`.
- **Ingen sanntidspresisjon.** Garantien er «ikke før `scheduled_for`», med en beste-innsats `tick_lag`-metrikk. Ingen øvre grense på forsinkelse under last.
- **Ingen global ordning** mellom uavhengige jobber.
- **Ingen beskyttelse mot disk-korrupsjon** utover SQLites egen. Bruk Litestream/backup.
- **Ingen garanti om at logglinjer fra siste millisekunder før strømbrudd er bevart** (`logs.db` kjører `synchronous=NORMAL`).

### 4.3 Invarianter (I) — maskinelt sjekkbare

`pulseq fsck` kjører alle som SQL og rapporterer brudd som `integrity_violation`-events. Kjøres ved oppstart, hver time, og etter hver handling i property-testene.

| Id | Invariant |
|---|---|
| I1 | Ingen run i `aktiv` uten `lease_owner` og `lease_expires_at > now - skew`. |
| I2 | Ingen run i terminal tilstand har steg i `aktiv` eller attempts i `aktiv`. |
| I3 | `UNIQUE(job_id, run_key)` holder (DB-håndhevet; verifiseres likevel). |
| I4 | Høyst én attempt i `aktiv` per `(run_id, step_name)`. |
| I5 | `attempt_no` er tett 1..n uten hull per `(run_id, step_name)`. |
| I6 | Høyst én tick per `(schedule_id, scheduled_for)` (DB-håndhevet). |
| I7 | Hver `sensors.cursor`-verdi har en matchende `sensor_ticks.cursor_after` med `state='ferdig'`. Cursor kan ikke ha rykket frem uten en fullført tick. |
| I8 | Et steg i `klar`/`aktiv` har alle upstream-steg i `ferdig`. |
| I9 | Jobb-DAG-en er asyklisk og alle `depends_on` refererer eksisterende steg. |
| I10 | `runs.state='ferdig'` ⇒ ingen steg i `feilet`; `runs.state='feilet'` ⇒ minst ett steg i `feilet`. |
| I11 | `claim_epoch` er ikke-avtagende (verifiseres mot `events`-historikk). |
| I12 | Antall runs i `aktiv` per `(job_id, concurrency_key)` ≤ `jobs.max_concurrent`. |
| I13 | `ended_at >= started_at >= created_at` for alle rader; alle tidsstempler > 0. |
| I14 | Enhver run i `utsatt` har `defer_reason` satt. Enhver tick i `hoppet_over` har `skip_reason` satt. |
| I15 | Antall `events`-rader med `kind='state_change'` for en run = antall faktiske overganger utledet fra `from_state`/`to_state`-kjeden, og kjeden er sammenhengende. |
| I16 | Ingen attempt i `aktiv` med `owner` som ikke er en instans med `stopped_at IS NULL` og fersk heartbeat. |

---

## 5. Kritiske mekanismer i detalj

### 5.1 Klokker

```go
type Clock interface {
    Now() time.Time            // veggklokke, UTC — kun til logisk tid
    Since(m Mono) time.Duration// monoton — til lease, timeout, backoff
    Mark() Mono
    NewTimer(d time.Duration) *Timer
}
```

Klokkehoppdeteksjon kjøres hvert sekund: sammenlign wall-delta med monoton delta. Ved `|diff| > 5s`:

1. Skriv `clock_jump`-event med retning og størrelse.
2. Ved **negativt** hopp: pause scheduler og reaper i `lease_ttl`, og re-verifiser alle egne leases (`SELECT claim_epoch`). Dette hindrer at reaperen fencer levende eiere fordi wall-klokka plutselig hoppet forbi `lease_expires_at`.
3. Ved **positivt** hopp: la catch-up-logikken (§5.3) håndtere gapet, men med `catchup_window`/`catchup_max_runs` som demper.

Eierens lease-deadline holdes lokalt som monoton verdi. Eieren stopper arbeid når `Since(claimMark) > ttl - safety` uten vellykket fornyelse — uavhengig av hva veggklokka sier. Reaperen bruker wall + `clock_skew_allowance` (default 10 s). Asymmetrien er bevisst: eieren gir opp tidlig, reaperen tar over sent.

### 5.2 Lease og fencing

```sql
-- claim
UPDATE runs
   SET state='aktiv', lease_owner=:me,
       lease_expires_at=:now + :ttl,
       claim_epoch = (SELECT value FROM sequences WHERE name='epoch'),
       started_at = COALESCE(started_at, :now)
 WHERE id=:id
   AND state IN ('planlagt','utsatt')
   AND (resume_at IS NULL OR resume_at <= :now)
   AND cancel_requested_at IS NULL
   AND (SELECT COUNT(*) FROM runs r2
         WHERE r2.job_id = runs.job_id
           AND r2.state='aktiv'
           AND (r2.concurrency_key IS runs.concurrency_key)) < :max_concurrent
RETURNING claim_epoch;
```

`sequences.epoch` inkrementeres i samme transaksjon. Alle etterfølgende skriv fra eieren har `AND claim_epoch = :epoch` i WHERE. **Null rader oppdatert = eierskap tapt** ⇒ selv-fencing.

**Arkitekturtest:** en test som skanner alle SQL-strenger i `internal/store` og feiler hvis en `UPDATE`/`DELETE` mot `runs`, `run_steps` eller `step_attempts` mangler enten `claim_epoch`-predikat eller en eksplisitt `//nolint:fencing`-kommentar med begrunnelse. Fencing-hull skal ikke kunne oppstå ved uoppmerksomhet.

Heartbeat for **alle** aktive runs samles i én transaksjon per intervall (`UPDATE runs SET lease_expires_at=? WHERE lease_owner=? AND state='aktiv'`), slik at heartbeat-frekvensen ikke skalerer med antall runs. Med `synchronous=FULL` er dette forskjellen mellom 1 og N fsync-er per 10 sekunder.

### 5.3 Scheduler: tick-durabilitet, DST, catch-up

**Tick-materialisering i to transaksjoner:**

```
TX-A:  INSERT INTO schedule_ticks(schedule_id, scheduled_for, state='planlagt')
         ON CONFLICT DO NOTHING            -- for alle forfalte scheduled_for
       UPDATE schedules SET next_tick_at = <neste>, last_tick_at = <siste>
       INSERT INTO events(...)
TX-B:  (per tick) INSERT INTO runs(... run_key=<schedule_name>:<scheduled_for>) ON CONFLICT DO NOTHING
       INSERT INTO triggers(...)
       UPDATE schedule_ticks SET state='ferdig'|'hoppet_over', run_id=?, skip_reason=?
       INSERT INTO events(...)
```

Krasj mellom A og B: ticken ligger i `planlagt` og plukkes opp av rekonsilieringen. Fordi `run_key` er deterministisk avledet av `(schedule, scheduled_for)`, er B fullt idempotent. Krasj *i* A: hele A rulles tilbake, `next_tick_at` er uendret, og A gjentas — også idempotent.

**Tidssoner og DST.** All lagret tid er UTC. `robfig/cron/v3` brukes kun som *uttrykksparser*; iterasjonen er vår egen, fordi bibliotekets dokumenterte semantikk («jobs scheduled during daylight-savings leap-ahead transitions will not run») er en implisitt policy uten mulighet for å velge annet.

```
NextN(spec, tz, from, n) []time.Time   // returnerer UTC-tider
```

Policyer per schedule:

| Situasjon | Policy | Oppførsel |
|---|---|---|
| Vårhopp — lokal tid finnes ikke (Europe/Oslo 02:30, 29. mars) | `spring_forward=skip` (default) | Ingen kjøring. Tick materialiseres som `hoppet_over`, `skip_reason='dst_ikke_eksisterende_tid'`. |
| | `spring_forward=shift` | Kjør ved første eksisterende lokale tidspunkt etter overgangen (Vixie cron-lignende). |
| Høsthopp — lokal tid finnes to ganger (Oslo 02:30, 25. oktober) | `fall_back=first` (default) | Kun første forekomst (UTC-tidligste). Andre forekomst materialiseres som `hoppet_over`, `skip_reason='dst_dobbel_tid'`. |
| | `fall_back=both` | Begge forekomster kjører. De har ulik UTC-tid ⇒ ulik `run_key` ⇒ to distinkte runs. |
| Intervall-schedule (`every 5m`) | — | Ren UTC-aritmetikk. DST er per definisjon irrelevant. Dette er den anbefalte formen for alt under 1 times granularitet. |
| tzdata-oppdatering endrer fremtidige offsets | — | `meta.tzdata_version` sammenlignes ved oppstart; ved endring skrives `tzdata_changed`-event og fremtidige `next_tick_at` rekalkuleres. |

`time/tzdata` embeddes i binæret, slik at systemet aldri avhenger av at `/usr/share/zoneinfo` finnes (containere).

**Catch-up.**

| `catchup` | Oppførsel ved gap `[last_tick_at, now]` |
|---|---|
| `none` | Alle missede ticks materialiseres som `hoppet_over`, `skip_reason='catchup_avslatt'`. `next_tick_at` settes til første tick etter `now`. |
| `last` (default) | Kun siste missede tick kjøres. Øvrige `hoppet_over` med `skip_reason='catchup_kun_siste'`. |
| `all` | Alle missede ticks innenfor `catchup_window` kjøres, høyst `catchup_max_runs` materialiseres per scheduler-iterasjon (drypp, ikke storm). Ticks eldre enn vinduet: `hoppet_over`, `skip_reason='utenfor_catchup_vindu'`. |

Fordi hver enkelt misset tick får en rad — også de som hoppes over — kan `pulseq explain schedule <navn> --at <tid>` svare presist på hvorfor noe ikke kjørte. Dette er den direkte oppfyllelsen av kravet «skip with reason».

Beskyttelse mot catch-up-storm er ikke valgfri: standardverdiene (`last`, 24 t vindu, 1 run) betyr at en daemon som har vært nede i to uker starter én run ved oppstart, ikke 20 160.

### 5.4 Concurrency

Fordi `writeDB` har nøyaktig én forbindelse og transaksjoner er `IMMEDIATE`, er «les-så-skriv» innenfor én transaksjon atomisk uten optimistisk låsing. Det gjør concurrency-håndhevingen enkel og korrekt:

- `jobs.max_concurrent` (default **1** — trygg cron-erstatning).
- `concurrency_key_tpl` gir per-nøkkel-serialisering (f.eks. per kunde, per fil).
- `overflow_policy`: `queue` (default, run venter i `utsatt`), `skip` (tick blir `hoppet_over` med `skip_reason='concurrency'`), `replace` (kanseller eldste aktive run for nøkkelen først).
- Globale tak: `max_active_runs`, `max_active_steps` per daemon, satt i konfig.

I12 verifiserer at håndhevingen faktisk holder.

### 5.5 Sensorer: cursor og run_key

Sensor er et eksternt program. Kontrakt:

```
stdin:  {"sensor":"...","cursor":"...|null","last_tick_at":<ms|null>,"now":<ms>}
stdout: {"triggers":[{"run_key":"...","payload":{...}}],
         "cursor":"...","skip_reason":null,"truncated":false}
exit 0 = OK, ellers tick=feilet
```

Evalueringssekvens:

```
1. TX: claim sensor-lease (epoch++), INSERT sensor_ticks(state='aktiv', seq++, cursor_before)
       -- intensjonsraden. Krasj etter dette gir en synlig avbrutt tick.
2.     kjør programmet (ekstern I/O, timeout, prosessgruppe). INGEN DB-skriv her.
3. TX: INSERT triggers, INSERT runs ON CONFLICT DO NOTHING,
       UPDATE sensors SET cursor=:cursor_after,
       UPDATE sensor_ticks SET state='ferdig', cursor_after=:cursor_after, trigger_count=?
       INSERT events
       -- ALT eller INGENTING.
```

**Den absolutte regelen:** cursor committes aldri uten at de tilhørende run-radene committes i samme transaksjon. Krasj før steg 3 ⇒ hele evalueringen gjøres om ⇒ duplikate triggere ⇒ deduplisert av `UNIQUE(job_id, run_key)`. Det gir G4 (ingen tap) med en pris i form av gjentatt evaluering — den riktige avveiningen, siden tap er irreversibelt og gjentakelse ikke er det.

Øvrige regler:

- `run_key` prefikses internt med sensornavn, så to sensorer ikke kolliderer.
- `max_triggers_per_tick` begrenser fan-out; ved overskridelse settes `truncated=1` og cursor rykker **ikke** frem forbi det siste behandlede elementet (sensoren må selv returnere en delvis cursor). Dette er dokumentert som en del av kontrakten.
- `consecutive_failures` gir eksponentiell backoff på `next_tick_at`. Etter `N` (default 20) auto-pauses sensoren med `paused_reason` — fail-safe, slik at en nede ekstern tjeneste ikke blir hamret.
- `min_interval_ms` hindrer hot-loop hvis en sensor returnerer umiddelbart.
- Timeout ⇒ drep prosessgruppen ⇒ tick `feilet`, cursor uendret.

### 5.6 Prosesskjøring og resultat-spool — det viktigste designgrepet

Hvert steg-forsøk kjøres via shimen `pulseq exec`, ikke direkte:

```
daemon --fork--> pulseq exec --exec--> brukerens kommando
         |            |
         |            +-- setter prosessgruppe (Setpgid), egen pgid
         |            +-- leser watchdog-pipe fra daemon
         |            +-- skriver spool/attempts/<attempt_id>.json ved avslutning
         +-- watchdog-pipe (skriveende holdes åpen av daemon)
```

Dette løser fire problemer samtidig:

1. **Foreldreløse prosesser.** Watchdog-pipen får EOF når daemonen dør. Shimen dreper da hele prosessgruppen (`kill(-pgid, SIGTERM)` → grace → `SIGKILL`). Pipe-EOF er mer robust enn `PR_SET_PDEATHSIG`, som i Go er knyttet til tråden som gjorde `fork` og kan utløses feilaktig når Go-runtime avslutter tråden.
2. **Signalisolasjon.** Egen prosessgruppe gjør at Ctrl-C mot daemonen ikke tilfeldig dreper steg, og at drap treffer hele undertreet.
3. **PID-gjenbruk.** `pid_start_ticks` (felt 22 i `/proc/<pid>/stat`) lagres sammen med PID. Før drap verifiseres at prosessen fortsatt har samme starttid — ellers er PID-en gjenbrukt av noe helt annet.
4. **Resultat-outbox.** Shimen skriver resultatet til `spool/attempts/<attempt_id>.json` med `write → fsync → rename → fsync(dir)` **før** den avslutter:

```json
{"attempt_id":"…","claim_epoch":42,"pid":12345,"pid_start_ticks":998877,
 "started_at":…,"ended_at":…,"exit_code":1,"signal":0,"bytes_logged":4096}
```

Konsekvensen er at **resultatet skrives av barneprosessen, ikke av daemonen som kan ha krasjet**. Reconciler konsumerer spool-katalogen ved oppstart og kontinuerlig, matcher mot `step_attempts`, committer utfallet med `outcome_source='spool'`, og sletter filen. Dette lukker krasjvindu W8 (§6) fullstendig — det vinduet som ellers ville gitt flest unødige gjenkjøringer av steg som faktisk var ferdige.

**Policy `on_daemon_restart`:**
- `kill` (default): watchdog-EOF dreper steget; det retryes etter rekonsiliering.
- `detach` (fase 5): shimen overlever daemon-restart; daemonen re-adopterer via spool + `/proc`-verifisering av `PULSEQ_ATTEMPT_ID`. Gir daemon-oppgradering uten å avbryte lange steg.

**Miljø til steget:** `PULSEQ_RUN_ID`, `PULSEQ_RUN_KEY`, `PULSEQ_JOB`, `PULSEQ_STEP`, `PULSEQ_ATTEMPT`, `PULSEQ_ATTEMPT_ID`, `PULSEQ_SCHEDULED_FOR`, `PULSEQ_LOGICAL_FROM`, `PULSEQ_LOGICAL_TO`, `PULSEQ_PAYLOAD`, `PULSEQ_ARTIFACT_DIR`. `PULSEQ_ATTEMPT_ID` brukes også av `/proc`-sweepen til å finne prosesser som ikke tilhører noen kjent aktiv attempt.

### 5.7 Retry-semantikk på steg-nivå — eksakt

Per steg deklareres:

```yaml
steps:
  - name: last_inn
    cmd: ["/opt/etl/load.sh"]
    max_attempts: 3
    backoff: {kind: exponential, base: 10s, max: 10m, jitter: full}
    timeout: 30m
    retry_on: [exit:1, exit:75, timeout]     # default: alle exit != 0 + timeout
    idempotent: true
    on_crash: retry                          # retry | manual
    allow_failure: false
```

Definisjoner:

- **Vanlig feil** (prosessen kjørte og avsluttet med feil): utfallet er *kjent*. Retry hvis `retry_on` matcher og `attempts_used < max_attempts`. Backoff beregnes med monoton klokke; `next_attempt_at` lagres som wall-tid for reaperen.
- **Timeout**: prosessgruppen drepes, `error_class='timeout'`, utfall er kjent (mislykket), men *sideeffekten kan ha skjedd delvis*. Behandles som vanlig feil hvis `idempotent: true`.
- **Krasj/lease-tap** (`outcome_unknown=1`): utfallet er **ukjent**. Dette er den ærlige tilstanden, og den skal aldri skjules.
  - `on_crash: retry` (default): attempt → `feilet` med `error_class='avbrutt'`, nytt forsøk planlegges. Teller mot `max_attempts`, men har i tillegg egen teller `crash_count` med eget tak `max_crash_count` (default 5) for å unngå krasj-løkke på en giftig run.
  - `on_crash: manual`: run → `utsatt` med `defer_reason='krever_manuell_avklaring'`. Krever `pulseq run resolve <id> --step X --outcome ferdig|feilet --reason "..."`. Dette er den eneste korrekte semantikken for ikke-idempotente steg (utbetalinger, e-post, irreversible API-kall).
  - Spool-filen (§5.6) fjerner det meste av usikkerheten: hvis den finnes, er utfallet kjent selv om daemonen krasjet.
- **Poison-pill-beskyttelse** (jf. Sidekiq super_fetch): når `crash_count > max_crash_count` settes run til `feilet` med `error_class='giftig'` og karantene-event. Systemet skal aldri krasje seg selv i en løkke.
- `allow_failure: true` lar nedstrøms steg fortsette selv om steget feiler; run kan da bli `ferdig` med et `feilet` steg (I10 unntas eksplisitt for slike steg).

### 5.8 Kansellering — intensjon adskilt fra effekt

`pulseq run cancel <id>` setter **kun** `cancel_requested_at`, `cancel_reason`, `cancel_requested_by`. Det er en forespørsel, ikke en tilstand.

- Run i `planlagt`/`utsatt`: samme transaksjon setter også `state='kansellert'` (ingen prosess å drepe).
- Run i `aktiv`: eieren observerer flagget ved neste heartbeat (≤10 s), dreper prosessgruppen, committer `state='kansellert'` (T11).
- Eier død: reaperen ser utløpt lease + `cancel_requested_at` ⇒ T13 direkte.

Denne separasjonen gjør kansellering krasj-sikker: forespørselen er durabel før noe drepes, og effekten kan gjentas uten skade.

### 5.9 Rekonsiliering ved oppstart

Kjøres ved daemon-start og deretter hvert 30. sekund (reaper-varianten). Fullt idempotent — kan avbrytes og gjentas når som helst.

```
R0  Ta flock. Les boot_id. Hvis boot_id != meta.current_boot_id:
      maskinen har restartet ⇒ ingen barneprosess kan ha overlevd ⇒
      alle attempts i 'aktiv' er definitivt døde (ikke bare antatt døde).
      Oppdater meta.current_boot_id.
R1  INSERT instances(...). Marker tidligere instanser på denne hosten
      med samme boot_id som stopped_at=now, stop_reason='krasjet'.
R2  Konsumer spool/attempts/*.json:
      match mot step_attempts på attempt_id + claim_epoch.
      Commit kjent utfall (outcome_source='spool'). Slett fil.
      Ukjent attempt_id ⇒ arkiver filen under spool/ukjent/ og skriv warn-event.
R3  UPDATE step_attempts SET state='avbrutt', error_class='avbrutt', ended_at=now
      WHERE state='aktiv' AND (owner IN <døde instanser> OR eierens lease er utløpt).
R4  UPDATE run_steps SET outcome_unknown=1 for de berørte stegene.
R5  Per berørt run: T12 (aktiv → utsatt, epoch++, crash_count++).
      Deretter §5.7-regler: on_crash=retry ⇒ planlegg attempt_no+1;
      on_crash=manual ⇒ defer_reason='krever_manuell_avklaring'.
      crash_count > max ⇒ T16 (giftig).
R6  UPDATE sensor_ticks SET state='feilet' WHERE state='aktiv' AND eier død.
      Cursor røres ikke — det er hele poenget (G4).
R7  Re-materialiser schedule_ticks i 'planlagt' (idempotent via run_key).
      Rekalkuler next_tick_at for alle schedules; håndter gap via catch-up-policy.
R8  Frigi leases på schedules og sensors eid av døde instanser (epoch++).
R9  /proc-sweep: finn prosesser med PULSEQ_ATTEMPT_ID i environ som ikke
      matcher en attempt i 'aktiv' eid av denne instansen.
      Verifiser pid_start_ticks. SIGTERM prosessgruppe → grace 10s → SIGKILL.
R10 UPDATE runs SET state='planlagt' WHERE state='utsatt' AND resume_at <= now.
R11 Kjør fsck (I1–I16). Brudd ⇒ integrity_violation-event + eksponer i
      helsesjekk. Ved kritiske brudd (I3, I6, I9) ⇒ nekt å starte,
      krev 'pulseq fsck --repair' med operatørbekreftelse.
```

R0 er en av de mest verdifulle detaljene: `boot_id` gjør det mulig å **vite** at ingen prosess overlevde, i stedet for å vente på at lease-en utløper. Rekonsiliering etter maskinrestart blir dermed umiddelbar og sikker.

---

## 6. Krasjvindu-katalog

Hvert vindu er nummerert, har en definert konsekvens, og en tilsvarende crash-injection-test (§7.1).

| # | Vindu | Konsekvens ved krasj | Håndtering |
|---|---|---|---|
| W1 | `next_tick_at` beregnet, før commit | Ingen effekt | Gjenberegnes; deterministisk (G9) |
| W2 | Tick committet, før run-materialisering | Tick i `planlagt` | R7, idempotent via `run_key` |
| W3 | Run committet, før claim | Run i `planlagt` | Normal plukking |
| W4 | Claim committet, før attempt-INSERT | Run `aktiv`, utløpt lease | T12 → re-claim. **Ingen sideeffekt hadde skjedd.** |
| W5 | Attempt committet, før `fork` | Attempt `aktiv`, `pid IS NULL` | `outcome_unknown=1`; /proc-sweep gir ingen treff ⇒ ingen prosess startet ⇒ trygg retry |
| W6 | Etter `fork`, før PID committet | Foreldreløs prosess mulig | Watchdog-pipe dreper den; /proc-sweep som backstop |
| W7 | Under kjøring | Steg drepes av watchdog | `outcome_unknown=1` → §5.7 |
| W8 | Barn avsluttet, før resultat committet | **Ville gitt unødig gjenkjøring** | **Lukket av spool-fil (§5.6)** — utfallet er kjent |
| W9 | Resultat committet, før neste steg planlagt | Run `aktiv`, utløpt lease | T12 → re-claim; DAG-tilstand er durabel |
| W10 | Sensor evaluert, før cursor-commit | Evaluering gjentas | Dedup via `run_key` (G4) |
| W11 | Midt i cursor-transaksjonen | Atomisk | SQLite ruller tilbake |
| W12 | `cancel_requested_at` committet, før drap | Prosess lever videre | Eier eller reaper fullfører (T11/T13) |
| W13 | Midt i WAL-skriving / checkpoint | — | SQLite WAL-recovery; `synchronous=FULL` gjør committede transaksjoner durable |
| W14 | Maskinrestart | Alle prosesser døde | R0: `boot_id`-endring gir umiddelbar, sikker rekonsiliering |
| W15 | Midt i rekonsilieringen | Delvis rekonsiliert | Alle R-steg er idempotente; gjentas |
| W16 | Midt i migrasjon | Delvis skjema | Én transaksjon per migrasjon, `user_version` i samme transaksjon |

W5 er det eneste vinduet som ikke kan lukkes fullstendig: det finnes alltid et mikroskopisk intervall mellom `execve` og at prosessen kan observeres. Derfor er G3 formulert som at-least-once, og derfor finnes `on_crash: manual`. Dette skal stå i dokumentasjonen, ikke skjules bak et løfte systemet ikke kan holde.

---

## 7. Testing

Testene er ikke et tillegg — de er hvordan garantiene i §4 gjøres til noe annet enn påstander.

### 7.1 Crash-injection

`internal/faults` definerer navngitte krasjpunkter (W1–W16 + finere granularitet). I produksjonsbygg er `faults.Point(name)` en tom funksjon som kompilatoren fjerner. Under test styres den av `PULSEQ_CRASH_AT`.

```go
faults.Point("W8:etter_barn_exit_for_commit")
```

Harness:

1. Start `pulseq daemon` som **ekte subprosess** med `PULSEQ_CRASH_AT=<punkt>`.
2. Daemonen kaller `syscall.Kill(os.Getpid(), SIGKILL)` når punktet nås — ekte krasj, ingen `defer`, ingen flush, ingen ryddig avslutning.
3. Restart daemonen.
4. Kjør `pulseq fsck` (I1–I16) — må passere.
5. Sammenlign sluttilstand mot forventet tilstandsmengde (ikke én enkelt tilstand — flere er lovlige).
6. Verifiser at ingen foreldreløs prosess lever (`/proc`-skann).
7. Verifiser effekt-telling: en testkommando som appender til en fil, slik at duplikater telles. Assertion er `count >= 1` og `count <= 1 + antall_krasj` — at-least-once med bundet duplikasjon.

Matrisen kjøres for hvert krasjpunkt × hvert scenario (enkelt steg, DAG med fan-out, sensor med triggere, schedule med catch-up, kansellering under kjøring). Dette er den viktigste testsuiten i prosjektet og bygges i fase 1, ikke til slutt.

### 7.2 Property-based / modellbasert (pgregory.net/rapid)

Modell = enkel Go-struct som implementerer de tillatte overgangene fra §3. SUT = ekte implementasjon mot ekte SQLite (tmpfs).

```go
func TestRunStateMachine(t *testing.T) {
    rapid.Check(t, func(t *rapid.T) {
        sut, model := newSUT(t), newModel()
        t.Repeat(map[string]func(*rapid.T){
            "tick":            ..., // scheduler-iterasjon
            "claim":           ...,
            "heartbeat":       ...,
            "expire_lease":    ..., // hopp fake-klokka forbi lease_ttl
            "step_succeed":    ...,
            "step_fail":       ...,
            "step_timeout":    ...,
            "cancel":          ...,
            "crash_restart":   ..., // lukk DB brått, kjør rekonsiliering
            "clock_jump":      ..., // ±rapid.IntRange(-7200, 7200) sekunder
            "sensor_tick":     ...,
            "operator_retry":  ...,
            "": func(t *rapid.T) { assertInvariants(t, sut) },   // I1–I16 etter hver handling
        })
        assertModelEquivalent(t, sut, model)
    })
}
```

Kjøres med `-rapid.checks=10000` i nattlig CI, `-rapid.checks=100` i PR-CI. Feilende seed lagres som `.rapid/*.fail` og sjekkes inn som regresjonstest.

### 7.3 Deterministisk tid

`testing/synctest` (stabil fra Go 1.25) brukes for alle loop-, timeout- og backoff-tester: falsk klokke, deterministisk goroutine-planlegging, ingen `time.Sleep` i tester. En arkitekturtest sikrer at ingen kode utenfor `internal/clock` kaller `time.Now`/`time.After`/`time.Sleep`.

### 7.4 DST- og tidssone-gullstandard

Tabelldrevne tester med forventede UTC-lister, generert offline og sjekket inn som fixtures:

| Sone | Dato | Hva testes |
|---|---|---|
| Europe/Oslo | 2026-03-29 | Vårhopp 02:00→03:00, begge policyer |
| Europe/Oslo | 2026-10-25 | Høsthopp 03:00→02:00, begge policyer |
| America/Santiago | sørlig sommertid | Motsatt rekkefølge av overganger |
| Australia/Lord_Howe | — | 30-minutters DST-hopp |
| Asia/Kolkata | — | Ikke-heltalls offset (+05:30), ingen DST |
| Pacific/Kiritimati | historisk | Datolinjehopp: en hel dag forsvinner |
| UTC | — | Referanse |

I tillegg: differensialtest mot uavhengig implementasjon (fixtures generert med Pythons `zoneinfo` + `croniter`), sjekket inn, kjørt i CI uten nettverk.

### 7.5 Øvrige

- **Klokkehopp-test:** fake clock hopper ±1 t, ±25 t, og NTP-lignende småsprang. Verifiser at ingen lease fences feilaktig og at ingen tick dupliseres.
- **Catch-up-storm:** hopp fake clock 30 døgn frem. Verifiser at `catchup_window`/`catchup_max_runs` holder, og at *alle* missede ticks likevel har en rad med `skip_reason`.
- **Idempotens-test:** kjør samme attempt-utføring to ganger, verifiser at DB-tilstanden er bit-identisk.
- **PID-gjenbrukstest:** manipuler `pid_start_ticks` og verifiser at drapet nektes.
- **Fuzz:** cron-parser (panics), sensor-JSON-parser (panics, minne), `run_key`-generering (kollisjoner).
- **WAL-recovery:** SIGKILL midt i en stor skriveburst × 1000 iterasjoner; `PRAGMA integrity_check` må alltid passere.
- **Soak:** 24 timer, tilfeldig SIGKILL hvert 30.–120. sekund, kontinuerlig `fsck`, sluttverifisering av at effekt-telling er innenfor at-least-once-grensene. Kjøres nattlig.
- **Ytelsesgulv:** benchmark commit-rate med `synchronous=FULL`; feil hvis under terskel (regresjonsvern for utilsiktet `NORMAL`).

---

## 8. Observabilitet

Fordi hver beslutning allerede er en rad, er observabilitet nesten gratis:

- `pulseq run show <id>` — full kjede: trigger → tick → run_key → claims (med epoch) → attempts → utfall → artefakter.
- `pulseq run logs <id> [--step X] [--attempt N] [-f]` — fra `logs.db`, per attempt.
- `pulseq explain run <id>` — hvorfor er den i denne tilstanden? Ren `events`-spørring.
- `pulseq explain schedule <navn> --at <tid>` — hvorfor kjørte den ikke? Leser tick-raden og dens `skip_reason`.
- `pulseq explain sensor <navn>` — siste ticks, cursor-bevegelse, skip-grunner, dedupliserte triggere.
- `pulseq schedule preview <navn> --n 20` — neste 20 tider i lokal tid **og** UTC, med DST-markering.
- `pulseq sensor tick <navn> --dry-run` — evaluer uten å skrive cursor eller opprette runs.
- `pulseq fsck [--repair]` — invarianter I1–I16.
- `pulseq doctor` — PRAGMA-verifisering, diskplass, klokkeavvik mot NTP, tzdata-versjon, foreldreløse prosesser, spool-restanse.
- Strukturert logg (`log/slog`, JSON) med faste felter: `run_id`, `job`, `step`, `attempt`, `attempt_id`, `claim_epoch`, `schedule`, `sensor`, `cursor`, `instance_id`.
- `/metrics` (Prometheus text): `tick_lag_seconds`, `runs_active`, `runs_by_state`, `lease_expirations_total`, `outcome_unknown_total`, `crash_reconciliations_total`, `spool_backlog`, `writer_commit_seconds`, `invariant_violations_total`. De fire siste er de viktigste pålitelighetssignalene.

---

## 9. Teknologivalg

| Valg | Begrunnelse | Vurdert alternativ |
|---|---|---|
| **Go 1.25+** | `testing/synctest` er stabil fra 1.25 og er avgjørende for deterministiske tidstester. Statiske binærer, enkel prosesshåndtering. | — |
| **`modernc.org/sqlite`** | Ren Go, CGO-fri ⇒ statisk binær, triviell krysskompilering, ingen glibc-avhengighet. Ca. 2× tregere enn CGO-varianten, men vår skrivelast er lav og dominert av `fsync`, ikke av SQL-parsing. | `mattn/go-sqlite3` (raskere, men krever CGO). Driveren ligger bak et `Store`-interface og en byggetagg, så bytte er en konfigurasjonsendring. |
| **`synchronous=FULL` på `state.db`** | `NORMAL` i WAL kan rulle tilbake committede transaksjoner ved strømbrudd. Det bryter G1 og dermed hele designet. | `NORMAL` — avvist for beslutningsdata, brukt for `logs.db`. |
| **To DB-filer** | Isolerer loggflom (høy frekvens, lav verdi) fra beslutningstransaksjoner (lav frekvens, høy verdi). Løser samtidig «én skriver»-begrensningen elegant. | Én fil — avvist: loggskriving ville konkurrert om den eneste skriveforbindelsen. |
| **`robfig/cron/v3` kun som parser** | Moden uttrykksparser. Men dens DST-semantikk er implisitt og ikke konfigurerbar («leap-ahead» hoppes stille over). Vi eier iterasjonen selv for å kunne tilby eksplisitt policy og materialiserte skip-grunner. | `adhocore/gronx`, egen parser. Egen parser avvist som unødvendig risiko. |
| **`time/tzdata` embedded** | Binæret må virke i `scratch`-containere uten `/usr/share/zoneinfo`. | Systemets tzdata — avvist som skjør. |
| **`pgregory.net/rapid`** | Førsteklasses støtte for stateful/modellbasert testing (`t.Repeat`) med automatisk shrinking og reproduserbar seed. Dette er nøyaktig verktøyet for state machine-verifisering. | `gopter`, håndskrevne tabelltester. |
| **`log/slog` (stdlib)** | Strukturert logging uten avhengighet. Egen handler som speiler til `events`/`logs.db`. | `zerolog`, `zap` — unødvendig for vår volum. |
| **`spf13/cobra`** | CLI-first er et produktkrav; completions og hjelp gratis. Eneste større avhengighet. | stdlib `flag` — vurdert, men CLI-flaten er stor nok til å rettferdiggjøre cobra. |
| **Migrasjoner via `embed.FS` + `user_version`** | Ingen tredjepart, én transaksjon per migrasjon ⇒ krasj-sikker. | `goose`, `golang-migrate`. |
| **Rå `database/sql` + håndskrevet SQL** | Fencing-predikater og partielle indekser må være eksplisitte og lesbare. ORM skjuler nettopp det som må være synlig. | `sqlc` som mulig fase-2-forbedring (typesikkerhet uten å skjule SQL). |
| **`flock` for single-node-eksklusjon** | Enkleste korrekte løsning. Leases håndterer restart-vinduet og forbereder multi-node. | Bare leases — avvist: unødvendig risiko når OS-en kan garantere eksklusjon. |
| **Litestream som valgfri sidecar** | Kontinuerlig WAL-replikering til objektlager gir sekunders RPO og point-in-time-restore, uten å komplisere Pulseq. Dokumenteres, bygges ikke inn. | Innebygd backup — avvist, ikke vår kjerne. |
| **Web UI: stdlib `net/http` + `html/template`** | Fase 2. Ingen JS-byggekjede, ingen node_modules, én binær. | React/SPA — avvist, bryter «veldig liten kjerne». |

---

## 10. MVP-avgrensning

**Med i MVP** (alt som påvirker krasjkorrekthet):

- Cron- og intervall-schedules med tidssone og eksplisitt DST-policy.
- Sensorer med cursor, `run_key`, skip-grunn, timeout, backoff, auto-pause.
- Runs, steg-DAG, steg-retry med backoff/timeout, kansellering.
- Lease + fencing + heartbeat; `flock`.
- Full rekonsiliering (R0–R11), spool-basert resultatgjenoppretting, `/proc`-sweep.
- `catchup: none|last`, `catchup_window`, `catchup_max_runs`.
- `concurrency`: `max_concurrent`, `concurrency_key`, `overflow_policy`.
- Strukturert logg, `events`, `explain`, `fsck`, `doctor`, `preview`.
- CLI: `init, job apply/list/show, run start/list/show/logs/cancel/retry/replay/resolve, schedule apply/list/preview/pause/resume, sensor apply/list/preview/tick/pause/resume, explain, fsck, doctor, gc, daemon`.
- Crash-injection-suite og property-based state machine-tester.

**Utsatt** (påvirker ikke krasjkorrekthet):

- Web UI, Postgres-backend, multi-node, distribuerte workere.
- `catchup: all`, backfill-kommando, kalenderregler utover cron.
- Dynamic fan-out, artifact lineage utover enkle referanser.
- Notifikasjoner (webhook/kommando) — bygges i fase 4 med egen outbox-tabell og at-least-once-levering.
- Group commit i skriveren, `sqlc`, `on_daemon_restart: detach`.
- Sub-sekund tick-oppløsning.

Begrunnelsen for skillet er enkel: pålitelighetsegenskaper må designes inn fra første commit, mens funksjoner kan legges til når som helst. Å ettermontere fencing eller idempotens i et system som allerede har brukere er en migrering, ikke en feature.

---

## 11. Faseplan fra tomt repo

### Fase 0 — Fundament (uke 1)
Repo, Go-modul, CI (`go test -race`, `go vet`, `staticcheck`, arkitekturtester).
`internal/clock` med fake-implementasjon og klokkehoppdeteksjon.
`internal/store`: migrasjonsrammeverk, to pooler, `Writer.Tx`, PRAGMA-assert ved oppstart.
`instances`, `sequences`, `meta`, `events`, `flock`.
`internal/faults` med no-op i produksjonsbygg.
Tester: PRAGMA-assert, migrasjon-idempotens, SIGKILL under skriveburst → `integrity_check` OK.
**Leveranse:** `pulseq init`, `pulseq doctor`, tom `pulseq fsck`.
**Ferdigkriterium:** 1000 SIGKILL-iterasjoner uten korrupsjon.

### Fase 1 — Runs og steg (uke 2–3) — *tyngdepunktet*
`jobs`/`job_versions`, DAG-validering (asyklisk, topologisk sortering).
`runs`, `run_steps`, `step_attempts`; `internal/model` som ren state machine.
Executor: claim med lease+epoch, batch-heartbeat, selv-fencing.
`internal/exec`: `pulseq exec`-shim, prosessgruppe, watchdog-pipe, spool-skriving, `pid_start_ticks`.
Retry/backoff/timeout, `on_crash`-policy, `outcome_unknown`, poison-pill.
Kansellering (T4/T11/T13). Rekonsiliering R0–R11. `fsck` med I1–I16.
CLI: `job apply/list`, `run start/list/show/logs/cancel/retry/resolve`.
**Tester:** hele crash-injection-matrisen for W3–W9, W12, W14–W15; rapid state machine; `/proc`-sweep; PID-gjenbruk.
**Ferdigkriterium:** en DAG kan kjøres til ende med SIGKILL injisert på hvert krasjpunkt, uten invariantbrudd og med effekt-telling innenfor at-least-once-grensene.

### Fase 2 — Schedules (uke 4)
Schedule-iterator med tidssone og DST-policy; `NextN`.
`schedule_ticks`, TX-A/TX-B-materialisering, catch-up (`none`/`last`), jitter, pause/resume, scheduler-lease.
CLI: `schedule apply/list/preview/pause/resume`, `explain schedule`.
**Tester:** DST-gullstandard (alle soner i §7.4), differensialtest, klokkehopp, catch-up-storm, W1–W2.
**Ferdigkriterium:** G9 (deterministisk planlegging) verifisert ved å kjøre to daemoner med ulik oppetid mot samme spec og sammenligne tick-mengdene.

### Fase 3 — Sensorer (uke 5)
Sensor-protokoll (JSON over stdin/stdout), `sensor_ticks`, cursor-transaksjon, `min_interval`, timeout, `consecutive_failures` med backoff og auto-pause, `max_triggers_per_tick` med `truncated`.
Trigger-dedup med `dedup_of`-sporing.
CLI: `sensor apply/list/preview/tick/pause/resume`, `explain sensor`.
**Tester:** W10–W11; cursor rykker aldri frem uten runs (G4, verifisert med rapid); duplikate triggere dedupliseres; hengende sensor drepes.
**Ferdigkriterium:** 10 000 rapid-iterasjoner med tilfeldige krasj der ingen trigger går tapt.

### Fase 4 — Observabilitet og drift (uke 6)
`logs.db` med egen skriver, loggkvote per attempt, rotasjon og retensjon.
`explain`-kommandoene, `doctor`, `/metrics`, health-endepunkt.
`pulseq gc` med batchvise transaksjoner (500 rader) for å ikke blokkere skriveren.
Notifikasjoner via outbox-tabell, at-least-once levering med egen retry.
Disk-guard: ved lav diskplass gå i `degradert` modus — nekt nye runs, la pågående fullføre.
**Ferdigkriterium:** «hvorfor kjørte ikke X 03:00?» kan besvares med én kommando i alle tilfeller.

### Fase 5 — Hardening (uke 7–8)
24-timers soak med tilfeldig SIGKILL. Group commit i skriveren. Indeks- og spørringsoptimalisering. `on_daemon_restart: detach`. `catchup: all` med drypp. Backfill-kommando. Dry-run.
Dokumentasjon: **garantikontrakten** (§4) som produktdokumentasjon, inkludert ikke-garantiene.
**Ferdigkriterium:** soak-test grønn tre netter på rad; alle invarianter holder; ingen `outcome_unknown` uten forklaring.

### Fase 6 — Etter MVP
Web UI (stdlib). Postgres-backend bak `Store`-interfacet (leases og fencing er allerede designet for det; `SELECT … FOR UPDATE SKIP LOCKED` erstatter single-writer-antagelsen — dette er den eneste antagelsen som må gjennomgås). Multi-node med rådgivende låser. Dynamic fan-out. Artifact lineage.

---

## 12. Risikoer

| # | Risiko | Sannsynlighet | Konsekvens | Tiltak |
|---|---|---|---|---|
| R1 | `synchronous=FULL` begrenser til ~100–200 commits/s | Høy | Gjennomstrømming | Logg i egen fil; batch-heartbeat (én transaksjon uansett antall runs); group commit i fase 5; dokumentert kapasitetsgrense |
| R2 | Watchdog-hull gir foreldreløse prosesser | Middels | Dobbel kjøring | Pipe-EOF **og** prosessgruppe **og** `/proc`-sweep — tre uavhengige lag; egen crash-test |
| R3 | PID-gjenbruk ⇒ drap av uskyldig prosess | Lav | Alvorlig | `pid_start_ticks` verifiseres før hvert drap; alltid drep pgid, aldri bar PID |
| R4 | Feil i egen DST-iterator | Middels | Feil kjøretid, dupliserte runs | Gullstandard-fixtures for 7 soner; differensialtest mot uavhengig implementasjon; `preview` viser både lokal og UTC |
| R5 | Fencing-hull (les epoch → I/O → skriv senere) | Middels | Dobbel kjøring | Arkitekturtest som avviser `UPDATE` uten `claim_epoch`-predikat; rapid-handling `expire_lease` |
| R6 | `outcome_unknown` gir enten duplikat eller stall | Sikker (iboende) | Avhenger av steg | Eksplisitt per-steg-policy; spool-fil fjerner det meste av usikkerheten; dokumentert i garantikontrakten |
| R7 | Klokkehopp bakover ⇒ for tidlig fencing | Middels | Avbrutte steg | Eier bruker monoton klokke; reaper bruker wall + `clock_skew_allowance`; scheduler pauses ved stort negativt hopp |
| R8 | Krasj-løkke på en giftig run | Middels | Systemstans | `crash_count` + `max_crash_count` ⇒ karantene med `error_class='giftig'` |
| R9 | Jobbspec endres mens runs er aktive | Høy | Inkonsistent semantikk | `job_versions`; run peker på versjon; endring lager ny versjon |
| R10 | Loggflom fyller disk ⇒ alle skriv feiler | Middels | Total stans | Kvote per attempt, retensjon, `gc`, disk-guard med degradert modus |
| R11 | tzdata-oppdatering flytter fremtidige ticks | Lav | Overraskelse | `meta.tzdata_version` sammenlignes; `tzdata_changed`-event; `preview` etter oppgradering |
| R12 | `modernc.org/sqlite`-feil eller ytelsesproblem | Lav | Bytte påkrevd | `Store`-interface + byggetagg for `mattn`; benchmark i CI |
| R13 | Sensor-program er ikke idempotent i `run_key` | Høy (brukerfeil) | Dupliserte eller manglende runs | `run_key` er påkrevd i protokollen; `sensor preview` viser genererte nøkler; `explain sensor` viser dedupliserte triggere |
| R14 | Operatør bruker `run retry` (T14) på en run som faktisk fullførte sideeffekten | Middels | Dobbel effekt | T14 krever eksplisitt kommando, logges som `operator_reopen`, og CLI advarer når `outcome_unknown=1` |
| R15 | Single-writer blir en flaskehals ved mange samtidige steg | Middels | Latens | Skrivekø med prioritet (state-overganger foran statistikk); måling via `writer_commit_seconds`; Postgres-vei finnes |

---

## 13. Åpne spørsmål å avklare tidlig

1. **Default for `on_crash`.** Planen setter `retry` som default fordi at-least-once er den deklarerte garantien. Alternativet — `manual` som default — er tryggere, men gjør systemet irriterende for de 95 % av jobbene som er idempotente. Anbefaling: `retry` som default, men `pulseq job apply` **advarer** når et steg mangler eksplisitt `idempotent`-deklarasjon.
2. **`max_concurrent` default 1.** Trygt som cron-erstatning, men overraskende for parallelle arbeidslaster. Anbefaling: behold 1, gjør det svært synlig i `job show` og i skip-grunnen.
3. **Retensjon av `events`.** Full revisjonslogg vokser. Anbefaling: behold `state_change`-events for terminale runs i N dager (default 90), øvrige i 14 dager.
4. **Skal `pulseq exec` kunne kjøres av bruker?** Nei — den er intern. Men den må være i samme binær for at watchdog-kontrakten skal versjoneres sammen med daemonen.
5. **Sub-sekund `scheduled_for`.** Alt lagres i millisekunder, men scheduleren tikker på sekundnivå i MVP. Datamodellen trenger ikke endres senere.

---

## Kilder

- [Temporal — Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures) og [The four types of Activity timeouts](https://temporal.io/blog/activity-timeouts) — heartbeat/lease-timeout-design og at-least-once-semantikk.
- [Dagster — schedules and sensors](https://docs.dagster.io/api/dagster/schedules-sensors) og [clarify guarantees around sensor cursors and failure recovery](https://github.com/dagster-io/dagster/issues/6941) — `run_key`-idempotens og cursor-transaksjonalitet.
- [River — Go/Postgres job queue](https://riverqueue.com/docs) og [Using with SQLite](https://riverqueue.com/docs/sqlite) — unike jobber, lederelection, transaksjonell køing.
- [Sidekiq — Reliability / super_fetch](https://github.com/sidekiq/sidekiq/wiki/Reliability) — orphan-deteksjon via heartbeat-utløp og poison-pill-håndtering.
- [Martin Kleppmann — How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) — fencing tokens.
- [How Debian Cron Handles DST Transitions](https://blog.healthchecks.io/2021/10/how-debian-cron-handles-dst-transitions/) — DST-policyer for vår-/høsthopp.
- [Airflow — Dag Runs og catchup](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html) — catch-up-semantikk og catch-up-storm.
- [SQLite's Durability Settings are a Mess](https://www.agwa.name/blog/post/sqlite_durability) og [SQLite Forum — sync=NORMAL, WAL](https://sqlite.org/forum/info/9d6f13e346231916) — `synchronous=FULL` vs `NORMAL` i WAL.
- [PSA: Your SQLite Connection Pool Might Be Ruining Your Write Performance](https://emschwartz.me/psa-your-sqlite-connection-pool-might-be-ruining-your-write-performance/) — single-writer-pool og `BEGIN IMMEDIATE`.
- [Go — Testing concurrent code with testing/synctest](https://go.dev/blog/synctest) — deterministisk tid i tester.
- [rapid — property-based testing for Go](https://github.com/flyingmutant/rapid) — `t.Repeat` for modellbasert state machine-testing.
- [(Mostly) Deterministic Simulation Testing in Go](https://www.polarsignals.com/blog/posts/2024/05/28/mostly-dst-in-go) — krasj- og feilinjeksjon.
- [robfig/cron v3](https://pkg.go.dev/github.com/robfig/cron/v3) — tidssoner og dokumentert leap-ahead-oppførsel.
- [Litestream](https://litestream.io/) — kontinuerlig SQLite-replikering og point-in-time-restore.
