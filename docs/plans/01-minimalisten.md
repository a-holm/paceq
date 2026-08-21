# Pulseq — prosjektplan: Minimalisten

> Perspektiv: UNIX-filosofi. Liten kjerne, komposisjon, tekst som grensesnitt.
> Forbilder: cron, systemd timers, runit, redo, SQLite selv.
> Måltall: **≤ 4000 linjer ikke-test Go i MVP**, **≤ 4 direkte avhengigheter**, **5 tabeller**, **1 binærfil**, **1 databasefil**.

---

## 1. Tesen

Pulseq er ikke en plattform. Pulseq er **en beslutningsmotor over SQLite pluss en prosessutfører**.

All verdi i prosjektbeskrivelsen kan reduseres til tre mekanismer:

| # | Mekanisme | Ansvar |
|---|-----------|--------|
| A | **Evaluator-løkke** | Bestemmer periodisk *om* arbeid skal starte, og materialiserer beslutningen som rader |
| B | **Transaksjonell tilstandsmaskin** | Holder runs og steps i en gjenopprettbar tilstand i én SQLite-fil |
| C | **Prosessutfører** | `fork`/`exec` av argv, med miljø inn og exit-kode ut |

Alt annet i beskrivelsen — sensors, schedules, observabilitet, explain, retries, DAG — er **konfigurasjon av eller spørringer over** disse tre. Der man tror man trenger et nytt subsystem, trenger man nesten alltid en ny kolonne eller et nytt program.

### 1.1 Fire reduksjoner som gjør kjernen liten

**Reduksjon 1 — schedules og sensors er samme ting.**
Begge er «en periodisk evaluator som returnerer et sett med `(run_key, env)` eller en skip-grunn». Forskjellen er kun hvor evaluatoren bor: cron-aritmetikk innebygd, eller et eksternt program. Én tabell (`source`), én løkke, én kodesti. Dette halverer schedule/sensor-koden og gjør at pause, timezone, historikk, concurrency og explain implementeres **én gang**.

**Reduksjon 2 — kjernen tolker aldri brukerkode.**
Et steg er `execve(argv, env)`. Ingen SDK, ingen plugin-ABI, ingen innebygd scripting. Brukerens språk er operativsystemets prosessmodell. Dette fjerner hele plugin-, versjonerings- og sandkasse-flaten som gjør Airflow/Dagster tunge.

**Reduksjon 3 — logger er filer, ikke rader.**
Ett filsystem-tre `$STATE/logs/<run>/<step>.<attempt>.log`. Da er `pulseq logs` i praksis `cat`, brukeren har `grep`/`less`/`tail -f`/`logrotate` gratis, og den ene skriveforbindelsen til SQLite belastes ikke med loggvolum. Kjernen skriver **null loggrader**.

**Reduksjon 4 — SQLite-filen er IPC.**
Ingen socket, ingen gRPC, ingen HTTP i MVP. CLI-en skriver korte transaksjoner rett i databasen; daemonen ser dem innen ett tick. Én fil er både state, kø, historikk og kontrollplan.

### 1.2 SQLites én-skriver-begrensning er en gave, ikke et problem

Én orchestrator trenger nøyaktig **ett serialisert beslutningspunkt** for at «hvem eier denne runen» skal være avgjørbart uten distribuert konsensus. SQLite gir det gratis. Vi omgår ikke begrensningen — vi bygger korrektheten på den.

---

## 2. Harde grenser: hva som ALDRI skal inn i kjernen

Disse er ikke «senere», de er **nei**. Hver enkelt har en definert komposisjonsvei ut av kjernen.

| Aldri i kjernen | Hvorfor | Løsning utenfor |
|---|---|---|
| Innebygd scriptspråk (Lua/Starlark/CEL/JS) | Gjør konfigurasjon til programmering, gjør statisk validering umulig | Skriv et program |
| Plugin-system (`.so`, RPC-plugins, Go-interfacer for brukere) | Versjonshelvete, ABI-brudd, låser brukeren til Go | Sensorer og steg *er* programmer |
| Notifikasjonsintegrasjoner (Slack, e-post, PagerDuty, webhooks ut) | Uendelig hale av integrasjoner, hemmeligheter, retry-logikk | `pulseq watch --json \| din-varsler`, eller en `on_failure`-job |
| Postgres / MySQL / multi-node | Krever leader election, klokkesynk, migrasjonslag — 3× kodebasen | Kjør én daemon per statedir; skalér ved å dele jobber |
| Container-/k8s-driver | Kjernen skal ikke kunne noe om Docker | Skriv `run = ["docker","run",...]` |
| Hemmeligheter/secret store | Sikkerhetsansvar vi ikke kan bære | `env_file` med `chmod 600` (systemd `EnvironmentFile=`-modellen) |
| Brukere, roller, auth, multi-tenancy | Filsystemrettigheter finnes allerede | Unix-brukere og rettigheter på statedir/configdir |
| Templating i konfig (Jinja/Go-template/variabel-interpolasjon) | Konfig blir uleselig og utestbar | Generer TOML med et program hvis du må |
| CLI-rammeverk (cobra/urfave) | Titalls avhengigheter og oppfordrer til subkommando-sprawl | `flag` + en `switch` |
| ORM / query builder / migrasjonsbibliotek | Skjuler nøyaktig det vi vil se: SQL-en | Håndskrevet SQL, `PRAGMA user_version` |
| Innebygd HTTP-API for jobbdefinisjon | Gjør DB til sannhetskilde, konfig blir uverifiserbar | Konfig er filer; `SIGHUP` laster inn |
| Metrics-SDK (OpenTelemetry, Prometheus-server) | Tung avhengighet for noen tellere | `pulseq stats --prometheus` skriver til stdout → textfile-collector |
| Implisitt shell | `sh -c` gir siteringsfeil og injeksjonsflater | `run` er argv-array; skriv `["/bin/sh","-lc","…"]` eksplisitt om du vil |
| Distribuert steg-utføring (steg av samme run på ulike noder) | Krever fencing tokens, artifact-transport, nettverksfeilmodeller | Én run kjører helt inne i én prosess |
| Betingelser/`if` i DAG-en | Uttrykksspråk snikinnfører reduksjon 2 bakveien | Steget avslutter tidlig med exit 0 |
| Auto-reload ved filendring (fsnotify) | Uforutsigbar produksjon | `pulseq check` + `SIGHUP`, som `nginx -t` + `reload` |

**Regelen for framtidige forslag:** en feature slipper inn i kjernen kun hvis den (a) ikke kan uttrykkes som et program, (b) trengs av *alle* brukere, og (c) ikke legger til en tabell eller en avhengighet. Ellers er svaret: skriv et program.

---

## 3. Arkitektur

### 3.1 Prosesser

```
                      /etc/pulseq/jobs/*.toml      (sannhetskilde for definisjoner)
                                 |
                            pulseq load / SIGHUP
                                 v
   +---------------------------------------------------------------+
   |  pulseqd  (én prosess per statedir, sikret med flock)          |
   |                                                                |
   |   tick-løkke (1 Hz)                                            |
   |     -> evaluate(source)   cron-aritmetikk | exec(sensor)       |
   |     -> WRITE TX: insert tick + triggers + cursor  (atomisk)    |
   |     -> reconcile: utløpte leases -> failed                     |
   |                                                                |
   |   dispatcher                                                   |
   |     -> WRITE TX: claim pending run (lease)                     |
   |     -> goroutine per run: topologisk steg-utføring             |
   |          -> exec(argv) i egen prosessgruppe                    |
   |          -> stdout/stderr -> fil                               |
   |          -> WRITE TX: steg-overgang                            |
   +---------------------------------------------------------------+
                    |                          |
              pulseq.db (WAL)           logs/<run>/<step>.<n>.log
                    ^
                    |  korte skrivetransaksjoner (busy_timeout)
              pulseq CLI  (list, explain, run, retry, cancel, pause, logs, watch)
```

Ingen nettverk. Ingen socket. Ingen kø-broker. `pulseqd` og `pulseq` er **samme binærfil** (`pulseq daemon`).

### 3.2 Pakkestruktur (6 pakker, hardt tak)

```
cmd/pulseq/main.go       ~200 l   subkommando-dispatch (flag)
internal/spec            ~450 l   TOML-innlasting, validering, syklusdeteksjon
internal/store           ~700 l   schema.sql (embed), skrive-/lesepool, all SQL
internal/engine          ~900 l   tick, evaluator, trigger, dispatcher, reconcile, tilstandsmaskin
internal/runner          ~400 l   exec, prosessgruppe, timeout, logfil, output-fil
internal/cli             ~800 l   subkommandoer, tabellutskrift, --json
```

`internal/store` er det eneste stedet som kjenner SQL. `internal/engine` er det eneste stedet som kjenner tilstandsmaskinen. `internal/runner` er det eneste stedet som kjenner `os/exec`. Ingen andre abstraksjonslag — særlig **ingen repository-interfacer «for testbarhet»**; testene bruker en ekte SQLite-fil i `t.TempDir()`.

Eneste interface i hele kodebasen: `Clock { Now() time.Time }`, fordi tid må kunne forfalskes i tester.

---

## 4. Datamodell

Fem tabeller. Hver ny tabell etter dette krever en eksplisitt beslutning.

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- 1) Jobbdefinisjon, speilet fra konfigfil. Filen er sannhet; raden er cache + historikk.
CREATE TABLE job (
  name            TEXT PRIMARY KEY,
  source_path     TEXT    NOT NULL,
  spec_hash       TEXT    NOT NULL,          -- sha256 av kanonisert spec
  spec_json       TEXT    NOT NULL,
  workdir         TEXT    NOT NULL DEFAULT '',
  env_file        TEXT    NOT NULL DEFAULT '',
  max_parallel    INTEGER NOT NULL DEFAULT 4,   -- steg samtidig i én run
  max_concurrent  INTEGER NOT NULL DEFAULT 1,   -- aktive runs av jobben, 0 = ubegrenset
  timeout_ms      INTEGER NOT NULL DEFAULT 0,
  paused          INTEGER NOT NULL DEFAULT 0,
  loaded_at       INTEGER NOT NULL
) STRICT;

-- 2) Trigger-kilde. Schedules OG sensors. Én tabell, én løkke.
CREATE TABLE source (
  id            INTEGER PRIMARY KEY,
  job_name      TEXT    NOT NULL REFERENCES job(name) ON DELETE CASCADE,
  name          TEXT    NOT NULL,
  kind          TEXT    NOT NULL CHECK (kind IN ('cron','interval','sensor')),
  expr          TEXT    NOT NULL,             -- cron-uttrykk | varighet | argv som JSON
  timezone      TEXT    NOT NULL DEFAULT 'UTC',
  interval_ms   INTEGER NOT NULL DEFAULT 0,   -- sensor: evalueringsfrekvens
  timeout_ms    INTEGER NOT NULL DEFAULT 30000,
  catchup       INTEGER NOT NULL DEFAULT 0,
  catchup_limit INTEGER NOT NULL DEFAULT 1,
  paused        INTEGER NOT NULL DEFAULT 0,
  cursor        TEXT,                          -- watermark, ugjennomsiktig for kjernen
  next_fire_at  INTEGER,                       -- unix ms UTC
  last_tick_at  INTEGER,
  fail_streak   INTEGER NOT NULL DEFAULT 0,    -- for backoff ved evalueringsfeil
  UNIQUE (job_name, name)
) STRICT;

-- 3) Append-only beslutningslogg. Hele "explain"-funksjonen er SELECT herfra.
CREATE TABLE tick (
  id           INTEGER PRIMARY KEY,
  source_id    INTEGER NOT NULL REFERENCES source(id) ON DELETE CASCADE,
  scheduled_at INTEGER,                        -- for cron: hvilket tick dette representerer
  started_at   INTEGER NOT NULL,
  ended_at     INTEGER,
  outcome      TEXT    NOT NULL CHECK (outcome IN ('fired','skipped','error')),
  reason       TEXT,                           -- skip_reason eller feilmelding
  emitted      INTEGER NOT NULL DEFAULT 0,     -- antall nye runs
  deduped      INTEGER NOT NULL DEFAULT 0,     -- antall run_key som allerede fantes
  cursor_after TEXT
) STRICT;
CREATE INDEX tick_by_source ON tick(source_id, started_at DESC);

-- 4) Én konkret kjøring.
CREATE TABLE run (
  id          INTEGER PRIMARY KEY,
  job_name    TEXT    NOT NULL REFERENCES job(name),
  source_id   INTEGER REFERENCES source(id) ON DELETE SET NULL,   -- NULL = manuell
  tick_id     INTEGER REFERENCES tick(id)   ON DELETE SET NULL,
  run_key     TEXT    NOT NULL,               -- idempotensnøkkel
  state       TEXT    NOT NULL CHECK (state IN ('pending','running','success','failed','cancelled')),
  env_json    TEXT    NOT NULL DEFAULT '{}',
  not_before  INTEGER NOT NULL DEFAULT 0,     -- "utsatt": backoff / planlagt start
  created_at  INTEGER NOT NULL,
  started_at  INTEGER,
  ended_at    INTEGER,
  lease_owner TEXT,                            -- "<host>/<pid>/<boot-nonce>"
  lease_until INTEGER,
  UNIQUE (job_name, run_key)                   -- HELE dedupliseringen bor her
) STRICT;
CREATE INDEX run_ready  ON run(not_before) WHERE state = 'pending';
CREATE INDEX run_active ON run(job_name)   WHERE state IN ('pending','running');
CREATE INDEX run_recent ON run(created_at DESC);

-- 5) Steg i en run. Grafen er snapshotet her ved run-opprettelse.
CREATE TABLE step (
  run_id      INTEGER NOT NULL REFERENCES run(id) ON DELETE CASCADE,
  name        TEXT    NOT NULL,
  ord         INTEGER NOT NULL,
  needs_json  TEXT    NOT NULL DEFAULT '[]',
  argv_json   TEXT    NOT NULL,
  state       TEXT    NOT NULL CHECK (state IN ('pending','running','success','failed','skipped')),
  attempt     INTEGER NOT NULL DEFAULT 0,
  max_attempt INTEGER NOT NULL DEFAULT 1,
  backoff_ms  INTEGER NOT NULL DEFAULT 0,
  timeout_ms  INTEGER NOT NULL DEFAULT 0,
  not_before  INTEGER NOT NULL DEFAULT 0,
  exit_code   INTEGER,
  error       TEXT,
  output_json TEXT    NOT NULL DEFAULT '{}',   -- det steget skrev til $PULSEQ_OUTPUT
  started_at  INTEGER,
  ended_at    INTEGER,
  PRIMARY KEY (run_id, name)
) STRICT;
```

**Bevisste utelatelser:**

- *Ingen `log`-tabell.* Logger er filer (reduksjon 3).
- *Ingen kant-/DAG-tabell.* `needs_json` på steget er grafen. Grafen er liten og leses alltid sammen med steget.
- *Ingen `artifact`-tabell i MVP.* `step.output_json` bærer både artefakt-pekere og verdier mellom steg. Ett mekanisme, to behov. Indeksering/lineage er fase 5.
- *Ingen spec-versjonstabell.* `step`-radene *er* snapshotet av definisjonen på kjøretidspunktet.
- *Ingen lock-tabell for scheduler-leder.* `flock()` på `$STATE/pulseqd.lock` er UNIX-primitivet for «kun én daemon».
- *Ingen per-steg lease.* En run eies i sin helhet av én prosess; run-lease dekker alle steg.

### 4.1 Tilstandsmaskiner

```
run:   pending ──claim──> running ──┬─> success
         ^  │                       ├─> failed      (retry -> pending)
         │  └──cancel───────────────┴─> cancelled
         └── not_before = t   ("utsatt": backoff, planlagt, catchup)

step:  pending ──> running ──┬─> success
         ^                   ├─> failed   (attempt < max  =>  pending, not_before = backoff)
         │                   └─ (oppstrøms feilet) ──> skipped
         └─────────────────────────────────────────┘
```

«Utsatt» er ikke en tilstand, det er `not_before > now`. Dette sparer to tilstander og all tilhørende overgangslogikk.

**Invarianter (håndhevet i test):**

1. En run går aldri til `running` uten en gyldig lease.
2. Et steg går aldri til `running` før alle `needs` er `success`.
3. Run er `success` ⟺ alle steg er `success` eller `skipped` uten feil oppstrøms.
4. `UNIQUE(job_name, run_key)` er den eneste dedupliseringsmekanismen — ingen applikasjonskode sjekker duplikater.
5. Ingen `exec` og ingen nettverks-I/O inne i en skrivetransaksjon. Aldri.

---

## 5. Grensesnitt

Pulseqs «SDK» er en håndfull miljøvariabler og to filformater. Det finnes ingen bibliotek å importere, på noe språk.

### 5.1 Konfigurasjon: TOML i en drop-in-katalog

`/etc/pulseq/jobs/*.toml` — som `systemd`s `*.d`-kataloger og `conf.d`-konvensjonen.

```toml
[job]
name           = "nightly-report"
workdir        = "/srv/report"
env_file       = "/etc/pulseq/env/report.env"   # valgfri, chmod 600
max_parallel   = 4
max_concurrent = 1
timeout        = "30m"

[[step]]
name    = "extract"
run     = ["/usr/local/bin/extract.sh"]         # argv, aldri shell
retries = 3
backoff = "30s"                                  # eksponentiell fra denne
timeout = "10m"

[[step]]
name = "transform"
needs = ["extract"]
run   = ["python3", "transform.py"]

[[step]]
name = "load"
needs = ["transform"]
run   = ["/usr/local/bin/load.sh"]

[[schedule]]
name          = "nightly"
cron          = "0 3 * * *"
timezone      = "Europe/Oslo"
catchup       = true
catchup_limit = 3

[[sensor]]
name     = "new-files"
run      = ["/usr/local/bin/watch-inbox.sh"]
interval = "60s"
timeout  = "30s"
```

Hvorfor **TOML**: entydig, ingen innrykksfeller (YAMLs klassiske problem), kommentarer, ingen typegjetting (`no` blir ikke `false`), og en jobbfil er flat nok til at TOMLs svake nesting ikke merkes. **Ikke YAML** (innrykk + implisitt typing = produksjonsfeil). **Ikke HCL** (et språk, ikke et format — bryter grense «ingen scripting»). **Ikke JSON** (ingen kommentarer).

Ukjente nøkler er **feil**, ikke advarsel. `pulseq check` validerer uten å laste.

### 5.2 Steg-kontrakten

Inn (miljø):

```
PULSEQ_RUN_ID      PULSEQ_JOB        PULSEQ_STEP     PULSEQ_ATTEMPT
PULSEQ_RUN_KEY     PULSEQ_SOURCE     PULSEQ_STATE_DIR
PULSEQ_OUTPUT      # sti å skrive JSONL til: {"key":"value"}
PULSEQ_OUTPUTS     # sti til flettet JSON av alle oppstrøms-steg sine outputs
```

Ut: **exit-kode**. 0 = suksess, alt annet = feil. `stdout` og `stderr` går til loggfilen.

Det er hele kontrakten. Et fullgodt steg kan være `["/bin/true"]`.

### 5.3 Sensor-kontrakten

Inn (miljø): `PULSEQ_SOURCE`, `PULSEQ_JOB`, `PULSEQ_CURSOR` (tom første gang), `PULSEQ_LAST_TICK`, `PULSEQ_STATE_DIR`.

Ut: **JSON Lines på stdout**, én linje per objekt, tolket etter nøkkel:

```json
{"run_key": "inbox/2026-08-21/a1b2.csv", "env": {"FILE": "/inbox/a1b2.csv"}}
{"run_key": "inbox/2026-08-21/c3d4.csv", "env": {"FILE": "/inbox/c3d4.csv"}}
{"cursor": "2026-08-21T04:59:12Z"}
```

eller

```json
{"skip": "ingen nye filer siden 2026-08-21T04:00:00Z"}
```

Exit 0 = tick gyldig; triggere, cursor og tick-raden committes **i én transaksjon**. Exit ≠ 0 = evalueringsfeil; cursor uendret, `fail_streak++`, eksponentiell backoff. Dette gir eksakt-én-gang cursor-fremdrift over minst-én-gang run-opprettelse — den riktige kombinasjonen.

En komplett sensor i shell:

```sh
#!/bin/sh
find /inbox -type f -newermt "@${PULSEQ_CURSOR:-0}" |
  while read -r f; do
    printf '{"run_key":"%s","env":{"FILE":"%s"}}\n' "$(basename "$f")" "$f"
  done
printf '{"cursor":"%s"}\n' "$(date +%s)"
```

Åtte linjer. Ingen import, ingen registrering, ingen deploy. **Dette er produktets unike salgsargument.**

### 5.4 CLI

```
pulseq check [fil...]            valider konfig, exit != 0 ved feil
pulseq load                      les /etc/pulseq/jobs, upsert job+source
pulseq daemon                    kjør tick-løkke og dispatcher (flock)

pulseq jobs                      list jobber
pulseq sources [job]             list schedules og sensors, next fire
pulseq runs [--job J] [--state S] [--limit N]
pulseq show <run>                steg-tre med tilstand, exit-kode, varighet
pulseq logs <run> [step] [-f]    cat/tail loggfil
pulseq explain <job/source>      hvorfor kjørte / kjørte ikke; siste ticks; neste tick
pulseq explain run <id>          hvorfor hvert steg fikk sin tilstand

pulseq run <job> [--key K] [--env K=V] [--wait]   manuell trigger
pulseq retry <run> [--step S]    kjør feilede + nedstrøms steg på nytt, i samme run
pulseq replay <run>              ny run, samme env, alt fra bunn
pulseq cancel <run>
pulseq pause|resume <job|job/source>
pulseq watch [--json]            strøm av tilstandsoverganger (JSONL)
pulseq prune --older-than 30d    slett gamle runs, ticks og loggfiler
pulseq stats [--prometheus]
```

Alle liste-kommandoer tar `--json` og skriver JSON Lines. Standard er tab-justerte kolonner for mennesker. `pulseq run --wait` avslutter med runens utfall som exit-kode — da komponerer Pulseq med *seg selv* og med cron.

---

## 6. Teknologivalg

| Valg | Begrunnelse | Vurdert alternativ og hvorfor forkastet |
|---|---|---|
| **`modernc.org/sqlite`** | Ren Go → `CGO_ENABLED=0`, statisk enkeltfil-binær, triviell krysskompilering. Skrivevolumet vårt er noen få transaksjoner per sekund, så CGO-ytelsen trengs ikke. | `mattn/go-sqlite3`: raskere, men krever CGO og ødelegger statisk bygg + krysskompilering. `zombiezen`: ingen `database/sql`-driver. Byttekostnaden er én `sql.Open`-linje i `internal/store` — vi bygger *ingen* driverabstraksjon for å bevare valget. |
| **`github.com/robfig/cron/v3`** | Brukes **kun som parser**: `ParseStandard(expr).Next(t)`. Vi bruker aldri dens scheduler eller goroutiner. Ingen egne avhengigheter. Støtter `CRON_TZ=`, `@every`, `@daily`. Streng standardparser uten `L`/`W`/`#` — utvidet syntaks er en feature vi *ikke* vil ha. | `adhocore/gronx`: støtter `L`/`W`/`#`, altså mer uttrykkskraft enn ønsket. Egen parser: ~500 linjer og en klasse subtile feil vi ikke trenger å eie. (Vendoring av kun parserfilen er en åpen mulighet hvis vi vil ned til 3 avhengigheter.) |
| **`github.com/BurntSushi/toml`** | Referanseimplementasjon, ingen avhengigheter, `MetaData.Undecoded()` gir gratis avvisning av ukjente nøkler. | `pelletier/go-toml/v2` er også godt; valget er smakssak. YAML/HCL avvist i §5.1. |
| **`log/slog` (stdlib)** | `slog.NewJSONHandler(os.Stderr, …)` gir strukturert JSONL med `job`, `source`, `run`, `step`, `attempt`, `run_key`, `cursor`. journald fanger stderr. `AddSource` av i produksjon. | zerolog/zap: raskere, men vi logger hundrevis av linjer per minutt, ikke millioner. Null grunn til en avhengighet. |
| **`flag` (stdlib)** | `flag.NewFlagSet` per subkommando + `switch os.Args[1]`. ~150 linjer. | cobra: ~20 transitive avhengigheter, generert hjelpetekst vi ikke liker, og oppfordrer til subkommando-sprawl. Hard grense. |
| **`os/exec` + `syscall` (stdlib)** | `SysProcAttr{Setpgid: true}`, drep hele prosessgruppen med `syscall.Kill(-pgid, …)`: SIGTERM, vent `grace`, så SIGKILL. Uten prosessgruppe blir barnebarn foreldreløse. | Ingen. |
| **`embed` + `PRAGMA user_version`** | Migrasjoner er en `[]string` av SQL-steg indeksert på versjonsnummer. ~40 linjer. | goose/atlas/golang-migrate: en avhengighet for 40 linjer. |
| **`text/tabwriter`, `encoding/json`, `net/http`+`html/template` (fase 5)** | Stdlib dekker hele presentasjonslaget. | Ingen JS-byggkjede. Noensinne. |

**Sum: 3 direkte avhengigheter i MVP.** Alle uten transitive avhengigheter.

### 6.1 SQLite-oppsett — konkret

```go
// Skrivepool: nøyaktig én forbindelse, alltid IMMEDIATE-transaksjoner.
w, _ := sql.Open("sqlite", state+"/pulseq.db?"+
    "_txlock=immediate&_timeout=5000&"+
    "_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&"+
    "_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
w.SetMaxOpenConns(1)

// Lesepool: mange forbindelser, WAL gir snapshot-isolasjon.
r, _ := sql.Open("sqlite", state+"/pulseq.db?_timeout=5000&"+
    "_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
r.SetMaxOpenConns(8)
```

Fire regler som gjør én-skriver-problemet borte:

1. **`_txlock=immediate` på skrivepoolen.** Utsatte transaksjoner som oppgraderer til skriv midt i, gir `SQLITE_BUSY` som ikke kan retryes trygt. Ta skrivelåsen med én gang.
2. **`SetMaxOpenConns(1)` på skrivepoolen.** Go serialiserer for oss; SQLite ser aldri to skrivere fra samme prosess.
3. **`busy_timeout=5000` for tverr-prosess.** CLI og daemon konkurrerer om samme lås; SQLite venter i stedet for å feile.
4. **Alle skrivetransaksjoner er under 1 ms.** Håndheves av invariant 5 (ingen `exec`/nettverk i transaksjon) og av en test som teller transaksjonsvarighet under lastkjøring.

Avvist alternativ: **flere SQLite-filer**. Det ville ødelagt hele korrektheitsargumentet — «insert tick + triggers + cursor atomisk» krever at de tre bor i samme transaksjon, altså samme fil.

---

## 7. MVP: hva som er med og hva som kuttes

### 7.1 Med i MVP

- Cron- og interval-schedules, per-source timezone, `next`-preview, pause/resume, catchup med tak.
- Sensors som eksterne programmer, JSONL-protokoll, cursor, multi-trigger, skip-grunn, backoff ved feil.
- Idempotens via `UNIQUE(job_name, run_key)`; «maks én aktiv run per jobb» via `max_concurrent`.
- DAG med `needs`, parallelle steg, `max_parallel`, syklusdeteksjon ved innlasting.
- Retry per steg med eksponentiell backoff; timeout per steg og per job; prosessgruppe-drap.
- `retry` (bare feilede + nedstrøms, i samme run) og `replay` (ny run fra bunn).
- Run/step-historikk, tick-historikk med skip-grunner, `explain` for både source og run.
- Loggfiler per steg-forsøk; strukturert JSONL fra daemonen til stderr.
- Lease + rekonsiliering ved oppstart; `flock` mot dobbeltstart.
- Full CLI med `--json` overalt.

### 7.2 Kuttet fra MVP, med begrunnelse

| Kuttet | Begrunnelse | Kommer i |
|---|---|---|
| Web-UI | CLI dekker alle observabilitetskravene. UI er lesing av samme data. | Fase 5 |
| Backfill | `catchup` dekker det vanlige tilfellet (maskinen var nede). Vilkårlig historisk backfill er en egen kommando, ikke en kjerneendring. | Fase 5 |
| Dynamisk fan-out i DAG-en | Sensorer *er* fan-out (én run per fil). Fan-out inne i en run krever dynamisk grafmutasjon = ny tabell + ny tilstandslogikk. | Fase 5, hvis noen ber om det |
| Artefakt-lineage-graf | `step.output_json` gir sporbarhet mellom steg allerede. Grafen over runs er en indeks, ikke en mekanisme. | Fase 5 |
| Notifikasjoner | Aldri i kjernen. `pulseq watch --json` finnes fra fase 4. | Aldri |
| Kalenderregler utover cron (`OnCalendar`-stil) | `cron` + `@every` dekker >95 %. Ny grammatikk = ny parser = ny feilklasse. | Aldri |
| Jitter / `RandomizedDelaySec` | Thundering herd er et flernode-problem. Vi er én node. | Aldri |
| Postgres | Se harde grenser. | Aldri |
| Prioriteter og køer | Concurrency-limits dekker det reelle behovet. Prioritetskøer inviterer til svelting og tuning-mareritt. | Aldri |

---

## 8. Faseinndeling: fra tomt repo til ferdig produkt

Hver fase avsluttes med en **fungerende binær** og en **ende-til-ende-test**. Ingen fase etterlater død kode.

### Fase 0 — Skjelett (≈2 dager)

- `go mod init`, `cmd/pulseq/main.go`, `pulseq version`.
- `internal/store`: `schema.sql` via `embed`, `user_version`-migrasjon, skrive-/lesepool, `store.Write(ctx, fn)`-helper.
- **Ytelsesport:** benchmark som viser ≥ 500 skrivetransaksjoner/s med `modernc.org/sqlite` på målmaskin. Hvis den feiler, er det *nå* vi bytter driver — ikke etter 3000 linjer.
- Ferdig: `pulseq init` oppretter statedir og skjema.

### Fase 1 — Jobber og manuell kjøring (≈1 uke)

- `internal/spec`: TOML-innlasting, streng validering (ukjente nøkler = feil), syklusdeteksjon med utskrift av syklusstien.
- `internal/runner`: exec med argv, env, `Setpgid`, timeout → SIGTERM → grace → SIGKILL, loggfil per forsøk, `$PULSEQ_OUTPUT`/`$PULSEQ_OUTPUTS`.
- `internal/engine`: topologisk steg-utføring med `max_parallel`, retry med backoff, `skipped` nedstrøms.
- CLI: `check`, `load`, `jobs`, `run <job>` (synkron), `runs`, `show`, `logs`.
- **Ferdig produkt allerede her:** en bedre `make` + `xargs` for serverdrift. Kan tas i bruk internt.

### Fase 2 — Daemon og schedules (≈1 uke)

- `pulseq daemon`: `flock`, 1 Hz tick-løkke, graceful shutdown på SIGTERM, `SIGHUP` = reload konfig.
- Cron/interval-evaluator, `next_fire_at` i UTC, catchup med `catchup_limit`.
- Run-kø: `pending` → claim med lease → `running`; lease-fornyelse hvert 10. sekund, 60 s levetid.
- `reconcile()` ved oppstart: utløpte leases → steg `failed(lease_expired)` → retry-policy.
- CLI: `sources`, `pause`, `resume`, `cancel`.
- systemd-unit med `StateDirectory=pulseq`, `ConfigurationDirectory=pulseq`, `Restart=always`.
- **Ferdig produkt:** cron-erstatning med historikk, retries og DAG-er.

### Fase 3 — Sensors (≈4 dager)

- Sensor-evaluator: exec med timeout og prosessgruppe, JSONL-parser, `fail_streak`-backoff.
- Atomisk commit av `tick` + `run`-rader + `cursor` i én transaksjon.
- Dedupliseringstelling via `INSERT … ON CONFLICT DO NOTHING` + `changes()`.
- `pulseq explain <job/source>` og `pulseq explain run <id>`.
- Eksempelsensorer i `examples/`: filsystem, HTTP-etag, SQL-watermark, S3-listing. **Alle i shell, ingen Go.**
- **Ferdig produkt:** MVP-en fra prosjektbeskrivelsen er nådd.

### Fase 4 — Herding og utgivelse (≈1 uke)

- `retry` / `replay`, `max_concurrent` håndhevet transaksjonelt, `prune`, `stats`, `watch --json`.
- `--json` på alle liste-kommandoer; golden-file-tester på CLI-utdata.
- Krasjtester: `SIGKILL` daemonen midt i en run, restart, assert rekonsiliering. Fylltest av tilstandsmaskinen med tilfeldig hendelsesrekkefølge.
- DST-tester: vår-fram og høst-tilbake i `Europe/Oslo` for `0 2 * * *`.
- Dokumentasjon: én `README.md`, én `man`-side, `examples/`. **Ingen dokumentasjonsnettsted.**
- LOC-revisjon mot 4000-linjers budsjettet. Overskridelse = noe skal ut, ikke opp med budsjettet.
- v1.0: statisk binær for `linux/amd64` og `linux/arm64`.

### Fase 5 — Etter MVP, kun hvis etterspurt

- Lese-only web-UI: **én Go-fil**, `net/http` + `html/template`, serverrendret HTML, null JavaScript-bygg, null CSS-rammeverk. Binder til `127.0.0.1` som standard. Deler `internal/store`s lesepool.
- `pulseq backfill <job/source> --from --to [--dry-run]`.
- Artefakt-indeks: én tabell som indekserer `step.output_json`-nøkler for søk. Kun hvis noen faktisk søker.

**Total: ~4 uker til v1.0.**

---

## 9. Risikoer

| # | Risiko | Sannsynlighet | Mitigering |
|---|---|---|---|
| 1 | `modernc.org/sqlite` for treg eller feilaktig under lease-churn | Middels | Ytelsesport i fase 0, *før* kodebasen låser oss. Driverbytte er én linje fordi vi ikke abstraherer. |
| 2 | Lang skrivetransaksjon sulter CLI-en | Middels | Invariant 5 (ingen I/O i transaksjon) + `busy_timeout` + en lasttest som måler p99 transaksjonsvarighet. |
| 3 | DST: cron i lokal tid hopper over eller dobler en veggklokketime | Høy | `next_fire_at` alltid UTC; `Schedule.Next` i sourcens `Location`; ISC-cron-semantikk dokumentert eksplisitt; catchup begrenset av `catchup_limit`. Egne tester for begge overganger. |
| 4 | Hengende sensor blokkerer tick-løkken | Høy | Hard timeout per evaluator; evaluatorer kjører i goroutiner med serialisering *per source*, ikke globalt; prosessgruppe-drap. |
| 5 | Loggfiler vokser ubegrenset | Høy | `max_log_bytes` per steg (trunkering med markør), `pulseq prune`. Ingen logrotate-integrasjon i kjernen. |
| 6 | Zombieprosesser hvis et steg forker | Middels | `Setpgid` + drap av hele gruppen; dokumentert at steg ikke skal daemonisere. |
| 7 | **Feature-press**: «bare legg til én webhook-mottaker» | **Svært høy** | Kapittel 2 er mitigeringen. Svaret er alltid: skriv et program. Denne risikoen er den eneste som kan drepe prosjektet. |
| 8 | Klokkehopp (NTP-justering, VM-suspend) gir dobbeltkjøring | Lav | `run_key` = planlagt tick i RFC3339 → `UNIQUE` fanger dobbelt. Klokkehopp bakover kan aldri gi duplikat. |
| 9 | Bruker forventer flernode og oppdager `flock` for sent | Middels | Dokumenter i første avsnitt av README. Skaleringsveien er «del jobbene på flere statedirs», ikke «kjør to daemoner». |
| 10 | DAG-semantikkdiskusjoner (betingelser, fan-out, matriser) | Høy | Kun `needs`. Alt annet uttrykkes ved at et steg avslutter tidlig, eller ved å splitte i to jobber. |
| 11 | Krasj mellom `exec` av steget og commit av `running`-tilstanden | Lav | Skriv `running` *før* `exec`; ved krasj rekonsilieres den til `failed(lease_expired)` og retry-policy avgjør. At-least-once er kontrakten, og den er dokumentert. |

---

## 10. Testing

- **Ingen mocks.** Testene åpner en ekte SQLite-fil i `t.TempDir()`. Alt annet skjuler nøyaktig de feilene vi bryr oss om (låsing, transaksjonsgrenser, `UNIQUE`-konflikter).
- **Ett interface:** `Clock`. Fylt tid gjør cron-, catchup-, backoff- og lease-tester deterministiske.
- **Tilstandsmaskin-fylltest:** generer tilfeldige hendelsessekvenser (claim, krasj, retry, cancel, lease-utløp) og assert invariantene 1–4 etter hver overgang.
- **Golden-file CLI-tester** på `--json`-utdata: utdataformatet er et offentlig grensesnitt og skal brekke synlig.
- **Krasjtester** som faktisk `SIGKILL`-er daemonen.
- **Eksempelsensorene i `examples/` kjøres i CI** — de er både dokumentasjon og integrasjonstest av protokollen.

---

## 11. Hva Pulseq bevisst ikke er

| | Airflow / Dagster | Temporal | Windmill / Kestra | **Pulseq** |
|---|---|---|---|---|
| Brukerens grensesnitt | Python-SDK | Språk-SDK, determinisme-krav | Webeditor + DSL | **argv + exit-kode** |
| Distribusjon | Scheduler + webserver + DB + workers | Klynge | Server + DB + workers | **Én binær, én fil** |
| Asset-/lineage-modell | Ja, sentral | Nei | Delvis | **Nei** |
| Utvidbarhet | Providers/plugins | Aktiviteter i SDK | Skript i webeditor | **Andre programmer** |
| Tilstandslagring | Postgres | Cassandra/Postgres | Postgres | **SQLite, én fil** |
| Kodelinjer å lese for å forstå systemet | 10⁵–10⁶ | 10⁶ | 10⁵ | **< 4 · 10³** |

Det siste tallet er produktet. En operatør skal kunne lese hele Pulseq på en ettermiddag og etterpå vite nøyaktig hvorfor jobben hans ikke kjørte i natt. Alt i denne planen tjener det målet, og alt som truer det er avvist i kapittel 2.

---

## 12. Definisjon av ferdig (v1.0)

1. Alle MVP-punkter i prosjektbeskrivelsen er dekket (§7.1).
2. ≤ 4000 linjer ikke-test Go, ≤ 4 direkte avhengigheter, 5 tabeller, 6 pakker.
3. `CGO_ENABLED=0 go build` gir én statisk binær; ingen kjøretidsavhengigheter utover libc-fri Linux.
4. Krasj under kjøring gjenopprettes uten manuelt inngrep.
5. Én `README.md` og én `man`-side dekker alt. Ingen dokumentasjonsnettsted, fordi det ikke er nok å dokumentere.
6. En ny bruker kan skrive sin første sensor i shell på under fem minutter uten å lese noe annet enn §5.3.
