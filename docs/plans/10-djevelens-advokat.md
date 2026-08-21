# Pulseq — plan 10: Djevelens advokat / pre-mortem

**Rolle:** pre-mortem-analytiker. Antakelse: det er februar 2028, Pulseq er dødt. Dette dokumentet er obduksjonsrapporten skrevet på forhånd, pluss den planen som faktisk overlever den.

**Tese i én setning:** Pulseq dør ikke av dårlig Go-kode. Pulseq dør fordi (a) nisjen allerede er tatt av et prosjekt som gjorde 80 % av det samme først, (b) DAG-motoren og web-UI-et spiser 12 måneder, og (c) ingen — heller ikke utvikleren selv — kjørte det i produksjon før måned 9.

---

## 1. Obduksjonsrapport (skrevet 2028-02, om et prosjekt startet 2026-08)

### Måned 0–2: "Kjernen først"
Utvikleren startet der det var mest gøy: datamodellen. Job, Step, Run, Schedule, Sensor, Cursor, Trigger, ArtifactRef — åtte tabeller, en state machine med seks tilstander, og en migreringsmekanisme. Skjemaet ble skrevet om fire ganger fordi `Run` og `Trigger` viste seg å ha uklar grense (er en trigger som ble avvist av concurrency-limit en Run i tilstand `skipped`, eller ikke en Run i det hele tatt?). Ingenting kjørte ennå.

### Måned 2–6: DAG-motoren
Topologisk sortering tok en ettermiddag. Deretter kom det virkelige arbeidet, som ingen budsjetterer for:

- retry på steg-nivå × parallelle steg = hvem eier feilen når et parallelt søsken feiler mens et annet retryer?
- avbrudd/cancel midt i en parallell fan-out; SIGTERM-propagering til barneprosesser; foreldreløse prosesser
- "kjør bare feilede steg på nytt" krever at stegresultater er innholdsadresserte, ellers er partial re-run bare et løfte
- timeout per steg vs. per run vs. per retry-forsøk — tre forskjellige klokker
- gjenoppretting etter krasj: hvilke steg var faktisk `running` og hvilke var bare markert `running` i basen da prosessen døde?

Måned 6 var DAG-motoren "nesten ferdig". Det fantes fortsatt ingen sensor. Prosjektbeskrivelsen sa at sensors skulle være førsteklasses primitiv; virkeligheten var at de var uskrevne.

### Måned 6–9: Sensor-abstraksjonen, versjon 1 til 3
Versjon 1: sensors som Go-interface, kompilert inn. Fungerte perfekt for utvikleren, ubrukelig for alle andre — for å skrive en sensor måtte du forke Pulseq og bygge binæren på nytt.

Versjon 2: plugin-SDK basert på Go `plugin`-pakken. Krevde identisk Go-versjon, identiske build-flagg, fungerte ikke på musl, ikke på macOS, ikke cross-compilet. Skrotet etter tre uker.

Versjon 3: en generisk "sensor DSL" med uttrykksevaluering, HTTP-klient, JSON-path, tilstandsmaskin og retry-policy. Dette var et lite programmeringsspråk. Det tok to måneder, hadde sin egen dokumentasjon, og ingen skrev noen gang en sensor i det bortsett fra utvikleren, som skrev tre.

Samtidig kom cursor-semantikken skjevt ut. `run_key` (idempotens) og `cursor` (posisjon) ble blandet: hvis du resettet cursoren fordi du ville kjøre på nytt, skjedde det ingenting, fordi run_key-ene var uendret og deduplikatoren tok dem. Dette er nøyaktig samme felle Dagster gikk i og måtte skille de to begrepene for å komme ut av — bare uten Dagsters ressurser til å rette det opp.

### Måned 9–13: Web-UI-fellen
CLI-en var faktisk god. Men "en enkel web-UI" i prosjektbeskrivelsen ble til: en React-app, Vite, en API-flate med 22 endepunkter, auth (fordi Pulseq kjører vilkårlige shell-kommandoer, så et åpent endepunkt er RCE-som-tjeneste), sesjonshåndtering, CORS, embedding via `go:embed`, og et npm-avhengighetstre med jevnlige Dependabot-varsler. Frontend ble 60 % av arbeidet i denne perioden og 0 % av differensiatoren.

### Måned 13–15: SQLite-veggen
Da to jobber med hver fire steg kjørte samtidig, og sensor-evaluatoren tikket hvert 10. sekund, og CLI-en gjorde et `list`, kom `SQLITE_BUSY: database is locked`. Diagnosen tok tre uker fordi feilen var intermitterende:

- `database/sql` åpnet flere skriveforbindelser gjennom poolen
- transaksjoner startet som `deferred` og forsøkte låseoppgradering midt i — `busy_timeout` gjelder ikke for oppgraderingsdeadlock i WAL, den returnerer `SQLITE_BUSY` umiddelbart
- loggskriving gikk rett i SQLite; en jobb som produserte 200 MB stdout blokkerte skedulereren
- to `pulseq`-prosesser (en systemd-tjeneste og en glemt terminal) skrev til samme fil

"Løsningen" ble en kø-goroutine foran én skriveforbindelse — riktig svar, men implementert som brannslukking etter at datamodellen antok fri samtidighet overalt, så det krevde omskriving av halve persistenslaget.

### Måned 15–18: Ingen brukere, og så ingen utvikler
Show HN i måned 16 fikk 40 poeng og tre kommentarer. Alle tre var varianter av samme spørsmål: *"Hvordan er dette forskjellig fra Dagu?"* — som allerede er ett Go-binærfil, YAML-DAG-er, cron, retries, logger og web-UI, uten database. Utvikleren hadde ikke et godt svar, fordi det ærlige svaret ("sensors med cursor") var den delen som fortsatt ikke var ferdig.

Ingen fant prosjektet ved søk, fordi "Pulseq" allerede er et etablert åpen kildekode-rammeverk for MR-pulssekvenser med støtte fra Siemens, Bruker og GE. Enhver Google- eller GitHub-søk på navnet returnerte MRI-forskning.

Måned 18: siste commit var en avhengighetsoppdatering. Ingen jobber i produksjon, heller ikke utviklerens egne — de lå fortsatt i crontab, fordi crontab virket.

---

## 2. Feilmåter og mottiltak

| # | Feilmåte | Tidlig varselsignal | Mottiltak (bindende) |
|---|---|---|---|
| **F1** | **Scope creep mot mini-Dagster.** Assets, lineage, partitions, backfill, dynamic fan-out, ressursgraf. | Ordet "asset" eller "partition" dukker opp i en issue. | Ikke-mål-listen i §3 er en kontrakt. Ny funksjon krever at en av *dine egne* jobber i produksjon er blokkert uten den. |
| **F2** | **"Yet another orchestrator"-tretthet.** Nisjen har Dagu, Cronicle, Windmill, Kestra, Prefect, Temporal, systemd timers, K8s CronJobs. | Du klarer ikke svare på "hvorfor ikke Dagu?" på under 15 sekunder. | Skriv differensiator-setningen *før* første linje kode, og legg den øverst i README. Den er: **"Cron kan bare spørre 'er klokka X?'. Pulseq kan spørre 'har det kommet noe nytt?' — og fortelle deg presis hvorfor det ikke gjorde det."** Alt som ikke støtter den setningen er ikke-mål. |
| **F3** | **DAG-motoren sluker all tid.** Topologisk sort er trivielt; retry × parallellitet × cancel × timeout × crash recovery er ikke. | Du har brukt mer enn 2 uker sammenhengende på stegavhengigheter. | **MVP har ingen DAG.** Steg er en ordnet liste, sekvensiell, fail-fast. DAG utsettes til fase 3 og har harde begrensninger (§6). |
| **F4a** | **Sensor-abstraksjonen for smal.** Bare filer, eller bare HTTP. Første bruker med et litt annet behov må forke. | Du legger til en `sensor_type: "..."`-enum-verdi nummer fire. | Eksekverbar-kontrakten (§5): en sensor er *hvilket som helst program* som leser JSON på stdin og skriver JSON på stdout. Uendelig bred, null API-flate. |
| **F4b** | **Sensor-abstraksjonen for generell.** Plugin-SDK, DSL, uttrykksspråk. | Du skriver en parser. | Ingen Go-plugin-pakke. Ingen DSL. Ingen uttrykksevaluering. Kun exec-kontrakt + **nøyaktig én** innebygd sensor (filsystem-watermark), for å ha en 30-sekunders demo. |
| **F4c** | **cursor/run_key-forvirring** (Dagsters dokumenterte felle). Cursor-reset gjør ingenting fordi run_key-deduplikering fortsatt biter. | Første gang du må forklare hvorfor "reset cursor" ikke rekjørte noe. | Skill dem hardt i dokumentasjon og API. `cursor` = *hvor jeg har lest til* (sensorens ansvar, ugjennomsiktig streng for Pulseq). `run_key` = *hvilken enhet dette gjelder* (Pulseqs ansvar, dedupliseres). `pulseq sensor reset <navn> [--forget-run-keys]` — to separate flagg, aldri implisitt sammenkobling. |
| **F5** | **SQLite-samtidighetsbugs.** `SQLITE_BUSY`, låseoppgraderingsdeadlock, WAL-vekst, to prosesser på samme fil. | Første intermitterende "database is locked" i CI. | Arkitektonisk, ikke lappverk: to `sql.DB`-håndtak (§5). Skriv: `MaxOpenConns(1)` + `_txlock=immediate`. Les: `MaxOpenConns(N)`. WAL + `busy_timeout=5000` + `synchronous=NORMAL`. **Logger går aldri i SQLite** (F13). Prosess-eksklusivitet med `flock` på datakatalogen. Én dedikert regresjonstest som hamrer 50 samtidige runs. |
| **F6** | **Vedlikeholdsbyrde for én utvikler → burnout-platå.** Prosjektet ser aktivt ut på overflaten mens designbeslutninger og feilsøking hoper seg opp. | Issue-backlog eldre enn 30 dager; du gruer deg til å åpne repoet. | Harde budsjetter (§4): maks 8000 linjer kjerne-Go, maks 5 direkte go.mod-avhengigheter. `SCOPE.md` i rot med ikke-mål, som issue-svar kan lenke til. Erklær eksplisitt "ferdig" — Pulseq 1.0 er et *avsluttet* program, ikke en plattform. |
| **F7** | **Web-UI-fellen.** Frontend blir majoriteten av arbeidet og null av differensiatoren, pluss auth, npm-CVE-er og en angrepsflate mot et program som kjører vilkårlige shell-kommandoer. | `package.json` eksisterer. | **Ingen web-UI før fase 4, og kun hvis kill-criteria i §7 er passert.** Da: server-rendret HTML fra Go-templates, `go:embed`, null npm, null JavaScript-byggesteg, read-only, kun bind til `127.0.0.1` som standard. Ingen mutasjons-endepunkter over HTTP i 1.0. |
| **F8** | **Konfigformat-bikeshedding.** YAML vs TOML vs HCL vs Starlark vs Go-SDK. | En issue med tittel "have you considered X for config". | **Avgjort nå, ikke gjenåpnes: YAML, én fil per job, ingen templating-språk.** Kun `${ENV_VAR}` og `${PULSEQ_*}`-substitusjon. Ingen Jinja, ingen Go-templates, ingen betingelser i konfig. Trenger du logikk? Skriv et skript — det er hele poenget. |
| **F9** | **Cron er godt nok → ingen brukere.** For rene tidsutløste jobber *er* cron godt nok, og det er allerede installert. | Du kan ikke navngi en av dine egne jobber som cron gjør dårlig. | Ikke konkurrer på tid. Konkurrer på (1) *hendelser* og (2) *forklarbarhet*. Migreringssti inn: `pulseq import-crontab` som leser `crontab -l` og genererer jobfiler. Migreringssti ut: hver jobfil skal kunne kjøres av `pulseq run <job>` fra en vanlig crontab-linje, så Pulseq aldri er et gissel. |
| **F10** | **Tidssone/DST/catch-up-bugs.** Dobbeltkjøring i overgangen fra sommertid, eller manglende kjøring i den ikke-eksisterende timen. Fast-forward-logikk som permanent utsetter jobber som varer lenger enn intervallet. | Første rapport om "kjørte to ganger i natt". | Injiserbar klokke overalt — ingen direkte `time.Now()` i domenelogikk (F14). IANA-tz per schedule, lagre alltid UTC. **Catch-up i MVP har én policy: `on_missed: skip \| run_once`.** Ingen backfill-vindu, ingen N-kjøringer-etterslep. `pulseq next <schedule> --count 20 --at <tidspunkt>` for å inspisere DST-oppførsel uten å vente. Egen testsuite for Europe/Oslo DST-overganger begge veier. |
| **F11** | **At-least-once uten reell idempotens → duplikater i produksjon.** Én dobbel utsendelse av en rapport dreper tilliten permanent for en solo-utvikler. | — (dette oppdages først når det smeller) | `run_key` er obligatorisk for sensor-triggere, UNIQUE-constraint i basen, dedupliseringsvindu konfigurerbart. `max_concurrent_runs: 1` er **standardverdi**, ikke opt-in. Dokumenter ærlig og øverst: *"Pulseq garanterer at kjøringen starter minst én gang. Gjør jobben din idempotent."* Ikke lov exactly-once noe sted. |
| **F12** | **Prosessmodell-drift mot distribuert system.** "Worker cluster", Postgres-backend, distribuert lås, leader election. Distribuert-system-kostnad uten distribuert-system-budsjett. | Ordene "leader election" eller "Postgres" i en design-note. | **Pulseq er én prosess på én maskin. Punktum, gjennom 1.0.** Workere er goroutines i samme prosess, ikke separate prosesser. Ingen Postgres-abstraksjonslag "for senere" — et databaseabstraksjonslag du ikke bruker er ren kostnad. Skriv rå SQL mot SQLite. |
| **F13** | **Observabilitet som ballong.** Full stdout/stderr i SQLite → database på flere GB, treg VACUUM, tregt alt. | DB-fil over 100 MB. | Logger til **filer på disk**: `<data>/logs/<run_id>/<step>.log`. I SQLite ligger kun sti, størrelse, exit-kode og de siste 4 KB (for `pulseq status`). Innebygd rotasjon: `retention_days` med standardverdi, kjørt av en intern housekeeping-tick. Dette er også F5-mottiltak. |
| **F14** | **Utestbar tidsavhengig kode → flaky tester → tester slås av → regresjoner.** | Første `time.Sleep` i en test. | `Clock`-interface injisert fra dag 1; falsk klokke i alle tester. Ingen `time.Sleep` i tester — noen gang. Determinisme er ikke luksus i en skeduler, det er hele produktet. |
| **F15** | **Sikkerhet.** Pulseq kjører vilkårlige shell-kommandoer. Et eksponert API eller UI uten auth er RCE-som-tjeneste. Én CVE er nok til å drepe et solo-vedlikeholdt prosjekt. | Standardkonfig binder til `0.0.0.0`. | Ingen nettverkslytting i MVP i det hele tatt. CLI snakker med daemonen over en **unix-socket** med filrettigheter, ikke TCP. Når UI kommer: `127.0.0.1` som standard, read-only, dokumentert at man setter en reverse proxy foran. `SECURITY.md` som er ærlig om trusselmodellen: *"Alle som kan skrive til jobkatalogen kan kjøre kode som pulseq-brukeren. Behandle den som `/etc/cron.d`."* |
| **F16** | **Navnekollisjon: prosjektet er usøkbart.** "Pulseq" er et etablert åpen kildekode-rammeverk for MR-pulssekvenser (pulseq.github.io, GitHub `pulseq-admin/pulseq`, PyPulseq), med interpretere for Siemens, Bruker, GE og Philips, og et tiår med publikasjoner. | Søk på "pulseq" nå. | **Bytt navn før første publisering.** Kostnaden er null i dag og uoverkommelig senere. Krav til nytt navn: null treff på GitHub-søk, ledig `.dev`-domene, uttalbart, ikke et vanlig substantiv. Behold gjerne "Pulseq" som internt arbeidsnavn til fase 2-porten, men publiser aldri under det. |

---

## 3. Harde grenser: ting som eksplisitt IKKE skal bygges

Dette er ikke "senere". Dette er "nei", med mindre en kill-criteria-port eksplisitt åpner det.

**Aldri (gjennom 1.0):**
- Asset-graf, data-lineage, partisjoner, materialiseringer. Dette *er* Dagster; å bygge det er å tape mot Dagster.
- Distribuert kjøring, flere noder, leader election, distribuert lås.
- Postgres-backend eller et database-abstraksjonslag som forbereder det.
- Plugin-system: ingen Go `plugin`, ingen delte biblioteker, ingen WASM, ingen embedded Lua/Starlark/Tengo.
- Et uttrykks- eller templating-språk i konfigurasjon.
- Dynamic fan-out (steg som genererer N steg ved kjøretid). Sensor-multi-trigger dekker 90 % av behovet ved å lage N *runs* i stedet.
- Betingede kanter i grafen (`if`/`when` på avhengigheter).
- Innebygd secrets-håndtering. Bruk miljøvariabler og `systemd` `EnvironmentFile` / `LoadCredential`.
- Innebygde integrasjoner: S3, Slack, Postgres, Kafka, e-post. Alt dette er `curl`, `aws`, `psql` — altså et steg.
- Multi-tenancy, brukere, roller, RBAC.
- Container-runtime-integrasjon (Docker/K8s-executors). Et steg kan kjøre `docker run`; det holder.
- Innebygd metrikk-/tracing-stack. Skriv Prometheus-tekstformat på en socket eller en fil, ferdig.

**Utsatt bak eksplisitt port (se §7):**
- DAG mellom steg (fase 3)
- Web-UI (fase 4, read-only)
- Varsler/notifications (fase 4 — og da som **én** ting: kjør et program ved terminal tilstand. Ikke SMTP, ikke Slack-SDK.)
- Backfill (fase 4 eller aldri)
- Artifact-referanser og sporbarhet (fase 4 eller aldri — se §4)

---

## 4. Forenklinger av prosjektbeskrivelsen

Konkrete avvik fra `prosjektbeskrivelse.md`, med begrunnelse.

| Beskrivelsen sier | Planen sier | Hvorfor |
|---|---|---|
| MVP: "Basic DAG dependencies" | **Kuttet fra MVP.** Steg er en sekvensiell liste. | F3. Sekvensielle steg med retry og gjenopptakelse dekker ETL, rapporter, vedlikehold — de faktiske brukstilfellene som er listet i beskrivelsen selv. DAG er der tiden forsvinner. |
| "En worker-prosess som utfører jobber" | Workere er **goroutines i samme prosess**. | F12. Separate prosesser tvinger frem IPC, helsesjekk, foreldreløse prosesser og en distribuert state machine — ingen av delene er begrunnet på én maskin. Steg-*kommandoene* er selvsagt egne prosesser (`exec`). |
| "Persistens i SQLite for single-node eller Postgres for multi-node" | **SQLite, kun.** Postgres er fjernet fra planen. | F12. "Eller Postgres" tvinger frem et abstraksjonslag som koster fra dag 1 og betaler seg aldri. |
| "Lease/lock på scheduler og sensor-evaluering" | **Fjernet.** Én prosess = låsen. `flock` på datakatalogen. | F12. Lease-mekanikk er svaret på et problem (flere skedulerere) som denne planen definerer bort. |
| Schedules: kalenderregler, missed-run catch-up, concurrency limits, skip-with-reason | **MVP: cron + interval + tz + pause/resume + `on_missed: skip\|run_once` + `max_concurrent_runs` (standard 1).** Kalenderregler (arbeidsdager, siste-dag-i-måned) kuttet. | Kalenderregler er en uendelig hale (helligdagskalendere per land). Skriv et `precondition`-steg som avslutter med exit-kode 75 = "hopp over" — dekker alt. |
| Sensors: poll, cursor, state, multi-trigger, som fire kategorier | **Én mekanisme:** eksekverbar som returnerer `{cursor, triggers[]}` eller `{skip_reason}`. Kategoriene er bruksmønstre, ikke typer i koden. | F4a/F4b. Kategoriene i beskrivelsen er beskrivende, ikke strukturelle. Å kode dem inn som typer er den smale fellen. |
| "Artifact reference" som primærobjekt | **Nedgradert til et nøkkel/verdi-felt på run.** En run kan skrive JSON til `$PULSEQ_OUTPUT`; Pulseq lagrer det og eksponerer det i neste steg som `$PULSEQ_INPUT`. Ingen egen artefakt-modell, ingen lineage. | Artefaktsporing er inngangsporten til asset-grafen. Nøkkel/verdi dekker "steg B trenger filstien steg A skrev". |
| "En enkel web-UI eller CLI" | **CLI, og bare CLI, gjennom fase 3.** | F7. "Eller" i beskrivelsen er en åpning; lukk den. |
| Observabilitet: logs i state-lageret | **Logger på disk, metadata i SQLite.** | F13/F5. |
| Fase 2: backfill, watermark-triggers, web UI, notifications, dynamic fan-out, artifact lineage, dry-run/explain | **`explain` flyttes til MVP** (det er differensiatoren). Dynamic fan-out og artifact lineage **strykes helt**. Resten står bak porter. | F2. `explain` er hele salgsargumentet; å utsette det er å utsette produktet. |

---

## 5. Forenklet arkitektur (den jeg faktisk anbefaler)

### Prosessmodell

```
pulseq (én binær, én prosess, én maskin)
│
├─ ticker-goroutine (én, ~1 Hz, injisert klokke)
│    → beregner forfalte schedules og sensor-ticks
│    → sender TriggerRequest på en kanal
│
├─ sensor-evaluator (goroutine-pool, N=4)
│    → exec sensorprogram, timeout, les JSON fra stdout
│    → produserer triggere ELLER skip_reason
│
├─ dispatcher (én goroutine — hele beslutningslaget er enkelttrådet)
│    → dedupliserer på run_key, sjekker concurrency-limit
│    → skriver Run til DB, eller skriver et SkipEvent med grunn
│
├─ executor (goroutine-pool, N=konfigurerbar)
│    → exec steg-kommandoer, streamer stdout/stderr til fil
│
├─ writer (én goroutine, eier writeDB)
│    → alle skriv går gjennom denne
│
└─ control-socket (unix domain socket)
     → CLI-kommandoer: status, explain, run, pause, ...
```

**Nøkkelvalg:** *hele beslutningslaget (dispatcher) er enkelttrådet.* Det fjerner en hel klasse av race conditions gratis, og en skeduler for én maskin trenger aldri mer enn én kjerne til beslutninger. All parallellitet ligger i I/O og i eksekvering av kommandoer.

### Persistens

To `sql.DB`-håndtak mot samme fil — dette er mottiltaket mot F5, og det må ligge i fundamentet, ikke pålimes:

```go
// SKRIV: nøyaktig én forbindelse, immediate-lås fra transaksjonsstart.
// _txlock=immediate unngår deferred→exclusive låseoppgradering,
// som er tilfellet der busy_timeout IKKE hjelper og du får SQLITE_BUSY direkte.
writeDB, _ := sql.Open("sqlite",
    "file:"+p+"?_journal=WAL&_timeout=5000&_txlock=immediate&_synchronous=NORMAL&_foreign_keys=1")
writeDB.SetMaxOpenConns(1)

// LES: flere forbindelser, WAL gir lesere som ikke blokkerer skriveren.
readDB, _ := sql.Open("sqlite",
    "file:"+p+"?_journal=WAL&_timeout=5000&_synchronous=NORMAL&_foreign_keys=1")
readDB.SetMaxOpenConns(8)
```

Pluss:
- `flock` på `<data>/.lock` ved oppstart → to prosesser feiler med tydelig melding i stedet for å konkurrere.
- Periodisk `PRAGMA wal_checkpoint(TRUNCATE)` i housekeeping-ticket, ellers vokser `-wal`-filen ubegrenset ved vedvarende lesere.
- Ingen ORM. Rå SQL, håndskrevne migreringer som nummererte `.sql`-filer i `go:embed`.
- Ingen BLOB-er. Loggene ligger på disk.

Tabeller i MVP — seks, ikke åtte:
`jobs` (avledet fra YAML, cache) · `runs` · `steps` (rader per run) · `schedules` · `sensors` (inkl. cursor-kolonne) · `events` (append-only: trigger, skip, tick, state-overgang — datagrunnlaget for hele `explain`).

`events` er det viktigste bordet. `explain` er en spørring mot `events`, ikke en egen mekanisme.

### Sensor-kontrakten (differensiatoren — hold den absurd liten)

Pulseq kjører sensorprogrammet med `PULSEQ_CURSOR` på stdin som JSON:

```json
{"cursor": "2026-08-21T04:00:00Z", "sensor": "nye-filer", "last_tick": "..."}
```

Programmet skriver JSON på stdout og avslutter med 0:

```json
{
  "cursor": "2026-08-21T05:00:00Z",
  "triggers": [
    {"run_key": "sha256:abc…", "job": "prosesser-fil", "env": {"FIL": "/data/in/a.csv"}},
    {"run_key": "sha256:def…", "job": "prosesser-fil", "env": {"FIL": "/data/in/b.csv"}}
  ]
}
```

eller:

```json
{"cursor": "…", "skip_reason": "ingen nye filer siden 04:00"}
```

Exit-kode ≠ 0 → sensor-feil, cursor **ikke** committet, feilen logges som event, backoff.

Egenskaper: skrivbar i bash på 5 linjer. Ingen SDK. Ingen ABI. Fungerer på alle språk. Testbar med `echo '{}' | ./min-sensor.sh`. Dette er den eneste utvidelsesflaten Pulseq har, og den er allerede stabil fordi den er JSON over rør.

**Semantikk som må stå spikret i dokumentasjonen fra dag 1** (F4c):
- `cursor` er ugjennomsiktig for Pulseq. Pulseq committer den kun ved vellykket tick. Pulseq tolker den aldri.
- `run_key` dedupliseres av Pulseq innenfor `dedup_window` (standard: for alltid). Sensoren skal ikke bruke run_key til å lagre tilstand.
- De to nullstilles uavhengig: `pulseq sensor reset <navn> --cursor` og `--run-keys`.

### Jobbdefinisjon (YAML, avgjort, ikke gjenåpnes)

```yaml
name: nightly-rapport
schedule:
  cron: "0 3 * * *"
  timezone: Europe/Oslo
  on_missed: run_once      # eller: skip
max_concurrent_runs: 1
steps:
  - name: hent
    run: /usr/local/bin/hent-data.sh
    timeout: 10m
    retry: {attempts: 3, backoff: exponential, initial: 30s}
  - name: rapporter
    run: python3 /opt/rapport.py
    timeout: 30m
```

Ingen `depends_on` i MVP. Ingen templating. `${VAR}`-substitusjon fra miljø og fra `$PULSEQ_*`.

### CLI-flate (maks 12 toppnivåkommandoer, hardt tak)

```
pulseq run <job>              kjør nå, i forgrunnen, strøm logg
pulseq list                   jobber, schedules, sensorer, neste tick
pulseq status [run-id]        siste kjøringer og tilstand
pulseq logs <run-id> [step]   
pulseq explain <job|sensor>   ★ hvorfor kjørte / kjørte ikke dette?
pulseq next <schedule>        forhåndsvis N neste tidspunkter (--at for what-if)
pulseq check <sensor>         evaluer sensor uten å committe cursor
pulseq pause|resume <navn>
pulseq retry <run-id> [--from-step]
pulseq sensor reset <navn> --cursor|--run-keys
pulseq import-crontab
pulseq serve                  daemon
```

`explain` er flaggskipet. Utdata skal være prosa, ikke JSON-dump:

```
$ pulseq explain nye-filer
Sensor 'nye-filer' — sist evaluert 04:32:10 (for 2m 14s siden), hver 60s.
Siste 5 tick:
  04:32:10  skip   "ingen nye filer siden 04:00"
  04:31:10  skip   "ingen nye filer siden 04:00"
  04:30:10  trigger 2 → 1 startet, 1 dedupliseres (run_key sha256:abc… sett 03:15:02)
  04:29:10  feil   exit 1: "permission denied: /data/in"
  04:28:10  skip   "ingen nye filer siden 04:00"
Cursor: "2026-08-21T04:00:00Z" (uendret siden 04:00:11)
Jobb 'prosesser-fil' er ikke pauset. Concurrency 1/1 ledig.
```

Dette er hele produktet. Alt annet er infrastruktur for å kunne skrive ut denne teksten.

---

## 6. MVP: hva som er inne, og hva som ble kuttet

**Inne i v0.1 (mål: 6 uker):**
1. Én binær, `pulseq serve` som systemd-tjeneste
2. YAML-jobber fra en katalog, hot reload ved endring
3. Cron- og interval-schedules med IANA-tz, pause/resume, `on_missed`
4. Sekvensielle steg, `exec`, timeout, retry med backoff
5. Sensors via exec-kontrakt + cursor + run_key-deduplisering + skip_reason
6. Én innebygd sensor: filsystem-watermark (`glob` + mtime/størrelse)
7. `max_concurrent_runs`, standard 1
8. SQLite (WAL, én skriver), logger på disk, `events`-tabell
9. CLI: `run, list, status, logs, explain, next, check, pause, resume, retry`
10. Strukturert logg (JSON) til stderr for daemonen; menneskelesbart i CLI
11. Gjenoppretting ved oppstart: runs i `running` uten levende prosess → `interrupted`, med tydelig event

**Kuttet fra MVP (versus beskrivelsen):** DAG-avhengigheter, kalenderregler, backfill, web-UI, notifications, artifact lineage, dynamic fan-out, Postgres, multi-node, plugin-API.

**Kuttet permanent:** alt i §3 "Aldri".

**Fase 3-DAG, når og hvis den kommer, har disse harde grensene:**
- Kun `needs: [steg-a, steg-b]`, statisk, kjent ved parse-tid
- Fail-fast: første feilende steg stopper hele runen; ingen `continue_on_failure`
- Parallellitetstak per run, standard 4
- Ingen betingede kanter, ingen fan-out ved kjøretid, ingen datapassering utover `$PULSEQ_INPUT`/`$PULSEQ_OUTPUT`
- Hvis dette tar mer enn 3 uker: revert, og steg forblir sekvensielle for alltid

---

## 7. Faser, porter og kill criteria

Hver port er et **stopp-punkt**. Ikke gå videre uten å ha svart ja. Skriv datoen og svaret ned.

### Fase 0 — Beslutninger (2 dager, før kode)
- Nytt navn valgt (F16). Domene og GitHub-org sjekket.
- Differensiator-setningen skrevet ned og limt inn i README.
- `SCOPE.md` med ikke-mål-listen fra §3 committet **først**.
- **Port 0:** Kan du på 15 sekunder svare "hvorfor ikke Dagu / cron / systemd timers?" Hvis nei — ikke start.

### Fase 1 — Tracer bullet (uke 1–2)
Mål: én cron-jobb, ett steg, kjørt av Pulseq, med logg og historikk i SQLite. Ingen sensorer, ingen retry, ingen CLI utover `serve/list/logs`.
- **Demo-milepæl D1:** `pulseq serve` erstatter én ekte crontab-linje på din egen maskin, og har kjørt uten inngrep i 7 døgn.
- **Port 1:** Hvis dette tok mer enn 2 uker, er ambisjonsnivået feilkalibrert — kutt scope før du fortsetter.

### Fase 2 — Sensorer og explain (uke 3–6) ← *dette er produktet*
Sensor-exec-kontrakt, cursor, run_key-dedup, skip_reason, `events`-tabellen, `pulseq explain`, `pulseq check`, retry, `on_missed`.
- **Demo-milepæl D2:** en 5-linjers bash-sensor som trigger en jobb per ny fil i en katalog, satt opp fra scratch på under 5 minutter, filmet som asciinema. Hvis demoen ikke er overbevisende på 60 sekunder, er ikke produktet det heller.
- **Demo-milepæl D3:** `pulseq explain` besvarer "hvorfor kjørte ikke X i natt?" for et reelt tilfelle.
- **★ KILL CRITERIA K1 (uke 6):** Kjører **minst 3 av dine egne, ekte** jobber i produksjon på Pulseq, med crontab-linjene deaktivert? Hvis nei → stopp. Du bygger noe du selv ikke vil bruke.
- **★ KILL CRITERIA K2 (uke 8):** Har `explain` faktisk spart deg feilsøkingstid minst 3 ganger? Før logg. Hvis nei → differensiatoren er ikke reell; enten finn en ny eller avslutt som personlig verktøy.

### Fase 3 — Herding og første eksterne bruker (uke 7–12)
Samtidighets-stresstest (F5), DST-testsuite (F10), crash-recovery-tester, `import-crontab`, dokumentasjon, `SECURITY.md`, én binær per plattform via GoReleaser. **Ingen nye funksjoner.**
- **Demo-milepæl D4:** en fremmed installerer og kjører en sensor-jobb uten å spørre deg om hjelp.
- **★ KILL CRITERIA K3 (uke 12):** Har minst **én person som ikke er deg** kjørt Pulseq i 14 sammenhengende dager? Hvis nei → ikke gjør det til et produkt. Frys funksjonaliteten, behold det som ditt eget verktøy, slutt å polere. Dette er en gyldig og god utgang.
- **Port 3 (DAG):** Åpnes kun hvis K3 er passert **og** minst to reelle jobber er blokkert av mangelen på stegavhengigheter. Ellers forblir steg sekvensielle.

### Fase 4 — Kun etter K3 (uke 13+)
Read-only web-UI (Go-templates, `go:embed`, null npm, `127.0.0.1`), `on_failure: <program>` som eneste varslingsmekanisme, eventuelt minimal DAG.
- **★ KILL CRITERIA K4 (uke 26):** Kjerne-Go utenom tester over 8000 linjer? Eller mer enn 5 direkte go.mod-avhengigheter? Da har du bygget mini-Dagster. Frys, fjern noe, eller avslutt.

### Kontinuerlige budsjetter (brudd = stopp og rydd)
| Budsjett | Tak |
|---|---|
| Kjerne-Go, ekskl. tester og generert kode | 8 000 linjer |
| Direkte avhengigheter i go.mod | 5 |
| Binærstørrelse | 25 MB |
| Toppnivå CLI-kommandoer | 12 |
| Nedlasting → første kjøring | 5 minutter |
| Konfignøkler totalt | 30 |
| Kaldstart av daemon | 200 ms |

---

## 8. Risikoregister

| Risiko | Sanns. | Konsekvens | Håndtering |
|---|---|---|---|
| Navnekollisjon gjør prosjektet usøkbart (F16) | **Sikker** | Høy | Bytt navn i fase 0. Gratis nå, umulig senere. |
| Dagu dekker allerede behovet godt nok | Høy | Kritisk | Test hypotesen i K1/K3, ikke i kode. Aksepter utfallet. |
| Utvikleren mister interessen etter måned 6 | Høy | Kritisk | Korte faser, ekte produksjonsbruk fra uke 6, eksplisitt "ferdig"-definisjon, gyldig utgang ved K3. |
| Sensor-kontrakten viser seg for tynn (trenger f.eks. hemmeligheter, streaming, langtkjørende) | Middels | Middels | Kontrakten er JSON over rør — utvidbar med nye *felter* uten brudd. Motstå å gjøre den til en protokoll. |
| SQLite-samtidighet biter til tross for mottiltakene | Middels | Middels | Enkelttrådet skriver + enkelttrådet dispatcher gjør at det nesten ikke *kan* skje. Dedikert 50-samtidige-runs-regresjonstest fra fase 3. |
| DST/tz-bugs gir dobbeltkjøring | Middels | Høy | `max_concurrent_runs: 1` som standard demper konsekvensen. Egen DST-testsuite. `pulseq next --at` for inspeksjon. |
| Sikkerhetshendelse (RCE via eksponert endepunkt) | Lav (med unix-socket) | Kritisk | Ingen TCP i MVP. Ærlig trusselmodell i `SECURITY.md`. UI read-only og loopback-bundet. |
| Feature requests presser mot mini-Dagster | Høy | Middels | `SCOPE.md` som standardsvar. "Nei" er billigere enn en avhengighet. |
| Logglagring sprenger disk | Middels | Lav | `retention_days` med fornuftig standard, housekeeping-tick, rotasjon. |
| Go-økosystem-avhengighet råtner (jf. `robfig/cron`, uvedlikeholdt siden 2020) | Middels | Lav | Maks 5 avhengigheter. Cron-parsing er ~200 linjer — vurder å skrive den selv fremfor å arve en uvedlikeholdt avhengighet. |

---

## 9. Den ene setningen å henge på veggen

> **Pulseq er ferdig når tre av mine egne cron-jobber er slettet fra crontab, og `explain` har spart meg for feilsøking tre ganger. Alt jeg bygger etter det punktet må rettferdiggjøre seg selv mot en bruker som ikke er meg.**

---

### Kilder brukt i pre-mortem-analysen

- [State of Open Source Workflow Orchestration Systems 2025 — pracdata.io](https://www.pracdata.io/p/state-of-workflow-orchestration-ecosystem-2025) (Luigi/Azkaban som de facto forlatt; Maestros usikre fremtid)
- [Dagu — self-hostable workflow orchestrator, ett Go-binærfil](https://github.com/dagucloud/dagu) og [Dagu: cron-alternativ](https://dagu.sh/cron-alternative)
- [Dagster: run_status_sensor overveldes; cursor/run_key-problemer (issue #19224)](https://github.com/dagster-io/dagster/issues/19224)
- [Dagster Sensors — cursor vs. run_key-semantikk](https://docs.dagster.io/guides/automate/sensors)
- [Dumb Ways for an Open Source Project to Die — Andrew Nesbitt](https://nesbitt.io/2026/05/19/dumb-ways-for-an-open-source-project-to-die.html) (burnout-platå, scope creep, én-maintainer-risiko)
- [SQLITE_BUSY til tross for timeout — Bert Hubert](https://berthub.eu/articles/posts/a-brief-post-on-sqlite3-database-locked-despite-timeout/) og [Understanding SQLITE_BUSY](http://activesphere.com/blog/2018/12/24/understanding-sqlite-busy) (låseoppgradering, hvorfor `_txlock=immediate`)
- [File Locking And Concurrency In SQLite Version 3](https://sqlite.org/lockingv3.html)
- [Airflow: catchup/backfill-fallgruver](https://www.jongwow.kr/en/data-engineering/airflow-dag-design-principles) og [We're All Using Airflow Wrong](https://medium.com/bluecore-engineering/were-all-using-airflow-wrong-and-how-to-fix-it-a56f14cb0753) (operator-paradigmet blander orkestrering og eksekvering)
- [Why We Deleted Our Visual Workflow Builder](https://enterprisecontextmanagement.substack.com/p/why-we-deleted-our-visual-workflow) (UI-canvas som jager virkeligheten i stedet for å representere den)
- [netresearch/go-cron](https://github.com/netresearch/go-cron) (robfig/cron uvedlikeholdt siden 2020, 50+ åpne PR-er)
- [Pulseq — open source framework for MR pulse sequences](https://pulseq.github.io/) (navnekollisjonen)
