# Pulseq — prosjektplan fra CLI- og DX-perspektivet

> Planlegger 03. Perspektiv: kommandoflaten, jobbdefinisjonsformatet, sensor-kontrakten,
> feilmeldinger, explain/preview, og veien fra `go install` til første kjørte jobb.

---

## 1. Designfilosofi

Pulseq konkurrerer ikke på funksjonsmengde. Den konkurrerer på at et menneske
klokka 03:14 kan svare på "hvorfor kjørte ikke dette?" på under ett minutt.
Alt annet er underordnet. Fem prinsipper styrer alle valg nedenfor:

1. **Ingen daemon for å komme i gang.** `pulseq run hei` må virke rett etter
   `go install`. Planleggeren er en oppgradering, ikke en forutsetning.
2. **Beslutninger er data, ikke logglinjer.** Planleggeren skriver en rad i
   `decisions` for hver eneste beslutning — startet, hoppet over, utsatt,
   deduplisert. `explain` er en renderer over denne tabellen, ikke en
   re-utledning i ettertid. Uten denne invarianten lyver `explain` før eller siden.
3. **Feilmeldingen er dokumentasjonen.** Hver feil har kode, kildeposisjon,
   årsak og et neste steg som kan kopieres og limes.
4. **Formatet skal være lesbart dag 1.** Vi ofrer uttrykkskraft for at en
   sysadmin som flytter fra cron skal forstå filen uten å lese en manual.
5. **Komponerbar mot skript.** TTY → mennesketekst. Pipe → JSON. Exit-koden
   skiller mellom "verktøyet feilet" og "jobben feilet".

---

## 2. Teknologivalg

| Område | Valg | Begrunnelse | Vurdert alternativ |
|---|---|---|---|
| CLI-rammeverk | `spf13/cobra` + `pflag` | Kommandogrupper i hjelpeteksten, completion for bash/zsh/fish/powershell ut av boksen, og `ValidArgsFunction` for **dynamisk** completion (jobbnavn og run-ID-er hentet fra SQLite). Genererer også man-sider. | `urfave/cli/v3` (lettere, men svakere completion-modell); egen parser (for dyrt) |
| YAML-parser | `goccy/go-yaml` | Har `FormatError` og `Path.AnnotateSource` som gir kildeutdrag med linjenummer og caret rett ut av boksen, pluss `DisallowUnknownField()`. Dette er hele grunnlaget for diagnostikk-kvaliteten i kapittel 8. | `gopkg.in/yaml.v3` (posisjonsinfo finnes, men må bygges opp manuelt) |
| SQLite-driver | `modernc.org/sqlite` (ren Go) | `CGO_ENABLED=0` betyr at `go install` og kryss-kompilering bare virker, og at binæren er én statisk fil. Det er en DX-beslutning, ikke en ytelsesbeslutning. Prisen er ~2× tregere INSERT, som vi kompenserer for ved å holde logger utenfor databasen (§10.3). | `mattn/go-sqlite3` (raskere, men cgo ødelegger distribusjonen). Beholdes bak byggtag `sqlite_cgo` som rømningsvei. |
| Cron-parsing | `robfig/cron/v3` (kun `Parser`/`Schedule`) | Moden spec-parser med `@every`/`@daily`-deskriptorer og `Next()`. Vi bruker **ikke** dens runner — vi trenger lease, catchup og decision-logg selv. | `adhocore/gronx` (fin `NextTickAfter`, mindre utbredt) |
| Terminal-rendering | `charmbracelet/lipgloss` | Tabeller, farge, bredde-tilpasning uten å dra inn en hel TUI-runtime. | `bubbletea` — kun til `pulseq top` i fase 5 |
| Skjema-generering | `invopop/jsonschema` | Genererer JSON Schema fra Go-structene i `internal/spec` → editor-autofullføring gratis via `yaml-language-server`-modeline. | Håndskrevet skjema (divergerer garantert) |
| Run-ID | `oklog/ulid` | Sorterbar, tidsstemplet, prefiks-søkbar som git-SHA-er (`pulseq runs show 01JQ8`). | UUIDv4 (ikke sorterbar, ikke prefiks-vennlig) |
| CLI-testing | `rogpeppe/go-internal/testscript` | Testfiler er skript som ligner det brukeren skriver; golden-output rett i testfilen. | Håndskrevne Go-tester (verre å lese, verre å vedlikeholde) |
| Starlark (fase 5) | `go.starlark.net` | Hermetisk, ingen I/O, deterministisk — trygt for `preview`. | CUE (for bratt læringskurve for målgruppen), Jsonnet (mindre kjent) |
| Valgfri polering | `charmbracelet/fang` | Stilsatt hjelp, feil, versjon, man-sider over Cobra. Eksperimentell — vurderes i fase 5, ikke MVP. | — |

---

## 3. Jobbdefinisjonsformat: valg og begrunnelse

### 3.1 Vurderte alternativer

| Format | For | Mot | Dom |
|---|---|---|---|
| **YAML** | Null læringskostnad for målgruppen. Editorstøtte overalt. JSON Schema gir autofullføring gratis. 1:1 med IR-en. | Ingen løkker/funksjoner. Implisitt typing (Norway-problemet). Innrykk. | **Valgt som kanonisk overflate** |
| HCL2 | Uttrykk, `for_each`, virkelig gode diagnostikk-primitiver. Kjent for Terraform-folk. | Verktøyspesifikt språk, ukjent for sysadmins. Dårlig editorstøtte utenfor Terraform. | Avvist |
| CUE | Uslåelig validering og unifikasjon. Skjema og data i ett. | Bratt læringskurve dreper 5-minutters-målet. Stor avhengighet. | Avvist for MVP |
| Starlark | Perfekt for DRY og fan-out over N tenants. Hermetisk → trygg preview. | "Config som kode" gjør `pulseq jobs edit` og statisk inspeksjon vanskeligere. Overkill for én cron-jobb. | **Valgt som fase-5-frontend, ikke som primærformat** |
| Ren Go | Type-sikkerhet, ingen parser. | Krever rekompilering av binæren for å endre en cron-streng. Diskvalifiserende for drift. | Avvist |

### 3.2 Arkitekturen som gjør valget billig

Motoren leser aldri YAML. Den leser en kanonisk JSON-IR, `pulseq.job.v1`.
Frontends kompilerer til IR-en:

```
jobs/*.yaml    ─┐
jobs/*.star    ─┼─▶  pulseq.job.v1 (JSON)  ─▶  planlegger / executor / explain
generator: ./x ─┘
```

Konsekvenser:

* `pulseq jobs compile <navn>` skriver IR-en til stdout. Alt annet — `explain`,
  `preview`, `graph`, `dry-run` — virker likt uansett frontend.
* IR-en har en `spec_hash`. Hver run lagrer hashen den kjørte med, så
  `pulseq runs show` kan si *"definisjonen har endret seg siden denne runen"*.
* Et nytt format er en ny frontend, ikke en ny motor.
* Rømningsluke: `generator:` peker på et vilkårlig program som skriver IR til
  stdout. Da kan folk bruke Python, Nix, jsonnet eller hva de vil, uten at vi
  eier problemet.

### 3.3 Templating — den eneste harde regelen

Go `text/template`, og **kun i verdifelt, aldri i strukturen**. YAML-en parses
først, deretter renderes strengverdiene. Vi preprosesserer aldri filteksten.
Det er forskjellen mellom en lesbar konfigfil og Helm.

* Kontekster: `.params`, `.trigger`, `.run`, `.job`, `.step`, `.workdir`, `.env`
* Funksjoner: en fast, kort liste — `now`, `dateAdd`, `dateFmt`, `default`,
  `required`, `env`, `secret`, `artifact`, `upper`, `lower`, `trim`, `join`.
  Ingen full sprig; flaten skal kunne dokumenteres på én skjerm.
* Rendering skjer **én gang**, ved run-planlegging, og resultatet persisteres.
  `pulseq runs show <id> --rendered` viser den eksakte kommandoen som ble kjørt.

### 3.4 `pulseq.yaml` — prosjektrot

```yaml
version: 1
project: analytics

db: .pulseq/state.db
log_dir: .pulseq/logs

jobs:
  - jobs/**/*.yaml
  - sensors/**/*.yaml

defaults:
  timeout: 1h
  shell: ["/bin/bash", "-euo", "pipefail", "-c"]
  retry:
    max: 2
    backoff: exponential
    initial: 30s
    max_delay: 10m

env_file: .env
env:
  TZ: Europe/Oslo

notify:
  on_failure:
    - run: ./bin/varsle-vakt "{{ .job.name }} feilet: {{ .run.id }}"
```

### 3.5 `jobs/nightly-report.yaml` — en full DAG

```yaml
# yaml-language-server: $schema=../.pulseq/job.schema.json
job: nightly-report
description: Bygger og sender daglig salgsrapport.

params:
  date:
    type: date
    default: "{{ dateAdd -1 \"day\" now | dateFmt \"2006-01-02\" }}"
    description: Rapportdato (YYYY-MM-DD).
  dry_run:
    type: bool
    default: false

concurrency:
  key: "nightly-report/{{ .params.date }}"
  max: 1
  on_conflict: skip          # skip | queue | cancel_previous

timeout: 45m
workdir: auto                # ny temp-katalog per run, ryddes med mindre --keep

env:
  PGHOST: db.internal
  PGPASSWORD: "secret:file:/run/secrets/pg"

steps:
  - name: extract
    run: ./bin/extract --date {{ .params.date }} --out {{ .workdir }}/raw.parquet
    retry:
      max: 3
      backoff: exponential
      initial: 10s
      on_exit_codes: [75]    # kun EX_TEMPFAIL er verdt et nytt forsøk
    artifacts:
      - name: raw
        path: "{{ .workdir }}/raw.parquet"

  - name: transform
    needs: [extract]
    run: ./bin/transform --in {{ artifact "raw" }} --out {{ .workdir }}/report.csv
    artifacts:
      - name: report
        path: "{{ .workdir }}/report.csv"

  - name: validate-rader
    needs: [transform]
    run: test $(wc -l < {{ artifact "report" }}) -gt 100

  - name: sjekk-sum
    needs: [transform]        # parallelt med validate-rader
    run: ./bin/sjekk-sum {{ artifact "report" }}

  - name: send
    needs: [validate-rader, sjekk-sum]
    if: '{{ not .params.dry_run }}'
    run: ./bin/send-rapport {{ artifact "report" }}
    continue_on_error: false

schedules:
  - name: nightly
    cron: "0 3 * * *"
    timezone: Europe/Oslo
    catchup: skip            # skip | one | all
    max_catchup: 3
    jitter: 2m

on_failure:
  - run: ./bin/varsle-vakt "nightly-report feilet på steg {{ .step.name }}"
```

### 3.6 `sensors/dropzone.yaml` — sensor med eksternt skript

```yaml
sensor: s3-dropzone
description: Trigg import per ny fil i dropzone.
job: import-file

interval: 60s
timeout: 30s
max_triggers_per_tick: 100
max_consecutive_failures: 10

exec:
  run: ./sensors/nye-objekter.sh
  env:
    BUCKET: acme-dropzone
  format: json               # json | ndjson | lines
```

### 3.7 Innebygde sensortyper — 80 % uten noen subprosess

```yaml
sensor: innboks
type: file
job: import-file
interval: 15s
watch:
  path: /var/spool/innboks
  glob: "*.csv"
  stable_for: 10s            # ikke trigg før filen har stått i ro
  run_key: "{{ .file.path }}:{{ .file.mtime }}"
```

```yaml
sensor: api-endringer
type: http
job: sync-endring
interval: 5m
request:
  url: "https://api.example.com/changes?since={{ .cursor | default \"1970-01-01\" }}"
  headers:
    Authorization: "Bearer {{ secret \"env:API_TOKEN\" }}"
select:
  triggers: "$.items[*]"
  run_key: "$.id"
  cursor: "$.items[-1:].updated_at"
```

```yaml
sensor: nye-ordre
type: sql
job: behandle-ordre
interval: 30s
dsn: "env:ANALYTICS_DSN"
query: |
  select id, updated_at
  from ordre
  where updated_at > coalesce(:cursor, '1970-01-01')
  order by updated_at
  limit 500
run_key: "{{ .row.id }}"
cursor: "{{ .row.updated_at }}"
```

### 3.8 Starlark-frontend (fase 5) og generator-luken

```python
# jobs/tenants.star
TENANTS = ["acme", "globex", "initech"]

def jobs():
    return [
        job(
            name = "sync-%s" % t,
            steps = [step(name = "sync", run = "./bin/sync --tenant %s" % t)],
            schedules = [cron("0 */4 * * *", timezone = "Europe/Oslo")],
        )
        for t in TENANTS
    ]
```

```yaml
# pulseq.yaml
jobs:
  - jobs/**/*.yaml
  - generator: ./gen/jobber.py    # skriver pulseq.job.v1 JSON til stdout
    cache_key: ./gen/kilder.txt
```

---

## 4. Sensor-kontrakten

**Subprocess, ikke Go-plugin.** `plugin`-pakken i Go krever identisk
Go-versjon og identiske avhengigheter mellom vert og plugin, virker ikke på
alle plattformer, og gjør distribusjon via `go install` umulig. Avvist.
En subprosess er språknøytral, testbar frittstående, og krever null SDK.

### 4.1 Inn

Sensorprosessen får konteksten på **to** måter samtidig — JSON på stdin for
skikkelige programmer, miljøvariabler for one-linere:

```json
{
  "sensor": "s3-dropzone",
  "job": "import-file",
  "cursor": "2026-08-21/03-11-02.csv",
  "tick_at": "2026-08-21T09:25:00Z",
  "last_tick_at": "2026-08-21T09:24:00Z",
  "last_status": "ok",
  "dry_run": false,
  "state_dir": "/proj/.pulseq/sensors/s3-dropzone"
}
```

Tilsvarende: `PULSEQ_SENSOR`, `PULSEQ_JOB`, `PULSEQ_CURSOR`,
`PULSEQ_TICK_AT`, `PULSEQ_LAST_TICK_AT`, `PULSEQ_DRY_RUN`, `PULSEQ_STATE_DIR`.

### 4.2 Ut

`format: json` (standard) — ett objekt på stdout:

```json
{
  "cursor": "2026-08-21/09-20-03.csv",
  "triggers": [
    {"run_key": "2026-08-21/09-14-51.csv", "params": {"key": "…09-14-51.csv"}},
    {"run_key": "2026-08-21/09-20-03.csv", "params": {"key": "…09-20-03.csv"}}
  ],
  "skip_reason": null,
  "next_tick_in": "30s"
}
```

`format: lines` — hver ikke-tomme linje er én trigger. `run_key` = linjen,
`params.value` = linjen, cursor = siste linje. `#`-linjer ignoreres. Dette er
5-minutters-sensoren:

```yaml
exec:
  run: |
    aws s3 ls s3://$BUCKET --recursive |
      awk -v c="$PULSEQ_CURSOR" '$4 > c {print $4}'
  format: lines
```

`format: ndjson` — én trigger per linje; cursor settes av en linje `{"cursor": …}`.

### 4.3 Regler og garantier

* Tom stdout + exit 0 → skip med grunn `"ingen output fra sensor"`.
* **Cursor rykker aldri fram ved feil.** Skrives kun når exit-koden er 0.
  Dette er sensorens viktigste garanti og må stå i dokumentasjonen med fete typer.
* `run_key` gir idempotens: unik indeks på `(job, run_key)`. Allerede sett
  nøkkel → ingen ny run, men en `decisions`-rad med `dedup_of`.
  Dedup skjer også *innenfor* én tick.
* `skip_reason` sammen med ikke-tomme `triggers`: triggerne vinner,
  skip-grunnen logges som notat.
* `job` kan settes per trigger og overstyrer sensorens `job` (multi-job-sensor).
* `next_tick_in` lar sensoren styre sin egen backoff.
* stderr er logg. Siste 4 KB vises i `pulseq sensors show`.

### 4.4 Exit-koder fra sensoren

| Kode | Betydning | Effekt |
|---|---|---|
| 0 | OK | Cursor lagres, triggere opprettes |
| 75 (`EX_TEMPFAIL`) | Forbigående | Kort backoff, teller **ikke** mot circuit breaker |
| 64 (`EX_USAGE`) | Konfigurasjonsfeil | Sensoren pauses, krever inngripen |
| annet ≠ 0 | Feil | Eksponentiell backoff, circuit breaker etter `max_consecutive_failures` |

### 4.5 Ressursgrenser

Hard `timeout` per tick (standard 30s). Prosessen startes i egen prosessgruppe
(`setpgid`) og hele gruppen drepes ved timeout — ellers overlever `curl`-barn
og lekker. `max_output` 4 MB. Sensoren kjøres aldri parallelt med seg selv.

### 4.6 Frittstående utvikling

```bash
pulseq sensors test s3-dropzone --print-input | ./sensors/nye-objekter.sh | jq
```

Sensoren kan altså utvikles og debugges helt uten Pulseq i loopen.

---

## 5. Kommandoflaten

### 5.1 Grammatikk

**Verb på toppnivå for de ~10 daglige handlingene** (systemctl/fly-følelsen),
**substantivgrupper for hele forvaltningsflaten** (gh/kubectl-følelsen).
Alle substantivgrupper er i flertall (`jobs`, `runs`, `schedules`, `sensors`,
`workers`) med entallsalias registrert — med **én bevisst unntak**: `run` er
reservert som verb, fordi `pulseq run nightly-report` er den mest skrevne
kommandoen i hele verktøyet og fortjener den korteste formen. Unntaket
håndteres av en feilmelding som lærer bort forskjellen (§8.3).

### 5.2 Globale flagg

```
-C, --dir <sti>        prosjektrot (standard: nærmeste pulseq.yaml oppover)
    --db <sti>         overstyr state-database
    --socket <sti>     daemon-socket; "none" tvinger direkte SQLite-modus
-o, --output <fmt>     text | wide | json | ndjson | yaml
                       (standard: text ved TTY, json ved pipe)
-q, --quiet            bare feil
-v, --verbose          repeterbar: -v = debug, -vv = trace
    --no-color         (respekterer også NO_COLOR / CLICOLOR_FORCE)
    --tz <sone>        tidssone for visning
    --absolute         absolutte tidsstempler i stedet for "for 3m"
-y, --yes              ikke spør om bekreftelse
```

### 5.3 Referansesyntaks

`job/<navn>` · `run/<ulid-eller-prefiks>` · `schedule/<jobb>.<navn>` ·
`sensor/<navn>` · `step/<run>.<steg>`. Et bart navn tolkes heuristisk;
er det tvetydig, viser feilmeldingen alternativene. Run-ID-er kan forkortes
til unikt prefiks, som git.

### 5.4 Toppnivå-verb

| Kommando | Gjør |
|---|---|
| `pulseq init [--example dag\|sensor\|report]` | Scaffolder prosjekt |
| `pulseq run <jobb> [flagg]` | Start en run nå |
| `pulseq ls` | Én skjerm: alle jobber, triggere, siste og neste kjøring |
| `pulseq ps` | Aktive runs |
| `pulseq status [<ref>]` | Statusblokk for jobb/run/schedule/sensor |
| `pulseq logs <ref> [-f]` | Logger |
| `pulseq explain <ref> [--at <t>]` | Hvorfor skjedde / skjedde ikke dette |
| `pulseq validate [sti…]` | Skjema- og semantikk-sjekk |
| `pulseq serve` | Start planlegger + sensorer + workers |
| `pulseq doctor` | Miljøsjekk |
| `pulseq top` | Live TUI (fase 5) |

### 5.5 Substantivgrupper

```
pulseq jobs      list | show | validate | compile | graph | history | enable | disable | edit
pulseq runs      list | show | logs | cancel | retry | replay | artifacts | watch | prune
pulseq schedules list | show | pause | resume | preview | backfill | history
pulseq sensors   list | show | pause | resume | test | tick | cursor {get,set,reset} | history
pulseq workers   list | drain
pulseq db        migrate | backup | restore | vacuum | stats
pulseq config    show [--origin] | path
pulseq schema    [job|sensor|project]
pulseq error     <kode>
pulseq completion {bash|zsh|fish|powershell} [install]
pulseq version   [--json]
```

### 5.6 `retry` versus `replay` — presis semantikk

Dette skillet må være knivskarpt, ellers blir det verktøyets vanligste
misforståelse.

* `pulseq runs retry <id>` — **fortsett samme run.** Beholder run-ID og
  run_key. Kjører kun steg med status `failed` eller `pending`, gjenbruker
  artefakter fra vellykkede steg. Ny `attempt` på de berørte stegene.
* `pulseq runs replay <id>` — **ny run.** Kopierer params, trigger og
  `spec_hash`. `--from <steg>` starter delvis, `--only <steg>` kjører
  nøyaktig ett steg, `--fresh` ignorerer artefaktgjenbruk,
  `--spec current` bruker gjeldende definisjon i stedet for den historiske.

### 5.7 Eksempler på kommandobruk

```bash
# Kjør nå, vent på resultat, la exit-koden reflektere jobben (for cron-migrering)
pulseq run nightly-report --param date=2026-08-01 --wait

# Se hva den ville gjort, uten å gjøre det
pulseq run nightly-report --dry-run
pulseq run nightly-report --dry-run=commands   # skriv ut eksakte shell-kommandoer

# Bare ett steg, mot en eksisterende run
pulseq runs replay 01JQ8 --only transform --fresh

# Neste ti tikk, i schedulens tidssone
pulseq schedules preview job/nightly-report.nightly --count 10

# Backfill to uker, to i slengen, forhåndsvist først
pulseq schedules backfill job/nightly-report.nightly \
  --from 2026-08-01 --to 2026-08-14 --concurrency 2 --dry-run

# Test en sensor uten bivirkninger
pulseq sensors test s3-dropzone

# Flytt en cursor tilbake for å re-prosessere
pulseq sensors cursor set s3-dropzone "2026-08-20/00-00-00.csv"

# Følg logger for et enkelt steg på tvers av forsøk
pulseq logs run/01JQ8 --step transform --all-attempts -f

# Maskinlesbart til overvåkning
pulseq runs list --since 24h --status failed -o ndjson | jq -r .id
```

---

## 6. `explain` — produktets viktigste kommando

### 6.1 Fundamentet

`explain` re-utleder aldri noe. Planleggeren, sensor-loopen og executoren
skriver en `decisions`-rad for hver beslutning:

```
decisions(id, at, actor, subject_ref, verdict, reason_code, reason_text, detail_json)
  actor   ∈ {scheduler, sensor, worker, cli}
  verdict ∈ {started, skipped, deferred, deduped, failed, cancelled}
```

Regelen er absolutt: **ingen beslutning uten en rad.** Dette håndheves av en
test som kjører planleggeren gjennom alle grenser og verifiserer at hver
kodevei som ikke starter en run, har skrevet en `skipped`- eller
`deferred`-rad.

### 6.2 Slik ser det ut

```
$ pulseq explain schedule/nightly-report.nightly

schedule/nightly-report.nightly — ingen run ved siste tikk

  Tikk 2026-08-21 03:00:00 +02:00 (Europe/Oslo)
    ✗ hoppet over — samtidighetsgrense nådd            [PQ3011]
      concurrency.max = 1 for job "nightly-report"
      run 01JQ8ZKXM7 startet 2026-08-20 03:00:04, kjører fortsatt (24t 3m)

      → avbryt den:   pulseq runs cancel 01JQ8ZKXM7
      → eller hev:    concurrency.max i jobs/nightly-report.yaml:14
      → eller kø opp: concurrency.on_conflict: queue

  Tikk 2026-08-20 03:00:00 +02:00
    ✓ startet run 01JQ8ZKXM7

  Neste tikk: 2026-08-22 03:00:00 +02:00 (om 22t 14m)
```

```
$ pulseq explain run/01JQ9

run 01JQ9F2M4K — job import-file — feilet

  Startet av    sensor "s3-dropzone", tikk 2026-08-21 09:25:00Z
  run_key       2026-08-21/09-14-51.csv
  cursor        "2026-08-21/03-11-02.csv" → "2026-08-21/09-20-03.csv"
  spec_hash     sha256:4f1c… (uendret siden)

  Feilet på steg "parse" etter 3 forsøk:
    forsøk 1  09:25:02  exit 75  → nytt forsøk om 10s   (on_exit_codes: [75])
    forsøk 2  09:25:13  exit 75  → nytt forsøk om 20s
    forsøk 3  09:25:34  exit 1   → ingen flere forsøk (1 ∉ on_exit_codes)

    siste 3 linjer stderr:
      parse: uventet kolonne "beløp_ex_mva" på linje 4021
      parse: forventet skjema v3, fant v2

  → logger:     pulseq logs 01JQ9F2M4K --step parse
  → prøv igjen: pulseq runs retry 01JQ9F2M4K
```

```
$ pulseq explain sensor/s3-dropzone --last 5

sensor/s3-dropzone — 5 siste tikk

  09:25:00  ✓ 2 triggere, 1 deduplisert   cursor 03-11-02 → 09-20-03
  09:24:00  – hoppet over: "ingen nye objekter"
  09:23:00  – hoppet over: "ingen nye objekter"
  09:22:00  ✗ exit 75 (forbigående) — "connection reset by peer"
            cursor uendret, backoff 30s
  09:21:00  – hoppet over: "ingen nye objekter"

  Circuit breaker: lukket (0 av 10 påfølgende feil)
```

### 6.3 Tidsreise

`pulseq explain job/x --at "2026-08-20T03:00"` gjengir hva systemet faktisk
besluttet i det øyeblikket — direkte fra `decisions`, uansett hvordan
definisjonen har endret seg siden.

### 6.4 Preview og dry-run

| Kommando | Bivirkninger | Viser |
|---|---|---|
| `pulseq run <j> --dry-run` | ingen | Løst DAG, params, env, artefaktstier, planlagt rekkefølge og parallellitet |
| `pulseq run <j> --dry-run=commands` | ingen | Som over, pluss de eksakt renderede shell-kommandoene |
| `pulseq schedules preview` | ingen | Neste N tikk, med DST-hopp markert eksplisitt |
| `pulseq schedules backfill --dry-run` | ingen | Hvilke tikk som ville blitt kjørt, og hvilke som allerede har en run |
| `pulseq sensors test <s>` | ingen | Kjører sensoren med `dry_run: true`, viser triggere og dedup-dom, lagrer **ikke** cursor, oppretter **ingen** runs |
| `pulseq sensors tick <s>` | ekte | Én tvungen tick nå |

```
$ pulseq sensors test s3-dropzone

Tørrkjøring av sensor s3-dropzone — ingen runs, cursor lagres ikke.

  kommando   ./sensors/nye-objekter.sh
  cursor inn "2026-08-21/03-11-02.csv"
  varighet   412ms
  exit       0

  3 triggere:
    RUN_KEY                    PARAMS                DEDUP
    2026-08-21/09-14-51.csv    key=…09-14-51.csv     ny
    2026-08-21/09-20-03.csv    key=…09-20-03.csv     ny
    2026-08-21/03-11-02.csv    key=…03-11-02.csv     allerede kjørt (01JQ8ZK)

  cursor ut  "2026-08-21/09-20-03.csv"
  effekt     2 nye runs av job "import-file" ved en ekte tick

Kjør på ekte:  pulseq sensors tick s3-dropzone
```

```
$ pulseq jobs graph nightly-report

  extract ──▶ transform ─┬─▶ validate-rader ─┐
                         └─▶ sjekk-sum ──────┴─▶ send

  4 nivåer, maks parallellitet 2, kritisk sti 2m 11s (median siste 10 runs)
```

`--format mermaid|dot|json` for videre bruk.

---

## 7. Output, komposisjon og exit-koder

### 7.1 Output-modus

TTY → mennesketekst med farge og relative tidsstempler. Pipe → JSON, med
absolutte RFC3339-tidsstempler og uten trunkering. `-o` overstyrer alltid.
Progress, spinnere og advarsler går til **stderr**; data går til **stdout**.
Ved manglende UTF-8 i `LANG` faller symbolene tilbake til ASCII (`✓` → `OK`).

### 7.2 Exit-koder

| Kode | Betydning |
|---|---|
| 0 | OK |
| 1 | Uventet intern feil |
| 2 | Bruksfeil (ugyldig argument eller flagg) |
| 3 | Fant ikke ressursen |
| 4 | Validering feilet |
| 5 | **Runen feilet** (kun med `--wait`) |
| 6 | Opptatt / konflikt (lease holdt, samtidighetsgrense) |
| 7 | Timeout |
| 8 | Avbrutt (SIGINT) |

Skillet mellom 1 og 5 er kritisk: et skript må kunne skille "pulseq er ødelagt"
fra "jobben feilet". Det gjør migrering fra cron trygg — `pulseq run x --wait`
er en drop-in-erstatning for kommandoen som lå i crontab.

### 7.3 Signalhåndtering

SIGINT i `pulseq run` (forgrunn) → grasiøs avbrytelse: barneprosessgruppen får
SIGTERM, deretter SIGKILL etter `grace_period` (standard 10s). Runen får status
`cancelled`, ikke `failed`, og en `decisions`-rad med `actor: cli`. Andre
SIGINT dreper umiddelbart.

---

## 8. Feilmeldinger og diagnostikk

### 8.1 Anatomi

Hver diagnose har: **kode** (`PQ####`), **alvorlighet**, **kildeposisjon**,
**utdrag med caret**, **årsak**, og **minst ett konkret neste steg**.
`pulseq error PQ1042` skriver den lange forklaringen, à la `rustc --explain`.

Kodeserier: `PQ1xxx` parsing/skjema · `PQ2xxx` semantikk (sykluser, ukjente
referanser) · `PQ3xxx` kjøretid/planlegging · `PQ4xxx` sensor ·
`PQ5xxx` lagring/lease · `PQ9xxx` internt.

### 8.2 Eksempler

```
$ pulseq validate
✗ jobs/nightly-report.yaml:22:5 — PQ1042: ukjent felt "retries" i steg "transform"

   20 |   - name: transform
   21 |     needs: [extract]
>  22 |     retries: 3
            ^
   23 |     run: ./bin/transform …

   Mente du "retry"? Feltet er et objekt, ikke et tall:

       retry:
         max: 3

   pulseq error PQ1042   for full forklaring
```

```
✗ jobs/etl.yaml — PQ2003: syklisk avhengighet mellom steg

     load ──▶ transform ──▶ validate ──▶ load

   Bryt syklusen ved å fjerne "load" fra needs på steget "transform"
   (jobs/etl.yaml:31:7).
```

```
✗ jobs/report.yaml:9:11 — PQ1007: cron-uttrykket "0 3 * *" har 4 felt, forventet 5 eller 6

    9 |     cron: "0 3 * *"
                  ^^^^^^^^^

   Mente du "0 3 * * *" (hver dag kl. 03:00)?
   Deskriptorer virker også: @daily, @hourly, @every 15m
```

```
✗ PQ5002: en annen prosess holder skrive-leasen på .pulseq/state.db

   holder    pulseq serve (pid 4711), utløper om 8s
   forsøkte  pulseq run nightly-report

   → send til daemonen i stedet:  pulseq run nightly-report --socket auto
   → eller stopp daemonen:        systemctl --user stop pulseq
```

### 8.3 Selvlærende feilmeldinger

Den ene bevisste inkonsistensen i grammatikken (`run` som verb, `runs` som
substantiv) forsvares av meldingen:

```
$ pulseq run list
✗ PQ3001: fant ingen jobb med navnet "list"

   "run" er et verb i pulseq — det starter en jobb:
       pulseq run <jobb>          start en run nå

   Skulle du liste runs? Substantivet er i flertall:
       pulseq runs list           list de siste runs
       pulseq ps                  bare de aktive
```

`Did-you-mean` bruker Levenshtein over kjente jobbnavn, feltnavn og
underkommandoer, med terskel på redigeringsavstand ≤ 2 eller prefiksmatch.

### 8.4 Advarsler ved validering

`pulseq validate` gir også `warning`-nivå: steg som ikke kan nås fra noen rot,
retry på et steg med `continue_on_error: true` (meningsløst), schedules med
`catchup: all` uten `max_catchup`, sensor-intervall kortere enn observert
median tick-varighet, hardkodede hemmeligheter som ser ut som tokens.
`--strict` gjør advarsler til feil (for CI).

---

## 9. Shell-completion og førstegangsopplevelse

### 9.1 Completion

Cobra genererer for bash, zsh, fish og powershell. `pulseq completion install`
skriver til riktig katalog for det aktive skallet i stedet for å be brukeren
pipe manuelt.

Dynamisk completion via `ValidArgsFunction` mot lese-poolen i SQLite:
jobbnavn, sensornavn, schedule-navn, steg-navn (avhengig av valgt jobb),
run-ID-prefikser (de 50 siste), artefaktnavn, statusverdier, tidssoner.

**Hardt budsjett: 150 ms.** Overskrides det — for eksempel fordi databasen er
låst — returnerer completion tom liste i stedet for å henge. En completion som
fryser terminalen er verre enn ingen completion. Dette dekkes av en egen test.

`cobra.AppendActiveHelp` brukes til å si hva som mangler når det ikke finnes
noe å foreslå ("skriv et jobbnavn — ingen jobber funnet, kjør `pulseq init`").

### 9.2 Fra `go install` til første run

```
$ go install go.pulseq.dev/cmd/pulseq@latest         # ~20 s, ingen cgo

$ pulseq init
Opprettet:
  pulseq.yaml                 prosjektkonfig
  jobs/hei.yaml               en eksempeljobb
  .pulseq/                    tilstand (lagt til i .gitignore)
  .pulseq/job.schema.json     autofullføring i editoren

Neste steg:
  pulseq run hei              kjør jobben nå
  pulseq serve                start planleggeren

$ pulseq run hei
run 01JQ8ZKXM7  job hei
 ▸ si-hei
   hei fra pulseq, klokka er 14:32
 ✓ si-hei                                              0.01s
✓ run 01JQ8ZKXM7 ok                                    0.02s

  pulseq ls                   se alt
  pulseq explain run/01JQ8    se hvorfor
```

Målt tid: under ett minutt. `pulseq serve` er steg tre, ikke steg null.
Dette er hele grunnen til at CLI-en må kunne kjøre en run in-process (§10.4).

`jobs/hei.yaml`:

```yaml
# yaml-language-server: $schema=../.pulseq/job.schema.json
job: hei
description: Minste mulige jobb.
steps:
  - name: si-hei
    run: echo "hei fra pulseq, klokka er $(date +%H:%M)"
schedules:
  - cron: "@every 5m"
```

`pulseq init --example dag|sensor|report` gir større, kommenterte startpunkter.

### 9.3 `pulseq doctor`

```
$ pulseq doctor
✓ pulseq 0.4.1 (linux/amd64, go1.26)
✓ prosjektrot        /srv/analytics/pulseq.yaml
✓ database           .pulseq/state.db (WAL, skjema v7, 12 MB)
✓ skriv-lease        ledig
✓ shell              /bin/bash 5.2
✗ daemon             kjører ikke
                     → pulseq serve
                     → eller: pulseq install-service --user
⚠ logg-katalog       .pulseq/logs er 2,1 GB
                     → pulseq runs prune --older-than 30d
✓ tidssone           Europe/Oslo (tzdata funnet)
✓ completion         zsh, installert
```

`pulseq install-service` genererer en systemd-unit (bruker eller system) —
den korteste veien fra "det virker lokalt" til "det kjører på serveren".

---

## 10. Arkitektur

### 10.1 Pakkeinndeling

```
cmd/pulseq/main.go
internal/cli/            cobra-kommandoer, én fil per substantivgruppe
internal/cli/render/     text/wide/json/yaml/ndjson-renderere, tabeller, farge
internal/cli/diag/       diagnoser, PQ-koder, did-you-mean, kildeutdrag
internal/spec/           IR-structer (pulseq.job.v1), JSON Schema-generering
internal/loader/         yaml-frontend, starlark-frontend, generator-luke, spec_hash
internal/store/          SQLite, single-writer-kø, migrasjoner, spørringer
internal/decide/         decisions-tabellen — alt explain leser
internal/engine/         DAG-planlegger, step-executor, retry, artefakter
internal/schedule/       cron/interval/kalender, next-tick, catchup, DST
internal/sensor/         tick-loop, exec-kontrakt, innebygde typer, cursors
internal/lease/          scheduler-/sensor-/skriv-lease med fencing token
internal/logsink/        strukturerte logger til fil, tail, rotering
internal/daemon/         serve, supervisjon, unix-socket-API
```

### 10.2 Datamodell (kjernetabeller)

```
jobs(name PK, source_path, spec_hash, spec_json, enabled, updated_at)
runs(id ULID PK, job, status, trigger_id, run_key, params_json, spec_hash,
     started_at, ended_at, exit_code, attempt, cancelled_by)
steps(run_id, name, attempt, status, started_at, ended_at, exit_code,
      log_path, rendered_cmd, PRIMARY KEY(run_id, name, attempt))
artifacts(run_id, step, name, uri, size, sha256)
schedules(id PK, job, name, expr, kind, tz, catchup, max_catchup, jitter,
          paused, last_tick, next_tick)
sensors(name PK, job, interval, paused, cursor, last_tick, last_status,
        consecutive_failures, breaker_state)
triggers(id PK, source_type, source_name, run_key, created_at,
         run_id NULL, dedup_of NULL)
decisions(id PK, at, actor, subject_ref, verdict, reason_code,
          reason_text, detail_json)
leases(name PK, holder, expires_at, fencing_token)

UNIQUE INDEX run_keys ON triggers(source_name, run_key)   -- idempotens
INDEX decisions_subject ON decisions(subject_ref, at DESC) -- explain-oppslag
```

### 10.3 SQLite med én skriver

Tre grep løser begrensningen:

1. **To connection pools.** `writeDB` med `SetMaxOpenConns(1)` og
   `_txlock=immediate`; `readDB` read-only med N forbindelser. WAL-modus,
   `busy_timeout=5000`, `synchronous=NORMAL`, `foreign_keys=on`.
2. **Serialisert skriver.** Alle skrivinger går gjennom `store.Writer`, en
   goroutine med kø. Høyfrekvente skrivinger (steg-heartbeats, statusbytter)
   batches i én transaksjon per ~50 ms. Internt oppstår aldri `SQLITE_BUSY`.
3. **Logger ligger ikke i databasen.** `.pulseq/logs/<run-id>/<steg>.<forsøk>.jsonl`
   på disk, med `log_path` som rad. Dette fjerner den desidert største
   skrivelasten fra SQLite, gjør `pulseq logs -f` til en billig tail, og gjør
   loggene grep-bare uten Pulseq. Rotering og `runs prune` rydder.

På tvers av prosesser holdes én skriver av `leases`-tabellen: bare den som
holder `writer`-leasen skriver. Fencing token forhindrer at en sovnet prosess
skriver etter at leasen er tatt over.

### 10.4 Dobbeltmodus: CLI uten daemon

```
1. Finn prosjektrot (pulseq.yaml oppover, eller -C)
2. Finnes unix-socket?  →  HTTP/JSON mot daemonen
3. Ellers               →  åpne SQLite direkte
                            lesekommandoer: alltid greit
                            skrivekommandoer: ta writer-lease
                            pulseq run: kjør en embedded worker i CLI-prosessen
```

All forretningslogikk ligger i `internal/engine` og `internal/schedule`.
Både daemonen og CLI-en er tynne kall inn i de samme funksjonene, og begge
bruker de samme rendererne — så outputen er identisk i begge moduser. En
kontraktstest kjører hele `testscript`-suiten to ganger, én gang per modus.

Socket: `$XDG_RUNTIME_DIR/pulseq/<prosjekthash>.sock`, HTTP/1.1 + JSON.
`--socket none` tvinger direktemodus (nyttig i CI og ved feilsøking).

### 10.5 Konfigurasjonspresedens

flagg → `PULSEQ_*`-miljøvariabler → prosjektets `pulseq.yaml` →
`~/.config/pulseq/config.yaml` → innebygde standarder.

`pulseq config show --origin` viser hvor hver enkelt verdi kom fra. Billig å
implementere, og fjerner en hel kategori av "hvorfor gjelder ikke innstillingen min".

### 10.6 Hemmeligheter

Referanser, ikke verdier: `env:NAVN`, `file:/sti`, `exec:kommando`.
Oppløses ved run-planlegging, aldri persistert i klartekst i `runs.params_json`.
Loggmaskering bygges fra de oppløste verdiene, så en hemmelighet som lekker ut
via stdout blir erstattet med `••••` i loggfilen.

---

## 11. MVP-kutt

**Inne i MVP:** cron- og interval-schedules, sensorer med cursor og run_key,
run-historikk, retry på stegnivå, DAG med `needs`, `pulseq run/ls/ps/status/
logs/validate/serve/init`, `jobs`/`runs`/`schedules`/`sensors`-gruppene,
strukturerte logger, JSON-output, exit-koder, completion, `explain` (redusert:
schedule og run), `--dry-run`.

**Bevisst utenfor MVP:**

| Kuttet | Begrunnelse |
|---|---|
| Web-UI | `ls`, `status` og `explain` dekker behovet. Web-UI er fase 5, og skal først være read-only. |
| Postgres / multi-node | SQLite på én node er hele produktets premiss. Postgres er en fase-6-diskusjon. |
| Starlark- og CUE-frontend | IR-en gjør dem billige å legge til senere. Ingen grunn til å betale nå. |
| Dynamic fan-out i DAG | Sensorer med multi-trigger dekker 90 % av behovet. |
| Container-steg (`image:`) | Kun `run:` med shell i MVP. |
| Distribuerte workers | Lokale prosesser. `workers`-gruppen finnes, men lister bare lokale. |
| Artefakt-lagring | Vi lagrer referanse + sha256 + størrelse, ikke innholdet. |
| Notifications utover `on_failure`-hooks | En hook som kjører en kommando er nok; ingen SMTP/Slack-integrasjon i kjernen. |
| bubbletea-TUI (`top`) | Fase 5. |
| Secrets-manager-integrasjon | `env:`/`file:`/`exec:` dekker Vault, sops og systemd credentials indirekte. |

---

## 12. Faser

### F0 — Skjelett (uke 1)
Repo, `cmd/pulseq`, cobra-rot med globale flagg, `version`, `doctor`, `init`.
`internal/spec` med IR-structer og JSON Schema-generering. goccy-parser med
`DisallowUnknownField`. `internal/cli/diag` med PQ-koder, kildeutdrag og
did-you-mean. `pulseq validate` og `pulseq schema`. Ingen database ennå.
**Leverer:** man kan skrive en jobbfil og få en god feilmelding.

### F1 — Kjøre noe (uke 2–3)
`internal/store` med migrasjoner og single-writer-kø. DAG-planlegger,
step-executor, retry, timeout, prosessgruppe-drap. Logger til fil.
`pulseq run` (in-process, uten daemon), `runs list/show/logs`, exit-koder,
`-o json`. `testscript`-suite.
**Leverer:** 5-minutters-opplevelsen i §9.2. Dette er milepælen som avgjør alt.

### F2 — Tid (uke 4–5)
Cron-/interval-parsing med tidssone og DST-håndtering. Tick-loop med lease.
Catchup med `max_catchup`. `pulseq serve`, `install-service`.
`schedules list/show/preview/pause/resume`. `ls`, `ps`, `status`.
Unix-socket-API og dobbeltmodus-koblingen.
**Leverer:** cron-erstatning med innsyn.

### F3 — Hendelser (uke 6–7)
Sensor-tick-loop, subprocess-kontrakten i sin helhet, cursor-persistens,
run_key-dedup, circuit breaker. Innebygde typer `file`, `http`, `exec`.
`sensors list/show/test/tick/cursor/pause/resume`.
**Leverer:** det som skiller Pulseq fra cron.

### F4 — Forklaring (uke 8)
`decisions` innført gjennomgående, med testen som håndhever invarianten.
`pulseq explain` for job, run, schedule og sensor, inkludert `--at`.
`--dry-run` og `--dry-run=commands`. `jobs graph`. `runs retry/replay`.
`schedules backfill`. `pulseq error <kode>`. Completion med 150 ms-budsjett
og `completion install`. Man-sider.
**Leverer:** produktets faktiske salgsargument.

### F5 — Polering (uke 9–11)
`pulseq top` (bubbletea). Read-only web-UI over samme data.
`on_failure`-hooks og artefakt-lineage. `db backup/restore`.
Starlark-frontend og `generator:`-luken. `type: sql`-sensor. Distribusjon:
GoReleaser, homebrew-tap, `.deb`/`.rpm`, statiske binærer for fem plattformer.
Nettsted med skjema-URL for editor-autofullføring.

### F6 — Skala (etterpå)
Postgres-backend, flere noder, distribuerte workers, WASM-sensorer.

---

## 13. Risikoer

| # | Risiko | Tiltak |
|---|---|---|
| 1 | Kommandoflaten vokser ukontrollert | Hard regel: nye kommandoer må passe `<substantiv> <verb>`, ha `--help` med minst ett eksempel, og maks 10 toppnivå-verb totalt. Brudd stoppes i review. |
| 2 | `run` (verb) mot `runs` (substantiv) forvirrer | Alias for alle andre grupper, pluss den selvlærende feilmeldingen i §8.3. Måles i brukertest med fem personer i F2. |
| 3 | YAML-templating utvikler seg til Helm | Templates kun i verdifelt, aldri preprosessering av filteksten. Funksjonslisten er frosset og må få plass på én skjerm. |
| 4 | `modernc.org/sqlite` for tregt | Logger holdes utenfor databasen. Benchmark i F1: 10 000 runs × 5 steg. Rømningsvei: byggtag `sqlite_cgo` for `mattn/go-sqlite3`. |
| 5 | Sensor-subprosess henger eller lekker barn | Hard timeout, `setpgid` + drap av hele prosessgruppen, `max_output`, circuit breaker, breaker-status synlig i `sensors show`. |
| 6 | Dobbeltmodus gir to kodeveier som divergerer | All logikk i `engine`/`schedule`; daemon og CLI er tynne kall. Hele `testscript`-suiten kjøres i begge moduser i CI. |
| 7 | DST og sommertid gir doble eller manglende kjøringer | Lagre alltid UTC + IANA-sone. Egne tester for 2×- og 0×-timen. `schedules preview` markerer hoppet eksplisitt i outputen. |
| 8 | Catchup-storm etter nedetid | Standard `catchup: skip`, `max_catchup` påkrevd hvis `catchup: all`. `explain` oppgir hvor mange tikk som ble hoppet over og hvorfor. |
| 9 | cgo sniker seg inn og ødelegger `go install` | `CGO_ENABLED=0`-kryssbygg til fem plattformer er en blokkerende CI-gate fra F0. |
| 10 | Golden-file-tester på CLI-output blir sprø | Frosset klokke, fast terminalbredde, `--no-color`, ASCII-modus. Én golden per kommando, ikke per variant; variantene testes mot den strukturerte modellen. |
| 11 | `explain` blir usann fordi noen kodevei glemte decision-raden | Test som traverserer alle grener i planleggeren og feiler hvis en beslutning ikke etterlot en rad. Invarianten dokumenteres i `internal/decide/doc.go`. |
| 12 | Dynamisk completion henger når databasen er låst | 150 ms-budsjett med tom fallback, egen test som kjører completion mot en låst database. |
| 13 | Hemmeligheter lekker til logg eller `runs show` | Kun referanser persisteres. Loggmaskering bygges fra oppløste verdier. Egen test som planter en hemmelighet og griper etter den i alle utdata. |

---

## 14. Åpne spørsmål til de andre planleggerne

1. **Eier motoren `params`-typesystemet, eller er alt strenger?** Jeg foreslår
   et minimalt sett (`string`, `int`, `bool`, `date`, `enum`) fordi det gir
   validering og completion. Kan bli en diskusjon mot "hold kjernen liten".
2. **Skal `runs retry` bevare run-ID?** Jeg har antatt ja (§5.6). Det påvirker
   run_key-unikhet og hvordan `decisions` grupperes.
3. **Sensor som skriver runs til flere jobber** — jeg tillater `job` per
   trigger. Hvis kjernen vil holde 1:1 mellom sensor og jobb, må §4.3 endres.
4. **Artefakter: referanse eller innhold?** Jeg har kuttet kopiering fra MVP.
   Lineage i fase 5 kan kreve at vi tar det opp igjen tidligere.
5. **Web-UI-ets datakilde** — hvis den leser SQLite direkte, må lese-poolen
   og `decisions`-indeksene dimensjoneres for det fra F4.

---

## Kilder

- [Command Line Interface Guidelines (clig.dev)](https://clig.dev/)
- [PatternFly CLI handbook](https://www.patternfly.org/developer-resources/cli-handbook/)
- [Dagster: Sensors](https://docs.dagster.io/guides/automate/sensors)
- [Dagster: schedules & sensors API](https://docs.dagster.io/api/dagster/schedules-sensors)
- [Cobra — completions og custom help](https://github.com/spf13/cobra)
- [goccy/go-yaml — FormatError og AnnotateSource](https://github.com/goccy/go-yaml)
- [SQLite in Go, with and without cgo (DataStation)](https://datastation.multiprocess.io/blog/2022-05-12-sqlite-in-go-with-and-without-cgo.html)
- [sqlite-cgo-no-cgo benchmark](https://github.com/multiprocessio/sqlite-cgo-no-cgo)
- [Can configuration languages solve configuration complexity? (Brian Grant)](https://itnext.io/can-configuration-languages-dsls-solve-configuration-complexity-eee8f124e13a)
- [Kestra vs Dagster — deklarativ YAML mot Python-DSL](https://kestra.io/vs/dagster)
- [robfig/cron v3](https://pkg.go.dev/github.com/robfig/cron/v3)
- [starlark-go](https://github.com/google/starlark-go)
