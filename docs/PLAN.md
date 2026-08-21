# paceq — masterplan (tom repo → v1.0)

> Endelig plan per 2026-08-21. Referansene «SYNTESE §…» og «NN-… §…» i planen peker til
> plandokumentene fra planleggingsfasen. De ligger på git-taggen `plans`, ikke i aktiv kode;
> hent dem med `git show plans:docs/plans/<fil>`.
> Én utvikler med AI-assistanse, start 2026-08-25.

---

## A. Endelige beslutninger (kortform)

| Tema | Beslutning | Ref |
|---|---|---|
| DAG i MVP | Ja, statisk `needs`, egen milepæl M4, harde grenser (ingen betingelser/fan-out) | SYNTESE §3.1 |
| Rekkefølge | Utføring → schedules → sensorer → DAG → explain/release | §3.2 |
| Konfigformat | YAML (goccy, strict) → kanonisk JSON-IR + `spec_hash`; ingen templating | §3.3 |
| Cron | `adhocore/gronx` som parser + egen iterator m/ eksplisitt DST-policy | §3.4 |
| CLI | `spf13/cobra`; verb + substantivgrupper; exit-kode-tabell | §3.5 |
| IPC | Dual-mode: les direkte (RO), skriv via socket eller flock+direkte | §3.6 |
| Sikkerhet | Én binær, same-user i MVP; 08s dag-én-liste bindende; priv-sep post-1.0 | §3.7 |
| Durabilitet | `synchronous=NORMAL` default, `FULL` som konfig | §3.8 |
| Logger | NDJSON-filer, datoshardet, 16 MiB-kvote m/ head/tail, `error_tail` i DB | §3.9 |
| Navn | «paceq», besluttet 2026-08-21 mot kriteriene i §3.4 | ADR-0002 |
| SQLite | Én fil, to pooler, writer(1)+`_txlock=immediate`, WAL, modernc, STRICT | §3.11 |
| Postgres | Nei; kun `internal/store` som eneste SQL-eier (null-kost port) | §3.12 |
| Lease | Run-nivå + epoch-fencing; rolle-leases for scheduler/sensor/reaper | §3.13 |
| Tilstander | queued/running/succeeded/failed/cancelled; «utsatt» = available_at+defer_reason | §3.14 |
| Dedup | `run_keys(source, epoch, run_key)`; dedup-epoch løser reset-fella | §3.22 |

Budsjetter: ≤8 runtime-deps, ≤12 000 linjer kjerne-Go, binær <30 MB, `status` <100 ms.

---

## B. Milepæler

| ID | Tittel | Start | Mål | Demo-/exit-kriterium |
|---|---|---|---|---|
| M0 | Fundament og persistens | 2026-08-25 | 2026-09-04 | Konkurransetest (32 skrivere, 0 `SQLITE_BUSY`) og ytelsesport (≥500 tx/s) grønn i CI; `paceq init/doctor/version` virker; SCOPE/SECURITY committet |
| M1 | Kjørbare jobber | 2026-09-07 | 2026-09-25 | Flerstegs (sekvensiell) jobb kjøres fra CLI uten daemon: logg, retry, historikk, exit-koder; SIGKILL midt i run → restart konvergerer uten invariantbrudd |
| M2 | Daemon og schedules | 2026-09-28 | 2026-10-16 | `systemctl start paceq` gir cron-erstatning: tidssone/DST-gullstandard grønn; daemon nede 6 t → catchup-policy gjør nøyaktig det den sier; dual-mode-CLI |
| M3 | Sensorer | 2026-10-19 | 2026-11-06 | 5-linjers shell-sensor gir én run per ny fil; gjentatt tick = 0 duplikater; `reset` gir replay; SIGKILL midt i sensor-commit gir aldri tap eller duplikat |
| M4 | DAG | 2026-11-09 | 2026-11-27 | Diamant-DAG kjører to steg parallelt; feil ⇒ nedstrøms `skipped`; `retry --failed` gjenbruker vellykkede steg. Tidsboks 3 uker (fallback: kutt parallellitet) |
| M5 | Explain, import og v0.1 | 2026-11-30 | 2026-12-18 | «Hvorfor kjørte ikke X i natt?» besvares med én kommando i alle scenarier; egen crontab importert og kjørende (kill-kriterium K1: ≥3 egne jobber i prod) |
| M6 | Drift, varsling og herding (v0.2) | 2027-01-04 | 2027-01-29 | Indusert feil gir nøyaktig ett varsel; skyggerapport etter 7 døgn; 24 t soak m/ tilfeldig SIGKILL grønn 3 netter på rad (K2: explain har spart feilsøkingstid 3×) |
| M7 | UI, innebygde sensorer, backfill (v0.3–0.4) | 2027-02-01 | 2027-02-26 | Skjermbilde av skip-tidslinjen forklarer seg selv; «reager på nye S3-filer» i <10 linjer YAML uten skript; backfill med dry-run |
| M8 | Stabilisering → v1.0 | 2027-03-01 | 2027-03-31 | Oppgradering fra v0.1-DB verifisert i CI; formater frosset; pakker (deb/brew/AUR); navnebeslutning effektuert; v1.0 |

(Juleferie 2026-12-19 → 2027-01-03 er lagt inn som buffer/slakk.)

---

## C. Label-taksonomi (GitHub)

- **type:** `type:feature` · `type:bug` · `type:test` · `type:infra` · `type:docs` · `type:decision` · `type:release`
- **area:** `area:store` · `area:spec` · `area:engine` · `area:exec` · `area:scheduler` · `area:sensors` · `area:cli` · `area:observability` · `area:security` · `area:ui` · `area:release` · `area:product`
- Prioritet (P0–P3) og Estimat (S/M/L/XL) håndteres som Project-felt; speiles i issue-teksten under.
- Milepæl = GitHub Milestone M0..M8.

---

## D. Issue-backlog

### Milepæl M0 — Fundament og persistens

### [M0-01] Repo-skjelett og pakkelayout
- **Epic:** Fundament
- **Milepæl:** M0
- **Labels:** area:release, type:infra
- **Prioritet:** P0
- **Estimat:** S
- **Avhenger av:** ingen
- **Spek:** Opprett `go.mod` (Go 1.25), `cmd/paceq/main.go`, `internal/{store,model,spec,cronx,scheduler,sensor,engine,runner,cli,obs,explain,notify,id,clock,testutil}`, Makefile, `.editorconfig`, LICENSE (permissiv, jf. 09 §12.5).
  Akseptanse:
  - `go build ./...` gir statisk binær med `CGO_ENABLED=0`.
  - Avhengighetsretning (cli→engine→store→model; model importerer intet internt) håndhevet av test over `go list -deps`.
  - Se 05-go-arkitekten.md §4 (layout) og 11-distribuert-pragmatikeren.md §2 (avhengighetsregel).

### [M0-02] CI-pipeline
- **Epic:** Fundament
- **Milepæl:** M0
- **Labels:** area:release, type:infra
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-01
- **Spek:** GitHub Actions: build, `go vet`, `staticcheck`, `gosec`, `govulncheck`, `go test -race`, gofumpt-sjekk, kryssbygg linux/amd64+arm64 med `CGO_ENABLED=0` som blokkerende gate (03 §13 risiko 9). Ukentlig govulncheck på main.
  Akseptanse:
  - Grønn pipeline på tomt prosjekt; cgo-lekkasje feiler bygget.
  - Se 08-sikkerhetsskeptikeren.md §5 (leverandørkjede-krav).

### [M0-03] Styringsdokumenter: SCOPE, SECURITY, garantier
- **Epic:** Styring
- **Milepæl:** M0
- **Labels:** area:product, type:docs
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-01
- **Spek:** `SCOPE.md` (ikke-mål-listen fra SYNTESE §4.11 + differensiator-setningen fra 10 §2/F2), `SECURITY.md` med trusselmodell og eksplisitte ikke-mål (08 §2.4), `docs/garantier.md`-skjelett (at-least-once, ikke-garantier — 02 §4). Committes FØR funksjonell kode (10 fase 0).
  Akseptanse:
  - «Hvorfor ikke Dagu/cron/systemd timers?» besvares i README-utkast på ≤15 sek lesing (10 port 0).
  - Issue-mal lenker til SCOPE.md.

### [M0-04] Beslutning: produktnavn (eier)
- **Epic:** Styring
- **Milepæl:** M0
- **Labels:** area:product, type:decision
- **Prioritet:** P1
- **Estimat:** S
- **Avhenger av:** ingen
- **Spek:** Eier valgte «paceq» 2026-08-21. Arbeidsnavnet «pulseq» ble forkastet: GitHub-organisasjonen var tatt av tredjepart, og navnet gir permanent søkekollisjon med MR-rammeverket pulseq.github.io / PyPulseq. Målingene og de forkastede kandidatene står i docs/adr/0002-product-name.md.
  Akseptanse:
  - Beslutningen er dokumentert mot kriteriene fra 09-produktlederen.md §3.4: ledig .dev-domene, ledig GitHub-org, entydig førstesidetreff, uttalbart, ledig binærnavn i Debian/Homebrew. Oppfylt av ADR-0002.
  - Navnet er effektuert i modulsti, binær og docs (#93), ikke utsatt til M8.

### [M0-05] internal/store: to pooler og PRAGMA-disiplin
- **Epic:** Persistens
- **Milepæl:** M0
- **Labels:** area:store, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-01
- **Spek:** Writer-pool `MaxOpenConns(1)` + `_txlock=immediate`; reader `mode=ro` N=NumCPU. DSN/pragmaer per SYNTESE §3.11 (WAL, NORMAL, busy_timeout, foreign_keys, journal_size_limit 64MiB, temp_store). PRAGMA-verifisering ved oppstart (avvik = feil, 02 §1.3). NFS/CIFS-deteksjon via statfs → nekt oppstart (05 §3.3, 07 §1.3). `WithTx`/`WithRead`-hjelpere; writer-håndtak privat.
  Akseptanse:
  - Arkitekturtest: ingen pakke utenfor store refererer skrivbart håndtak.
  - `synchronous=FULL` tilgjengelig som konfignøkkel.
  - Se 07-databasespesialisten.md §1.3 (konkret oppsett).

### [M0-06] Migrasjonsmotor
- **Epic:** Persistens
- **Milepæl:** M0
- **Labels:** area:store, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-05
- **Spek:** Egen migrator (~150 linjer): `//go:embed migrations/*.sql`, forward-only, én tx per migrasjon, sha256-checksum av anvendte filer (avvik = hard feil), `PRAGMA user_version`-gjerde (gammel binær nekter mot nyere skjema), migrator-lease.
  Akseptanse:
  - Test: migrering fra tom DB og fra hver historisk versjon; redigert migrasjonsfil ⇒ feil.
  - Se 07-databasespesialisten.md §5 (kontrakt, rebuild-prosedyre).

### [M0-07] Grunnskjema + auto_vacuum + golden-schema-test
- **Epic:** Persistens
- **Milepæl:** M0
- **Labels:** area:store, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-06
- **Spek:** Migrasjon 0001: `meta`, `schema_migrations`, `leases`, `daemon_sessions`, `outages`. STRICT, INTEGER UTC-ms. `paceq db init` setter `auto_vacuum=INCREMENTAL` (kan ikke endres billig senere — 07 §6.3). Golden-schema-test: dump av `sqlite_schema` diffes mot innsjekket `schema.golden.sql`.
  Akseptanse:
  - `doctor` advarer hvis auto_vacuum=NONE på eksisterende DB.
  - Se 07-databasespesialisten.md §3.1 og §5 (gyldne skjematester).

### [M0-08] Konkurransetest og ytelsesport
- **Epic:** Persistens
- **Milepæl:** M0
- **Labels:** area:store, type:test
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-05
- **Spek:** CI-test: 32 goroutiner skriver samtidig i 10 s mot ekte fil (`t.TempDir()`, aldri `:memory:`) — assertion: null `SQLITE_BUSY`. Benchmark-port: ≥500 skrive-tx/s med modernc; under terskel ⇒ driverbeslutning revurderes NÅ (01 fase 0). SIGKILL under skriveburst ×1000 → `PRAGMA integrity_check` OK.
  Akseptanse:
  - Testene er skrivestrategiens eksistensbevis og kjøres i hver CI-runde.
  - Se 07-databasespesialisten.md §7 (testing) og 02 §7.5 (WAL-recovery).

### [M0-09] internal/clock og internal/id
- **Epic:** Fundament
- **Milepæl:** M0
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** S
- **Avhenger av:** M0-01
- **Spek:** Minimalt `Clock`-interface (wall + mono, fake for test — 02 §5.1 forenklet); regel: `time.Now` kun i clock-pakken (lint-håndhevet). ULID-generering via `oklog/ulid/v2`. Lease-tid sammenlignes aldri på tvers av prosesser (11 §4.5).
  Akseptanse:
  - Lint-test feiler på `time.Now` utenfor `internal/clock`.
  - synctest brukes der SQLite-I/O ikke inngår (05 §11 forbehold).

### [M0-10] flock, instansidentitet og boot_id
- **Epic:** Persistens
- **Milepæl:** M0
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** S
- **Avhenger av:** M0-07
- **Spek:** `flock` på `$STATE/paceq.lock` (to daemoner umulig, tydelig feilmelding). `daemon_sessions`-rad ved oppstart med versjon + heartbeat; `boot_id` fra `/proc/sys/kernel/random/boot_id` lagres i meta — maskinrestart kan da bevises (ingen barneprosess overlevde ⇒ umiddelbar sikker rekonsiliering).
  Akseptanse:
  - To prosesser mot samme statedir: nr. 2 feiler med forklaring.
  - Se 02-palitelighetsingenioren.md §5.9 R0–R1.

### [M0-11] CLI-skjelett: init, version, doctor (grunn)
- **Epic:** Fundament
- **Milepæl:** M0
- **Labels:** area:cli, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-07
- **Spek:** cobra-rot med globale flagg (`-o json|text`, `--db`, `-q/-v`, `--no-color`, NO_COLOR-respekt), `paceq version`, `paceq init` (statedir + skjema + eksempeljobb som faktisk virker — 09 §6.3), `paceq doctor` grunnversjon (DB-sti, WAL-modus, skjemaversjon, diskplass, auto_vacuum). Exit-kode-tabellen fra 03 §7.2 etableres som konstant.
  Akseptanse:
  - TTY → tekst, pipe → JSON (03 §7.1); umask 0077 og rettighetssjekk fail-closed (08 §3.9).

---

### Milepæl M1 — Kjørbare jobber

### [M1-01] jobspec: YAML → kanonisk JSON-IR
- **Epic:** Jobbdefinisjon
- **Milepæl:** M1
- **Labels:** area:spec, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M0-11
- **Spek:** goccy/go-yaml med `DisallowUnknownField`, alias-/dybde-/størrelsesgrenser (08 T13). Felter: `name`, `description`, `env`, `env_file`, `inherit_env`, `workdir`, `timeout`, `max_concurrent` (default 1 — produktbeslutning, 09 §7), `steps[{name, run(argv), shell?, timeout, retry{max,backoff,initial,max_delay,jitter}, needs?}]`, `schedules[]`, `sensors[]`. Kanoniser til JSON-IR med `spec_hash` (sha256). Valideringsfeil med linje/kolonne + caret + «neste steg» (03 §8). `run` er argv-array; `shell: true` eksplisitt m/ advarsel (08 §3.2). Ingen templating.
  Akseptanse:
  - `paceq validate` gir posisjonerte feil; ukjente felt = feil; fuzz-test på parseren panikker aldri.
  - Se 03-cli-designeren.md §3 og 08 §3.2.

### [M1-02] Skjema: definisjoner og utføring
- **Epic:** Persistens
- **Milepæl:** M1
- **Labels:** area:store, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M0-07
- **Spek:** Migrasjoner: `jobs`, `job_versions` (immutabel, `UNIQUE(job_name, spec_hash)`), `runs`, `steps`, `step_deps`, `run_events`, `artifacts`, `ticks`, `triggers`, `run_keys` (WITHOUT ROWID), `schedules`, `sensors` — hele kjernemodellen fra SYNTESE §4.2 med partial-indekser (claim, reaper, concurrency_key) og CHECK-constraints.
  Akseptanse:
  - Golden-schema oppdatert; `UNIQUE(source_kind, source_name, scheduled_for)` på ticks (NULL-trikset, 07 §3.3).
  - Se 07-databasespesialisten.md §3 (skjema-forbilde, tilpasset SYNTESE §4.2).

### [M1-03] paceq apply/load: idempotent versjonering
- **Epic:** Jobbdefinisjon
- **Milepæl:** M1
- **Labels:** area:spec, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M1-01, M1-02
- **Spek:** Les jobs-katalog (`file`-modus er eneste modus — API-et får aldri spec-skrive-endepunkt i 1.0, 08 §3.4), upsert `jobs` + ny `job_versions`-rad KUN ved endret `spec_hash`. Ugyldig fil ⇒ forrige gyldige definisjon beholdes, feil eksponeres (05 §7). Hver reload logges med filhash.
  Akseptanse:
  - `apply` to ganger på samme spec ⇒ ingen ny versjon; runs peker alltid på sin versjon.

### [M1-04] internal/model: tilstandsmaskiner som rene funksjoner
- **Epic:** Utføringskjerne
- **Milepæl:** M1
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-09
- **Spek:** Run- og steg-maskinene fra SYNTESE §4.6 som rene funksjoner uten I/O (`NextState(cur, event, guards)`); «utsatt» = `available_at`+`defer_reason`, ikke egen tilstand. Tabelldrevne tester for alle lovlige og ulovlige overganger; CHECK-constraints i skjemaet speiler reglene (dobbel håndhevelse, 07 §7).
  Akseptanse:
  - 100 % grenkdekning på modellen; DB avviser overganger modellen forbyr.

### [M1-05] Reason-kode-katalog med CI-håndhevelse
- **Epic:** Forklarbarhet
- **Milepæl:** M1
- **Labels:** area:observability, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M1-04
- **Spek:** `internal/reason`: lukket katalog (tick-/trigger-/run-/steg-nivå) med kort tekst, forklaring og tiltaksforslag per kode — én kilde som også genererer docs. Inkluder `STEP_FAILED_SPAWN` og `STEP_FAILED_SIGNAL` som egne koder (06 §2.1).
  Akseptanse:
  - CI-test: ingen terminal tilstand kan lagres med `reason_code` NULL/UNKNOWN (06 §2.1-regelen).
  - Se 06-sre-observabilitet.md §2.1 (katalog-forbilde).

### [M1-06] runner: prosessutføring
- **Epic:** Utføringskjerne
- **Milepæl:** M1
- **Labels:** area:exec, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M0-09
- **Spek:** `exec.CommandContext` med `Setpgid`, `cmd.Cancel`=SIGTERM til `-pgid`, `WaitDelay`=kill_grace (05 §6.5). Obligatorisk timeout (default 1 t, hardt systemtak — 08 §3.2). Miljøkontrakt fra SYNTESE §4.5 (deny-by-default baseline, `PACEQ_*` inkl. `PACEQ_IDEMPOTENCY_KEY`). Exit 0/75/>128-semantikk. `os.Root` for spec-styrte stier (08 T11).
  Akseptanse:
  - Zombie-test: steg spawner barnebarn som ignorerer SIGTERM → hele gruppen død etter cancel (05 §11).
  - fakecmd-testbinær i `testdata/` (sov, feil, ignorer SIGTERM, spawn barnebarn).

### [M1-07] Loggfiler: NDJSON, kvote, error_tail
- **Epic:** Utføringskjerne
- **Milepæl:** M1
- **Labels:** area:observability, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M1-06
- **Spek:** `$STATE/logs/<yyyy-mm-dd>/<run_id>/<step>.<attempt>.ndjson`; linjer `{ts,stream,seq,line}`; kvote 16 MiB/forsøk med head(25 %)/tail(75 %)-trunkering + markørlinje; `error_tail` ~4 KiB til steps-raden ved terminering; `log_path/log_bytes/log_truncated` i DB. Daemonen skriver (jobben får pipe); 0600/0700.
  Akseptanse:
  - `paceq logs <run> [--step] [-f]` fungerer; trunkering detekterbar via `seq`.
  - Se 06-sre-observabilitet.md §3.2 og SYNTESE §3.9.

### [M1-08] Engine: sekvensiell steg-utføring
- **Epic:** Utføringskjerne
- **Milepæl:** M1
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M1-02, M1-04, M1-06
- **Spek:** Materialiser run + steps (frosset fra job_version) → kjør steg i rekkefølge → tilstandsoverganger med nøyaktig én `run_events`-rad per overgang i samme tx (02 G10). Ingen exec/nettverk i skrivetransaksjon. Run-aggregering (succeeded ⇔ alle steg ok). Manuell trigger med `origin=manual`, ULID run-id.
  Akseptanse:
  - Trestegsjobb ende-til-ende; run_events-kjeden er sammenhengende og komplett.

### [M1-09] Retry per steg med backoff
- **Epic:** Utføringskjerne
- **Milepæl:** M1
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M1-08
- **Spek:** Retry som datastruktur (max, exponential/fixed, initial, max_delay, full jitter — Temporal-mønsteret, 04 §1.2). Ikke egen tilstand: `failed → pending` m/ `attempt++`, `next_attempt_at` (05 §6.6). Exit 75 alltid retrybar; hvert forsøk egen loggstrøm som bevares (09 US-05).
  Akseptanse:
  - Backoff-monotoni/tak/jitter testet; forsøkshistorikk synlig i `runs show`.

### [M1-10] CLI: run, runs, logs, status (grunn)
- **Epic:** Utføringskjerne
- **Milepæl:** M1
- **Labels:** area:cli, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M1-08, M1-07
- **Spek:** `paceq run <job> [--param k=v] [--wait]` (in-process executor, ingen daemon nødvendig — femminutters-opplevelsen 03 §9.2); `runs list/show`, `logs`, grunn-`status`. Exit 5 = jobben feilet vs 1 = paceq feilet (03 §7.2). Run-ID prefiks-oppslag som git. `--json` overalt.
  Akseptanse:
  - `go install` → `init` → `run hei` under 3 min uten daemon (09 §6.1).

### [M1-11] testscript-suite og golden-output
- **Epic:** Utføringskjerne
- **Milepæl:** M1
- **Labels:** area:cli, type:test
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M1-10
- **Spek:** `rogpeppe/go-internal/testscript` for CLI-flyter; golden-filer på `--json`-output (offentlig grensesnitt, skal brekke synlig); frosset klokke, fast bredde, `--no-color` (03 risiko 10).
  Akseptanse:
  - Suite kjøres i CI; `-update`-flagg for regenerering.

### [M1-12] Krasjtest-harness v1
- **Epic:** Utføringskjerne
- **Milepæl:** M1
- **Labels:** area:engine, type:test
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M1-08
- **Spek:** `internal/faults`: navngitte krasjpunkter (no-op i prod-bygg), styrt av env; harness starter ekte daemon-subprosess, SIGKILL ved punkt, restart, kjør invariant-sjekk (fsck-light), verifiser effekt-telling `1 ≤ n ≤ 1+antall_krasj` (at-least-once med bundet duplikasjon). Dekker M1-vinduene (run/steg-materialisering, før/etter exec, før/etter commit).
  Akseptanse:
  - Matrisen kjøres i CI for enkeltsteg-scenariet; utvides i M2/M3/M4.
  - Se 02-palitelighetsingenioren.md §6 (W-katalogen) og §7.1 (harness).

---

### Milepæl M2 — Daemon og schedules

### [M2-01] paceq serve: daemon-livssyklus
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M1-08, M0-10
- **Spek:** errgroup-arkitektur per SYNTESE §4.1 (scheduler, sensor-runtime-plass, dispatcher enkelttrådet, executor-pool, reaper, janitor, api). Intern notify-buss (kanal, coalescing) + tickere som sikkerhetsnett. To-faset shutdown: stopp inntak ≤100 ms, drain-timeout 30 s, SIGTERM→grace→SIGKILL til prosessgrupper, ikke-fullførte steg → pending uten forbrukt forsøk, `wal_checkpoint(TRUNCATE)` sist (05 §3.2).
  Akseptanse:
  - SIGTERM under kjøring: ren nedstenging, ingen foreldreløse; andre SIGTERM = umiddelbar kill.

### [M2-02] Rolle-leases
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M2-01
- **Spek:** `leases`-tabell med idempotent acquire/renew-setning (INSERT ON CONFLICT med epoch-bump ved overtakelse — 11 §4.2). TTL 15 s, fornyelse 5 s; DELETE ved ren avslutning. Begrunnelse på single-node: overlappende restart / glemt terminal-daemon (11 §4.2).
  Akseptanse:
  - To serve-prosesser (flock på ulike dirs mot samme DB i test): nøyaktig én leder; overtakelse ≤15 s ved kræsj.

### [M2-03] internal/cronx: iterator med DST-policy
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:scheduler, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M0-09
- **Spek:** gronx som uttrykksparser/-primitiv; egen iterator: `Next/Prev/Between(from, to, tz)` som returnerer UTC-tider. Policyer per schedule: `spring_forward: skip(default)|shift`, `fall_back: first(default)|both` — hoppet/dublert time materialiseres alltid som tick med skip-grunn. `@every`/interval = ren UTC-aritmetikk. `time/tzdata` embedded; tzdata-versjonssjekk med event ved endring. Hard iterasjonsgrense mot uttrykk uten treff (08 T12).
  Akseptanse:
  - `0 2 * * *` Europe/Oslo over begge overganger gir nøyaktig policy-definert resultat.
  - Se 02-palitelighetsingenioren.md §5.3 (policytabell).

### [M2-04] DST-gullstandard og differensialtest
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:scheduler, type:test
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M2-03
- **Spek:** Innsjekkede fixtures med forventede UTC-lister: Europe/Oslo (2026-03-29 og 2026-10-25, begge policyer), America/Santiago, Australia/Lord_Howe (30-min), Asia/Kolkata (+05:30), Pacific/Kiritimati, UTC. Differensialtest mot uavhengig implementasjon (fixtures generert offline med Python zoneinfo+croniter), kjørt uten nettverk. 10 000 tilfeldige uttrykk diffet mot gronx rå.
  Akseptanse:
  - Endring i fixtures krever begrunnelse i commit (04 §8).
  - Se 02 §7.4 (soneliste) og 04 fase 2 (differansetest).

### [M2-05] Scheduler-loop: tick-materialisering og catch-up
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:scheduler, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M2-02, M2-03, M1-02
- **Spek:** Under scheduler-lease: for forfalte schedules beregn ticks via `Between`, anvend `catchup: skip(default)|last|all` med `catchup_limit` og vindu (drypp, ikke storm — 02 §5.3); én tx per tick: `INSERT ticks ON CONFLICT DO NOTHING` (UNIQUE = idempotens) + trigger + run (`run_key=<schedule>:<scheduled_for>`) + `next_tick_at`. Alle hoppede ticks får rad med reason_code. Deterministisk: to daemoner med ulik oppetid gir identiske tick-mengder (02 G9).
  Akseptanse:
  - Daemon nede 6 t → restart gjør nøyaktig det policyen sier; ingen dobbelttick under krasjharness.
  - Se 02 §5.3 (TX-A/TX-B) og 05 §6.1.

### [M2-06] Run-lease, fencing og reaper
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M2-01
- **Spek:** Claim = én UPDATE…RETURNING under IMMEDIATE (epoch++, lease TTL 60 s); batch-heartbeat hvert 20 s i ÉN tx for alle runs (02 §5.2); renew-svar bærer cancel-flagg (11 §4.3); alle resultatskriv CAS-er på `lease_epoch` — 0 rader = selv-fencing, forkast resultat. Reaper hvert 10 s: utløpt lease → epoch++, requeue m/ backoff eller failed; `crash_count`+`max_crash_count` → poison-karantene (02 §5.7). Lease-fornyelse i egen goroutine som aldri blokkeres av arbeidet (11 R3).
  Akseptanse:
  - Fencing-test: worker med utdatert epoch får 0 rader og forkaster; SIGKILL på daemon → run reclaimet innen TTL.

### [M2-07] Rekonsiliering ved oppstart
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M2-06, M0-10
- **Spek:** Idempotent sekvens (forenklet R0–R11): boot_id-sjekk (endret ⇒ alle aktive attempts beviselig døde); `/proc`-sweep etter prosesser med `PACEQ_RUN_ID` i environ som ikke matcher aktiv run — verifiser pid-starttid før drap av pgid (02 R3/R9); utløpte leases → reaper-logikk; hengende ticks → error; pending schedule-ticks re-materialiseres (idempotent via run_key); gap-deteksjon: `outages`-rad + syntetiske `missed`-ticks med `TICK_MISSED_DAEMON_DOWN` (06 §7.3).
  Akseptanse:
  - Daemon stoppet 30 min → outage-rad + korrekte missed-ticks innen 10 s etter oppstart (06 SLO 5).
  - Se 02-palitelighetsingenioren.md §5.9.

### [M2-08] Unix-socket og dual-mode CLI
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:cli, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M2-01, M1-10
- **Spek:** JSON over unix-socket (`$RUNTIME_DIR/paceq.sock`, 0660); ingen TCP. CLI-oppløsning: lesing alltid direkte RO-SQLite (virker med daemon nede — hardt krav for explain); skriving via socket når daemon svarer, ellers flock + direkte gjennom samme store-kode. `--socket none` tvinger direktemodus. Kontraktstest: hele testscript-suiten kjøres i BEGGE moduser (03 risiko 6).
  Akseptanse:
  - Identisk output i begge moduser; daemon nede ⇒ lesekommandoer virker med «(daemon nede)»-markør.
  - Se 03-cli-designeren.md §10.4.

### [M2-09] Concurrency-håndhevelse
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M2-05
- **Spek:** `max_concurrent` default 1 (produktbeslutning — sletter brukerens flock-innpakninger, 09 US-02). Overlap-policy `skip(default)|queue`: skip ⇒ tick `skipped` med reason + peker til blokkerende run; queue ⇒ run i «utsatt» (available_at + defer_reason). Admission control som les-regn-skriv i Go innenfor én IMMEDIATE-tx (07 §4.2).
  Akseptanse:
  - Invariant-test: aktive runs per job ≤ max_concurrent under 50 samtidige forsøk.

### [M2-10] CLI: schedules-gruppen
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:cli, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M2-05, M2-08
- **Spek:** `schedules list/show/preview/pause/resume`, `runs cancel` (sett `cancel_requested_at`; eier observerer ved renew — 02 §5.8), `ls` (én skjerm: alle jobber, siste/neste kjøring). `preview --next N` viser lokal tid OG UTC med DST-overganger markert (forebygger tidssonefeil — 09 US-01/JTBD-7).
  Akseptanse:
  - Preview over overgangsdøgn markerer hoppet/dublert time eksplisitt.

### [M2-11] systemd-integrasjon
- **Epic:** Daemon & schedules
- **Milepæl:** M2
- **Labels:** area:security, type:feature
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M2-01
- **Spek:** `deploy/paceq.service`: Type=notify, watchdog (ping kun når kontroll-loopen faktisk tikker — 06 §7.2), StateDirectory/RuntimeDirectory, hardening-direktiver + kommentert avslappet variant (sandkasse arves av jobber — 06 risiko 10). sd_notify-protokollen håndskrevet (~30 linjer, ingen dep). `paceq install-service`-hjelper.
  Akseptanse:
  - `systemctl start paceq` → READY etter migrasjoner; watchdog-timeout betyr «tar ikke beslutninger».
  - Se 06-sre-observabilitet.md §7.2.

---

### Milepæl M3 — Sensorer

### [M3-01] Sensor-spec og skjema-aktivering
- **Epic:** Sensorer
- **Milepæl:** M3
- **Labels:** area:sensors, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M1-01, M1-02
- **Spek:** `sensors[]` i jobspec: `name`, `run` (argv), `interval` (min 1 s), `min_interval`, `timeout` (default 30 s), `max_triggers_per_tick` (default 100). Kun `kind=exec` i MVP (uendelig bred kontrakt, null API-flate — 10 F4a/F4b). Sensor-rader materialiseres ved apply; cursor/dedup_epoch initieres.
  Akseptanse:
  - Validering: intervall-grenser, argv-regler som for steg.

### [M3-02] Evaluator-runtime
- **Epic:** Sensorer
- **Milepæl:** M3
- **Labels:** area:sensors, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M1-06, M2-01
- **Spek:** Per-sensor serialisering (aldri parallell med seg selv), semafor N=4 globalt; kontrakt fra SYNTESE §4.4: JSON på stdin + `PACEQ_*`-env, ett JSON-objekt på stdout; timeout → drap av prosessgruppe → tick=error; stdout-tak 1 MiB; stderr-utdrag 4 KiB lagres på ticken. Treg sensor blokkerer aldri scheduler-loopen (04 §1.1-lærdommen om Dagsters 60 s-felle).
  Akseptanse:
  - Hengende sensor drepes og påvirker ikke andre sensorer; kontrakten dokumentert i referansedokument.

### [M3-03] Atomisk sensor-transaksjon
- **Epic:** Sensorer
- **Milepæl:** M3
- **Labels:** area:sensors, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M3-02
- **Spek:** Sekvens: (1) tx: tick `running` + cursor_before; (2) exec UTENFOR transaksjon; (3) tx: triggers + runs (`INSERT … ON CONFLICT DO NOTHING`) + cursor + tick-utfall + events — ALT eller INGENTING. Absolutt regel: cursor committes aldri uten tilhørende runs i samme tx (G4 — 02 §5.5). Krasj før (3) ⇒ re-evaluering ⇒ dedup via run_key. `skip_reason` fra sensor lagres på tick.
  Akseptanse:
  - Krasjharness: SIGKILL mellom exec og commit gir null tap, null duplikater (kjernen i M3-demoen).
  - Se 02-palitelighetsingenioren.md §5.5 og 11 §5.3.

### [M3-04] Dedup: run_keys og dedup-epoch
- **Epic:** Sensorer
- **Milepæl:** M3
- **Labels:** area:sensors, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M3-03
- **Spek:** `run_keys(source_id, epoch, run_key)` PK WITHOUT ROWID; insert-first, aldri check-first (TOCTOU — 11 §5.2); dedup også innenfor én tick (dagster#26753 — 04 §1.1); dedupliserte triggere får rad med peker til original run. `sensor reset` bumper epoch (cursor og run_keys nullstilles alltid uavhengig — 10 F4c); retention 365 d med dokumentert konsekvens.
  Akseptanse:
  - 50 filer → 50 runs; gjenkjøring → 0; reset → 50 nye; samme run_key ×2 i én tick → 1 run.
  - Se 04-orkestrator-veteranen.md §4.3 (dedup-epoch).

### [M3-05] Sensor-robusthet: backoff, breaker, trunkering
- **Epic:** Sensorer
- **Milepæl:** M3
- **Labels:** area:sensors, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M3-02, M3-03
- **Spek:** Exit 75 = forbigående (kort backoff, teller ikke mot breaker); annen feil: eksponentiell backoff (`interval·2^min(n,6)`, tak 1 t), auto-pause etter 10 med `paused_reason` (fail-safe mot nede tjeneste). `min_interval` mot hot-loop. `max_triggers_per_tick`: ta N første, delvis cursor, `truncated`-event, umiddelbar re-tick (automatisert chunking — 04 §5.2). Koalesering av identiske skips med `repeat_count` (06 §1.3).
  Akseptanse:
  - Breaker-status synlig i `sensors show`; koalesert skip = én rad + oppdatert teller.

### [M3-06] CLI: sensors-gruppen
- **Epic:** Sensorer
- **Milepæl:** M3
- **Labels:** area:cli, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M3-03, M3-04
- **Spek:** `sensors list/show/test/tick/pause/resume`, `sensors reset [--cursor <v>] [--forget-run-keys]`, `cursor get/set`. `test` = dry-run: kjører evaluering mot ekte tilstand, viser triggere + dedup-dom, skriver INGENTING (09 US-08, 03 §6.4). `--print-input` for frittstående sensor-utvikling (03 §4.6).
  Akseptanse:
  - `sensors test` etterlater DB bit-identisk; utviklingsløkken `--print-input | ./sensor | jq` fungerer.

### [M3-07] Eksempelsensorer i CI
- **Epic:** Sensorer
- **Milepæl:** M3
- **Labels:** area:sensors, type:docs
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M3-06
- **Spek:** `examples/sensors/`: filsystem-watermark, HTTP-ETag, SQL-watermark, S3-listing — alle i shell, ingen Go (01 fase 3). Kjøres i CI som integrasjonstest av kontrakten; er samtidig dokumentasjon. En 5-linjers sensor er produktets salgsargument (01 §5.3).
  Akseptanse:
  - CI kjører alle eksempler mot fixtures; en sensor skrives fra scratch på <5 min etter docs (10 D2).

### [M3-08] Property-test: cursor-garantien
- **Epic:** Sensorer
- **Milepæl:** M3
- **Labels:** area:sensors, type:test
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M3-03
- **Spek:** `pgregory.net/rapid`: modellbasert test med handlingene tick/krasj/restart/reset; invariant: hver cursor-verdi har en committet tick med tilsvarende `cursor_after`; ingen trigger går tapt over 10 000 iterasjoner med tilfeldige krasj (02 fase 3-ferdigkriterium).
  Akseptanse:
  - Feilende seeds sjekkes inn som regresjonstester.

---

### Milepæl M4 — DAG

### [M4-01] needs-validering og frosne kanter
- **Epic:** DAG
- **Milepæl:** M4
- **Labels:** area:spec, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M1-01
- **Spek:** `needs: [steg…]` i spec; Kahn-syklusdeteksjon med utskrift av syklussti ved apply/validate (aldri kjøretidsfeil); grenser: maks 200 steg, maks dybde/fan-out (08 §7.4); kanter fryses som `step_deps`-rader ved run-materialisering. HARDE grenser (10 §6): ingen betingede kanter, ingen dynamisk fan-out, ingen continue_on_error i 1.0.
  Akseptanse:
  - Syklus gir PQ-feil med sti og filposisjon; ukjent needs = valideringsfeil.

### [M4-02] Parallell utføring via claim-predikat
- **Epic:** DAG
- **Milepæl:** M4
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M4-01, M2-06
- **Spek:** Steg-klarhet = SQL-predikat: `NOT EXISTS (needs som ikke er succeeded)` — ingen graf i minnet, ingen gjenoppbygging etter krasj; parallellitet faller ut når to steg er klare samtidig; `max_parallel` per run via semafor i executor. Steg-overganger CAS-er på runens lease_epoch.
  Akseptanse:
  - Diamant-DAG kjører B og C parallelt; 50 samtidige steg m/ `-race`: hvert steg claimet nøyaktig én gang.
  - Se 05-go-arkitekten.md §6.4 (spørringen).

### [M4-03] Skip-propagering og run-aggregering
- **Epic:** DAG
- **Milepæl:** M4
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M4-02
- **Spek:** Feilet steg (retry uttømt) ⇒ hele nedstrøms transitive lukket → `skipped` med `reason=STEP_SKIPPED_UPSTREAM_FAILED` (rekursiv CTE — 07 §4.4); run-aggregat re-evalueres i samme tx; `failed` ⇔ minst ett steg failed.
  Akseptanse:
  - fsck-invariantene for DAG (steg running ⇒ alle needs succeeded; aggregat konsistent) er grønne i krasjharness.

### [M4-04] retry og replay
- **Epic:** DAG
- **Milepæl:** M4
- **Labels:** area:engine, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M4-02
- **Spek:** Semantikk fra 03 §5.6: `runs retry <id>` = samme run, kun failed/pending-steg, gjenbruk av vellykkede stegs artefakter, nytt attempt; `runs replay <id> [--from <steg>] [--failed]` = NY run med `replay_of`, samme `job_version` (aldri gjeldende spec — 08 §7.4). Operatør-retry av terminal run logges som `operator_reopen` og er eneste vei ut av terminal tilstand (02 T14); CLI advarer ved ukjent utfall.
  Akseptanse:
  - «Steg 4 feilet, rettet, kjørte om bare steg 4» fungerer (09 v0.3-demoen).

### [M4-05] Artefaktreferanser
- **Epic:** DAG
- **Milepæl:** M4
- **Labels:** area:exec, type:feature
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M1-06, M4-02
- **Spek:** Steget skriver NDJSON til `$PACEQ_OUTPUT` (artefaktreferanser: name/uri/size/checksum + videreførte params); Paceq leser etter exit → `artifacts`-rader; `$PACEQ_INPUTS` = flettet JSON fra oppstrøms steg. Referanser, aldri innhold; ingen lineage-graf (04 §6: hold porten til asset-modellen lukket).
  Akseptanse:
  - Filsti fra steg A tilgjengelig i steg B uten avtale ut av båndet (09 US-13).

### [M4-06] concurrency_key
- **Epic:** DAG
- **Milepæl:** M4
- **Labels:** area:engine, type:feature
- **Prioritet:** P2
- **Estimat:** S
- **Avhenger av:** M2-09
- **Spek:** Valgfri `concurrency_key` per run (fra params/run_key-mal uten templating: fast felt); partial-unique-indeks `WHERE state IN (queued,running)` håndhever maks én aktiv per nøkkel (05 §5); konflikt ⇒ utsatt m/ defer_reason eller skip per policy.
  Akseptanse:
  - Indeksen er håndhevelsen; ingen applikasjonssjekk.

### [M4-07] DAG ende-til-ende- og kaostest
- **Epic:** DAG
- **Milepæl:** M4
- **Labels:** area:engine, type:test
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M4-03, M4-04
- **Spek:** Utvid krasjharness med DAG-scenarier (fan-out, retry under parallellitet, cancel midt i fan-out); kaostest: 500 runs mens daemonen SIGKILL-es tilfeldig; invarianter: ingen run_key med to fullførte runs, sum stegtilstander konsistent, ingen foreldreløse prosesser (05 §11).
  Akseptanse:
  - Nattlig CI-jobb grønn; M4-exit-demoen scriptet.

---

### Milepæl M5 — Explain, import og v0.1

### [M5-01] paceq explain
- **Epic:** Forklarbarhet
- **Milepæl:** M5
- **Labels:** area:observability, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M2-05, M3-03, M1-08
- **Spek:** `explain job|schedule|sensor|run <ref> [--since]` — ren presentasjon over ticks/triggers/runs/run_events, ALDRI re-utledning; prosa-output med årsak + tiltaksforslag fra reason-katalogen; `--json` som stabil kontrakt (fremtidig UI får ingen egen datamodell — 06 §16). Virker med daemonen nede (RO-vei). `internal/explain` er eneste lesevei til historikk.
  Akseptanse:
  - Output-formen fra 10 §5 (sensor-eksemplet) og 06 §4 gjenskapes; svarer også når alt er OK (09 US-16).

### [M5-02] Explain-sjekklistetest
- **Epic:** Forklarbarhet
- **Milepæl:** M5
- **Labels:** area:observability, type:test
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M5-01, M1-05
- **Spek:** Én test per «kjørte ikke»-scenario som asserterer at grunnen er LAGRET og vises: paused, no_tick_due, already_running/concurrency, sensor_skip, run_key_deduped, dependency_failed, daemon_down-gap, catchup_disabled, DST-skip, breaker-pause, poison-karantene (04 fase 5: sjekkliste, ikke påstand).
  Akseptanse:
  - Scenariolisten er komplett mot reason-katalogen; ny kode uten scenario feiler review-sjekk.

### [M5-03] paceq status
- **Epic:** Forklarbarhet
- **Milepæl:** M5
- **Labels:** area:cli, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M5-01
- **Spek:** Én linje per jobb: siste utfall, tidspunkt, varighet, neste kjøring; exit ≠ 0 ved ubekreftet feil (MOTD-/overvåkingsbruk — 09 US-04); hint-linje ved avvik: «kjørte ikke i natt — kjør `paceq explain …`» (09 R12). Krav: <100 ms med 100 jobber / 100 000 historikkrader (partial-indekser + EXPLAIN QUERY PLAN-assertions).
  Akseptanse:
  - Ytelsestest med generert datasett i CI.

### [M5-04] paceq import crontab
- **Epic:** Migrering fra cron
- **Milepæl:** M5
- **Labels:** area:cli, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M1-01
- **Spek:** Les crontab (bruker/fil/--all-users), ikke-destruktiv, skriv lesbar YAML. Oversettelser per 09 §5.1-tabellen: CRON_TZ/MAILTO/PATH/SHELL, `flock` → `max_concurrent:1`, `>/dev/null` fjernes m/ advarsel, `cd X &&` → workdir, `%`-escaping, utolkbare linjer beholdes ordrett med TODO. Importrapport med tellinger og neste-steg-kommandoer.
  Akseptanse:
  - Korpustest mot ekte crontabs (offentlige dotfiles): >90 % tolket i første forsøk (09 R6).
  - Se 09-produktlederen.md §5.1 (full oversettelsestabell).

### [M5-05] Retention, vakuum og backup
- **Epic:** Drift
- **Milepæl:** M5
- **Labels:** area:store, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M1-02
- **Spek:** Janitor: batchet sletting (`DELETE … LIMIT 500`, pause mellom); policyer: logger 14 d, runs 90 d MEN minst 50 siste per job, skip-ticks 7 d, run_keys 365 d (06 §9.4, 07 §6.2); `incremental_vacuum(2000)` nattlig; `wal_checkpoint(TRUNCATE)` ved stille; `paceq prune` manuell. Backup: `VACUUM INTO` nattlig med `quick_check`-verifisering av kopien + backup-alder i doctor (07 §6.5); full `VACUUM` kun bak eksplisitt flagg. `paceq export run <id>` → tar.gz (bevar bevis før retention — 06 §9.4).
  Akseptanse:
  - Retention holder aldri skrivelåsen >50 ms (målt); backup uten verifisering = feilet backup.

### [M5-06] /metrics og alerts-pakke
- **Epic:** Drift
- **Milepæl:** M5
- **Labels:** area:observability, type:feature
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M2-08
- **Spek:** Håndskrevet Prometheus-tekstformat (ingen client_golang — SYNTESE §3.17) på socket + valgfri 127.0.0.1-bind. Kjernesett: `last_success_timestamp` + `freshness_sla` (fra spec `expected_within`), tick_lag, runs_by_state, lease_reclaims, wal/db-bytes, writer-ventetid. Forbud mot ID-labels (06 §6.4). `deploy/paceq-alerts.yml` med generiske regler (JobStale, TickStalled, WALGrowth …).
  Akseptanse:
  - Kardinalitetstest: 1 000 jobber → serieantall under grense (06 §6.4).
  - Se 06-sre-observabilitet.md §6.2–6.3.

### [M5-07] Release-pipeline
- **Epic:** Release v0.1
- **Milepæl:** M5
- **Labels:** area:release, type:infra
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-02
- **Spek:** GoReleaser: statiske binærer linux/amd64+arm64 (+darwin), `-trimpath`, checksums, reproduserbart bygg verifisert ved dobbeltbygg i CI (08 §5); versjon i `paceq version --json`; CHANGELOG for mennesker (09 §9.3). cosign/SBOM utsettes til M8.
  Akseptanse:
  - Tagget commit → ferdige artefakter automatisk.

### [M5-08] Dokumentasjon v0.1
- **Epic:** Release v0.1
- **Milepæl:** M5
- **Labels:** area:product, type:docs
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M5-01, M5-04
- **Spek:** README-kontrakt (≤1 skjerm før første kommando, asciinema av import+explain, «hva dette ikke er» — 09 §9.3); `docs/garantier.md` ferdigstilt (at-least-once, run_key vs cursor, ikke-garantier i klartekst — 02 §4.2, 09 R8); sensor-kontraktreferanse; tutorial «fra crontab på 5 minutter» + «din første sensor»; CLI-referanse generert fra cobra. Alle kodeeksempler kjøres i CI.
  Akseptanse:
  - Ny bruker skriver sin første sensor på <5 min uten annen lesning (01 §12.6).

### [M5-09] v0.1-release og K1-evaluering
- **Epic:** Release v0.1
- **Milepæl:** M5
- **Labels:** area:release, type:release
- **Prioritet:** P0
- **Estimat:** S
- **Avhenger av:** M5-01, M5-02, M5-03, M5-04, M5-05, M5-06, M5-07, M5-08, M4-07
- **Spek:** Tag v0.1 «Cron som husker». Demosetning: «jeg spør hvorfor backupen ikke gikk i natt, og den svarer» (09 §8).
  Akseptanse (kill-kriterium K1 fra 10 §7):
  - ≥3 av utviklerens egne, ekte jobber kjører i produksjon på Paceq med crontab-linjene deaktivert.
  - Hvis nei: STOPP og re-evaluer scope før M6 (10: «du bygger noe du selv ikke vil bruke»).

---

### Milepæl M6 — Drift, varsling og herding (v0.2)

### [M6-01] Varsling: outbox + exec-notifier
- **Epic:** Varsling & drift
- **Milepæl:** M6
- **Labels:** area:observability, type:feature
- **Prioritet:** P0
- **Estimat:** L
- **Avhenger av:** M1-02, M5-01
- **Spek:** `outbox`-tabell; varsel skrives i SAMME tx som tilstandsendringen (aldri «feilet men ikke varslet»); leveringsloop med backoff; notifier = `exec` (kommando får hendelse som JSON på stdin) + `stderr` for dev — ingen Slack/SMTP-integrasjoner, kun oppskrifter (e-post/ntfy/Slack-webhook). `on_failure`/`on_success`-kroker per jobb; `expected_within` → `job.sla_breached`-hendelse. Throttle + group_by. `notifications list` for revisjon.
  Akseptanse:
  - Indusert feil → nøyaktig ett varsel + én outbox-rad `sent`; varsel inneholder run-ID, steg, error_tail og retry-kommando (09 US-03).
  - Se 06-sre-observabilitet.md §8 og 07 §3.6 (outbox).

### [M6-02] Skyggemodus og shadow report
- **Epic:** Migrering fra cron
- **Milepæl:** M6
- **Labels:** area:scheduler, type:feature
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M5-04, M2-05
- **Spek:** `--shadow`: hele planleggeren kjører, hver tick registreres i historikken, INGENTING utføres. `paceq shadow report`: diff mot faktisk cron-oppførsel, avdekk tidssonefeil og overlapp brukeren ikke visste om («wow»-øyeblikk 3 — 09 §5.2).
  Akseptanse:
  - Rapporten viser per jobb: samsvar, avvik med årsak, overlapp Paceq ville stoppet.

### [M6-03] cutover og rollback
- **Epic:** Migrering fra cron
- **Milepæl:** M6
- **Labels:** area:cli, type:feature
- **Prioritet:** P1
- **Estimat:** S
- **Avhenger av:** M6-02
- **Spek:** `paceq cutover`: kommenterer ut importerte crontab-linjer med jobbnavn-referanse, backup til `~/.paceq/crontab.backup.<dato>`; `--rollback` legger tilbake. Prinsipp: brukeren kan alltid gå tilbake på ett minutt (09 §5.3).
  Akseptanse:
  - cutover + rollback er idempotente og tapsfrie (testet mot korpus).

### [M6-04] exec-shim: watchdog-pipe og resultat-spool
- **Epic:** Herding
- **Milepæl:** M6
- **Labels:** area:exec, type:feature
- **Prioritet:** P1
- **Estimat:** L
- **Avhenger av:** M2-07
- **Spek:** `paceq exec`-shim (intern, samme binær): egen pgid, watchdog-pipe fra daemon (EOF ⇒ shim dreper prosessgruppen — foreldreløse umulige ved daemon-død), skriver resultatfil til `spool/attempts/<id>.json` med fsync+rename FØR exit (lukker krasjvindu W8: barn ferdig, resultat ikke committet ⇒ unødig gjenkjøring). Reconciler konsumerer spool ved oppstart.
  Akseptanse:
  - Krasjtest W8: daemon SIGKILL etter steg-exit → restart committer utfall fra spool, ingen gjenkjøring.
  - Se 02-palitelighetsingenioren.md §5.6 (hele designet).

### [M6-05] Disk-guard og loggkvoter totalt
- **Epic:** Herding
- **Milepæl:** M6
- **Labels:** area:observability, type:feature
- **Prioritet:** P1
- **Estimat:** S
- **Avhenger av:** M5-05
- **Spek:** Global logg-cap (default 10 GiB, eldste shard slettes først); <10 % ledig disk ⇒ degradert modus: nekt nye runs, la pågående fullføre, tydelig reason (02 fase 4, 06 §7.1); WAL-størrelse-alarm >64 MiB (kanarifugl for langlevd leser — 07 §6.4).
  Akseptanse:
  - Full-disk-simulering gir kontrollert degradering, ikke korrupsjon.

### [M6-06] doctor komplett og fsck
- **Epic:** Herding
- **Milepæl:** M6
- **Labels:** area:observability, type:feature
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M2-07
- **Spek:** `paceq fsck [--repair]`: invariantene fra SYNTESE §4.6 som SQL (subset av 02 I1–I16), kjøres ved oppstart + timeplan; kritiske brudd ⇒ nekt start uten operatørbekreftelse. `doctor` komplett: PRAGMA-verdier, NTP-avvik, tzdata-versjon, backup-alder+verifisering, spool-restanse, foreldreløse prosesser, effektiv systemd-sandkasse, jobber uten freshness-SLA (06 SLO 6).
  Akseptanse:
  - Hvert doctor-funn har tiltaksforslag; fsck-brudd genererer events.

### [M6-07] Soak- og kaostest nattlig
- **Epic:** Herding
- **Milepæl:** M6
- **Labels:** area:engine, type:test
- **Prioritet:** P1
- **Estimat:** L
- **Avhenger av:** M1-12, M3-08, M4-07
- **Spek:** Nattlig CI: 24 t (kort-variant 1 t i PR) med tilfeldig SIGKILL hvert 30.–120. s, kontinuerlig fsck, effekt-telling innenfor at-least-once-grensene; rapid state machine-test med full handlingsmeny (claim, krasj, cancel, lease-utløp, klokkehopp ±25 t) og `-rapid.checks=10000` (02 §7.2, §7.5).
  Akseptanse:
  - Grønn tre netter på rad før v0.2 tagges (02 fase 5-kriteriet).

### [M6-08] v0.2-release og K2-evaluering
- **Epic:** Varsling & drift
- **Milepæl:** M6
- **Labels:** area:release, type:release
- **Prioritet:** P0
- **Estimat:** S
- **Avhenger av:** M6-01, M6-02, M6-03, M6-05, M6-06, M6-07
- **Spek:** Tag v0.2 «Den varsler meg» — første versjon som er trygg å drifte (09 §8).
  Akseptanse (kill-kriterium K2 fra 10 §7):
  - Loggført at `explain` har spart reell feilsøkingstid ≥3 ganger.
  - Mål: én bruker (gjerne utvikleren) har slått av cron helt i 30 døgn.

---

### Milepæl M7 — UI, innebygde sensorer og backfill (v0.3–v0.4)

### [M7-01] Web-UI: read-only grunnlag
- **Epic:** Web-UI
- **Milepæl:** M7
- **Labels:** area:ui, type:feature
- **Prioritet:** P1
- **Estimat:** L
- **Avhenger av:** M5-01
- **Spek:** `html/template` + `embed.FS`, server-rendret, NULL npm/JS-byggekjede (enstemmig på tvers av planene); binder 127.0.0.1; read-only gjennom hele 1.0 (UI-et blir aldri sannheten — 09 R10); streng CSP, ingen inline script (08 §3.4); leser KUN via `explain --json`-laget og lese-poolen (aldri egen Tx — checkpoint-svelt, 07 §6.4).
  Akseptanse:
  - Jobbliste + run-liste med filter; `package.json` eksisterer ikke i repoet (10 F7).

### [M7-02] Web-UI: run-detalj og skip-tidslinje
- **Epic:** Web-UI
- **Milepæl:** M7
- **Labels:** area:ui, type:feature
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M7-01
- **Spek:** Run-detalj med steggraf, forsøkshistorikk og logg (fra error_tail + fil); tidslinje for schedules/sensorer der skip-grunner er førsteklasses hendelser; explain gjengitt i UI.
  Akseptanse:
  - Skjermbildet av skip-tidslinjen forklarer seg selv for en som ikke har lest docs (09 v0.4-exit).

### [M7-03] Innebygde sensortyper: file, http, sql
- **Epic:** Innebygde sensorer
- **Milepæl:** M7
- **Labels:** area:sensors, type:feature
- **Prioritet:** P1
- **Estimat:** L
- **Avhenger av:** M3-02, M3-04
- **Spek:** Samme tick-/cursor-/dedup-maskineri som exec, ingen subprosess: `file` (path/glob, mtime/checksum-cursor, `stable_for` mot halvopplastede filer — 04 §5.3); `http` (ETag/hash/JSONPath-cursor) med SSRF-vern: deny loopback/link-local/RFC1918, DNS-revalidering, redirect-revalidering, størrelsestak (08 §3.6); `sql` (dsn, query med `:cursor`, cursor_column).
  Akseptanse:
  - «Reager på nye S3-/katalogfiler» løses i <10 linjer YAML uten skript (09 v0.5-exit).

### [M7-04] Backfill
- **Epic:** Backfill
- **Milepæl:** M7
- **Labels:** area:scheduler, type:feature
- **Prioritet:** P2
- **Estimat:** M
- **Avhenger av:** M2-05
- **Spek:** `paceq backfill <schedule> --from --to [--max-parallel N] [--dry-run]`: setter inn historiske ticks i `schedule_ticks`-journalen (idempotent via UNIQUE), materialiserer runs med `origin=backfill`, respekterer concurrency; dry-run viser hvilke ticks som kjøres vs allerede har run (03 §6.4). Bevisst avgrenset — aldri Airflows catchup-storm (04 §6).
  Akseptanse:
  - Backfill av to uker med `--max-parallel 2` køer riktig og dobbeltkjører aldri eksisterende ticks.

### [M7-05] import systemd-timere
- **Epic:** Migrering fra cron
- **Milepæl:** M7
- **Labels:** area:cli, type:feature
- **Prioritet:** P3
- **Estimat:** M
- **Avhenger av:** M5-04
- **Spek:** `paceq import systemd`: les `*.timer`+`*.service`, oversett OnCalendar/Persistent/RandomizedDelaySec/ExecStart/WorkingDirectory/Environment (09 §5.4). Lav prioritet — kuttes først ved tidspress.
  Akseptanse:
  - Standard timer-par oversettes korrekt; utolkbart beholdes med TODO.

### [M7-06] v0.3/v0.4-release
- **Epic:** Web-UI
- **Milepæl:** M7
- **Labels:** area:release, type:release
- **Prioritet:** P1
- **Estimat:** S
- **Avhenger av:** M7-02, M7-03, M7-04
- **Spek:** Tag v0.3 (innebygde sensorer + backfill) og v0.4 (UI). Første Show HN/deling skjer NÅ — når UI gir skjermbilder (09 §9.4: førsteinntrykk kommer én gang).
  Akseptanse:
  - Demomateriell: skjermbilde av skip-tidslinje + asciinema av import/explain.

---

### Milepæl M8 — Stabilisering → v1.0

### [M8-01] Formatfrys og utfasingspolicy
- **Epic:** Stabilisering
- **Milepæl:** M8
- **Labels:** area:product, type:feature
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M7-06
- **Spek:** Frys jobspec-skjema, CLI-flate, `--json`-strukturer, sensor-/steg-kontrakt; semver-forpliktelse + dokumentert utfasingspolicy; JSON-schema for jobspec publiseres (editor-autofullføring — 03 §2).
  Akseptanse:
  - Golden-testene markeres som kontraktstester; brudd krever major.

### [M8-02] Oppgraderingstester fra alle 0.x
- **Epic:** Stabilisering
- **Milepæl:** M8
- **Labels:** area:store, type:test
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-06
- **Spek:** CI-matrise: DB-snapshots fra v0.1/v0.2/v0.3/v0.4 migreres til HEAD og verifiseres (fsck + golden-schema + stikkprøvespørringer). `VACUUM INTO`-kopi før migrering automatisk (06 §9.2).
  Akseptanse:
  - Oppgradering fra v0.1-database uten manuelle steg (09 v0.6-exit).

### [M8-03] Lasttest og ytelseskrav
- **Epic:** Stabilisering
- **Milepæl:** M8
- **Labels:** area:engine, type:test
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M5-03
- **Spek:** 500 jobber / 10 000 runs-historikk: `status` <100 ms p99, explain <1 s p99 ved 10⁶ ticks (06 SLO 3), 100 samtidige runs uten `SQLITE_BUSY` (04 §11), hvilende minne <40 MB, kaldstart <200 ms. EXPLAIN QUERY PLAN-assertions på varme spørringer.
  Akseptanse:
  - Tallene dokumenteres i README («testet til X samtidige jobber» — 09 R4).

### [M8-04] Pakking og distribusjon
- **Epic:** Stabilisering
- **Milepæl:** M8
- **Labels:** area:release, type:infra
- **Prioritet:** P1
- **Estimat:** M
- **Avhenger av:** M5-07
- **Spek:** deb/rpm (nfpm), Homebrew-tap, AUR; systemd-unit i pakkene; install.sh. cosign keyless-signering + CycloneDX-SBOM på release-artefakter (08 §5); verifiseringsinstruks i README (signering uten dokumentert verifisering er teater).
  Akseptanse:
  - `apt install` → kjørende schedule på <5 min (04 fase 6-kriteriet).

### [M8-05] Dokumentasjonssite (Diátaxis)
- **Epic:** Stabilisering
- **Milepæl:** M8
- **Labels:** area:product, type:docs
- **Prioritet:** P1
- **Estimat:** L
- **Avhenger av:** M5-08
- **Spek:** Fire adskilte kategorier per 09 §9.2: tutorials (crontab-5-min, første sensor, første DAG med bevisst feil), how-tos (varsling, overlappsvern, catch-up, feilsøking, backup, proxy), referanse (skjema felt-for-felt, CLI autogenerert, sensor-kontrakt, tilstandsmaskin tegnet, alle reason-koder), forklaring (garantier, cursor vs run_key, hvorfor SQLite, ÆRLIG sammenligning cron/systemd/Dagu/Airflow). Alle eksempler kjøres i CI.
  Akseptanse:
  - Feilsøkingssiden treffer søk på «paceq job did not run»; engelsk språk.

### [M8-06] Sikkerhetsgjennomgang og fuzz-komplettering
- **Epic:** Stabilisering
- **Milepæl:** M8
- **Labels:** area:security, type:test
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-03
- **Spek:** Intern sikkerhetsgjennomgang mot trusselmodellen T1–T18 (08 §2.3) med skriftlig disposisjon per trussel; fuzz-korpus komplett (jobspec-parser, sensor-JSON, cron-uttrykk, importer) i nightly; verifiser dag-én-listen (08 §6) punkt for punkt; RLIMIT_CORE=0; secrets-lekkasjetest (plant hemmelighet, grep alle utdata — 03 risiko 13).
  Akseptanse:
  - Hver T1–T18 har status implementert/akseptert-ikke-mål med begrunnelse.

### [M8-07] Effektuer navnebeslutning
- **Epic:** Styring
- **Milepæl:** M8
- **Labels:** area:product, type:decision
- **Prioritet:** P0
- **Estimat:** M
- **Avhenger av:** M0-04
- **Spek:** Renamen av modulsti, binær og docs er gjort i M0 (#93). Igjen her står bare det som ikke er kode: kjøp av paceq.dev, reservasjon av GitHub-org og pakkenavn, og SEO-tiltak (entydig tagline, «paceq orchestrator» som søkefrase).
  Akseptanse:
  - Ingen brukervendt flate har uavklart navn ved v1.0 (09 R5, 10 F16).

### [M8-08] v1.0-release
- **Epic:** Stabilisering
- **Milepæl:** M8
- **Labels:** area:release, type:release
- **Prioritet:** P0
- **Estimat:** S
- **Avhenger av:** M8-01, M8-02, M8-03, M8-04, M8-05, M8-06, M8-07
- **Spek:** Tag v1.0 «Stabil kontrakt»: ingen nye funksjoner, frosne formater, migreringsgaranti fra alle 0.x, dokumentert utfasingspolicy (09 §8).
  Akseptanse:
  - Konfigformatet uten brytende endring siste 3 måneder; K4-budsjettsjekk (10 §7): kjerne ≤12 000 linjer, ≤8 runtime-deps — brudd betyr at noe fjernes, ikke at budsjettet økes.

---

## E. Avhengighetsgraf — kontroll

- Alle avhengigheter peker bakover i milepælrekkefølgen eller innen samme milepæl mot lavere nummer; grafen er asyklisk (verifisert manuelt: M0-interne kjeder 01→02/03/05→06→07→{08 via 05, 10, 11, 12}; M1 bygger kun på M0 + interne; M2 på M0/M1; M3 på M1/M2; M4 på M1/M2; M5 på M1–M4; M6 på M1–M5; M7 på M2/M3/M5; M8 på M0/M5/M7).
- Kuttlinjer ved tidspress: M7-05 (P3) først, deretter M7-04, M6-04; M4-parallellitet kan reduseres til sekvensiell topologisk (SYNTESE §3.1-tidsboksen). v0.1-verdien (M0–M5) beskyttes alltid.

## F. Post-1.0 (utenfor denne planen, krever egne designrunder)

Privilegieseparasjon (`paceq-exec`/Landlock/per-jobb-uid) · HTTPS-API + tokens + webhook-HMAC · Postgres/multi-node (endrer tillitsmodellen — 08 fase 6) · dynamisk fan-out · Litestream-integrasjonsguide · `on_daemon_restart: detach`.
