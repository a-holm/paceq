# Pulseq — 00-SYNTESE: gjennomgang av 11 planer og endelig arkitektur

> Sjefsarkitektens syntese, 2026-08-21. Grunnlag: `prosjektbeskrivelse.md` + planene 01–11.
> Endelig masterplan med milepæler og issue-backlog: `docs/PLAN.md`.

---

## 1. Overordnet lesning

Planene konvergerer på overraskende mye:

- **SQLite-skrivemodellen**: to pooler, writer med `MaxOpenConns(1)` + `_txlock=immediate`, WAL, `modernc.org/sqlite`. Nevnt eksplisitt i 01, 02, 03, 04, 05, 06, 07, 08, 10, 11. Dette låses (§3.11).
- **Logger som filer på disk, ikke DB-rader** (01, 03, 04, 05, 06, 08, 10, 11; 02 og 07 avviker delvis, se §3.9).
- **Sensor = ekstern prosess med JSON-kontrakt**, aldri Go-plugin/SDK/DSL (alle unntatt ingen).
- **Cursor + triggere + tick committes i ÉN transaksjon**; cursor rykker aldri frem ved feil (alle).
- **Idempotens via `run_key` + UNIQUE-indeks**, ikke applikasjonslogikk (alle).
- **Explain bygger på persisterte beslutninger** (ticks/skip-grunner/run_events), aldri re-utledning (01, 03, 04, 05, 06, 07, 10, 11).
- **Daemonen kjører aldri brukerkode in-process** (04s grunnregel #1, støttet av alle).
- **Lease + fencing-epoch** for eierskap (02, 04, 05, 07, 11).
- **Én binær, statisk, CGO-fri; ingen web-UI, Postgres, notifikasjonsplattform eller container-driver i MVP** (alle).

Reell uenighet finnes på ~20 punkter — avgjort i §3.

---

## 2. Vurdering av planene

### 01 — Minimalisten
**Sterkeste bidrag:** «logger er filer»-reduksjonen; «utsatt» modellert som `not_before > now` i stedet for egen tilstand; harde grenser-kapittelet (kap. 2) som mal for SCOPE-dokument; eksempelsensorer i shell som kjøres i CI (dokumentasjon = integrasjonstest); `pulseq run --wait` med runens utfall som exit-kode; LOC-/avhengighetsbudsjett som disiplin.
**Svakheter:** SQLite-filen som IPC (CLI skriver direkte mens daemonen kjører) undergraver én-skriver-disiplinen andre planer bygger korrekthet på; TOML taper mot målgruppens YAML-kompetanse; unifiseringen av schedules og sensors til én `source`-tabell gjør DST-policy og sensor-spesifikke felter (epoch, breaker) klønete; ingen fencing (kun flock); 4 ukers estimat urealistisk.
**Inn:** loggfil-modellen, not_before-modellen, examples-i-CI, `prune`, harde grenser som SCOPE.md-utgangspunkt, budsjett-tankegangen.
**Ut:** TOML (se stridspunkt 3), DB-som-IPC (stridspunkt 6), source-unifisering, flag-only CLI (stridspunkt 5).

### 02 — Pålitelighetsingeniøren
**Sterkeste bidrag:** Den mest presise tenkningen i hele materialet. Krasjvindu-katalogen W1–W16 med test per vindu; invariantene I1–I16 + `fsck`; rekonsiliering R0–R11 med `boot_id`-grepet (maskinrestart ⇒ ingen prosess overlevde, umiddelbar sikker opprydding); atomisk sensor-tx-sekvens; DST-policytabellen (`spring_forward`/`fall_back`) + gullstandard-tidssoner; `pgregory.net/rapid` modellbasert testing; poison-pill/`crash_count`; kanselleringssemantikk (forespørsel ≠ tilstand); eksplisitte ikke-garantier som produktdokumentasjon.
**Svakheter:** Tyngste scope: `logs.db` som egen fil, `synchronous=FULL`, exec-shim + spool i MVP, globale sequences, instances-maskineri — sammen langt over det én utvikler leverer på 8 uker. FULL-argumentet er reelt men taper mot at-least-once-semantikken (stridspunkt 8).
**Inn:** krasjvindu-katalogen som testplan (forenklet), invariant-subset + fsck, rekonsilieringssekvensen, boot_id, DST-policyfeltene og gullstandarden, rapid-testing, poison-pill, cancel-modellen, garantidokumentet.
**Ut/utsatt:** `logs.db`, FULL som default, exec-shim/spool (→ herdingsmilepæl M6), global epoch-sequence (per-run-epoch holder).

### 03 — CLI-designeren
**Sterkeste bidrag:** Kommandogrammatikken (verb på toppnivå + substantivgrupper, `run` som bevisst unntak med selvlærende feilmelding); exit-kode-tabellen (skillet 1 «pulseq feilet» vs 5 «jobben feilet» gjør cron-migrering trygg); feilmeldingsanatomien (kode, kildeposisjon, caret, neste steg); knivskarp `retry` vs `replay`-semantikk; dual-mode (socket + direkte SQLite) med kontraktstest i begge moduser; `testscript`-testing; dynamisk completion med 150 ms-budsjett; hemmeligheter som referanser.
**Svakheter:** YAML-over-JSON-IR med `generator:`-luke og Starlark-frontend er riktig arkitektur men for stor flate for MVP; templating i verdifelt er en injeksjons- og kompleksitetsrisiko (08/10 vinner den); lipgloss/jsonschema-avhengigheter unødvendige fra start.
**Inn:** grammatikken, exit-kodene, diagnostikk-kvaliteten (forenklet kodeserie), retry/replay-definisjonene, dual-mode + kontraktstest, testscript, IR-tanken (YAML → kanonisk JSON + `spec_hash` — uten flere frontends).
**Ut/utsatt:** Starlark, `generator:`, templating i MVP (kontekst via env), lipgloss, params-typesystem (strenger + JSON i MVP).

### 04 — Orkestrator-veteranen
**Sterkeste bidrag:** Prior-art-destillatet (tre lærdommer: brukerkode i orchestrator-prosessen henger; ikke-transaksjonelt dupliserer; ulagret grunn blir supporthenvendelse); **dedup-epoch** — den beste enkeltideen i materialet, løser Dagsters dokumenterte cursor-reset-felle (`UNIQUE(source, epoch, run_key)`, `sensor reset` bumper epoch); `stable_for` på filsensor; `job_version` som content-hash; chunking ved `max_triggers` med umiddelbar re-tick; skriftlig ikke-mål-liste for PR-avvisning.
**Svakheter:** sqlc + goose er avhengigheter for problemer vi løser på ~150 linjer selv; egen cron-parser er unødvendig risiko når vår egen iterator uansett eier DST-policyen; semaforer/kø-tabeller i MVP overdimensjonert.
**Inn:** dedup-epoch, grunnregel #1, job_version-hash, tick-UNIQUE, explain-sjekklistetesting, `stable_for` (M7), avvisningslisten.
**Ut:** sqlc/goose i MVP, egen full cron-parser, semaphore-tabell (jobbnivå `max_concurrent` + `concurrency_key` dekker).

### 05 — Go-arkitekten
**Sterkeste bidrag:** **DAG som SQL-predikat** — claim-spørringen med `NOT EXISTS` over `step_deps` ER DAG-utføringen: ingen graf i minnet, ingen gjenoppbygging etter krasj, parallellitet faller ut gratis. Dette er nøkkelen som gjør «DAG i MVP» billig nok (stridspunkt 1). I tillegg: frosset spec per run; `notify.Bus` med tickere som sikkerhetsnett («korrekt med bare tickere, raskt med bussen»); todelt shutdown-kontekst; NFS-nekt ved oppstart; `fakecmd`-testbinær; pakkelayout med håndhevet avhengighetsretning.
**Svakheter:** «ingen Clock-abstraksjon, synctest holder» ryker på hans eget forbehold (SQLite-I/O er ikke durably blocked); flag-only CLI taper (stridspunkt 5); egen `deferred`-tilstand unødvendig (01s not_before er bedre).
**Inn:** claim-predikatet, versjonspeker som spec-snapshot, notify-buss, drain-semantikk, fakecmd, pakkelayout-basis, NFS-sjekk.
**Ut:** flag-only CLI, expvar-metrics, deferred som egen tilstand.

### 06 — SRE/observabilitet
**Sterkeste bidrag:** **Årsakskodekatalogen** (lukket enum + tiltaksforslag + CI-test som forbyr terminale tilstander uten `reason_code`) — dette er mekanismen som hindrer at explain degenererer; tick-definisjonen (kun forfalte evalueringer, aldri loop-våkninger); **koalesering av repeterte skips** (`repeat_count` — 2 880 identiske sensor-skips/døgn blir én rad); **gap-deteksjon** (`daemon_sessions` + `outages` + syntetiske `TICK_MISSED_DAEMON_DOWN`); `error_tail` i DB så explain virker uten filaksess; freshness-SLA som metrikk (generisk alarmregel); «minst N siste per objekt»-retention; explain virker med daemonen nede; outbox for varsler.
**Svakheter:** client_golang + go-systemd øker dep-flaten mer enn MVP trenger; «historikkmodell før motor»-rekkefølgen er delvis upraktisk (fixtures-fasen F1 gir lite alene); metrikksettet er fase-2-stort.
**Inn:** reason-koder + håndhevelse, koalesering, gap-deteksjon, error_tail, NDJSON-loggformat med head/tail-trunkering, retention-regelen, outbox (M6), sd_notify (håndskrevet protokoll, ~30 linjer), freshness-SLA.
**Ut/justert:** client_golang (håndskrevet Prometheus-tekstformat i MVP), OTel (aldri i 1.0).

### 07 — Databasespesialisten
**Sterkeste bidrag:** Den dypeste SQLite-analysen: oppgraderingsdødlåsen presist forklart (hvorfor `busy_timeout` er en ikke-strategi); **ATTACH er ikke atomisk i WAL** (dreper flerfilsalternativet med fakta); checkpoint-svelt som mest sannsynlige produksjonsfeil + mottiltakene; **`auto_vacuum=INCREMENTAL` må settes ved opprettelse** (ikke-reverserbar beslutning); admission control i Go innenfor `BEGIN IMMEDIATE` («single-writer-gevinsten som forenkler halve prosjektet»); `run_keys` som langlevd `WITHOUT ROWID`-tabell som overlever retention; batchet retention; `VACUUM INTO` + verifisering; migrator med checksum + user_version-gjerde; kapasitetsbudsjettet (~0,5 skrive-tx/s reelt ⇒ 50–100× margin; risikoen er lås-holdetid, ikke gjennomstrømning).
**Svakheter:** `log_lines` i hoved-DB (selv batched) strider mot filkonsensus — hans egen plan hedger med senere splitt; sqlc-halvveis gir verktøyfriksjon.
**Inn:** nesten hele §1 og §6 (DSN, pragmaer, WAL-hygiene, retention, backup), run_keys-tabellen, migrator-designet, golden-schema-test, STRICT-tabeller, kapasitetsbudsjettet som dokumentasjon.
**Ut:** logger i DB, sqlc i MVP, `pending_deps`-tellere (05s claim-predikat er tilstandsløst og enklere; ved vårt volum koster `NOT EXISTS` ingenting).

### 08 — Sikkerhetsskeptikeren
**Sterkeste bidrag:** Riktig ramme («Pulseq ER en kommandokjører»); trusselmodellen T1–T18; innsikten *skriverett på jobbspec ≡ kodeutførelse* (author > operator); **skillekriteriet for MVP-sikkerhet**: ikke «hva er viktigst», men «hva kan ikke ettermonteres uten å endre tillitsmodellen» — dette gir en presis dag-én-liste (argv-only, ingen TCP, egen bruker, obligatorisk timeout + prosessgruppedrap, secrets aldri i klartekst i DB, logger 0600 skrevet av daemonen, `os.Root`-stier, SECURITY.md med ikke-mål); leverandørkjede-sjekklisten; SSRF-vern for HTTP-sensorer (M7).
**Svakheter:** 4-binær-modellen, Landlock, age-secrets og signerte specs er riktig retning men feil fase for et én-tillitssone-produkt for én utvikler; TOML-valget og egen cron-parser taper mot øvrige hensyn.
**Inn:** hele dag-én-listen, trusselmodell-dokumentet, `file`-modus som default (API-et har intet spec-skrive-endepunkt), rollemodell-tanken forberedt i socket-laget, supply-chain-CI, redaksjon dokumentert som sikkerhetsnett ikke grense.
**Ut/utsatt:** pulseq-exec/shim/Landlock/per-jobb-uid (post-1.0, egen designrunde med port), age/exec-backends, webhook-HMAC (kommer med webhook-featuren), TOML.

### 09 — Produktlederen
**Sterkeste bidrag:** Segmentdefinisjonen (5–100 jobber, 1–3 maskiner, 0 plattformfolk); **Dagu, ikke Airflow, er den reelle konkurrenten** ⇒ differensiatoren må være sensorer-med-cursor + spørrbar negativ informasjon, ikke «lettvekt»; **`import crontab` + skyggemodus + `cutover --rollback`** — den beste produktideen i materialet (byttekostnaden, ikke funksjonsmangel, er bindingen); `concurrency: 1` som *standardverdi* som produktbeslutning; wow-øyeblikkene som designmål; navnerisikoen dokumentert med kriterier; kvalitetsmål (status < 100 ms, 0 «unknown»-skip-grunner); Diátaxis-dokstrategi; release-tog med exit-kriterier som ikke er «koden er ferdig».
**Svakheter:** Ingen tekniske beslutninger (bevisst); både sensorer OG import i v0.1 presser scope — løses ved at import ligger sist i MVP-løpet (M5).
**Inn:** import/shadow/cutover, default-verdier, kvalitetsmål, exit-kriterier per milepæl, dokstrategi, navnebeslutnings-issue, README-kontrakten, anti-personaer i SCOPE.md.
**Ut:** ingenting substansielt — planen er komplementær.

### 10 — Djevelens advokat
**Sterkeste bidrag:** Obduksjonen treffer de tre reelle dødsårsakene (nisjen tatt, DAG/UI spiser tiden, ingen produksjonsbruk før måned 9); **kill-criteria og porter** (K1: kjører du selv 3 ekte jobber innen uke 6?) — adopteres som milepæl-exit-kriterier; budsjettene; tracer-bullet-sekvensering; **cursor/run_key-doktrinen** (to begreper, to reset-flagg, aldri implisitt kobling); enkelttrådet dispatcher som gratis race-eliminering; «explain er hele produktet — alt annet er infrastruktur for å skrive ut den teksten».
**Svakheter:** Å kutte DAG helt motsier prosjektbeskrivelsen — og 05s claim-predikat fjerner mesteparten av kostnaden 10 frykter (som gjelder *utvidet* DAG-semantikk: betingelser, fan-out, continue_on_error — som vi forbyr); Postgres-abstraksjonsforbudet er for absolutt (vi tar null-kost-varianten, se stridspunkt 12); navnebytte-kravet overstyres av eier (stridspunkt 10).
**Inn:** porter/kill-criteria, budsjetter (justert: ≤12 000 linjer kjerne, ≤8 runtime-deps), enkelttrådet dispatcher, F4c-doktrinen, sekvensering (ett steg ende-til-ende før DAG), `SCOPE.md` committet først.
**Ut:** DAG-kuttet (avgrenset i stedet), navnebytte-påbudet (→ beslutnings-issue), totalforbudet mot store-grensesnitt.

### 11 — Distribuert-pragmatikeren
**Sterkeste bidrag:** «Hva som IKKE trenger konsensus»-tabellen (verdt å ta inn i dokumentasjonen ordrett); **lease-tid beregnes alltid av databasen/prosessen, aldri sammenlignet på tvers av noder** (Store-metoder uten `now`-parameter); insert-first, aldri check-first (TOCTOU); trippel-nøkkelmodellen (`idempotency_key`/`concurrency_key`/`run_key`); **`PULSEQ_IDEMPOTENCY_KEY` til brukerens steg som dokumentert kontrakt** («at-least-once uten stabil nøkkel flytter bare problemet»); rolle-lease begrunnet på single-node (overlappende restart, glemt terminal-daemon); lease-fornyelse i egen goroutine som aldri blokkeres av arbeidet; run som claim-enhet.
**Svakheter:** Konformitetssuite + Capabilities + per-driver-migrasjoner + HTTPDispatcher + workers-registry er å betale for multi-node nå — mot 10s korrekte innvending om at ubrukte abstraksjoner er ren kostnad; spekulative `execution_unit`/steg-lease-kolonner droppes.
**Inn:** konsensus-tabellen, klokkeregelen, insert-first, nøkkel-trippelen, idempotensnøkkel-kontrakten, rolle-lease, run-claim, pull-modellen (trivielt sann in-process).
**Ut:** konformitetssuite/Capabilities/postgres-katalog i MVP, remote workers, gRPC-diskusjonen.

---

## 3. Stridspunkter — endelige beslutninger

### 3.1 DAG i MVP eller utsatt
**Beslutning: DAG med statisk `needs` er i MVP, men som egen, sen milepæl (M4) med harde grenser.**
Beskrivelsen krever «basic DAG dependencies»; 10 og 09 vil kutte/utsette. Avgjørende: 05s claim-predikat (`NOT EXISTS` over frosne `step_deps`-kanter) gjør DAG-utføring til én SQL-spørring — ingen graf-motor, ingen krasj-gjenoppbygging. Kostnaden 10 dokumenterer ligger i utvidet semantikk, som forbys: **kun statisk `needs`, feil ⇒ nedstrøms `skipped`, ingen betingede kanter, ingen dynamisk fan-out, ingen `continue_on_error` i 1.0** (10 §6-grensene). Sekvensering etter 10: M1 leverer sekvensielle steg ende-til-ende først; M4 er DAG. Tidsboks: overskrider M4 tre uker, kuttes parallellitet (kun topologisk sekvensiell) og resten flyttes.

### 3.2 Sensor-først vs DAG-først
**Beslutning: utføring → schedules → sensorer → DAG.** Utføringskjernen først (05: «en scheduler uten fungerende worker er umulig å teste»; 03/09: `pulseq run` uten daemon er femminutters-opplevelsen). Deretter schedules (cron-erstatningsverdien, M2), så sensorer (M3) — differensiatoren, foran DAG (M4) som er paritetsfunksjon (09 §3.2). Explain-datamodellen (ticks, reason-koder, run_events) ligger i skjemaet fra M1 slik at 06s krav «kan ikke ettermonteres» er oppfylt uten å bygge motoren baklengs.

### 3.3 Konfigformat
**Beslutning: YAML som eneste overflate, kompilert til kanonisk JSON-IR med `spec_hash`. Ingen templating i MVP.**
Flertall (03, 04, 05, 09, 10, 11) + det avgjørende produktargumentet: `import crontab` skal produsere noe målgruppen leser og redigerer, og målgruppen kan YAML (Compose/Actions). 08s innvendinger (billion laughs, Norway-problemet) håndteres teknisk: `goccy/go-yaml` med `DisallowUnknownField`, alias-/dybde-/størrelsesgrenser, og alle skalartyper valideres mot skjema. 03s IR-arkitektur beholdes i minimal form (én frontend; `spec_hash`; motoren leser aldri YAML) — den koster nesten ingenting og gjør formatvalget reversibelt. Templating: kuttet fra MVP (08 §11.2, 10 F8) — kontekst leveres som `PULSEQ_*`-miljøvariabler og `params` som JSON. TOML avvist; HCL/CUE/Starlark avvist.

### 3.4 Cron-parsing
**Beslutning: `adhocore/gronx` som uttrykksparser/-primitiv + egen iterator (`internal/cronx`) som eier tidssone, DST-policy og `Between()` for catch-up.**
robfig/cron er i praksis uvedlikeholdt siden 2020 og har implisitt DST-semantikk uten policyvalg (02 §9, 04, 11); egen full parser (04, 08) er ~300 linjer med en klasse subtile feil vi ikke trenger å eie. gronx er avhengighetsfri, vedlikeholdt, og har `NextTick`/`PrevTick` fra vilkårlig tidspunkt — nøyaktig primitivene catch-up og preview trenger (11 §9). DST-policy per schedule etter 02 §5.3: `spring_forward: skip|shift` (default skip), `fall_back: first|both` (default first); alt lagres UTC; `time/tzdata` embeddes. Verifisering: gullstandard-fixtures for 7 soner (02 §7.4) + differensialtest mot uavhengig implementasjon. Byttekostnad ved gronx-problemer: én pakke, iteratoren er vår.

### 3.5 CLI-rammeverk
**Beslutning: `spf13/cobra`.**
CLI-en ER produktflaten (explain, import, status, completion); 03s grammatikk med ~10 substantivgrupper, dynamisk completion mot lese-poolen og genererte man-sider er reell brukerverdi som koster uker å reimplementere på `flag`. Minimalistene (01, 05, 08, 10) har rett i at cobra er tyngste avhengighet — den aksepteres eksplisitt som den ene tunge, isolert til `cmd/` + `internal/cli`, og dep-budsjettet (≤8 runtime) er satt med den inkludert. Completion-budsjett 150 ms med tom fallback (03 §9.1).

### 3.6 IPC
**Beslutning: dual-mode, presist definert (03 §10.4 + 02 §1.1 + 06 §4):**
- **Lesing** (list/show/explain/logs/status): alltid direkte SQLite via read-only-pool. Fungerer med daemonen nede — «explain må virke under hendelsen» (06) er et hardt krav.
- **Skriving med daemon oppe:** unix-socket (`$RUNTIME_DIR/pulseq.sock`, 0660) → daemonens ene skriver.
- **Skriving uten daemon:** CLI tar `flock` selv og skriver direkte gjennom samme `internal/store`-kode; `pulseq run` kjører embedded executor in-process (femminutters-opplevelsen, 03 §9.2).
- `flock` på statedir garanterer at det aldri finnes to skrivere samtidig.
01s rene DB-IPC avvist (to samtidige skrivere ved kjørende daemon); 08s socket-only avvist (bryter explain-nede-kravet og null-daemon-onboarding). Kontraktstest: hele testscript-suiten kjøres i begge moduser (03 risiko 6).

### 3.7 Sikkerhetsarkitektur: 4 binærer vs én
**Beslutning: én binær og same-user-modus i MVP; 08s «kan ikke ettermonteres»-liste er bindende fra dag én; privilegieseparasjon er post-1.0 bak egen designport.**
08 sier selv at same-user-modus er MVP-standarden og at det da ikke finnes privilegert kode. Dag-én-listen adopteres i sin helhet: argv-array uten implisitt shell (`shell:` eksplisitt opt-in med advarsel), ingen TCP-lytter overhodet i MVP (kun unix-socket), egen systembruker + hardnet systemd-unit, obligatorisk timeout + drap av hel prosessgruppe, secrets aldri i klartekst i DB (MVP: `env_file`-referanser 0600), logger 0600 skrevet av daemonen (jobben får pipe), `os.Root` for spec-styrte stier, tom env-baseline med eksplisitt `inherit_env`, SECURITY.md med eksplisitte ikke-mål, go.sum/govulncheck/gosec i CI. `pulseq-exec`/`pulseq-shim`/Landlock/per-jobb-uid/age: post-1.0, egen trusselmodellrevisjon (08 fase 3-innhold). Web-UI (M7) binder 127.0.0.1, read-only, CSP, null npm.

### 3.8 `synchronous`: FULL vs NORMAL
**Beslutning: `NORMAL` som default; `FULL` som dokumentert konfignøkkel.**
02s argument (NORMAL kan rulle tilbake committede tx ved strømbrudd ⇒ bryter «durabel intensjon») er teknisk korrekt men taper mot systemets egen semantikk: hele designet er at-least-once med rekonsiliering (07 §6.6). Tapsvinduet er kun strømbrudd/OS-krasj (ikke prosesskrasj), og en tilbakerullet transaksjon er *konsistent* — tick+trigger+cursor committes atomisk, så en tilbakerulling gir re-evaluering, som dedupliseres av `run_key`. Schedule-ticks er deterministiske (`run_key` = schedule+scheduled_for) og re-materialiseres identisk. Prisen for FULL er 10–100× lavere commit-rate og N fsyncs der batching ellers ga én. Mottiltak beholdes: batch-heartbeat i én tx (02 §5.2), og garantidokumentet sier eksplisitt hva NORMAL betyr (06/09: ærlighet om garantier er en feature).

### 3.9 Logglagring
**Beslutning: filer på disk. Format og regler låses:**
- Sti: `$STATE/logs/<yyyy-mm-dd>/<run_id>/<step>.<attempt>.ndjson` — datoshard gjør retention til katalogsletting (06 §3.2).
- Linjeformat: NDJSON `{"ts":…,"stream":"stdout|stderr|sys","seq":N,"line":"…"}` — `seq` gjør tap/trunkering detekterbart.
- Kvote per forsøk: 16 MiB default; ved overskridelse beholdes hode (25 %) + hale (75 %) med markørlinje (06).
- `error_tail` (~4 KiB) speiles inn i `steps`-raden ved terminering, så explain/UI aldri trenger filaksess (06 §3.2).
- DB holder `log_path`, `log_bytes`, `log_truncated`.
- Skrives av daemonen (jobben får pipe, aldri direkte filtilgang — 08 T17); 0600/0700.
- Retention: 14 dager eller global byte-cap, eldste shard først; «minst N siste per objekt»-regelen gjelder DB-historikk (06 §9.4).
02s `logs.db` og 07s `log_lines` avvist: filer gir `tail -f`/`grep`/logrotate gratis, null belastning på den ene skriveren, og holder backup liten. Daemonens egen strukturerte logg: `log/slog` JSON til stderr → journald.

### 3.10 Navnet «pulseq»
**Beslutning: behold som arbeidsnavn; risiko dokumenteres; beslutnings-issue til eieren (M0-04) med frist før første offentlige release (M8-07 effektuerer).**
09 §3.4 og 10 F16 dokumenterer kollisjonen med MR-rammeverket Pulseq (pulseq.github.io, PyPulseq): søk, domener og pakkenavn blir tvetydige. 10 krever bytte før publisering; eieren har besluttet å beholde arbeidsnavnet. Syntesen: intern utvikling upåvirket; beslutningen MÅ tas før v0.3/første markedsføring (mens byttekostnaden er null); kriterier fra 09: ledig `.dev`-domene, ledig GitHub-org, entydig førstesidetreff, uttalbart, ledig binærnavn i Debian/Homebrew. Alt brukervendt (modulsti, binærnavn) holdes lett å rename frem til beslutningen.

### 3.11 SQLite-skrivemodell — låst
**Beslutning (bred konsensus, verifisert i 01 §6.1, 02 §1.3, 03 §10.3, 04 §4.6, 05 §3.3, 06 §9.1, 07 §1, 08 §7.2, 10 §5, 11 §7):**
- Én metadatafil `state.db`. Flere filer avvist: ATTACH-transaksjoner er ikke atomiske i WAL (07 §1.2-D1).
- To pooler: writer `SetMaxOpenConns(1)` + `_txlock=immediate` (unngår oppgraderingsdødlåsen `busy_timeout` ikke redder); reader `mode=ro`, N=NumCPU.
- PRAGMA: `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`, `foreign_keys=ON`, `journal_size_limit=64MiB`, `wal_autocheckpoint=1000`, `temp_store=MEMORY`; `auto_vacuum=INCREMENTAL` settes ved `init` (kan ikke endres billig senere — 07 §6.3). PRAGMA-verdier verifiseres ved oppstart; avvik = oppstartsfeil (02 §1.3).
- Driver: `modernc.org/sqlite` (CGO-fri, statisk binær); `mattn/go-sqlite3` bak byggetagg som rømningsvei; ytelsesport i M0 (≥500 skrive-tx/s) avgjør før kodebasen låser oss.
- Regler: ingen exec/nettverks-I/O i skrivetransaksjon, noensinne; all mutasjon via `internal/store`-metoder (writer-håndtaket er privat, håndhevet med arkitekturtest); admission control som les-regn-skriv i Go innenfor én IMMEDIATE-tx (07 §4.2); `RETURNING` konsumeres fullt før neste bruk av forbindelsen; nekt oppstart på NFS/CIFS; STRICT-tabeller; tid som INTEGER UTC-ms.
- Kapasitetsramme dokumenteres (07 §11): reell last < 1 skrive-tx/s; designfokus er lås-holdetid, ikke throughput.

### 3.12 Postgres-abstraksjon
**Beslutning: `internal/store` som eneste SQL-eier med oppgavespesifikke, atomiske metoder — men ingen konformitetssuite, ingen Capabilities, ingen postgres-katalog, ingen per-driver-migrasjoner i MVP.**
11s port-regler §6.1 (forretningsoperasjoner med atomisitetskrav, ikke CRUD; ingen drivertyper i signaturer; ingen multi-kall-invarianter) adopteres fordi de er god design uansett backend og koster null. 11s konformitetssuite og Capabilities er å betale for multi-node nå — 10 F12 vinner: ubrukt abstraksjon er ren kostnad. 04s billige regler følges der gratis: `ON CONFLICT` fremfor `INSERT OR IGNORE`, tidsberegning i Go. Postgres er en post-1.0-designrunde (også 08: multi-node endrer tillitsmodellen og skal ikke smugles inn som drivervalg).

### 3.13 Claim-enhet: run vs steg
**Beslutning: run-nivå lease; steg-overganger bærer runens `lease_epoch`.** (11 §3.3: artefakt-/workdir-lokalitet, færre skriv; 01/02 samme modell.) Steg-klarhet avgjøres av claim-predikatet (05) innenfor runens eierskap. `step`-claiming for heterogene workere er ikke-mål i 1.0; ingen spekulative kolonner (avvik fra 11).

### 3.14 «Utsatt»-tilstanden
**Beslutning: ikke egen tilstand.** `queued` + `available_at > now` + obligatorisk `defer_reason` (01 §4.1). CLI viser «utsatt (grunn)». Sparer to tilstander og all overgangslogikk; beskrivelsens seks tilstander mappes eksplisitt i dokumentasjonen.

### 3.15 retry vs replay
**Beslutning: 03 §5.6 ordrett.** `retry` = fortsett samme run (samme id/run_key, kun feilede/ventende steg, nytt attempt); `replay` = ny run med `replay_of`-peker, samme `job_version` (aldri «siste spec» — 08 §7.4), `--from <steg>`/`--failed`. Operatør-retry av terminal run logges som egen event-type og er eneste vei ut av terminal tilstand (02 T14).

### 3.16 exec-shim/watchdog/spool
**Beslutning: ikke i MVP; herdingsmilepæl M6.** MVP: `Setpgid` + drap av prosessgruppe + `/proc`-sweep ved oppstart (miljømarkør `PULSEQ_RUN_ID`, verifisert mot `pid_start_ticks` før drap — 02 R3). Shimen (watchdog-pipe-EOF dreper foreldreløse ved daemon-død; spool-fil lukker krasjvindu W8) er 02s beste mekanisme men tung; den ettermonteres trygt fordi den ikke endrer kontrakter.

### 3.17 Metrics
**Beslutning: håndskrevet Prometheus-tekstformat i MVP** (~100 linjer, null dep — 05/11); kjernesett: `last_success_timestamp` + `freshness_sla` (06s generiske alarmregel), tick-lag, runs by state, lease-reclaims, WAL-/DB-størrelse, skrivekø-ventetid. Forbud mot ID-labels (06 §6.4). client_golang/histogrammer revurderes post-1.0. `deploy/pulseq-alerts.yml` leveres.

### 3.18 Notifikasjoner
**Beslutning: ingen i MVP (v0.1); M6 leverer outbox-tabell + `exec`-notifier** (varsel i samme tx som tilstandsendringen, egen leveringsloop — 06 §8, 07 §3.6). Aldri innebygde Slack/SMTP-integrasjoner; `exec` + oppskrifter dekker alt.

### 3.19 Innebygde sensortyper
**Beslutning: MVP har kun `exec`-kontrakten + eksempelsensorer i shell som kjøres i CI (01 §Fase 3).** `file` (med `stable_for`), `http` (med SSRF-vern, 08 §3.6) og `sql` kommer i M7. 10 F4a/F4b-balansen: uendelig bred kontrakt, null API-flate.

### 3.20 Clock-abstraksjon
**Beslutning: minimalt `Clock`-interface i domenelogikk** (01s ene interface; 02s wall/mono-skille for lease vs plantid) **+ `testing/synctest` (Go 1.25) der det passer.** 05s «ingen abstraksjon» ryker på SQLite-I/O-forbeholdet. Lease-tid sammenlignes aldri på tvers av prosesser (11 §4.5).

### 3.21 Worker-prosessmodell
**Beslutning: goroutiner i én prosess** (10 §4-tabellen, 05 §3.1). Beskrivelsens «worker-prosess» oppfylles av at *steg-kommandoene* er egne prosesser. Dispatcher/planner enkelttrådet (10). Ingen `--roles`-splitting i 1.0.

### 3.22 Dedup-modell
**Beslutning: 04s dedup-epoch + 07s run_keys-tabell kombinert:** `run_keys(source_id, epoch, run_key)` PK, `WITHOUT ROWID`, lengste retention (365 d, dokumentert konsekvens). `sensor reset` bumper epoch (beholder dedup i drift OG gir ekte replay); `--forget-run-keys` for kirurgisk sletting (10 F4c: cursor og run_key nullstilles alltid uavhengig). Dedup også innenfor én tick (04, dagster#26753). Schedule-run_key = `<schedule>:<scheduled_for RFC3339>`, epoch 0.

---

## 4. Syntetisert arkitektur

### 4.1 Prosessmodell

Én statisk binær `pulseq` (Go 1.25+, `CGO_ENABLED=0`):

```
pulseq serve                      én prosess, flock på statedir
 ├─ scheduler-loop   (1 goroutine, rolle-lease "scheduler")
 ├─ sensor-runtime   (dispatcher + semafor N=4; én evaluering per sensor om gangen)
 ├─ dispatcher       (1 goroutine — HELE beslutningslaget er enkelttrådet:
 │                    trigger → dedup → concurrency → run-materialisering)
 ├─ executor-pool    (N goroutiner; claim run → lease → steg via claim-predikat
 │                    → exec i egen prosessgruppe → logg til fil)
 ├─ reaper           (utløpte leases → epoch++ → requeue/failed; crash_count/poison)
 ├─ janitor          (retention, incremental_vacuum, wal_checkpoint, backup)
 └─ api              (unix-socket; JSON; ingen TCP i MVP)

pulseq <cmd>          CLI: les direkte (RO-pool, virker med daemon nede);
                      skriv via socket, eller flock+direkte når daemon er nede;
                      `pulseq run` uten daemon = embedded executor in-process
```

Vekking via intern notify-buss (kanal-broadcast); tickere er sikkerhetsnett — systemet er korrekt med bare tickere (05 §3.2). Shutdown: to-faset kontekst, drain-timeout, ikke-fullførte steg tilbake til `pending` uten forbrukt forsøk.

### 4.2 Datamodell (kjernetabeller)

Alle tider INTEGER UTC-ms. STRICT. Tekststatus med CHECK. Run-ID = ULID.

```
meta, schema_migrations(version, name, checksum, applied_at)
daemon_sessions(id, version, started_at, last_seen_at, stopped_at, stop_reason)
outages(from_ts, to_ts, kind, missed_ticks)
leases(name PK, holder, epoch, expires_at, acquired_at)          -- rolle-leases

jobs(name PK, current_version_id, paused, max_concurrent DEFAULT 1, created/updated)
job_versions(id, job_name, version, spec_json, spec_hash, source_path, created_at,
             UNIQUE(job_name, spec_hash))                        -- immutabel; run peker hit

schedules(id, job_name, kind cron|interval, expr, timezone,
          spring_forward skip|shift, fall_back first|both,
          catchup skip|last|all, catchup_limit, catchup_window_ms,
          paused, last_tick_at, next_tick_at)
          -- partielt indeks (next_tick_at) WHERE paused=0

sensors(name PK, job_name, exec_json, interval_ms, min_interval_ms, timeout_ms,
        max_triggers_per_tick, paused, cursor, cursor_updated_at,
        dedup_epoch, consecutive_failures, next_eval_at)

ticks(id, source_kind schedule|sensor|manual, source_name, scheduled_for,
      started_at, finished_at, outcome triggered|skipped|error|missed,
      reason_code, reason_text, reason_data, trigger_count, deduped_count,
      cursor_before, cursor_after, repeat_count,
      UNIQUE(source_kind, source_name, scheduled_for))
      -- NULL scheduled_for for sensorer: én constraint, to semantikker (07 §3.3)

triggers(id, tick_id, job_name, run_key, params_json, created_at,
         outcome accepted|deduped|rejected, reason_code, run_id)
run_keys(source_id, epoch, run_key, first_seen_at, run_id,
         PRIMARY KEY(source_id, epoch, run_key)) WITHOUT ROWID    -- overlever retention

runs(id ULID PK, job_name, job_version_id, trigger_id, origin, run_key,
     state queued|running|succeeded|failed|cancelled,
     available_at, defer_reason, scheduled_for, params_json, concurrency_key,
     attempt, max_attempts, lease_owner, lease_epoch, lease_expires_at, heartbeat_at,
     cancel_requested_at, cancel_reason, crash_count, replay_of, reason_code,
     created/started/finished, error)
     -- partial unique: concurrency_key WHERE state IN (queued,running)
     -- partial index: claim (state=queued, available_at); reaper (lease_expires_at)

steps(run_id, name, state pending|running|succeeded|failed|skipped|cancelled,
      attempt, max_attempts, next_attempt_at, started/finished, exit_code, signal,
      error, reason_code, log_path, log_bytes, log_truncated, error_tail,
      PRIMARY KEY(run_id, name))
step_deps(run_id, step_name, depends_on, PK(alle)) WITHOUT ROWID  -- frosne kanter per run

run_events(id AUTOINCREMENT, run_id, step_name, at, kind, from_state, to_state,
           reason_code, actor, detail_json)   -- append-only; én rad per overgang, samme tx
artifacts(id, run_id, step_name, name, uri, size_bytes, checksum, created_at)
outbox(id, topic, payload, available_at, attempts, delivered_at, last_error)  -- M6
```

### 4.3 Skrivemodell
Se §3.11 (låst). Tilleggsregler: retention batched (`DELETE … LIMIT 500`, pause mellom); WAL-metrikk med alarm > 64 MiB; lesestier bruker aldri eksplisitt `Tx` og pagineres med nøkkel, aldri `OFFSET` (07 §6.4); events-/statusskriv kan batches (200 rader / 250 ms).

### 4.4 Sensor-kontrakt (offentlig, fryses ved v0.1)

Inn — JSON på stdin **og** miljøvariabler:
```json
{"sensor":"navn","job":"jobb","cursor":"…|null","last_tick_at":1234567890,
 "now":1234567890,"dry_run":false}
```
`PULSEQ_SENSOR`, `PULSEQ_JOB`, `PULSEQ_CURSOR`, `PULSEQ_DRY_RUN`.

Ut — ett JSON-objekt på stdout:
```json
{"cursor":"…","triggers":[{"run_key":"…","params":{…}}],"skip_reason":null}
```
Regler: exit 0 = OK; exit 75 = forbigående (kort backoff, teller ikke mot breaker); annet ≠ 0 = feil (eksponentiell backoff, auto-pause etter N=10). **Cursor committes aldri uten triggerne, i samme transaksjon; cursor uendret ved feil.** Cursor er opak. Dedup: `(sensor, dedup_epoch, run_key)`, også innenfor én tick. `max_triggers_per_tick` → trunkering + delvis cursor + umiddelbar re-tick. Timeout → drap av prosessgruppe, tick=error. stdout-tak 1 MiB; stderr → logg (4 KiB-utdrag lagres). Tom output + exit 0 = skip med grunn. `pulseq sensor test` = dry-run uten sideeffekter.

### 4.5 Steg-kontrakt (offentlig, fryses ved v0.1)

Miljø inn: `PULSEQ_RUN_ID`, `PULSEQ_JOB`, `PULSEQ_STEP`, `PULSEQ_ATTEMPT`, `PULSEQ_RUN_KEY`, `PULSEQ_IDEMPOTENCY_KEY` (stabil på tvers av retries/duplikater — dokumentert kontrakt, 11 §5.4), `PULSEQ_SCHEDULED_FOR`, `PULSEQ_PARAMS` (JSON), `PULSEQ_OUTPUT` (fil for NDJSON: artefakter/params ut), `PULSEQ_INPUTS` (flettet JSON fra oppstrøms). Env er deny-by-default: tom baseline + `PATH/HOME/TZ/LANG` + jobbens `env` + eksplisitt `inherit_env` (08 §3.2).
Ut: exit-kode. 0 = suksess; 75 = alltid retrybar; >128 = signal. `run:` er argv-array; `shell: true` eksplisitt opt-in med valideringsadvarsel.

### 4.6 State machines

```
run:   queued ──claim(lease,epoch++)──> running ──> succeeded
         ^  │(available_at>now = «utsatt», defer_reason)   ├──> failed
         │  └── cancel_requested ────────────────────────────> cancelled
         └── reaper: lease utløpt → epoch++, crash_count++, requeue (eller failed/poison)

step:  pending ──(claim-predikat: alle needs succeeded)──> running ──> succeeded
         ^                                                    ├──> failed (attempt<max ⇒ pending + next_attempt_at)
         │                                                    └──> cancelled
         └── oppstrøms feilet ──> skipped(reason=upstream_failed)
```
Invarianter (fsck, håndhevet i test og drift): run `running` ⇒ gyldig lease; terminal run ⇒ ingen aktive steg; steg `running` ⇒ alle needs `succeeded`; run `succeeded` ⇔ alle steg `succeeded|skipped` uten feil; hver overgang har nøyaktig én `run_events`-rad i samme tx; terminale tilstander har alltid `reason_code` (CI-håndhevet).

### 4.7 Observabilitetsmodell
Fem lag (06 §2): ticks (beslutning) → triggers (utløsning) → runs/steps (kjøring) → NDJSON-filer + error_tail (utdata) → metrics (aggregat). Lukket reason-kode-katalog med tiltaksforslag; koalesering av repeterte skips; gap-deteksjon med syntetiske `missed`-ticks; `pulseq explain job|schedule|sensor|run` som ren presentasjon over lagrede data, med `--json` som stabil kontrakt (fremtidig UI får ingen egen datamodell); explain virker med daemonen nede; `pulseq status` < 100 ms med hint til explain.

### 4.8 Sikkerhetsnivå per fase
- **M0–M5 (v0.1):** same-user; ingen nettverksflate (unix-socket 0660); argv-only; tom env-baseline; obligatorisk timeout + pgid-drap; `env_file` 0600 for secrets; logger 0600 via pipe; `os.Root`-stier; umask 0077 + rettighetssjekk (fail closed); SECURITY.md + trusselmodell; supply-chain-CI.
- **M6 (v0.2):** hardnet systemd-unit som default-leveranse; doctor viser effektive vern; audit-kvalitet på run_events (aktør, auth-metode).
- **M7 (v0.3/0.4):** web-UI read-only, 127.0.0.1, CSP, null npm; SSRF-vern i http-sensor.
- **Post-1.0 (egen designrunde):** per-jobb-uid via `pulseq-exec`, Landlock/`pulseq-shim`, sandkassenivåer med «strict feiler, degraderer ikke», age/systemd-creds, HTTPS-API + tokens, webhook-HMAC, signerte specs, cosign/SBOM/SLSA.

### 4.9 Teknologivalg (endelig)

| Behov | Valg | Merknad |
|---|---|---|
| Driver | `modernc.org/sqlite` | `mattn` bak byggetagg; ytelsesport i M0 |
| Cron | `adhocore/gronx` + egen iterator | DST-policy og Between er vår kode |
| YAML | `goccy/go-yaml` | strict, posisjonsfeil, grenser |
| CLI | `spf13/cobra` | eneste tunge dep, isolert |
| ID | `oklog/ulid/v2` | tidssortert, prefiks-søkbar |
| Samtidighet | `golang.org/x/sync` | errgroup/semaphore |
| Logging | `log/slog` (stdlib) | JSON-handler; ContextHandler-mønster |
| HTTP/metrics | stdlib `net/http`; håndskrevet expfmt | ingen chi/gin/client_golang |
| Migrasjoner | egen (~150 l), embed + checksum + user_version | ingen goose/atlas |
| Test | stdlib + `go-cmp`, `testscript`, `rapid`, synctest | test-deps teller ikke mot budsjett |

Budsjetter: ≤8 direkte runtime-avhengigheter; ≤12 000 linjer kjerne-Go (ekskl. tester); binær < 30 MB; kaldstart < 200 ms; `status` < 100 ms.

### 4.10 Teststrategi (bærebjelker)
1. **Konkurransetest i M0, før domenekode**: 32 goroutiner, ekte fil, null `SQLITE_BUSY` (07 §7).
2. **Krasj-suite**: navngitte SIGKILL-punkter (forenklet W-katalog fra 02 §6), restart, fsck-invarianter, effekt-telling innenfor at-least-once-grensene.
3. **DST-gullstandard**: fixtures for Europe/Oslo (begge overganger), America/Santiago, Australia/Lord_Howe, Asia/Kolkata, UTC + differensialtest mot uavhengig implementasjon (02 §7.4).
4. **Property-tester (rapid)**: state machine-modell mot ekte SQLite; «cursor rykker aldri frem uten committede triggere».
5. **testscript + golden** på CLI (`--json` er offentlig grensesnitt), kjørt i begge IPC-moduser.
6. **Eksempelsensorer i CI** = levende dokumentasjon av kontrakten.
7. **Ingen mocks mot store**; ekte SQLite i `t.TempDir()` (aldri `:memory:` — WAL må testes ekte).
8. **Explain-sjekkliste**: hvert «kjørte ikke»-scenario har en test som asserterer lagret grunn.
9. **Soak nattlig** (M6): 24 t tilfeldig SIGKILL, kontinuerlig fsck.

### 4.11 Ikke-mål gjennom 1.0 (SCOPE.md)
Asset-graf/lineage-graf/partisjoner · distribuert kjøring/multi-node/leader election utover lease · Postgres-driver · plugin-system i enhver form (Go plugin, WASM, embedded språk) · templating-/uttrykksspråk i konfig · dynamisk fan-out og betingede kanter · innebygde integrasjoner (Slack/SMTP/S3-klienter) · multi-tenancy/RBAC · container-executors · sub-sekund-planlegging · innebygd TSDB/loggsøk/alertmanager · exactly-once (lov aldri dette).

---

## 5. Restrisikoer etter syntesen

| Risiko | Håndtering |
|---|---|
| Scope: 80-issue-planen er stor for én utvikler | Kill-criteria fra 10 er milepæl-exit-kriterier (K1 i M5, K2 i M6); M7/M8 kuttbare uten å skade v0.1-verdien |
| DAG-milepælen sklir | Tidsboks 3 uker; fallback: sekvensiell topologisk uten parallellitet |
| gronx-avvik fra forventet cron-semantikk | Differensialtest + gullstandard; parserbytte er isolert bak `internal/cronx` |
| modernc-ytelse/feil | Ytelsesport M0; mattn bak byggetagg |
| Dagu-konkurransen | Differensiator (sensorer + explain) ferdig i v0.1; ærlig sammenligningsside (M8) |
| Navnekollisjon | Beslutnings-issue M0-04, effektuering M8-07, alt rename-billig frem til da |
| «database is locked» i felt | Modellen i §3.11 + konkurransetest + WAL-alarmer; dokumentert kapasitet |
| DST-feil | Egen policy + gullstandard + preview som viser overgangene |
