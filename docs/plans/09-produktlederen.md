# Pulseq — prosjektplan sett fra produkt og bruker

Planlegger 09, rolle: produktleder / brukeradvokat.
Denne planen tar ikke stilling til intern arkitektur utover der arkitekturen er synlig for brukeren. Den svarer på: hvem er dette for, hva vinner de, i hvilken rekkefølge bygger vi det, og hvorfor velger noen dette fremfor cron.

---

## 1. Produktpåstand

> **Pulseq er cron med hukommelse og forklaringsevne.**
> Ett binærfil, én fil med jobber, én database. Den husker hva som kjørte, den svarer på hvorfor noe ikke kjørte, og den kan trigge på hendelser og ikke bare klokkeslett.

Alt i planen under er underordnet den setningen. Hvis en funksjon ikke gjør setningen mer sann, hører den ikke hjemme i v1.0.

---

## 2. Målgruppe

### 2.1 Segmentet, definert i tall

Pulseq treffer den som har **for mange planlagte jobber til å ha oversikt, men for få til å rettferdiggjøre en plattform**. Operasjonelt:

| Dimensjon | Innenfor målgruppe | Utenfor |
| --- | --- | --- |
| Antall planlagte jobber | 5–100 | < 5 (cron holder), > 500 (Airflow-land) |
| Maskiner | 1–3 | Kubernetes-flåte |
| Dedikerte plattformfolk | 0 | ≥ 1 |
| Kjøretid per jobb | sekunder til timer | millisekunder (kø), dager (Temporal) |
| Toleranse for ny infrastruktur | «ett binærfil, ja» | «vi har allerede Postgres og Redis» |

Dette er et ekte, underbetjent segment. Verktøymarkedet er polarisert: cron og systemd i den ene enden, plattformer med scheduler + webserver + metadatabase + kø + workers i den andre. Mellomrommet har fått lite, og de som bor der løser det i dag med `flock`, `curl healthchecks.io/...` og en wiki-side.

### 2.2 Personaer

**P1 — Marit, systemansvarlig (primær, 60 % av fokus)**
Drifter 2 VM-er for en mellomstor virksomhet. 40 crontab-linjer fordelt på tre brukere og to maskiner. Skriver bash og litt Python. Har ingen kollega som deler ansvaret.
Smertepunkter, i rekkefølge etter hvor ofte de gjør vondt:
1. En jobb feilet i tre uker uten at noen merket det, fordi utdata gikk til `> /dev/null 2>&1`.
2. Hun vet ikke om jobben ikke kjørte, kjørte og feilet, eller fortsatt kjører.
3. Overlappende kjøringer: en treg backup startet på nytt før forrige var ferdig. Hun har lappet det med `flock`, men bare på tre av førti jobber.
4. `PATH` og miljøvariabler er annerledes under cron enn i skallet. Halve feilsøkingstiden går til dette.
5. Etter en omstart er hun ikke sikker på om noe ble hoppet over.
Marit vil ikke lære et rammeverk. Hun vil ha samme mentale modell som crontab, pluss svar.

**P2 — Andreas, dataplattform-team på tre (sekundær, 30 %)**
Bygger ETL for et lite analysemiljø. Har vurdert Airflow og lagt det fra seg: for mye å drifte for tolv pipelines. Kjører nå Makefiles i cron. Trenger avhengigheter mellom steg, retry per steg, backfill, og å kunne reagere på nye filer i objektlager.
Smertepunkter: ingen re-run av bare feilet steg; ingen oversikt over hva som kjørte i går natt; polling av S3 med hjemmesnekret watermark-fil som ryker.

**P3 — Ola, hjemmeserver (tertiær, 10 % — men den viktigste distribusjonskanalen)**
Kjører 15 jobber på en NUC: backup, mediesortering, sertifikatfornyelse, databasedump. Betaler ikke for noe. Skriver blogginnlegg og Reddit-poster. Krav: én binær, ingen Docker-tvang, lavt minneforbruk, ingen skytilkobling.
Ola konverterer ikke til inntekt, men Ola er grunnen til at Marit hører om Pulseq.

**Anti-personaer (vi sier nei til disse, høyt og eksplisitt)**
- Team som trenger multi-tenant RBAC og revisjonsspor for hundre brukere.
- Kubernetes-native team som allerede har Argo Workflows.
- Alle som beskriver behovet sitt med ordet «asset lineage across the warehouse».
- Sub-sekund-planlegging og jobbkøer med høy gjennomstrømning. Pulseq er en planlegger, ikke en kø.

### 2.3 Jobs-to-be-done

Formulert som brukeren ville sagt det, ikke som funksjoner:

- JTBD-1: *Når en nattlig jobb feiler, vil jeg vite det før brukerne gjør det, så jeg slipper å oppdage det gjennom en klage.*
- JTBD-2: *Når jeg ser på systemet om morgenen, vil jeg på ti sekunder vite om natten gikk bra, så jeg kan gå videre.*
- JTBD-3: *Når noe ikke kjørte, vil jeg ha et svar på hvorfor, ikke en tom logg.*
- JTBD-4: *Når jeg legger til en jobb, vil jeg vite at den ikke kan overlappe seg selv, så jeg slipper å huske `flock`.*
- JTBD-5: *Når en ny fil dukker opp, vil jeg behandle den én gang, så jeg slipper å polle blindt og deduplisere selv.*
- JTBD-6: *Når jeg har rettet feilen, vil jeg kjøre om bare det som feilet, så jeg slipper å vente på de tre stegene som allerede lyktes.*
- JTBD-7: *Når jeg endrer en tidsregel, vil jeg se de neste ti kjøringene før jeg lagrer, så jeg slipper å oppdage tidssonefeil i påsken.*

JTBD-1 til 4 er cron-flukten. JTBD-5 til 7 er det som gjør at de blir værende.

---

## 3. Posisjonering

### 3.1 Posisjoneringssetning

> For **systemansvarlige og små team som har vokst ut av crontab, men ikke inn i Airflow**,
> er **Pulseq** en **selvstendig planlegger og orchestrator**
> som gir **tidsstyring, hendelsesutløsere og sporbar historikk i én binærfil uten avhengigheter**.
> I motsetning til **cron**, som glemmer alt, og **Dagster/Airflow**, som krever en plattform,
> **husker Pulseq hver beslutning — inkludert beslutningen om ikke å kjøre — og kan forklare den.**

### 3.2 Konkurransekart

| Alternativ | Hva de gjør bra | Hvor de svikter målgruppen | Pulseqs svar |
| --- | --- | --- | --- |
| **cron** | Finnes overalt, null læring | Ingen historikk, ingen status, stille feil, ingen overlappsvern, miljøfeller | Beholder cron-uttrykk og cron-mentalmodell, legger til hukommelse |
| **systemd timers** | Robust, `Persistent=true` tar igjen tapte kjøringer, journald-logging | To unit-filer per jobb, ingen samlet oversikt over «alle mine jobber», ingen avhengigheter, ingen sensorer | Én deklarativ fil for alle jobber, ett sted å spørre |
| **Dagu** (nærmeste nabo: Go, ett binærfil, YAML-DAG, web-UI, file watching) | Reelt god i samme vektklasse, moden | Filbasert tilstand, ingen generell sensor-abstraksjon med cursor, svakere på «hvorfor kjørte ikke dette» | Sensorer med cursor som førsteklasses primitiv; spørrbar historikk i SQLite; `explain` |
| **Cronicle / Dkron / gocron** | Web-UI, flere maskiner | Ingen DAG-dybde eller hendelsesmodell; UI-først, CLI-etterpå | CLI-først, DAG-er, sensorer |
| **healthchecks.io / Cronitor** | Løser stille feil elegant med dead man's switch | Overvåker bare — du beholder cron og alle andre problemer | Pulseq er *kilden*, ikke observatøren. (Og skal integrere med dem, ikke kjempe.) |
| **Airflow / Dagster / Prefect** | Enorme økosystemer, riktig for store team | Krever plattform, database, workers, oppgradering, kompetanse. «DAG-purgatorium» | Samme kjernebegreper (schedule, sensor, run, retry), 1 % av driftskostnaden |
| **Windmill / Kestra / Temporal** | Kraftig, kø-basert, skalerer | Fortsatt en plattform å drifte; Temporal er en annen problemklasse (durable execution) | Ingen konkurranse — annen vektklasse |

**Ærlig vurdering:** Dagu er den reelle konkurrenten, ikke Airflow. «Lettvekt Go-binær med DAG-er og web-UI» er allerede opptatt. Vi kan derfor **ikke** posisjonere på «lettvekt» alene. Differensiatoren må være de to tingene ingen i vektklassen har gjort til hovedsak:

1. **Sensorer med cursor som førsteklasses primitiv.** Ikke bare filovervåking — en generell `check(context) -> [trigger] | skip_reason` med automatisk cursor-lagring og idempotent `run_key`. Dette er Dagsters modell, brakt ned i vektklassen der ingen har den.
2. **Negativ informasjon er like spørrbar som positiv.** «Dette kjørte ikke, og her er grunnen» er lagret data med tidsstempel, ikke fravær av data. `pulseq explain` er produktets signatur.

Alt annet — cron-uttrykk, retry, DAG, UI — er *paritetsfunksjoner*. De må finnes for at produktet skal være troverdig, men de selger det ikke.

### 3.3 Hva vi eksplisitt ikke er

Skrives i README, ikke bortgjemt: *ingen distribuert konsensus, ingen multi-tenancy, ingen asset-graf, ingen plugin-markedsplass, ingen sky-tjeneste, ingen sub-sekund-planlegging.* Å si nei tydelig er en funksjon for denne målgruppen — den er brent av verktøy som vokste.

### 3.4 Navnerisiko (flagges nå, ikke ved v1.0)

«Pulseq» er allerede navnet på et etablert open source-rammeverk for MR-pulssekvenser (pulseq.github.io, `pulseq-admin/pulseq`, PyPulseq), med akademisk publikasjonshistorikk. Konsekvenser: søk på «pulseq» leder ikke hit, «pulseq docs» er tvetydig, `pip install pypulseq` er noe helt annet, og en eventuell `pulseq.dev`/`pulseq.io` gir merkeforvirring i to fagmiljøer.
Anbefaling: behold `pulseq` som arbeidsnavn, men **ta navnebeslutningen før v0.3** (før det finnes brukere, blogginnlegg og lenker å bryte). Kriterier: ledig `.dev`-domene, ledig GitHub-org, entydig førstesidetreff, uttalbart på engelsk og norsk, ledig som binærnavn i Debian/Homebrew.

---

## 4. Brukerhistorier

Format: *Som «rolle» vil jeg «handling» slik at «utfall».* Akseptansekriterier er skrevet slik at de kan bli tester.

### 4.1 Nattlig rapportgenerering (P1, P2)

**US-01** — Som Marit vil jeg definere en jobb med cron-uttrykk og tidssone, slik at rapporten kjører 06:00 norsk tid også etter sommertidsomlegging.
- Tidssone settes per schedule, ikke globalt.
- `pulseq schedule preview rapport --next 10` viser ti neste kjøretidspunkt med UTC-forskyvning.
- En kjøring som treffer den doble timen ved sommertidsslutt kjører nøyaktig én gang, og det er dokumentert hva som skjer.

**US-02** — Som Marit vil jeg at en jobb aldri overlapper med seg selv, slik at jeg kan slette `flock`-innpakningene mine.
- `concurrency: 1` er standard per jobb.
- Når en kjøring hoppes over på grunn av dette, lagres den som en skip med grunn `already_running` og peker til kjøringen som blokkerte.
- Alternativ policy `queue` og `cancel_previous` finnes, men `skip` er standard.

**US-03** — Som Marit vil jeg få varsel når en jobb feiler, slik at jeg ikke oppdager det via en klage tre uker senere.
- Varsel ved: feilet kjøring, kjøring som overskrider `timeout`, jobb som ikke har lykkes innen `expected_within`.
- Kanal via kommandokrok (`on_failure: exec`) i v0.2, med ferdige oppskrifter for e-post, ntfy, Slack-webhook.
- Varselet inneholder run-ID, hvilket steg, siste 20 loggelinjer og kommandoen for å kjøre om.

**US-04** — Som Marit vil jeg se om natten gikk bra på under ti sekunder.
- `pulseq status` viser én linje per jobb: siste utfall, tidspunkt, varighet, neste kjøring.
- Utgangskode ≠ 0 hvis noe er feilet og ubekreftet, slik at kommandoen kan brukes i eget MOTD/overvåking.

### 4.2 Periodisk datavask (P1)

**US-05** — Som Marit vil jeg at et steg forsøkes på nytt automatisk ved forbigående feil, slik at en nettverksglipp ikke krever manuell innsats.
- Retry per steg med antall, backoff og valgfri jitter.
- Hvert forsøk er en egen loggstrøm med `attempt`-nummer; forsøk 1 forsvinner ikke når forsøk 2 lykkes.
- `pulseq runs show <id>` viser forsøkshistorikk.

**US-06** — Som Marit vil jeg definere miljøvariabler og arbeidskatalog eksplisitt per jobb, slik at «virker i skallet, feiler i cron» forsvinner.
- `env`, `workdir`, `shell` og `user` er felt i jobbdefinisjonen.
- `pulseq run --dry-run <jobb>` skriver ut nøyaktig kommandolinje og fullstendig miljø uten å kjøre.

### 4.3 Reaksjon på nye filer i objektlager / filsystem (P2, P3)

**US-07** — Som Andreas vil jeg starte én kjøring per ny fil, slik at jeg slipper å skrive dedupliseringslogikk.
- Sensor returnerer en liste med triggere; hver trigger har en `run_key`.
- Samme `run_key` starter aldri to kjøringer, uansett hvor mange ganger sensoren ser filen.
- Cursor (watermark) lagres automatisk etter vellykket evaluering; ved krasj midt i evalueringen er verste utfall at samme trigger sees igjen, og `run_key` fanger den.

**US-08** — Som Andreas vil jeg teste en sensor uten å starte noe, slik at jeg tør å endre den i produksjon.
- `pulseq sensor test <navn>` kjører evalueringen mot ekte tilstand, viser triggere som *ville* blitt startet og cursor som *ville* blitt skrevet, og skriver ingenting.
- `--cursor <verdi>` overstyrer startpunkt for testen.

**US-09** — Som Andreas vil jeg skrive en sensor uten å lære et SDK.
- En sensor i v0.1 er et vanlig program/skript som skriver JSON til stdout: enten `{"triggers":[{"run_key":"...","params":{...}}]}` eller `{"skip_reason":"..."}`.
- Cursor leveres inn som miljøvariabel og leses ut av samme JSON-objekt.
- Konsekvens: en sensor kan skrives i bash på fem linjer, og testes med `echo`.

### 4.4 Reaksjon på endring i en databasetabell (P2)

**US-10** — Som Andreas vil jeg trigge på nye rader siden forrige watermark, slik at jeg slipper en egen tilstandsfil.
- Innebygd sensortype `sql` med `dsn`, `query` og `cursor_column` dekker det vanligste tilfellet uten skript.
- Spørringen får `:cursor` bundet inn; høyeste sette verdi blir ny cursor.
- `pulseq sensor reset <navn> --cursor <verdi>` for kontrollert tilbakespoling.

### 4.5 Enkel batch-orchestration med avhengigheter (P2)

**US-11** — Som Andreas vil jeg definere steg med avhengigheter, slik at uavhengige steg kjører parallelt og resten venter.
- `needs: [steg-a, steg-b]` per steg; alt uten `needs` starter samtidig innenfor grensen `max_parallel`.
- Syklus i grafen er en valideringsfeil ved `pulseq validate`, ikke en kjøretidsfeil.

**US-12** — Som Andreas vil jeg kjøre om bare de feilede stegene etter at jeg har rettet feilen.
- `pulseq replay <run-id> --failed` gjenbruker vellykkede steg og starter fra det første feilede.
- `pulseq replay <run-id> --from <steg>` for manuell overstyring.
- Replay er en ny run med peker til opphavsrun, ikke en overskriving av historikken.

**US-13** — Som Andreas vil jeg sende en filreferanse fra ett steg til neste, slik at jeg slipper å avtale filstier ut av båndet.
- Et steg kan registrere en artefaktreferanse (sti/URI + metadata); etterfølgende steg leser den via miljøvariabel eller `pulseq artifact get`.
- Pulseq lagrer *referansen*, aldri innholdet.

### 4.6 Migrering fra eksisterende cron (P1, P3) — se kapittel 5

**US-14** — Som Marit vil jeg importere crontaben min uten å skrive noe på nytt.
**US-15** — Som Marit vil jeg kjøre Pulseq parallelt med cron i en uke før jeg slår av cron.

### 4.7 Tverrgående: forklaring og innsyn (alle)

**US-16** — Som bruker vil jeg spørre «hvorfor kjørte ikke dette», og få et svar.
- `pulseq explain <jobb>` svarer med den faktiske årsaken fra lagret tilstand, ikke gjetning. Dekkede årsaker minst: `paused`, `no_tick_due`, `already_running`, `concurrency_limit`, `sensor_skipped: <grunn>`, `run_key_seen`, `dependency_failed`, `scheduler_down_between <a> og <b>`, `catchup_disabled`.
- Kommandoen svarer også når alt er i orden: «kjørte 06:00, lyktes på 4m12s, neste 06:00 i morgen».

**US-17** — Som bruker vil jeg vite hva som skjedde mens tjenesten var nede.
- Ved oppstart skrives en rekonsilieringspost: perioden uten planlegger, hvilke ticks som falt bort, og hva som ble tatt igjen versus forkastet i henhold til `catchup`-policy.
- Kjøringer som var `running` da prosessen døde markeres `orphaned` med tidspunkt, ikke stående som evig aktive.

**US-18** — Som bruker vil jeg finne logglinjen for et spesifikt steg i et spesifikt forsøk uten å grep-e i en samlelogg.
- Logg er adresserbar: `pulseq logs <run-id> [--step <navn>] [--attempt <n>]`.
- Strukturert JSON på disk, menneskelesbar i terminalen som standard.

---

## 5. Migreringsfortellingen fra crontab

Dette er det viktigste enkeltkapittelet i planen. Målgruppens største kostnad ved å bytte er ikke å lære Pulseq — det er å flytte 40 linjer de ikke tør røre. Presedens finnes (SteadyCron `import crontab`, dagu-cron for DAGU, konverteringsverktøy i JAMS og JobScheduler): import er forventet i denne kategorien, ikke en bonus.

### 5.1 `pulseq import crontab`

```
pulseq import crontab                 # leser crontab for gjeldende bruker
pulseq import crontab --file /etc/crontab --user root
pulseq import crontab --all-users     # krever root, leser /var/spool/cron/crontabs/*
pulseq import crontab -o pulseq.yaml  # standard: skriv til stdout
```

Ikke-destruktiv. Rører aldri den eksisterende crontaben. Skriver lesbar YAML som mennesket skal kunne redigere etterpå — ikke maskingenerert grøt.

**Oversettelser som gjøres automatisk:**

| Cron-mønster | Pulseq-oversettelse |
| --- | --- |
| `0 6 * * *` | `schedule: {cron: "0 6 * * *", timezone: <fra CRON_TZ eller systemets>}` |
| `@daily`, `@reboot` | `@daily` → cron; `@reboot` → `on_start: true` med advarsel |
| `MAILTO=drift@…` | `on_failure` med e-postoppskrift, kommentert inn |
| `PATH=…`, `SHELL=…` | `env:` og `shell:` på jobbnivå |
| `flock -n /var/lock/x.lock cmd` | innpakningen fjernes, erstattes av `concurrency: 1` |
| `> /dev/null 2>&1` | fjernes + advarsel: «loggen din ble kastet; Pulseq beholder den nå» |
| `>> /var/log/x.log 2>&1` | fjernes + notis om at loggen nå finnes i `pulseq logs` |
| `cd /srv/app && ./run.sh` | `workdir: /srv/app`, `cmd: ./run.sh` |
| `2>&1 \| logger -t x` | fjernes + notis |
| `curl -fsS https://hc-ping.com/…` | beholdes uendret + notis om at Pulseq nå kan gjøre det samme |
| `%`-tegn i kommandoen | eskapert korrekt (cron behandler `%` som linjeskift — vanlig felle) |
| Ikke-tolkbar linje | beholdes ordrett som `cmd` med `# TODO: gjennomgå` |

**Jobbnavn** utledes fra kommandoen (skriptets basenavn), gjøres unike, og kan overstyres. Navn er en del av kontrakten — de vises i status, varsler og logg — så importen skal foreslå gode navn og be brukeren se over.

**Importrapporten** er en del av produktet, ikke en logglinje. Den avsluttes med:

```
42 linjer lest → 38 jobber, 4 til gjennomgang
  ⚠ 12 jobber kastet utdata til /dev/null (loggen beholdes fra nå)
  ⚠  3 jobber brukte flock → erstattet med concurrency: 1
  ⚠  1 jobb bruker @reboot → se docs/reboot.md
  ⚠  4 linjer kunne ikke tolkes, se kommentarer i pulseq.yaml

Neste steg:  pulseq validate pulseq.yaml
             pulseq import crontab --shadow      # kjør parallelt uten å utføre
```

### 5.2 Skyggemodus — det som gjør byttet trygt

`--shadow` er migreringens tillitsmekanisme og bør prioriteres høyere enn den ser ut til å fortjene.

I skyggemodus kjører Pulseq hele planleggeren, registrerer hver tick i historikken, men **utfører ingenting**. Etter en uke:

```
pulseq shadow report
  38 jobber, 1 412 planlagte ticks
  ✓ 36 jobber: Pulseq ville ha kjørt nøyaktig når cron kjørte
  ⚠ backup-db: 3 avvik — cron kjørte 03:00 CET, Pulseq 03:00 UTC (tidssone ikke satt)
  ⚠ sync-files: 7 ticks Pulseq ville hoppet over (overlapp) som cron faktisk startet
```

Den siste linjen er salgsargumentet: brukeren oppdager at hun har hatt overlappende kjøringer i månedsvis. Dette er «wow»-øyeblikk nummer tre (se 6.2).

### 5.3 Overgangen

```
pulseq cutover            # kommenterer ut importerte linjer i crontab med referanse til
                          # jobbnavn, tar sikkerhetskopi til ~/.pulseq/crontab.backup.<dato>
pulseq cutover --rollback # legger dem tilbake
```

Prinsipp: brukeren skal aldri stå i en tilstand der hun ikke kan gå tilbake på ett minutt. Vi går aldri fra `import` rett til `cutover` uten at brukeren ber om det.

### 5.4 Import fra systemd timers (v0.5, ikke tidligere)

`pulseq import systemd` leser `*.timer` + tilhørende `*.service` og oversetter `OnCalendar`, `Persistent`, `RandomizedDelaySec`, `ExecStart`, `WorkingDirectory`, `Environment`. Lavere prioritet: systemd-brukere har færre smertepunkter og er vanskeligere å overtale.

---

## 6. Onboarding og «wow»

### 6.1 Aksepterte tidsbudsjetter

| Milepæl | Budsjett |
| --- | --- |
| Fra landingsside til kjørende binær | 60 sekunder |
| Fra binær til første egne jobb kjørt | 3 minutter |
| Fra binær til hele crontaben importert | 5 minutter |
| Fra installasjon til første «å, det visste jeg ikke» | 10 minutter |

Overskrides det første budsjettet, er alt annet irrelevant. Derfor: statisk binær, ingen kjøretidsavhengigheter, ingen Docker-krav, ingen konfigurasjon nødvendig for første kjøring, database opprettes automatisk i `~/.pulseq/`.

### 6.2 De tre «wow»-øyeblikkene

Skal designes bevisst og verifiseres i brukertest, ikke oppstå tilfeldig.

1. **`pulseq import crontab`** (minutt 2). «Hele crontaben min ble til noe lesbart uten at jeg skrev en linje.» Reduserer byttekostnaden fra timer til null.
2. **`pulseq explain <jobb>`** (minutt 5). «Den svarte meg.» Første gang et planleggingsverktøy forklarer et fravær av handling. Dette er den setningen folk siterer i blogginnlegg.
3. **Skyggerapporten** (dag 7, eller i importrapporten hvis `flock` mangler). «Jeg har hatt overlappende kjøringer i et halvt år.» Verktøyet finner et problem brukeren ikke visste at hun hadde. Sterkeste retensjonsmekanisme vi har.

### 6.3 Førstegangsopplevelsen, ordrett

```
$ curl -sSfL https://<domene>/install.sh | sh          # eller: brew / apt / go install / last ned binær
$ pulseq init
  Opprettet ~/.pulseq/pulseq.db og ./pulseq.yaml med én eksempeljobb.
  Prøv:  pulseq run hello

$ pulseq run hello
  ✓ hello  0.02s  «hei fra Pulseq»

$ pulseq import crontab
  ...importrapport...

$ pulseq serve --systemd > /etc/systemd/system/pulseq.service
```

`pulseq init` skal gi en fil som *allerede fungerer*, ikke et skjelett med plassholdere. Tom skjerm er den vanligste onboarding-feilen i denne verktøykategorien.

### 6.4 Feilmeldinger er onboarding

Krav som gjelder hele produktet: hver feilmelding skal ha (a) hva som gikk galt, (b) hvor i konfigurasjonsfilen, med linjenummer, (c) hva brukeren kan gjøre nå. En feilmelding uten (c) er en feil i seg selv, og behandles som en bug.

---

## 7. MoSCoW-prioritering

Prioriteringen går **tvers gjennom** MVP-listen i prosjektbeskrivelsen, ikke langs den. Begrunnelsen står ved hvert punkt.

### Must have (uten dette er det ikke et produkt)

| Funksjon | Begrunnelse |
| --- | --- |
| Cron- og intervall-schedules med tidssone per schedule | Paritet med cron. Tidssone per schedule fordi global tidssone er den vanligste kilden til «kjørte på feil tidspunkt». |
| Kjøring av kommandoer med eksplisitt `env`, `workdir`, `shell` | Fjerner cron-fellen nr. 1. Billig å bygge, høy smertelindring. |
| Kjørehistorikk i SQLite med utfall, varighet, utgangskode | Selve hukommelsen. Uten dette er vi cron med ekstra trinn. |
| Per-run og per-step logg, adresserbar og strukturert | Uten adresserbar logg er historikken uten verdi. |
| `concurrency: 1` som **standard** | JTBD-4. Standardverdien er produktbeslutningen — cron har motsatt standard, og det koster brukerne penger. |
| Sensorer med cursor, `run_key` og `skip_reason` (skriptbasert kontrakt) | Differensiatoren. Kan ikke utsettes til fase 2 uten å bli enda en lettvekts-cron. |
| Persistert skip-grunn for hver ikke-kjøring | Forutsetningen for `explain`. Må ligge i datamodellen fra dag én; kan ikke ettermonteres billig. |
| `pulseq explain` | Signaturfunksjonen. |
| `pulseq schedule preview --next N` | Forebygger tidssonefeil før de skjer. Nesten gratis når schedule-motoren finnes. |
| Retry per steg med backoff | Forventet paritet med enhver moderne planlegger. |
| `pulseq import crontab` | Fjerner byttekostnaden. Uten den blir Pulseq et verktøy folk beundrer og ikke tar i bruk. |
| CLI: `run`, `status`, `list`, `logs`, `pause`/`resume`, `validate` | Grunnflaten. |
| Rekonsiliering ved oppstart (foreldreløse kjøringer, tapt planleggerperiode) | Førsteinntrykket etter første omstart avgjør tilliten. |
| Én binær, ingen avhengigheter, ingen obligatorisk konfigurasjon | Selve inngangsbilletten til segmentet. |

### Should have (v0.2–v0.4; produktet er svakt uten, men brukbart)

| Funksjon | Begrunnelse |
| --- | --- |
| Varsling ved feil (`on_failure`-krok) | JTBD-1. Ikke i v0.1 kun fordi importen og `explain` gir høyere verdi per byggetime, og fordi brukeren i mellomtiden kan bruke sin eksisterende varsling. |
| DAG-avhengigheter og parallelle steg | Cron-flyktningen har 40 *uavhengige* enlinjers jobber, ikke DAG-er. Verdien er høy for P2, moderat for P1. Derfor v0.3, ikke v0.1. |
| `replay --failed` / `--from <steg>` | Meningsløs før DAG-er finnes; sterk umiddelbart etterpå. |
| Missed-run catch-up med policy (`all`, `latest`, `none`) | Speiler `Persistent=true` i systemd. Trengs først når noen faktisk har hatt nedetid. |
| `--dry-run` | Reduserer frykt for å endre ting i produksjon. |
| Web-UI, kun lesing | Gjør produktet delbart med kolleger og gjør skjermbilder mulige — det er slik verktøy sprer seg. Men CLI-en må være komplett først, ellers blir UI-et sannheten og CLI-en et vedheng. |
| Innebygde sensortyper: `file`, `http`, `sql` | Får de tre vanligste tilfellene til å fungere uten at brukeren skriver et skript. |
| Backfill for tidsvindu | P2-behov, sjelden hos P1. |
| Heartbeat/dead man's switch ut mot healthchecks.io o.l. | «Hvem passer på passeren» — det åpenbare oppfølgingsspørsmålet. Vi integrerer, konkurrerer ikke. |

### Could have (v0.5+ hvis etterspurt)

Dynamisk fan-out; artefakt-lineage på tvers av kjøringer; skriving fra web-UI; `import systemd`; Postgres-backend for flere noder; Prometheus-endepunkt; hemmelighetshåndtering utover miljøvariabler; kjøring i container per steg; SSH-kjøring på ekstern maskin; kalenderregler (helligdager, «siste virkedag i måneden»).

### Won't have (i v1.0, sagt høyt)

Distribuert konsensus / HA-cluster; multi-tenant RBAC; asset-graf og datakatalog; plugin-markedsplass; SaaS-tilbud; sub-sekund-planlegging; innebygd Python-SDK (kontrakten er prosesser og JSON — det er poenget); innebygd datatransformasjon.

---

## 8. Release-plan

Hver utgivelse har én setning som beskriver brukerverdien, og et exit-kriterium som ikke er «koden er ferdig».

### v0.1 — «Cron som husker» (kjørbar demo)

**Brukerverdi:** Jeg flytter crontaben min inn og får for første gang historikk og et svar på hvorfor noe ikke kjørte.
**Innhold:** cron/intervall-schedules med tidssone; kommandokjøring med eksplisitt miljø; SQLite-historikk; per-run og per-step logg; `concurrency: 1` som standard; skriptbaserte sensorer med cursor, `run_key` og `skip_reason`; retry per steg; `import crontab`; CLI (`init`, `run`, `status`, `list`, `logs`, `explain`, `schedule preview`, `sensor test`, `pause`, `resume`, `validate`); rekonsiliering ved oppstart; `serve --systemd`.
**Ikke med:** DAG, UI, varsling, backfill, replay.
**Hvem tar det i bruk:** P3 (Ola) og modige P1-er.
**Exit-kriterium:** Tre eksterne personer har importert sin egen crontab, kjørt Pulseq i syv døgn uten manuell inngripen, og minst én av dem har oppdaget et reelt problem via `explain` eller skyggerapporten.
**Demosetning:** «Se her — jeg spør hvorfor backupen ikke gikk i natt, og den svarer.»

### v0.2 — «Den varsler meg» (den første som er trygg å drifte)

**Brukerverdi:** Jeg får beskjed når noe feiler, og jeg trenger ikke se etter selv.
**Innhold:** `on_failure`/`on_success`-kroker med ferdige oppskrifter (e-post, ntfy, Slack, generisk webhook); `expected_within` med varsel ved uteblitt suksess; utgående heartbeat til healthchecks.io/Cronitor; timeout per steg og per jobb; `--dry-run`; missed-run catch-up med policy; skyggemodus og `shadow report`; `cutover` med `--rollback`; forbedret importrapport.
**Hvem:** P1 (Marit) i produksjon.
**Exit-kriterium:** En bruker har slått av cron helt og ikke slått den på igjen etter 30 døgn.
**Demosetning:** «Cron er avinstallert på den maskinen.»

### v0.3 — «Steg og avhengigheter» (åpner for P2)

**Brukerverdi:** Jeg kan bygge en liten pipeline og kjøre om bare det som feilet.
**Innhold:** flerstegsjobber med `needs`; parallelle steg med `max_parallel`; retry og timeout per steg; `replay --failed` / `--from`; syklusdeteksjon i `validate`; artefaktreferanser mellom steg; `pulseq runs show` med steggraf i terminalen.
**Hvem:** P2 (Andreas).
**Exit-kriterium:** Ett eksternt team har flyttet en flerstegspipeline fra Makefile+cron til Pulseq.
**Demosetning:** «Steg fire feilet, jeg rettet spørringen, kjørte om bare steg fire.»

### v0.4 — «Noe å se på» (deling og spredning)

**Brukerverdi:** Jeg kan vise kollegene mine status uten å gi dem SSH-tilgang.
**Innhold:** lesende web-UI på én port, servert fra samme binær: jobbliste, kjørehistorikk, steggraf, loggvisning, tidslinje for schedules og sensorer med skip-grunner synlige som førsteklasses hendelser; `explain` gjengitt i UI; grunnleggende autentisering; `pulseq serve --listen`.
**Hvem:** P1 og P2 med kolleger. Dette er også utgivelsen som gir skjermbilder til blogginnlegg og HN-innlegg.
**Exit-kriterium:** Skjermbildet av skip-tidslinjen forklarer seg selv for en person som ikke har lest dokumentasjonen.
**Demosetning:** Ett skjermbilde der man ser at en jobb *ikke* kjørte, og hvorfor.

### v0.5 — «Sensorer uten skript» (differensiatoren i bredden)

**Brukerverdi:** Jeg reagerer på nye filer, HTTP-tilstand og databaseendringer uten å skrive kode.
**Innhold:** innebygde sensortyper `file` (glob + mtime/checksum-cursor), `http` (poll + ETag/JSONPath-cursor), `sql` (query + cursor-kolonne); multi-trigger fan-out fra ett sensor-tick; `sensor reset --cursor`; backfill for tidsvindu; `import systemd`.
**Hvem:** P2 og P3.
**Exit-kriterium:** «Reager på nye filer i S3» er løst i under ti linjer YAML, uten eksternt skript.
**Demosetning:** Ti linjer YAML som erstatter et hjemmesnekret pollende Python-skript med tilstandsfil.

### v0.6 — «Herdet» (stabilitetsutgivelsen før 1.0)

**Brukerverdi:** Jeg tør la den ligge urørt i et år.
**Innhold:** oppbevaringspolicy og opprydding av logg/historikk; `pulseq backup` og `restore`; databasemigrasjoner med testet oppgraderingssti fra hver tidligere versjon; belastningstest med 500 jobber og 10 000 kjøringer; dokumentert oppførsel ved full disk, klokkejustering og sommertid; Prometheus-endepunkt; pakker for Debian/Ubuntu, Homebrew og Arch.
**Exit-kriterium:** Oppgradering fra v0.1-database til v0.6 fungerer uten manuelle steg, og verifiseres i CI.

### v1.0 — «Stabil kontrakt»

**Brukerverdi:** Jeg kan bygge på dette uten å frykte at neste versjon flytter på ting.
**Innhold:** ingen nye funksjoner. Frosset konfigurasjonsformat, frosset CLI-flateoverflate, frosset JSON-utdata, semantisk versjonering, dokumentert utfasingspolicy, fullført dokumentasjon etter Diátaxis, migreringsgaranti fra alle 0.x-versjoner.
**Exit-kriterium:** Konfigurasjonsformatet har ikke hatt brytende endringer på tre måneder, og de siste ti innkomne feilrapportene handler om funksjonsønsker, ikke om at ting er ødelagt.

### Rekkefølgens logikk

- **Import før alt annet** fordi byttekostnaden, ikke funksjonsmangel, er den bindende begrensningen.
- **`explain` i v0.1** fordi den krever at skip-grunner ligger i datamodellen fra start; ettermontering er dyr.
- **Sensorer i v0.1** fordi de er differensiatoren; utsettes de, er v0.1 bare enda en cron-erstatter og får ikke oppmerksomhet.
- **DAG i v0.3, ikke v0.1** fordi den primære målgruppen ikke har DAG-er, og fordi DAG-motor er den største enkeltkostnaden i prosjektet.
- **UI i v0.4, ikke tidligere** fordi et UI som kommer før CLI-en er komplett, gjør UI-et til sannheten og CLI-en til et vedheng — det motsatte av posisjoneringen.
- **v0.6 før v1.0** fordi målgruppen straffer ustabilitet hardere enn den belønner funksjoner.

---

## 9. Dokumentasjonsstrategi

### 9.1 Prinsipp

Dokumentasjonen er ikke etterarbeid. For et infrastrukturverktøy uten selger er dokumentasjonen produktets eneste selger. Struktur følger **Diátaxis**: opplæring, oppskrifter, referanse og forklaring holdes fysisk adskilt, fordi de fire besvarer forskjellige behov og blander man dem, feiler alle fire.

### 9.2 Sidekart

**Opplæring (tutorials) — den som ikke kan noe ennå**
1. `Fra crontab til Pulseq på fem minutter` — hovedinngangen, følger nøyaktig 6.3.
2. `Din første sensor` — reager på nye filer i en katalog, i bash.
3. `Din første flerstegsjobb` — hent, transformer, publiser, med et bevisst innlagt feilende steg som leseren retter og kjører om.

**Oppskrifter (how-to) — den som har en oppgave**
Én side per konkret oppgave, alle testbare: importer crontab; kjør som systemd-tjeneste; varsle til Slack/e-post/ntfy; sett opp overlappsvern; ta igjen tapte kjøringer etter nedetid; feilsøk «hvorfor kjørte ikke jobben min»; roter og rydd logger; sikkerhetskopier databasen; poll et HTTP-API med cursor; trigge på nye rader i Postgres; kjør bak omvendt proxy; oppgrader trygt.

**Referanse — den som slår opp**
Konfigurasjonsskjema felt for felt med standardverdier og eksempler; komplett CLI-referanse (autogenerert fra kildekoden, slik at den ikke kan bli utdatert); sensorkontrakten (JSON inn/ut, miljøvariabler, utgangskoder); tilstandsmaskinen med alle tilstander og overganger tegnet; alle skip-grunner med presis betydning; utgangskoder; databaseskjema.

**Forklaring — den som lurer på hvorfor**
- `Hvorfor Pulseq finnes` — problemet, segmentet, hva vi bevisst ikke gjør.
- `Schedules og sensorer: to typer triggere` — kjernedelingen, lånt fra Dagster.
- `Garantier: at-least-once, run_key og hva idempotens betyr for deg` — den viktigste siden i hele dokumentasjonen; forklarer i klartekst at en jobb kan bli startet to ganger ved uheldig timing, og hva brukeren skal gjøre med det.
- `Hvorfor SQLite, og hva det betyr for deg` — se 10.3.
- `Cursor kontra run_key: når du trenger hvilken`.
- `Sammenligning: cron, systemd timers, Dagu, Airflow` — ærlig, med reelle tilfeller der de andre er riktig valg. Ærlig sammenligning er det billigste tillitsbyggende tiltaket vi har, og målgruppen gjennomskuer det motsatte umiddelbart.

### 9.3 Regler

- **README er en kontrakt.** Maks én skjerm før første kommando. Innhold i rekkefølge: én setning om hva det er; asciinema-opptak av import + explain; installasjon; 60-sekunders eksempel; «hva dette ikke er»; lenke til dokumentasjonen.
- **Dokumentasjon i produktet.** `pulseq explain`, `pulseq schedule preview`, `pulseq sensor test` og `--dry-run` er dokumentasjon som ikke kan bli utdatert. Prioriter dem over prosatekst der de overlapper.
- **Alle kodeeksempler kjøres i CI.** Et eksempel som ikke fungerer er verre enn ingen dokumentasjon.
- **CLI-referanse og konfigurasjonsskjema genereres fra kildekoden.** Håndskrevet referanse råtner.
- **Feilsøkingssiden er en førsteklasses side, ikke et vedlegg.** Den skal treffe søk som «pulseq job did not run».
- **Ett språk: engelsk** i dokumentasjon og produkt. Norsk kun i denne interne planen.
- **Endringslogg skrevet for mennesker**, med eksplisitt «hva du må gjøre» ved brytende endringer.

### 9.4 Distribusjon

Kanalene der målgruppen faktisk befinner seg: r/selfhosted og r/sysadmin (P1, P3); Hacker News «Show HN» ved v0.4 når UI-et gir skjermbilder — ikke før, første inntrykk kommer bare én gang; awesome-selfhosted; en teknisk gjennomgang av `explain`-designet som blogginnlegg (mekanismer sprer seg bedre enn produktannonseringer); Debian/Homebrew/Arch-pakker i v0.6 fordi «hvordan installerer jeg det» er det siste friksjonspunktet.

---

## 10. Suksessmetrikker

### 10.1 Aktiveringstrakt

Måles i brukertest og fra frivillige tilbakemeldinger — ikke telemetri. Målgruppen er telemetri-fiendtlig, og innsamling uten samtykke ville ødelagt tilliten som er hele posisjoneringen.

| Steg | Mål ved v0.4 |
| --- | --- |
| Installert → `pulseq run` lykkes | > 90 % |
| Kjørt → `import crontab` utført | > 50 % |
| Importert → tjenesten kjører som systemd-enhet | > 60 % |
| Kjører etter 7 døgn | > 70 % |
| Cron slått av etter 30 døgn | > 30 % |

Siste linje er den egentlige nordstjernen: **antall crontabs faktisk avviklet.** Alt annet er forløpsindikatorer.

### 10.2 Kvalitetsmål (harde, målbare i CI og i praksis)

- Kaldstart til `pulseq status` svarer: < 100 ms med 100 jobber og 100 000 historikkrader.
- Hvilende minneforbruk: < 40 MB med 100 jobber.
- Binærstørrelse: < 30 MB.
- Andel skip-hendelser der `explain` gir en presis, ikke-generisk årsak: 100 %. En `skip_reason: "unknown"` behandles som en bug med høy prioritet.
- Tapte kjøringer ved rent restart av tjenesten: 0.
- Feilmeldinger som mangler «hva gjør jeg nå»: 0.

### 10.3 Kvalitative signaler å lytte etter

Positive: noen skriver «pulseq explain» i et forum uten å forklare hva det er; noen sammenligner Pulseq med Dagu i stedet for med cron (vi har da nådd riktig vektklasse); en feilrapport handler om et grensetilfelle i sommertid (ekte produksjonsbruk).
Negative: gjentatte spørsmål om HA og clustering (feil målgruppe tiltrekkes, eller posisjoneringen er utydelig); folk bruker Pulseq som jobbkø (feil verktøy, avvis vennlig og tydelig); «jeg skjønner ikke forskjellen på cursor og run_key» (dokumentasjonssvikt på det mest kritiske punktet).

---

## 11. Risikoer

Sortert etter forventet skade, ikke sannsynlighet.

| # | Risiko | Hvorfor den gjør vondt | Tiltak |
| --- | --- | --- | --- |
| R1 | **Dagu (og nære slektninger) eier allerede «lettvekt Go-orchestrator»** | Vi bygger noe som allerede finnes og taper på modenhet | Ikke posisjoner på lettvekt. Sensorer med cursor og `explain` må være ferdige og gode i v0.1, ikke lovnader i veikartet. Ærlig sammenligningsside. |
| R2 | **Funksjonsspredning mot Airflow** | Produktet mister det eneste som gjør det attraktivt, og driftskostnaden nærmer seg alternativene | «Won't have»-listen er offentlig og bindende. Hver funksjonsforespørsel prøves mot produktpåstanden i kapittel 1. |
| R3 | **Sensorkontrakten blir for vanskelig** | Differensiatoren blir ubrukt, og vi står igjen som en cron-erstatter | v0.1-kontrakten er prosess + JSON på stdout, uten SDK. En sensor skal kunne skrives i fem linjer bash. Testes med ekte brukere i v0.1, ikke etter. |
| R4 | **SQLite-enkeltskriver blir synlig for brukeren** | Fryser eller «database is locked» ved den første travle natten ødelegger tilliten permanent, og vi ville sagt «det er derfor du valgte oss» | Brukervendt krav, ikke bare teknisk: `pulseq status` svarer alltid under 100 ms samtidig som 20 jobber kjører. Løses arkitektonisk (WAL, én skrivekø, lesere blokkeres aldri) — men her stilles kun kravet. Dokumenter grensene ærlig og oppgi et tall for hvor mange samtidige jobber som er testet. |
| R5 | **Navnekollisjon med MR-rammeverket Pulseq** | Ingen finner produktet; forvirring i to fagmiljøer; smertefullt å rette etter at det finnes lenker | Navnebeslutning tas før v0.3, mens det ennå ikke finnes brukere. Kriterier i 3.4. |
| R6 | **Migreringen fra crontab er ufullstendig** | Første kommando en ny bruker kjører gir 12 «kunne ikke tolkes»; produktet er dødt i det øyeblikket | Import-parseren testes mot et korpus av ekte crontabs (offentlige dotfiles-repoer, konfigurasjonsstyringspakker, egne innsamlede). Mål: > 90 % tolket i første forsøk. Ikke-tolkede linjer beholdes ordrett og fungerer likevel. |
| R7 | **Tvetydig identitet: er dette en cron-erstatter eller en dataorchestrator?** | To målgrupper med motstridende krav gir et produkt som ikke overbeviser noen | P1 er primær til og med v0.2. P2 hentes inn fra v0.3. Rekkefølgen er en beslutning, ikke en tilfeldighet. |
| R8 | **At-least-once misforstås som exactly-once** | Brukeren bygger en betalingsjobb på feil antakelse, får dobbel kjøring, mister tilliten | Garantisiden i dokumentasjonen sier det i klartekst. `pulseq validate` advarer mot jobber uten `run_key` der en sensor kan gi duplikater. Aldri markedsfør «exactly once». |
| R9 | **Vedlikeholderkapasitet** | Åtte utgivelser er mye; halvferdige verktøy dør stille og målgruppen kjenner igjen mønsteret | Omfanget per utgivelse er bevisst lite. v0.1 er liten nok til å bli ferdig. Utgivelseskadens og siste utgivelsesdato er synlig i README. |
| R10 | **UI-et blir sannheten** | CLI-first-posisjoneringen kollapser; vi blir Cronicle med færre funksjoner | UI-et er lesende til og med v1.0. Alt UI-et viser skal finnes i CLI-en først. |
| R11 | **Tidssoner og sommertid** | Feil kjøretidspunkt to ganger i året ødelegger tilliten mer enn en krasj | Tidssone per schedule fra v0.1. `schedule preview` viser omleggingene eksplisitt. Egen test-suite for omleggingsdøgn. Dokumentert oppførsel for både den doble og den manglende timen. |
| R12 | **Ingen bruker `explain`** | Signaturfunksjonen forblir usett | `pulseq status` viser en hint-linje ved uventet tilstand: «kjørte ikke i natt — kjør `pulseq explain nightly-report`». `explain` er med i README-opptaket og i tutorial 1. |

---

## 12. Åpne beslutninger som må tas før v0.1 starter

1. **Konfigurasjonsformat:** YAML, TOML eller HCL. Anbefaling: YAML, fordi målgruppen kjenner det fra Docker Compose og GitHub Actions, og fordi importen skal produsere noe de kan lese.
2. **Én fil eller en katalog med filer.** Anbefaling: begge — `pulseq.yaml` for de små, `pulseq.d/*.yaml` for de som vokser. Importen bør kunne dele på bruker.
3. **Er konfigurasjonsfilen eller databasen sannheten?** Anbefaling: filen er sannheten for definisjoner, databasen for historikk og tilstand (pause, cursor). Dette gjør versjonskontroll mulig, som er en stille, men sterk fordel mot cron.
4. **Navnebeslutning** (3.4) — senest før v0.3.
5. **Lisens.** Anbefaling: en permissiv lisens. Målgruppen er mistenksom mot lisenser som kan bli byttet ut senere, og vi har uansett ingen SaaS-plan å beskytte.
6. **Hvem er `pulseq run` sin standardbruker?** Kjøring som annen bruker enn tjenesten er et sikkerhetsspørsmål som må avklares før import fra `/etc/crontab` med flere brukere.
