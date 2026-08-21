# Paceq: Lettvekt orchestrator med schedules, sensors og DAG-utføring

## Tekniske rammer

- Språk: **Go (golang)**.
- Database: **SQLite** er sannsynligvis best. NB: SQLite tillater kun **én skriver om gangen** — må løses enten med flere SQLite-filer eller veldig forsiktig bruk (f.eks. én dedikert skriveforbindelse/kø, WAL-modus).

## Navn og formål

Dette er en liten, selvstendig orchestrator for Linux og servermiljøer som kombinerer cron-lignende tidsstyring med mer fleksible hendelsesutløsere. Målet er å gi det viktigste fra Dagster: planlagte kjøringer, event-basert trigging, tydelig historikk, og god observabilitet, men uten tung plattform, stor SDK-flate eller komplisert asset-model. Dagster beskriver selv schedules som tidsbaserte triggere og sensors som event-baserte triggere, og denne applikasjonen tar nettopp den delingen som kjerneidé.

## Kjerneidé

Systemet modellerer automatisering som:

1. Jobber.
2. Steg inni jobber.
3. Triggere som starter jobber.
4. En liten state machine for planlagt, aktiv, ferdig, feilet, utsatt og kansellert.
5. En worker-prosess som utfører jobber.

Det grunnleggende prinsippet er å holde "beslutningen om å starte arbeid" separat fra "utføringen av arbeid". Planleggeren bestemmer når noe skal skje, mens workerne gjør selve arbeidet. Dette gjør systemet lett å forstå, lett å debugge, og lett å kjøre på en enkelt maskin eller i et lite cluster.

## Primære objekter

- **Job**: en deklarativ beskrivelse av arbeid som skal gjøres.
- **Step**: et navngitt delsteg med egen logg, egen status og eget retry-regime.
- **Run**: én konkret kjøring av en job.
- **Schedule**: en tidsregel, typisk cron eller interval.
- **Sensor**: en regel som inspiserer ekstern tilstand og returnerer triggere.
- **Cursor**: lagret posisjon eller watermark for en sensor.
- **Trigger**: et resultat av schedule eller sensor som starter en run.
- **Artifact reference**: pekere til filer, objekter eller datasett som en run produserer.

## Schedules

Schedules skal være enkle, men mer fleksible enn cron alene. De bør støtte cron-uttrykk, enkle intervaller, kalenderregler og "next run" preview. Dagster sine schedules viser nettopp at schedule-laget bør kunne beskrive faste tider, generere run requests, og gi operatøren innsyn i neste tick og historikk. Systemet bør også støtte:

- Missed-run catch-up.
- Pause/resume.
- Timezone per schedule.
- Concurrency limits.
- "Skip with reason", så en schedule kan forklare hvorfor den ikke startet noe.

## Sensors

Sensors bør være et førstegangsbegrep, ikke et tillegg. En sensor er en liten evaluator som kjører periodisk, ser på ekstern tilstand, og enten returnerer triggere eller en forklarende skip-grunn. Det matcher Dagster sin modell, der sensors sjekker hendelser regelmessig og kan enten starte runs eller forklare hvorfor ingenting skjedde. En sensor kan være:

- Poll-baserte: sjekk API, database, objektlager, filsystem, webhook-kø.
- Cursor-baserte: følg en watermark, offset eller checksum.
- State-baserte: trigge når en ny versjon, ny rad, eller endret status oppdages.
- Multi-trigger: ett sensor-tick kan produsere flere triggere, for eksempel en per ny fil.

Sensorenes minste nyttige abstraksjon er:

- `check(context) -> [trigger] | skip_reason`
- cursor lagres automatisk
- idempotent `run_key` per trigget enhet

## Workflow-modell

Orchestratoren bør støtte både enkle single-step jobs og små DAG-er. Den trenger ikke full asset-graf for å være nyttig. Det viktigste er:

- Avhengigheter mellom steg.
- Mulighet til å kjøre bare oppdaterte eller feilede steg.
- Re-run på spesifikke steg.
- Retry på steg-nivå.
- Parallelle steg der det er trygt.
- Artefakt-sporbarhet mellom steg.

Dette gir nok struktur til ETL, rapportering, API-jobber, vedlikeholdsoppgaver og enklere dataflyter.

## Observabilitet

God observabilitet er et av de billigste og mest verdifulle grepene. Systemet bør ha:

- Per run-ID og per step-ID logs.
- Structured logs med jobnavn, sensornavn, schedule-ID, cursor og retry-attempt.
- Historikk for alle triggere og alle skip-grunner.
- En enkel web-UI eller CLI for å vise status.
- "Explain" kommando som sier hvorfor noe ikke kjørte.
- Preview av schedule og sensor-evaluering.

Dette er billig å implementere fordi det primært er en kombinasjon av logging, state persistence og en lesbar UI over samme data.

## Garantier og robusthet

Det mest nyttige her er:

- At-least-once start av runs.
- Idempotency keys for triggerede runs.
- Ikke mer enn én aktiv run per nøkkel om ønskelig.
- Rekonsiliering ved restart.
- Persistens i SQLite for single-node eller Postgres for multi-node.
- Lease/lock på scheduler og sensor-evaluering.

Systemet trenger ikke distribuert konsensus for å være verdifullt. Det holder langt med en tydelig state machine og gjenopprettbar metadatalagring.

## Det som gjør produktet unikt

Dette systemet kan være "systemd timers pluss sensors pluss små DAG-er", med en CLI-first opplevelse. Det unike bør være:

- En veldig liten kjerne.
- Lesbare beslutninger.
- Sensorer som er lett å skrive.
- Tydelig skip- og retry-logikk.
- Minimal plugin-flate.
- Enkelt å kjøre lokalt, på én VM eller i små teams.

## Typiske brukstilfeller

- Nightly report-generering.
- Periodisk datavask.
- Reaksjon på nye filer i objektlager.
- Reaksjon på endringer i en database-tabell.
- Enkel batch-orchestration.
- Oppgaver som i dag ligger i cron, men trenger bedre innsyn og robusthet.

## MVP-funksjoner

- Cron/interval schedules.
- Sensorer med cursor.
- Run history.
- Step retries.
- CLI for start/stop/list/replay.
- Lagring av state.
- Structured logs.
- Basic DAG dependencies.

## Fase 2-funksjoner

- Backfill.
- Watermark-baserte triggers.
- Web UI.
- Notifications.
- Dynamic fan-out.
- Artifact lineage.
- Dry-run og explain-plan.
