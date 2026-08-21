# Pulseq

Lettvekt orchestrator ("pulse queue") for Linux/servermiljøer. Kombinerer cron-lignende schedules med event-baserte sensors og små DAG-er — skrevet i Go, med SQLite som state-lager.

Kjerneidé: skill "beslutning om å starte arbeid" (scheduler/sensors) fra "utføring av arbeid" (workers). Liten kjerne, lesbare beslutninger, CLI-first.

Se [docs/prosjektbeskrivelse.md](docs/prosjektbeskrivelse.md) for full beskrivelse, og [docs/plans/](docs/plans/) for planleggingsdokumenter.

## Status

Planleggingsfase. Se GitHub issues for roadmap.
