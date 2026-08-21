# Pulseq

A lightweight orchestrator ("pulse queue") for Linux and server environments. It combines cron-like schedules with event-based sensors and small DAGs — written in Go, with SQLite as the state store.

Core idea: keep "deciding to start work" (scheduler/sensors) separate from "doing the work" (workers). Small core, readable decisions, CLI-first.

## Planning

The project is fully planned, from empty repo to v1.0:

- [Project board](https://github.com/users/a-holm/projects/2) — all 80 issues with priority, estimate, epic, dates and dependencies, across 9 milestones (M0–M8).
- [docs/PLAN.md](docs/PLAN.md) — the master plan: final decisions, milestones and the complete issue backlog.

Note: the master plan and the issues are written in Norwegian; everything else in the project is in English.

## Status

Planning phase. Implementation starts with milestone [M0 — Foundation and persistence](https://github.com/a-holm/pulseq/milestone/1).
