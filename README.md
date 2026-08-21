# Pulseq

A lightweight orchestrator ("pulse queue") for Linux and server environments. It combines cron-like schedules with event-based sensors and small DAGs — written in Go, with SQLite as the state store.

Core idea: keep "deciding to start work" (scheduler/sensors) separate from "doing the work" (workers). Small core, readable decisions, CLI-first.

## Planning

The project is fully planned, from empty repo to v1.0:

- [Project board](https://github.com/users/a-holm/projects/2) — all 80 issues with priority, estimate, epic, dates and dependencies, across 9 milestones (M0–M8).
- [docs/PLAN.md](docs/PLAN.md) — the master plan: final decisions, milestones and the complete issue backlog.

Note: the master plan and the issues are written in Norwegian; everything else in the project is in English.

## Status

Milestone [M0 — Foundation and persistence](https://github.com/a-holm/pulseq/milestone/1) is under way. The binary builds and prints help; no orchestration yet.

## Build

Requires Go 1.25 or newer. No C toolchain: the binary is built with `CGO_ENABLED=0` and is statically linked.

```
make build     # bin/pulseq
make test      # go test -race ./...
make lint      # go vet + staticcheck
make ci        # fmt-check, lint, test, build
```

Run `make hooks` once after cloning. It points git at `.githooks`, so formatting and `go vet` run before each commit and `make ci` runs before each push.

`make fmt` and `make lint` need [gofumpt](https://github.com/mvdan/gofumpt) and [staticcheck](https://staticcheck.dev) on your PATH.

## Architecture

Decisions that later work depends on are recorded in [docs/adr](docs/adr). The dependency direction between packages is enforced by tests in `internal/arch`, not by review: `internal/model` imports nothing internal, and all SQL lives in `internal/store`.

## License

[Apache License 2.0](LICENSE).
