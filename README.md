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

Building requires Go 1.25 or newer. The full local gate needs Go 1.26 or newer, or `GOTOOLCHAIN=auto`, because staticcheck declares Go 1.26. No C toolchain: the binary is built with `CGO_ENABLED=0` and is statically linked. The race detector in `make test` is the single exception. It builds the test binaries with cgo and never touches the shipped artifact.

```
make build     # bin/pulseq
make test      # go test -race -count=1 ./...
make lint      # go vet + staticcheck
make cross     # linux/amd64, linux/arm64, darwin/arm64, each asserted cgo free
make ci        # the full gate, the same set the ci workflow runs
```

Run `make hooks` once after cloning. It points git at `.githooks`, so formatting and `go vet` run against the staged content before each commit and `make ci` runs before each push. `core.hooksPath` is shared by every worktree of the repository but resolved against each worktree's own root, so a worktree checked out before `.githooks` existed runs no hooks and says nothing about it.

[gofumpt](https://github.com/mvdan/gofumpt), [staticcheck](https://staticcheck.dev), [gosec](https://github.com/securego/gosec) and [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) run through `go run` at the versions pinned at the top of the Makefile, so none of them has to be on your PATH. The first run compiles them; later runs come out of the Go build cache. `make govulncheck` queries the vulnerability database at vuln.go.dev and needs network access.

## Continuous integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every pull request and on every push to main. The `verify` job runs `make fmt-check`, `make vet`, `make staticcheck`, `make gosec`, `make govulncheck`, `make tidy-check`, `make test` and `make build`, one target per step so a red run names the gate that failed. The `cross-build` job runs `scripts/cross-build.sh` per platform. `make ci` runs that same set locally. Every gate blocks; nothing in the pipeline is advisory. The pipeline builds with the current Go 1.26 toolchain, which does not weaken the Go 1.25 floor above: `go vet` reports any standard library symbol newer than the version in go.mod.

The cross build is the gate behind the promise of one static binary per platform. For each target it proves that no package outside the standard library needs cgo, that the built binary records `CGO_ENABLED=0`, that Linux binaries are statically linked and built for the expected architecture, and that the binary stays under the 30 MB budget.

[.github/workflows/vuln.yml](.github/workflows/vuln.yml) runs govulncheck against main every Monday and on demand, because a vulnerability in a dependency is published when it is published, not when we push. Findings open an issue, or comment on the open one, and fail the run. [.github/dependabot.yml](.github/dependabot.yml) proposes weekly updates for Go modules and for actions; nothing merges by itself. Actions are pinned by commit SHA, and tool versions are bumped by hand in the Makefile.

## Architecture

Decisions that later work depends on are recorded in [docs/adr](docs/adr). The dependency direction between packages is enforced by tests in `internal/arch`, not by review: `internal/model` imports nothing internal, and all SQL lives in `internal/store`.

## License

[Apache License 2.0](LICENSE).
