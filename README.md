# paceq

> Cron can only ask "is it time yet?". paceq can ask "did something new arrive?", and it can tell you exactly why it did not.

A lightweight orchestrator ("pace queue") for Linux and server environments. It combines cron-like schedules with event-based sensors and small DAGs, written in Go, with SQLite as the state store.

Core idea: keep "deciding to start work" (scheduler/sensors) separate from "doing the work" (workers). Small core, readable decisions, CLI-first.

## Why not Dagu, cron or systemd timers?

cron and systemd timers fire and forget: no history, no status, and nothing to ask when a job stays silent. Dagu is the closest match, one Go binary with YAML DAGs, but its state lives in files and it has no general sensor with a cursor. paceq keeps every decision, including the decision not to run, in SQLite. Sensors with a cursor make events a first-class trigger, and `paceq explain` answers why last night was quiet.

## Scope, security and guarantees

- [SCOPE.md](SCOPE.md): who paceq is for, what it will never do through 1.0, and the gate a feature request has to pass.
- [SECURITY.md](SECURITY.md): the threat model, what has to be right from day one, what we explicitly do not defend against, and how to report a vulnerability.
- [docs/guarantees.md](docs/guarantees.md): the guarantees, the non-guarantees, the invariants, and what `synchronous=NORMAL` costs.

## Planning

The project is fully planned, from empty repo to v1.0:

- [Project board](https://github.com/users/a-holm/projects/2): the full backlog with priority, estimate, epic, dates and dependencies, across 9 milestones (M0 to M8).
- [docs/PLAN.md](docs/PLAN.md): the master plan with final decisions, milestones and the complete issue backlog.

Note: the master plan and the issues are written in Norwegian; everything else in the project is in English.

## Status

Milestone [M0, foundation and persistence](https://github.com/a-holm/paceq/milestone/1) is under way. The binary builds and prints help; no orchestration yet.

## Build

Building requires Go 1.25 or newer. The full local gate needs Go 1.26 or newer, or `GOTOOLCHAIN=auto`, because staticcheck declares Go 1.26. No C toolchain: the binary is built with `CGO_ENABLED=0` and is statically linked. The race detector in `make test` is the single exception. It builds the test binaries with cgo and never touches the shipped artifact.

```
make build     # bin/paceq
make test      # go test -race -count=1 ./...
make lint      # go vet + staticcheck
make cross     # linux/amd64, linux/arm64, darwin/arm64, each asserted cgo free
make ci        # the full gate, the same set the ci workflow runs
```

Run `make hooks` once after cloning. It points git at `.githooks`, so formatting and `go vet` run against the staged content before each commit and `make ci` runs before each push. One exception: a push that only deletes remote refs moves no code, so the pre-push hook skips the gate for it. `core.hooksPath` is shared by every worktree of the repository but resolved against each worktree's own root, so a worktree checked out before `.githooks` existed runs no hooks and says nothing about it.

[gofumpt](https://github.com/mvdan/gofumpt), [staticcheck](https://staticcheck.dev), [gosec](https://github.com/securego/gosec) and [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) run through `go run` at the versions pinned at the top of the Makefile, so none of them has to be on your PATH. The first run compiles them; later runs come out of the Go build cache. `make govulncheck` queries the vulnerability database at vuln.go.dev and needs network access.

## Continuous integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every pull request and on every push to main. The `verify` job runs `make fmt-check`, `make vet`, `make staticcheck`, `make gosec`, `make govulncheck`, `make tidy-check`, `make test`, `make gate`, `make fuzz` and `make build`, one target per step so a red run names the gate that failed. `make fuzz` gives the job file parser sixty seconds of fuzzing per pull request, because a job file is untrusted input. The `cross-build` job runs `scripts/cross-build.sh` per platform. `make ci` runs that same set locally. Every gate blocks; nothing in the pipeline is advisory. The pipeline builds with the current Go 1.26 toolchain, which does not weaken the Go 1.25 floor above: `go vet` reports any standard library symbol newer than the version in go.mod.

The cross build is the gate behind the promise of one static binary per platform. For each target it proves that no package outside the standard library needs cgo, that the built binary records `CGO_ENABLED=0`, that Linux binaries are statically linked and built for the expected architecture, and that the binary stays under the 30 MB budget.

## A red gate is red

A failing test is a fact about the code, and nothing in the gate is allowed to disagree with it. No step retries, no `|| true`, no `continue-on-error`, no rerun-until-green loop, and no `go test` without `-count=<n>` where a cached result could stand in for a run. Inside the tests, a `t.Skip` must state a capability the machine lacks or a mode the run is driven in; a reason that blames timing, load, the runner or CI is refused by [internal/arch](internal/arch/flaky_test.go), along with an empty one. The rule is enforced, not remembered: `internal/arch/flaky_test.go` scans `.github/workflows`, `.githooks`, `scripts` and the Makefile on every `make test` and fails the build on any way past a red run. When a gate goes red intermittently, the fix is the root cause — never a wider tolerance.

[.github/workflows/vuln.yml](.github/workflows/vuln.yml) runs govulncheck against main every Monday and on demand, because a vulnerability in a dependency is published when it is published, not when we push. Findings open an issue, or comment on the open one, and fail the run. [.github/dependabot.yml](.github/dependabot.yml) proposes weekly updates for Go modules and for actions; nothing merges by itself. Actions are pinned by commit SHA, and tool versions are bumped by hand in the Makefile.

## Architecture

Decisions that later work depends on are recorded in [docs/adr](docs/adr). The dependency direction between packages is enforced by tests in `internal/arch`, not by review: `internal/model` imports nothing internal, and all SQL lives in `internal/store`.

## License

[Apache License 2.0](LICENSE).
