# paceq

> Cron can only ask "is it time yet?". paceq can ask "did something new arrive?", and it can tell you exactly why it did not.

One static binary that runs your scheduled jobs, reacts to events with sensors,
and keeps every decision - including the decision **not** to run - in SQLite,
so `paceq explain` can answer "why did nothing happen last night?" from what
was actually decided, not from a guess.

See it in motion - [assets/demo-import-explain.cast](assets/demo-import-explain.cast),
a real recording of importing a crontab and explaining a job (`asciinema play
assets/demo-import-explain.cast`). Every command below is executable as it stands.

## Install

```
curl -sSfL -o install.sh https://raw.githubusercontent.com/a-holm/paceq/main/install.sh
sh install.sh          # detects your platform, verifies sha256, installs into ~/.local/bin
```

Or by hand: download the archive for your platform from
[releases](https://github.com/a-holm/paceq/releases), then verify it - the
check is part of installing, not an optional extra:

```
curl -sSfLO https://github.com/a-holm/paceq/releases/download/v0.1.0/checksums.txt
curl -sSfLO https://github.com/a-holm/paceq/releases/download/v0.1.0/paceq_0.1.0_linux_amd64.tar.gz
sha256sum -c checksums.txt --ignore-missing
tar xzf paceq_0.1.0_linux_amd64.tar.gz && ./paceq version --json
```

## 60 seconds

<!-- run -->
```bash
paceq init                 # a config, an example job, the state database
paceq apply                # load job files as immutable versions
paceq run hello            # run the example job now
```

```
run 01M0XSJAWCA6B40KFV8YD45TGR  job hello
✓ run 01M0XSJAWCA6B40KFV8YD45TGR  ok  9ms
```

And when you want to know what happened - now or at 3am:

<!-- run -->
```bash
paceq explain job hello
```

Bringing a crontab across? `paceq import crontab` reads it (never writes),
writes readable YAML job files, and keeps every original line as a comment:
the [five-minute crontab tutorial](docs/tutorials/01-from-crontab.md).

## Why not cron / systemd timers / Dagu?

cron and systemd timers fire and forget: when a job stays silent they have
nothing to ask. Dagu is a fine DAG runner, but its state lives in files and it
has no sensor with a cursor. paceq records every tick, trigger, run and step
with its reason code in SQLite, so "why didn't it run" is a query, not an
archaeology project.

## What this is not

Not a data platform: no asset graph, no lineage, no plugins, no templating
language, no multi-node clustering, no RBAC, no cloud service, no sub-second
scheduling - and never exactly-once. The full list of non-goals is
[SCOPE.md](SCOPE.md); the delivery guarantee is at-least-once, stated in plain
words in [docs/guarantees.md](docs/guarantees.md). A job *can* be started
twice under unlucky timing; make steps idempotent with `$PACEQ_IDEMPOTENCY_KEY`
and [guarantees](docs/guarantees.md) shows how.

## Documentation

| Start here | |
|---|---|
| [Tutorial: from crontab to paceq in five minutes](docs/tutorials/01-from-crontab.md) | import, apply, preview, explain |
| [Tutorial: your first sensor](docs/tutorials/02-first-sensor.md) | five lines of bash reacting to new files |
| [Troubleshooting](docs/troubleshooting.md) | "my job did not run last night", and friends |

| Reference | |
|---|---|
| [Guarantees and non-guarantees](docs/guarantees.md) | at-least-once in plain words, the invariants, `synchronous=NORMAL` |
| [Cursor vs run_key](docs/cursor-vs-run-key.md) | two notions, two reset flags, never implicitly coupled |
| [CLI reference](docs/reference/cli.md) | generated from the binary's own command tree |
| [Job file reference](docs/reference/jobspec.md) | every key, its default and its limit |
| [Sensor contract](docs/reference/sensor-contract.md) | the frozen subprocess contract (JSON in/out, exit codes) |
| [Step contract](docs/reference/step-contract.md) | the frozen environment a step runs in (`PACEQ_*`) |
| [Exit codes](docs/reference/exit-codes.md) | 1 means paceq failed; 5 means the job did |
| [Reason codes](docs/reference/reason-codes.md) | generated from the catalogue behind `paceq error <code>` |
| [Status contract](docs/reference/status-contract.md) | the frozen `paceq status` JSON document |

| Operations | |
|---|---|
| [Running under systemd](docs/operations/systemd.md) | unit, hardening, watchdog, sandbox inheritance |
| [Backup and retention](docs/operations/backup-and-retention.md) | prune horizons, VACUUM, safe backups, no `cp` on a live database |
| [Monitoring](docs/operations/monitoring.md) | `/metrics`, freshness SLAs, the shipped alert rules |
| [Upgrading](docs/operations/upgrading.md) | schema migrations, backup first, `fsck` after |

## Status

On main today: job files with schedules, steps and DAG `needs`; retry,
replay and concurrency keys; exec sensors over a frozen contract; `explain`,
`status`, `doctor`, `validate`, `fsck`, `logs`; crontab import; retention and
backup tooling; `/metrics`; one static binary per platform with a
reproducible, checksummed release pipeline. Runs are started by hand
(`paceq run`) and sensors evaluate on demand (`paceq sensors tick`,
`sensors test`); the daemon (`paceq serve`) carries the scheduling,
dispatch, execution and recovery loops, and the activation wiring that feeds
applied catalogs into those loops on its own is the work in flight towards
the v0.1 cut - [CHANGELOG.md](CHANGELOG.md) states exactly where each piece
stands. Windows is not supported: unix sockets, process groups and flock are
load-bearing parts of the design.

The project is fully planned through v1.0 - see the
[project board](https://github.com/users/a-holm/projects/2) and
[docs/PLAN.md](docs/PLAN.md) (the plan is written in Norwegian; everything
user-facing is in English).

## Build

Building requires Go 1.27 or newer (the version go.mod asks for; `GOTOOLCHAIN=auto` fetches it if needed). No C toolchain: the binary is built with `CGO_ENABLED=0` and is statically linked. The race detector in `make test` is the single exception. It builds the test binaries with cgo and never touches the shipped artifact.

```
make build     # bin/paceq
make docs      # regenerate the generated reference pages
make test      # go test -race -count=1 ./...
make lint      # go vet + staticcheck
make cross     # linux/amd64, linux/arm64, darwin/arm64, each asserted cgo free
make ci        # the full gate, the same set the ci workflow runs
```

Run `make hooks` once after cloning. It points git at `.githooks`, so formatting and `go vet` run against the staged content before each commit and `make ci` runs before each push. One exception: a push that moves no code skips the gate, and that is a push that only deletes remote refs as well as a push with nothing to push (git fires the hook with no ref lines when everything is up to date). `core.hooksPath` is shared by every worktree of the repository but resolved against each worktree's own root, so a worktree checked out before `.githooks` existed runs no hooks and says nothing about it.

[gofumpt](https://github.com/mvdan/gofumpt), [staticcheck](https://staticcheck.dev), [gosec](https://github.com/securego/gosec) and [govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) run through `go run` at the versions pinned at the top of the Makefile, so none of them has to be on your PATH. The first run compiles them; later runs come out of the Go build cache. `make govulncheck` queries the vulnerability database at vuln.go.dev and needs network access.

See the M4 exit demo walk a DAG through green → failure → skipped downstream → operator retry back to green:

```
go build -o ./bin/paceq ./cmd/paceq
PATH=$PWD/bin:$PATH scripts/demo-m4.sh --down   # no daemon; flock path
PATH=$PWD/bin:$PATH scripts/demo-m4.sh --up     # daemon serving .paceq/paceq.sock
```

## Continuous integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every pull request and on every push to main. The `verify` job runs `make fmt-check`, `make vet`, `make staticcheck`, `make gosec`, `make govulncheck`, `make tidy-check`, `make test`, `make gate`, `make fuzz` and `make build`, one target per step so a red run names the gate that failed. `make fuzz` gives the job file parser sixty seconds of fuzzing per pull request, because a job file is untrusted input. The `cross-build` job runs `scripts/cross-build.sh` per platform. `make ci` runs that same set locally. Every gate blocks; nothing in the pipeline is advisory. The pipeline builds with the current Go 1.27 toolchain, which matches the floor in go.mod exactly: `go vet` reports any standard library symbol newer than the version in go.mod.

The cross build is the gate behind the promise of one static binary per platform. For each target it proves that no package outside the standard library needs cgo, that the built binary records `CGO_ENABLED=0`, that Linux binaries are statically linked and built for the expected architecture, and that the binary stays under the 30 MB budget.

A release is cut by pushing a `v*` tag. The tag workflow builds the tagged commit twice and refuses to ship unless both builds agree byte for byte, then leaves a draft release for review. Every release carries `checksums.txt`; [install.sh](install.sh) fails hard on any mismatch.

## A red gate is red

A failing test is a fact about the code, and nothing in the gate is allowed to disagree with it. No step retries, no `|| true`, no `continue-on-error`, no rerun-until-green loop, and no `go test` without `-count=<n>` where a cached result could stand in for a run. Inside the tests, a `t.Skip` must state a capability the machine lacks or a mode the run is driven in; a reason that blames timing, load, the runner or CI is refused by [internal/arch](internal/arch/flaky_test.go), along with an empty one. The rule is enforced, not remembered: `internal/arch/flaky_test.go` scans `.github/workflows`, `.githooks`, `scripts` and the Makefile on every `make test` and fails the build on any way past a red run. When a gate goes red intermittently, the fix is the root cause, never a wider tolerance.

Generated documentation is part of the gate: [docs/reference/cli.md](docs/reference/cli.md) is generated from the cobra tree and [docs/reference/reason-codes.md](docs/reference/reason-codes.md) from the reason catalogue, and their staleness tests fail `make test` if either drifts from the code. Run `make docs` after changing help text or catalogue entries, and commit the result together with the change.

[.github/workflows/vuln.yml](.github/workflows/vuln.yml) runs govulncheck against main every Monday and on demand, because a vulnerability in a dependency is published when it is published, not when we push. Findings open an issue, or comment on the open one, and fail the run. [.github/dependabot.yml](.github/dependabot.yml) proposes weekly updates for Go modules and for actions; nothing merges by itself. Actions are pinned by commit SHA, and tool versions are bumped by hand in the Makefile.

## Architecture

Decisions that later work depends on are recorded in [docs/adr](docs/adr). The dependency direction between packages is enforced by tests in `internal/arch`, not by review: `internal/model` imports nothing internal, and all SQL lives in `internal/store`.

## License

[Apache License 2.0](LICENSE).
