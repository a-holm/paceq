# ADR-0001: Foundation

Status: accepted
Date: 2026-08-21

## Context

This is the first code in the repository. Nothing here is a feature. The point is to fix the small set of choices that every later issue would otherwise reopen: language version, module path, licence, package layout, dependency direction, build mode and where quality gates run.

Two of these choices are load-bearing rather than cosmetic. The dependency direction is an architecture decision, so it is enforced by a test instead of by review. `CGO_ENABLED=0` is the product promise, so it is set in the build itself instead of being left to the environment.

## Decision 1: Go 1.25

The `go` directive in `go.mod` is `go 1.25`. `testing/synctest` became stable in that release, and a fake clock is the only way to test a scheduler and its backoff without `time.Sleep`. Nothing in the project may raise the directive without a follow-up ADR, because the directive is the floor for anyone building from source.

Newer toolchains are fine for development. The directive states the minimum, not the toolchain in use.

## Decision 2: Module path `github.com/a-holm/pulseq`

The name "pulseq" is a working title. The naming decision is open (issue #26), so the module path is kept rename-cheap: it appears in `go.mod`, in one import in `cmd/pulseq/main.go`, and in one constant in the architecture test. A rename is a single search and replace across three files plus the directory name under `cmd/`.

Consequence: documentation and examples do not spell out the module path unless they have to.

## Decision 3: Apache License 2.0

The licence is permissive, as the plan requires. Apache-2.0 over MIT for two reasons:

1. It grants patent rights explicitly. MIT is silent on patents, and silence is what corporate legal review stops on. The target user runs this inside a company.
2. Section 5 states that contributions arrive under the same licence, so contributions can be accepted without a separate contributor agreement.

The cost is length: the licence text is long and carries an appendix, where MIT fits on a screen. That cost is paid once.

The `LICENSE` file holds the canonical text, and `README.md` links to it.

## Decision 4: Package layout and dependency direction

```
cmd/pulseq/        thin main: signals, then internal/cli
internal/
  cli/             argument parsing, subcommands, output rendering, exit codes
  store/           SQLite: pools, PRAGMAs, migrations, every SQL query
    migrations/    numbered .sql files, embedded by store
  model/           domain types and state machines, no I/O
  spec/            job YAML into a canonical JSON intermediate representation
  cronx/           schedule occurrences: Next, Prev, Between, DST policy
  scheduler/       tick materialisation and catch-up
  sensor/          sensor evaluator runtime and cursors
  engine/          run and step orchestration, claim, lease
  runner/          step execution as a process: process group, signals, log pipes
  obs/             logging setup, metrics, doctor checks
  explain/         the only read path into history
  notify/          in-process non-blocking broadcast bus
  id/              time-sortable identifier generation
  clock/           Clock interface and its fake
  testutil/        throwaway databases, fixtures, helper binaries
  arch/            no production code, hosts the architecture tests
```

The direction is `cli -> engine -> store -> model`. `internal/model`, `internal/id` and `internal/clock` import nothing under `internal/` at all, which is what keeps the domain types usable from every layer without a cycle.

`internal/arch` enforces this over **direct** imports, not transitive ones. Transitive enforcement would be wrong: `engine` may import `runner`, and `cli` imports `engine`, but `cli` still may not reach for `runner` itself.

Packages with no rule in the table are unconstrained for now. The plan does not fix their direction yet, and enforcing a guess is worse than enforcing nothing. The test asserts that every package named in the table exists, so a rename cannot silently empty a rule.

All SQL lives in `internal/store`. The same test scans every `.go` file outside `internal/store` for `database/sql` imports and for string literals starting with `SELECT `, `INSERT `, `UPDATE ` or `DELETE `. This invariant is what makes `store` the single port a later Postgres backend would need.

The scan skips `internal/arch` itself, because the checker necessarily spells out the patterns it hunts for. That package will never hold SQL.

## Decision 5: `CGO_ENABLED=0` is not negotiable

The Makefile exports `CGO_ENABLED=0` for every target. A static binary with no toolchain requirement is the reason `modernc.org/sqlite` was chosen over the cgo drivers, and it is what keeps `go install` and cross compilation working from one machine. A cgo leak breaks both, and it breaks them quietly: the build still succeeds on the machine that has the C toolchain.

One exception, scoped to the test target: the race detector requires cgo, so `make test` runs with `CGO_ENABLED=1`. This affects the test process only. The artifact produced by `make build` is never built that way.

## Decision 6: Quality gates run locally first, CI is the backstop

`make hooks` points `core.hooksPath` at `.githooks`. `pre-commit` runs the fast checks, gofumpt on staged Go files plus `go vet ./...`. `pre-push` runs `make ci` in full.

The reason is cost. GitHub Actions minutes are a budget, and a red build discovered after a push has already spent them. A developer machine has the toolchain, the module cache and the test cache warm, so the same checks cost seconds there. CI still runs everything, because a hook can be bypassed and a contributor may not have run `make hooks`.

## Decision 7: Dependency budget of 8 direct dependencies

The budget covers direct runtime and test dependencies together and is enforced by a test that parses `go.mod`. Indirect dependencies are not counted, since they are consequences of the direct ones rather than decisions.

The skeleton has **zero** direct dependencies. The standard library covers everything it does.

Every dependency added later needs a line in this section:

| Module | Purpose | Status |
|---|---|---|
| `modernc.org/sqlite` | SQLite driver in pure Go, which is what makes `CGO_ENABLED=0` possible | planned |
| `github.com/spf13/cobra` | command tree, flag handling and shell completion for the CLI | planned |
| `github.com/goccy/go-yaml` | job file parsing with line and column positions in errors; `gopkg.in/yaml.v3` is archived | planned |
| `github.com/robfig/cron/v3` | standard cron expression parser, used for parsing only, not for its runner | planned |
| `golang.org/x/sync` | `errgroup` and `semaphore` for structured startup and shutdown | planned |
| `github.com/google/go-cmp` | readable diffs in tests | planned |

That is six of eight, leaving two slots. Anything beyond needs an ADR that says what it replaced.

The CLI library is the one entry worth flagging: the master plan picks cobra, while the Go architecture draft argued for stdlib `flag` and a hand-written router. The skeleton does not settle it, because the stub only prints help text and stdlib does that for free. Issue #51 decides and spends the slot if it picks cobra.
