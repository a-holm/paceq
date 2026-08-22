# ADR-0001: Foundation

Status: accepted
Date: 2026-08-21

## Context

This is the first code in the repository. Nothing here is a feature. The point is to fix the small set of choices that every later issue would otherwise reopen: language version, module path, licence, package layout, dependency direction, build mode and where quality gates run.

Two of these choices are load-bearing rather than cosmetic. The dependency direction is an architecture decision, so it is enforced by a test instead of by review. `CGO_ENABLED=0` is the product promise, so it is set in the build itself instead of being left to the environment.

## Decision 1: Go 1.25

The `go` directive in `go.mod` is `go 1.25.0`. `modernc.org/sqlite` states its own floor at that patch release, and `go mod tidy` raises the directive to match, so the patch level is written out. `testing/synctest` became stable in that release, and a fake clock is the only way to test a scheduler and its backoff without `time.Sleep`. Nothing in the project may raise the directive without a follow-up ADR, because the directive is the floor for anyone building from source.

Newer toolchains are fine for development. The directive states the minimum, not the toolchain in use.

## Decision 2: Module path `github.com/a-holm/paceq`

The module path follows the product name settled in ADR-0002. It is spelled out in three places only: `go.mod`, one import in `cmd/paceq/main.go`, and one constant in the architecture test.

Consequence: documentation and examples do not spell out the module path unless they have to.

## Decision 3: Apache License 2.0

The licence is permissive, as the plan requires. Apache-2.0 over MIT for two reasons:

1. It grants patent rights explicitly. MIT is silent on patents, and silence is what corporate legal review stops on. The target user runs this inside a company.
2. Section 5 states that contributions arrive under the same licence, so contributions can be accepted without a separate contributor agreement.

The cost is length: the licence text is long and carries an appendix, where MIT fits on a screen. That cost is paid once.

The `LICENSE` file holds the canonical text, and `README.md` links to it.

## Decision 4: Package layout and dependency direction

```
cmd/paceq/         thin main: signals, then internal/cli
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

The direction is `cli -> engine -> store -> model`. `internal/model`, `internal/id` and `internal/clock` import nothing under `internal/` at all, which is what keeps the domain types usable from every layer without a cycle. `internal/cli` is the top layer: nothing under `internal/` may import it, whether or not the importing package has a row of its own.

`internal/arch` enforces this over **direct** imports, not transitive ones. Transitive enforcement would be wrong: `engine` may import `runner`, and `cli` imports `engine`, but `cli` still may not reach for `runner` itself.

Test files are held to the same table, read from `TestImports` and `XTestImports`. A rule that only covers production files is a rule with a `_test.go` shaped hole in it. One relaxation: test files may also import `internal/testutil`, which exists for no other purpose. Failure messages say which import list the edge came from.

`internal/testutil` has a row of its own, `{model, clock, id, store}`, so the relaxation cannot be used as a detour. Without it, a test in `internal/model` could reach `internal/engine` transitively through a helper, and the rule that `model` depends on nothing would hold only on paper.

Packages with no rule in the table are unconstrained for now, except for the `internal/cli` rule, which is universal. The plan does not fix the remaining directions yet, and enforcing a guess is worse than enforcing nothing. The test asserts that every package named in the table exists, so a rename cannot silently empty a rule.

All SQL lives in `internal/store`. The same test scans every `.go` file outside `internal/store` for two things:

1. Imports of `database/sql` or `database/sql/driver`. This is the guard that actually holds. No package can execute SQL without one of them, so the direction cannot be broken while this passes. When the driver lands in #28, `modernc.org/sqlite` and `modernc.org/sqlite/lib` join this list: `lib` is a standalone port of the C API and reaches the database without going through `database/sql` at all.
2. String literals starting with a statement keyword: the four DML verbs, plus `REPLACE `, `PRAGMA `, `ATTACH `, `WITH `, `BEGIN `, and the DDL `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE`, `DROP TABLE`. `PRAGMA` and `BEGIN` are there because connection setup and transaction control are store-owned too, not only queries. `ATTACH` is there because multi-file databases are ruled out outright, so any occurrence is a mistake. `VACUUM` and `ANALYZE` are deliberately left off: they are bare single-word statements with no operand to anchor on, and the maintenance paths that issue them go through the `PRAGMA` rule already.

Runs of whitespace are collapsed before matching, so `CREATE  TABLE` does not slip through on a stray second space.

The literal scan matches case sensitively. Uppercase keywords are the project's SQL convention, and matching case insensitively would flag ordinary prose: a literal such as `"with %d retries left"` starts with `WITH ` once upper-cased. The cost is that lowercase SQL slips past the literal scan. That is acceptable, because it cannot be executed without an import the first guard already blocks. The literal scan is a tripwire on top of the import guard, not the guard itself.

The scan skips `internal/arch` itself, because the checker necessarily spells out the patterns it hunts for. That package will never hold SQL.

## Decision 5: `CGO_ENABLED=0` is not negotiable

The Makefile exports `CGO_ENABLED=0` for every target. A static binary with no toolchain requirement is the reason `modernc.org/sqlite` was chosen over the cgo drivers, and it is what keeps `go install` and cross compilation working from one machine. A cgo leak breaks both, and it breaks them quietly: the build still succeeds on the machine that has the C toolchain.

One exception, scoped to the test target: the race detector requires cgo, so `make test` runs with `CGO_ENABLED=1`. This affects the test process only. The artifact produced by `make build` is never built that way.

## Decision 6: Quality gates run locally first, CI is the backstop

`make hooks` points `core.hooksPath` at `.githooks`. `pre-commit` runs the fast checks, gofumpt plus `go vet ./...`. `pre-push` runs `make ci` in full, with one exception for every push that moves no code: a push whose every ref line carries the zero sha in the local sha field deletes remote refs only, and a push that hands the hook no ref lines at all pushes nothing (git fires the hook even when everything is up to date). The hook skips the gate for both.

The reason is cost. GitHub Actions minutes are a budget, and a red build discovered after a push has already spent them. A developer machine has the toolchain, the module cache and the test cache warm, so the same checks cost seconds there. CI still runs everything, because a hook can be bypassed and a contributor may not have run `make hooks`.

`pre-commit` checks a materialised copy of the index, produced with `git checkout-index`, not the working tree. Checking the working tree is the obvious implementation and it is wrong: with unformatted content staged and the working tree already fixed, the commit passes and the bad blob lands. That is exactly what a partial `git add` produces.

`core.hooksPath` is repository-level configuration, shared by every worktree, but it is resolved relative to each worktree's own root. A worktree checked out at a commit without `.githooks` therefore runs no hooks at all, silently, rather than failing. CI is the backstop for that case.

## Decision 7: Budget of 8 direct runtime dependencies

The budget is **8 direct runtime dependencies**. Test-only dependencies are outside it, as the plan states. Indirect dependencies are not counted either, since they are consequences of the direct ones rather than decisions.

A test enforces this. It gets the direct requirements from `go mod edit -json`, which reports the `Indirect` flag, and never parses `go.mod` by hand: comment placement in `go.mod` is subtle enough that a hand parser reads `require ( // runtime deps` as a single dependency named `(` and then reports a nine-dependency module as one.

Runtime and test-only are separated by reachability, not by naming convention. A module reached from the product's own packages is runtime. A module reached only through test files is test-only.

Three details make that classification survive contact with real code:

1. The graph is walked for `linux`, `darwin` and `windows` and unioned. A dependency behind `//go:build windows` is a runtime dependency, and a walk on the host alone cannot see it. This matters directly: the plan keeps `mattn/go-sqlite3` behind a build tag as an escape hatch, and `internal/runner` will need platform-specific process handling.
2. `internal/testutil` is left out of the runtime roots. It is an ordinary package, so a naive walk counts everything it imports as shipping in the product, when in fact it is only ever imported by test files. Its own row in the dependency table is what keeps that exclusion safe.
3. A direct requirement reached by neither walk is **counted as runtime**, and logged as unreachable rather than reported as an error. It is most likely gated behind a build tag this walk does not set, so counting it as runtime is the conservative reading, and refusing to guess the other way keeps a hole out of the budget.

The test deliberately does **not** assert that every direct requirement is reachable. `go mod tidy` resolves across every platform and build tag, so it correctly keeps a requirement that only a `//go:build windows` file imports, and a test that calls that unused fails on a green tree while telling the developer to run the command they already ran. Staleness is `go mod tidy -diff` in CI (#22), which is the tool that can actually be right about it.

`modernc.org/sqlite` is the first direct dependency, added with `internal/store` in #28. `github.com/oklog/ulid/v2` is the second, added with `internal/id` in #41. `github.com/spf13/cobra` is the third, added with the command line in #51, and `github.com/goccy/go-yaml` the fourth, added with `internal/spec` in #46. Everything else in the table is still unspent.

Every dependency added later needs a line in this table. The choices below are fixed by PLAN.md §A and SYNTESE §4.9, not open questions:

| Module | Kind | Purpose |
|---|---|---|
| `modernc.org/sqlite` v1.57.0 | runtime | SQLite driver in pure Go, which is what makes `CGO_ENABLED=0` possible. Pinned: the plan keeps `mattn/go-sqlite3` behind a build tag as the escape hatch, so a driver regression must be a deliberate version bump rather than a silent upgrade |
| `github.com/spf13/cobra` v1.10.2 | runtime | command tree, flag handling and shell completion for the CLI. Pinned: the command tree and the completion scripts it generates are a user interface, so a change to either is a deliberate bump |
| `github.com/goccy/go-yaml` v1.19.2 | runtime | job file parsing with line and column positions in errors; `gopkg.in/yaml.v3` is archived. Only the `parser`, `ast` and `token` packages are used: `internal/spec` walks the syntax tree itself rather than unmarshalling, which is what gives every diagnostic a position and keeps the alias limits enforceable before anything is expanded. Pinned: the job file format is a user interface, and a parser that changed what it accepts would change it |
| `github.com/adhocore/gronx` | runtime | cron expression parser. The iterator, `Between` and the DST policy are our code, in `internal/cronx` |
| `github.com/oklog/ulid/v2` v2.1.2 | runtime | time-sortable, prefix-searchable identifiers for `internal/id`. Pinned: the id format is a storage format, and a ULID that changed its encoding or its monotonic entropy behaviour would reorder existing rows |
| `golang.org/x/sync` | runtime | `errgroup` and `semaphore` for structured startup and shutdown |
| `github.com/google/go-cmp` | test | readable diffs in tests |
| `github.com/rogpeppe/go-internal` v1.14.1 | test | `testscript` for CLI golden tests, where `--json` output is a public interface |
| `pgregory.net/rapid` | test | property tests for the state machine against real SQLite |

That is six runtime dependencies of eight, four of them spent, leaving two slots. Anything beyond needs an ADR that says what it replaced.

The CLI library is the one entry worth flagging: PLAN.md §A picks cobra, while the Go architecture draft argued for stdlib `flag` and a hand-written router. #51 spends the slot on cobra. What buys it is the flag model the command line rests on: persistent flags that every subcommand inherits, a `--db` and `-o` that mean the same thing everywhere, per-command argument validation that can return a paceq error rather than a printed usage block, and the completion and man page generation the management surface needs from M1 on. `flag` gives none of that without a router, a flag inheritance mechanism and a help renderer written here and maintained here.
