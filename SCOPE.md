# Scope

> Cron can only ask "is it time yet?". paceq can ask "did something new arrive?", and it can tell you exactly why it did not.

That sentence is the contract, not a slogan. Anything that does not support it is a non-goal (plans: 10 F2). This document exists so that a "no" costs one link instead of one argument, and so a "yes" has to pass a stated gate.

## Who paceq is for

The target user has too many scheduled jobs to keep in their head, and too few to justify a platform (plans: 09 §2.1).

| Dimension | In scope | Out of scope |
|---|---|---|
| Scheduled jobs | 5 to 100 | fewer than 5 (cron is enough), more than 500 (Airflow country) |
| Machines | 1 to 3 | a Kubernetes fleet |
| Dedicated platform staff | 0 | 1 or more |
| Runtime per job | seconds to hours | milliseconds (that is a queue), days (that is Temporal) |
| Appetite for new infrastructure | "one binary, fine" | "we already run Postgres and Redis" |

## Anti-personas

We say no to these out loud (plans: 09 §2.2, 09 §3.3):

- Teams that need multi-tenant RBAC and an audit trail for a hundred users.
- Kubernetes-native teams that already run Argo Workflows.
- Anyone who describes their need with the phrase "asset lineage across the warehouse".
- Sub-second scheduling and high-throughput job queues. paceq is a scheduler, not a queue.

## Non-goals through 1.0

This list is a contract (plans: 00 §4.11, 10 §3, 09 §3.3). None of it is "later". It is "no", unless a feature passes the gate at the bottom of this document.

- Asset graph, data lineage, partitions, materializations.
- Distributed execution, multi-node, leader election beyond the single-node role lease.
- A Postgres driver, or a database abstraction layer that prepares for one.
- Plugin systems in any form: Go `plugin`, shared libraries, WASM, embedded Lua, Starlark or Tengo.
- A templating or expression language in configuration.
- Dynamic fan-out (steps that generate N steps at runtime) and conditional edges in the graph.
- Built-in integrations: Slack, SMTP, S3, Kafka, Postgres clients. Those are one step running `curl`, `aws` or `psql`.
- Multi-tenancy, users, roles, RBAC.
- A cloud service or a hosted offering. paceq is a binary you run yourself.
- Container executors (Docker or Kubernetes). A step can run `docker run`; that is enough.
- Sub-second scheduling.
- Built-in secrets management. Use environment variables, a systemd `EnvironmentFile`, or `LoadCredential`. paceq stores references to secrets, never the secrets themselves.
- A built-in time series database, log search engine, alertmanager or tracing stack. paceq writes Prometheus text format and runs a program on failure.
- exactly-once. This is never promised. See [docs/guarantees.md](docs/guarantees.md).

## Hard limits inside the DAG

The DAG milestone ships static dependencies only (plans: 00 §3.1, 10 §6):

- Only `needs: [step-a, step-b]`, static, known at parse time.
- A failed step marks everything downstream `skipped`. There is no `continue_on_error` in 1.0.
- No conditional edges, no runtime fan-out.
- No data passing between steps beyond the documented output and input files.
- A per-run parallelism cap.

## Budgets

A breach means something gets removed, not that the budget gets raised. The rule is from 10 §7. The figures are from 00 §4.9, which raised 10 §7's stricter caps of 5 dependencies, 8000 lines and 25 MB; the same section accepts cobra as the one heavy dependency. The figures below govern. The rule is not raised again.

| Budget | Cap |
|---|---|
| Direct runtime dependencies in `go.mod` | 8 |
| Core Go, excluding tests and generated code | 12000 lines |
| Binary size | 30 MB |
| Daemon cold start | 200 ms |
| `paceq status` | 100 ms |

Test-only dependencies do not count against the dependency budget. Every new direct runtime dependency needs an ADR (plans: 08 §5).

## How a "no" becomes a "yes"

A feature request that lands on the non-goal list is refused by default. The gate to reopen one is narrow and it is the same gate for everyone (plans: 10 F1, 10 §7):

1. Name one job you run in production today that is blocked without the feature. Not slowed down, not made prettier: blocked, with the workaround you tried.
2. Show that no combination of an existing step, a shell script and an exec sensor solves it. The extension surface is a program that reads JSON on stdin and writes JSON on stdout; most requests are already reachable through it.
3. Show what it costs against the budgets above, and name what gets removed if it breaks one.
4. If the request contradicts a non-goal, argue that the non-goal is wrong. Arguing that the feature is useful is not an answer to a non-goal.

paceq 1.0 is a finished program, not a platform (plans: 10 F6). "No" is cheaper than a dependency.
