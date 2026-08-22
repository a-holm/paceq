# gen-cron-fixtures

Offline generator for the DST gold standard fixtures checked in under
`internal/cronx/testdata/golden/`. The fixtures are the independent opinion
that issue #52 exists to create: they come from Python `zoneinfo` (PEP 495
fold semantics) plus `croniter`, a code line that shares nothing with the Go
iterator in `internal/cronx`.

CI never runs this script. CI never runs Python. CI never touches the network.
The generator runs on a developer machine, its output is committed, and the Go
tests read the committed bytes.

## When you may change a fixture

Almost never, and never in passing.

A red fixture test means one of two things: our iterator is wrong, or the
fixture is wrong. The first is a bug to fix in `internal/cronx`. The second
happens when tzdata itself moved (a zone rule changed upstream) or when a
policy decision was revisited through an accepted proposal. In both cases the
expectation changed, and a changed expectation needs a written reason next to
it. That is the rule from plan 04 section 8 point 2, and it is enforced:

* Every commit that touches a file under `internal/cronx/testdata/golden/`
  must carry a `FIXTURE-CHANGE:` line in its commit message, followed by the
  reason.
* The GitHub Actions job named `fixture-change` fails a pull request where
  such a commit lacks the line. Locally, `make fixture-change` runs the same
  check against `origin/main`.
* Hand editing a JSON file instead of regenerating defeats the determinism
  check and will be caught in review. Regenerate instead.

## How to regenerate

One time setup:

    python3 -m venv /tmp/cronxgen-venv
    /tmp/cronxgen-venv/bin/pip install croniter

Regenerate and inspect:

    /tmp/cronxgen-venv/bin/python tools/gen-cron-fixtures/gen.py
    git diff --stat -- internal/cronx/testdata/golden/

Commit with the reason:

    git commit -m "#NN: regenerate the DST gold standard

    FIXTURE-CHANGE: <the reason, for example: tzdata 2027a moved the
    Santiago spring transition by one hour>."

## Determinism

Two runs on one machine produce byte identical output. Keys are sorted,
indentation is fixed at two spaces, files end with exactly one newline. If a
regeneration touches files whose inputs did not change, stop and investigate;
something in the environment (Python version, croniter version, system tzdata)
moved without your knowledge.

Each fixture records its provenance in `generated_by`, for example:

    "generated_by": "croniter 6.2.4 / python 3.12.3 / tzdata 2026c"

`TestGoldenRowsClassifyAgainstTheRuntimeZone` compares every cron row against
the Go runtime's own zone database. Category errors (a row marked skipped that
the runtime says is an ordinary instant, or the reverse) fail hard. Instant
level mismatches print a `WARNING possible tzdata drift` line and stay green:
historical instants legitimately move when tzdata moves, which is exactly the
situation the warning exists to surface early (plan 02 R11). A warning means:
diff the regeneration, confirm the upstream cause, and commit with
`FIXTURE-CHANGE:`.

## What the corpus covers

| Window | Zone | What it catches |
|---|---|---|
| oslo-spring-202603 | Europe/Oslo | spring gap 02:00 to 03:00, four expressions, skip and shift |
| oslo-fall-202610 | Europe/Oslo | repeated hour 02:00 to 03:00, four expressions, first and both |
| santiago-spring-202609 | America/Santiago | southern hemisphere: spring forward in September, midnight gap |
| santiago-fall-202604 | America/Santiago | southern hemisphere: fall back in April, repeated 23:00 hour |
| lord-howe-spring-202610 | Australia/Lord_Howe | 30 minute gap; catches a hardcoded `time.Hour` in detection |
| lord-howe-fall-202604 | Australia/Lord_Howe | 30 minute repeat, instances 30 minutes apart in UTC |
| kolkata-202601 | Asia/Kolkata | non integer offset +05:30, no DST |
| utc-oslowindow-202603 | UTC | reference over the Oslo spring window |
| kiritimati-1994 | Pacific/Kiritimati | dateline jump: all of 1994-12-31 missing |

Policies are recorded per fixture as strings (`skip`, `shift`, `first`,
`both`) so the data stays decoupled from the Go constants.

Two conventions the driver must honor:

1. A row for a nonexistent wall time carries the bookkeeping instant of the
   pre gap reading (PEP 495 fold 0), `local: "-"`, `skipped: true`, and
   `skip_reason: "dst_nonexistent"`. On Pacific/Kiritimati that bookkeeping
   instant equals the next day's real occurrence, so expected rows are sorted
   non decreasing, not strictly ascending.
2. Interval expressions (`@every ...`) are pure UTC arithmetic anchored at the
   Unix epoch. They never carry skipped rows, and their local rendering can
   legitimately land on a duplicated or missing wall clock.

## Version notes

Keep the pinned versions aligned with what generated the committed fixtures.
Bump them here in the same commit as a fixture regeneration.

* croniter: 6.2.4
* python: 3.12.3
* tzdata (system, Ubuntu 24.04): 2026c
