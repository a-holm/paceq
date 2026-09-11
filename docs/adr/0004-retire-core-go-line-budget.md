# ADR-0004: Retire the core Go line budget

Status: accepted
Date: 2026-09-11
Decision maker: Johan Holm

## Context

SCOPE.md budgets what paceq is allowed to cost: direct runtime dependencies, binary size, daemon cold start, `paceq status` latency. It also capped core Go, meaning `internal/` and `cmd/` without tests and generated code, at a fixed line count. The rule attached to every budget is that a breach is paid by removing something, never by raising the cap (plans: 10 §7).

The line cap is the one budget that rule cannot be applied to.

- The cap has never been met. Core Go is more than five times the figure.
- Everything the cap stands in for passes on its own terms. Every non-goal in SCOPE.md holds, direct runtime dependencies sit under their cap, and the binary is well under 30 MB.
- Nothing the non-goals forbid is in the tree, so the non-goals yield no removable lines.
- `store`, `cli` and `spec` together are around three times the whole cap on their own. No feature put them there. They arrive with the package layout and with the SQL surface the design already committed to.

Meeting the cap means deleting roughly four fifths of core Go. No version of that removes only waste.

## Decision

The core Go line budget is retired. SCOPE.md carries no cap on the size of the source tree, and no cap on it is written anywhere else.

The alternative was to keep the cap and obey it. That means cutting shipped functionality until the count fits: working code, and the tests that hold it up, deleted to move a number. It is rejected. The cap is a proxy for "paceq stays a small, finished program". What it is a proxy for is stated directly in SCOPE.md and holds today, so satisfying the proxy by making the product smaller would trade the thing for its measurement.

The budget is removed rather than raised. A larger figure is the same decision deferred: unreachable again later, and raised again then, which is what 10 §7 forbids.

What the cap stood in for is governed directly, and each part is enforced:

| What governs | Enforced by |
|---|---|
| The non-goal list in SCOPE.md | the reopening gate in SCOPE.md, applied per request |
| 8 direct runtime dependencies | `TestRuntimeDependencyBudget` in `internal/arch` |
| 30 MB binary | `scripts/cross-build.sh`, on every cross target |

The non-goals are the load-bearing half. They say what paceq refuses to be, they are checked one request at a time, and a tree that keeps all of them cannot grow into a platform whatever it measures.

## Consequences

1. The size of core Go is not gated. Nothing fails when the tree grows, and no line count is reported for it either. Both halves are deliberate: a figure that is printed but not enforced becomes a target again as soon as someone reads it.
2. Growth is caught by what it costs instead. A feature that needs a ninth runtime dependency, that pushes the binary over 30 MB, or that contradicts a non-goal is refused on those grounds, and the refusal names a cost the user can feel.
3. The K4 release check keeps the budgets that remain, and keeps its rule: a breach means something gets removed, not that the budget gets raised.
4. A cap on source size can only come back through a new ADR, and that ADR has to show a figure that is reachable by removing something.
