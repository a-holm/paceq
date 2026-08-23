# DST gold standard: seams that wait for M2-03

Issue #52 ships the gold standard data and its guards now. The suites that
drive `internal/cronx` itself need the iterator from issue #47, which was not
merged when this was written (no branch, no commits past main at the time of
work). This note pins exactly what waits, so finishing is a mechanical step.

## The interface this work already codes against

From the issue #47 body, unchanged here:

```go
func Parse(expr string) (Schedule, error)

func (s Schedule) Next(from time.Time, tz *time.Location, p Policy) (Occurrence, error)
func (s Schedule) Prev(from time.Time, tz *time.Location, p Policy) (Occurrence, error)
func (s Schedule) Between(from, to time.Time, tz *time.Location, p Policy) ([]Occurrence, error)
```

`Between` is half open `(from, to]`, ordered by the schedule's local timetable
(UTC instants rise except across DST seams; see "Chaining Next across a DST
seam" in the cronx package doc), and includes rows with
`Skipped: true`. Policy strings in fixtures map onto the constants:
`skip`/`shift` for spring forward, `first`/`both` for fall back. Occurrence
fields used by the driver: `At`, `LocalWall`, `Skipped`, `SkipReason`.

## Ready to write the moment #47 lands

1. **Golden driver** (`internal/cronx/golden_test.go`). For every fixture:
   parse, load zone, call `Between(FromUTC, ToUTC, tz, policy)`, normalize to
   `{at, local, skipped, skip_reason}` rows, compare exact against
   `expected`. Failure text must name the fixture, point at
   `tools/gen-cron-fixtures/gen.py`, and state the `FIXTURE-CHANGE:` rule.
   The harness generators (`randomCronExprs`, `partitionCuts`) and the
   contract tests are already merged; the driver only adds the comparison.

2. **Differential test** (`internal/cronx/differential_test.go`). 10000
   expressions from `randomCronExprs(rand.New(rand.NewPCG(0xC0FFEE, 0xCAFE)),
   10000)`, run in UTC only, compared against raw
   `gronx.NextTickAfter(expr, from, false)` from
   `github.com/adhocore/gronx`. Disagreement about validity fails; both nil
   but different instants fails. gronx carries an ADR-0001 runtime row
   already; adding it to go.mod is #47's change, and this test imports it
   once that lands.

3. **Determinism, G9** (`internal/cronx/determinism_test.go`). For corpus
   windows: union of `Between` over pieces cut by `partitionCuts` must equal
   whole-window `Between` row for row, skipped rows included. At least 1000
   iterations per expression over the fixture zones, fixed seed. If rapid
   (`pgregory.net/rapid`, ADR-0001 test row) is preferred over a plain loop,
   pin `-rapid.seed` so runs stay reproducible.

4. **Clock hop test** (`internal/cronx/clockhop_test.go`). Two fake clocks
   from `internal/clock.NewFake` at different uptimes materialize the same
   schedule window while jumping plus or minus one hour, plus or minus 25
   hours, and small NTP-like steps mid window. The tick sets must be
   identical, which is G9 stated as a daemon property. Purely a Between
   consumer; no new clock API needed.

5. **Mutation proofs on the iterator**, the four named in the issue:
   fall_back first swapped to last (Oslo October red), nonexistent row dropped
   (Oslo March red), `time.Hour` hardcoded in duplicate detection (Lord Howe
   red), local time stored as scheduled_for (every non UTC zone red). Each
   mutant compiles, lands, is reachable through `Between`, and ends with
   `git checkout --`.

## Contract questions to reconcile with #47 before the driver goes green

These are decisions the fixtures have taken where the issue texts left room.
If #47 decides differently, regenerate or adjust with `FIXTURE-CHANGE:` and a
reason, never silently:

1. **Shift target.** `spring_forward=shift` runs at the first existing wall
   clock after the transition, Vixie style. Oslo `30 2 * * *` therefore runs
   at 03:00 CEST (01:00Z), not at 03:30. Several gap walls collapsing into
   one instant produce ONE row, the earliest; shadowed walls are not rows.
2. **Skipped row timestamps.** A `dst_nonexistent` row keeps the pre gap
   reading of the wall time (PEP 495 fold 0). On Pacific/Kiritimati that
   instant equals the next real occurrence, so expected rows sort non
   decreasing rather than strictly ascending, and `LocalWall` is `-`.
3. **Interval anchoring.** `@every 90m` is pure UTC arithmetic anchored at
   the Unix epoch, independent of the caller's window. If the iterator anchors
   differently (for example at window start), the two `every90m` fixtures are
   the ones to revisit.
4. **Dom and dow.** The differential corpus never restricts both day fields,
   because union versus intersection semantics differ across classic engines.
   Whichever reading #47 documents becomes the contract; the restriction keeps
   the differential out of that debate.

## Deliberately not done in this pass

* Nightly random seed variant (100000 expressions): needs the differential
  test to exist first; add a scheduled workflow then.
* Performance budget under 30 seconds: measurable once the full suite exists.
* Deleting-a-skipped-row mutants: invisible until the driver compares full
  sequences; the contract tests see structure and categories, not counts.
