#!/usr/bin/env python3
"""Generate the DST gold standard fixtures for internal/cronx/testdata/golden.

Run this OFFLINE on a developer machine. CI never runs Python and never
touches the network (plan 02 section 7.4, plan 04 section 8 point 3).

The fixtures are an independent opinion about schedule occurrences. They come
from Python zoneinfo (PEP 495 fold semantics) plus croniter, a code line with
nothing in common with the Go iterator they check. Wall clock candidates come
from croniter running on NAIVE datetimes, so the zone database never influences
which wall times are candidates; existence, duplication and policy outcomes are
classified afterwards with explicit round trips through the zone.

Rules bound to this directory:
  * Changing a committed fixture requires a FIXTURE-CHANGE: line with a reason
    in the commit message. A red test is not a reason to edit the expectation;
    it is a reason to investigate. See tools/gen-cron-fixtures/README.md.
  * The generator is deterministic: same inputs, byte identical output. Keys
    are sorted, indentation is fixed, every file ends with one newline.

Usage: gen.py [--out DIR]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from croniter import croniter
    from importlib.metadata import version as pkg_version
except ImportError:
    sys.exit("croniter is required: pip install croniter")


# ---------------------------------------------------------------------------
# Zone classification with explicit round trips. Nothing here trusts the
# constructor's silent normalization: a wall time is counted as real only when
# some instant renders back onto exactly that wall clock.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Wall:
    y: int
    mo: int
    d: int
    h: int
    mi: int
    s: int

    @staticmethod
    def of(naive: datetime) -> "Wall":
        return Wall(naive.year, naive.month, naive.day, naive.hour, naive.minute, naive.second)

    def naive(self) -> datetime:
        return datetime(self.y, self.mo, self.d, self.h, self.mi, self.s)

    def __str__(self) -> str:
        return f"{self.y:04d}-{self.mo:02d}-{self.d:02d} {self.h:02d}:{self.mi:02d}:{self.s:02d}"

    def key(self) -> tuple:
        return (self.y, self.mo, self.d, self.h, self.mi, self.s)


def offsets_around(tz: ZoneInfo, center: datetime) -> list[timedelta]:
    """Distinct UTC offsets in effect within 30 hours either side of center."""
    found: dict[timedelta, bool] = {}
    for hours in range(-30, 31):
        probe = (center + timedelta(hours=hours)).astimezone(tz)
        off = probe.utcoffset()
        assert off is not None
        found[off] = True
    return sorted(found)


def real_instants(tz: ZoneInfo, w: Wall) -> list[datetime]:
    """Every UTC instant whose local rendering is exactly the wall time w."""
    pseudo = w.naive().replace(tzinfo=timezone.utc)
    out: list[datetime] = []
    seen: set[int] = set()
    for off in offsets_around(tz, pseudo):
        cand = pseudo - off
        if Wall.of(cand.astimezone(tz).replace(tzinfo=None)) == w and int(
            cand.timestamp()
        ) not in seen:
            seen.add(int(cand.timestamp()))
            out.append(cand)
    out.sort()
    return out


def classify(tz: ZoneInfo, w: Wall) -> tuple[str, list[datetime]]:
    """normal, duplicate or nonexistent, plus the real instants in order."""
    hits = real_instants(tz, w)
    if len(hits) == 1:
        return ("normal", hits)
    if len(hits) == 2:
        return ("duplicate", hits)
    if len(hits) > 2:
        raise RuntimeError(f"{tz.key} {w}: {len(hits)} instants render one wall clock")
    return ("nonexistent", hits)


def pre_gap_instant(tz: ZoneInfo, w: Wall) -> datetime:
    """The fold=0 reading of a gap time: wall minus the offset before the gap.

    This is the timestamp a skipped row carries. It is bookkeeping, not a real
    instant, and the golden driver treats it as such. On zones with a full day
    missing (Pacific/Kiritimati, 1994-12-31) it can equal the next real
    occurrence, which is why fixture rows are sorted non decreasing.
    """
    pseudo = w.naive().replace(tzinfo=timezone.utc)
    offs = offsets_around(tz, pseudo)
    return pseudo - offs[0]


def shift_target(tz: ZoneInfo, w: Wall) -> datetime:
    """Smallest instant whose local wall clock lies strictly after w.

    This is the spring_forward=shift outcome: run at the first existing local
    moment after the transition, the Vixie-cron-like reading from plan 02
    section 5.3. The wall clock of an instant only ever moves backwards at a
    repeated hour, and no spring window in the corpus contains one, so a plain
    binary search over instants is sound.
    """
    pseudo = w.naive().replace(tzinfo=timezone.utc)

    def wall_after(u: datetime) -> bool:
        return Wall.of(u.astimezone(tz).replace(tzinfo=None)).key() > w.key()

    lo = pseudo - timedelta(hours=26)
    hi = pseudo + timedelta(hours=26)
    if not wall_after(hi):
        raise RuntimeError(f"{tz.key}: no wall clock after {w} found within 26 hours")
    while hi - lo > timedelta(seconds=1):
        mid = lo + (hi - lo) / 2
        if wall_after(mid):
            hi = mid
        else:
            lo = mid
    return hi


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def fmt_offset(off: timedelta) -> str:
    total = int(off.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    return f"{sign}{total // 3600:02d}:{(total % 3600) // 60:02d}"


def local_string(tz: ZoneInfo, instant: datetime) -> str:
    local = instant.astimezone(tz)
    off = local.utcoffset()
    assert off is not None
    return f"{local.strftime('%Y-%m-%d %H:%M:%S')} {fmt_offset(off)}"


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class Policy:
    spring_forward: str  # skip | shift
    fall_back: str  # first | both
    tag: str

    def as_json(self) -> dict[str, str]:
        return {"spring_forward": self.spring_forward, "fall_back": self.fall_back}


SPRING_SKIP = Policy("skip", "first", "skip")
SPRING_SHIFT = Policy("shift", "first", "shift")
FALL_FIRST = Policy("skip", "first", "first")
FALL_BOTH = Policy("skip", "both", "both")
DEFAULT = Policy("skip", "first", "default")


@dataclass
class Row:
    at: str
    local: str
    skipped: bool
    skip_reason: str = ""

    def as_json(self) -> dict:
        out = {"at": self.at, "local": self.local, "skipped": self.skipped}
        if self.skip_reason:
            out["skip_reason"] = self.skip_reason
        return out


# ---------------------------------------------------------------------------
# Candidate generation: croniter on NAIVE datetimes, intervals on the epoch grid
# ---------------------------------------------------------------------------

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_every(expr: str) -> timedelta | None:
    m = re.fullmatch(r"@every (\d+)([smh])", expr.strip())
    if not m:
        return None
    n = int(m.group(1))
    unit = {"s": 1, "m": 60, "h": 3600}[m.group(2)]
    return timedelta(seconds=n * unit)


def cron_walls(expr: str, from_utc: datetime, to_utc: datetime) -> list[Wall]:
    """Wall clock candidates from croniter with the zone removed from the loop."""
    # Start 48 hours before the window expressed as a pseudo local time. Cron
    # expressions in the corpus repeat daily or finer, so the margin always
    # covers the first hit inside the window.
    start_naive = (from_utc - timedelta(hours=48)).replace(tzinfo=None)
    it = croniter(expr, start_naive)
    out: list[Wall] = []
    limit = (to_utc + timedelta(hours=48)).replace(tzinfo=None)
    while True:
        naive: datetime = it.get_next(datetime)
        if naive > limit:
            break
        out.append(Wall.of(naive))
    return out


def interval_rows(expr: str, tz: ZoneInfo, from_utc: datetime, to_utc: datetime) -> list[Row]:
    dur = parse_every(expr)
    if dur is None:
        raise RuntimeError(f"not an interval expression: {expr!r}")
    # Pure UTC arithmetic anchored at the epoch, so the occurrence set depends
    # on nothing but the window. Interval schedules never skip.
    step = dur.total_seconds()
    k = int((from_utc - EPOCH).total_seconds() // step) + 1
    rows: list[Row] = []
    while True:
        instant = EPOCH + dur * k
        if instant > to_utc:
            break
        rows.append(Row(at=iso(instant), local=local_string(tz, instant), skipped=False))
        k += 1
    return rows


def cron_rows(expr: str, tz: ZoneInfo, policy: Policy, from_utc: datetime,
              to_utc: datetime) -> list[Row]:
    rows: list[Row] = []
    shadowed: set[int] = set()

    def emit(instant: datetime, skipped: bool = False, reason: str = "",
             hidden_local: bool = False) -> None:
        if not (from_utc < instant <= to_utc):
            return
        if hidden_local:
            # A gap wall never happened, so it has no local rendering.
            rows.append(Row(at=iso(instant), local="-", skipped=True, skip_reason=reason))
        else:
            rows.append(Row(at=iso(instant), local=local_string(tz, instant),
                            skipped=skipped, skip_reason=reason))

    for w in cron_walls(expr, from_utc, to_utc):
        kind, hits = classify(tz, w)
        if kind == "nonexistent":
            if policy.spring_forward == "shift":
                # Run after the transition. Two gap walls inside one gap land
                # on the same instant; the later wall is shadowed and the
                # fixture keeps one row per distinct instant.
                target = shift_target(tz, w)
                if int(target.timestamp()) in shadowed:
                    continue
                shadowed.add(int(target.timestamp()))
                emit(target)
            else:
                emit(pre_gap_instant(tz, w), True, "dst_nonexistent", hidden_local=True)
        elif kind == "duplicate":
            first, second = hits
            emit(first)
            emit(second, policy.fall_back != "both",
                 "" if policy.fall_back == "both" else "dst_duplicate")
        else:
            emit(hits[0])
    rows.sort(key=lambda r: r.at)
    return rows


def build_fixture(fid: str, expr: str, tz_name: str, policy: Policy, from_utc: datetime,
                  to_utc: datetime, generated_by: str) -> dict:
    tz = ZoneInfo(tz_name)
    if parse_every(expr):
        expected = interval_rows(expr, tz, from_utc, to_utc)
    else:
        expected = cron_rows(expr, tz, policy, from_utc, to_utc)
    return {
        "id": fid,
        "expr": expr,
        "tz": tz_name,
        "policy": policy.as_json(),
        "from_utc": iso(from_utc),
        "to_utc": iso(to_utc),
        "generated_by": generated_by,
        "expected": [r.as_json() for r in expected],
    }


# ---------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------

_TZPKG = subprocess.run(
    ["dpkg-query", "-W", "-f=${Version}", "tzdata"], capture_output=True, text=True
).stdout.strip()

GENERATED_BY = (
    f"croniter {pkg_version('croniter')} / python {sys.version.split()[0]}"
    f" / tzdata {_TZPKG.split('-')[0]}"
)

WINDOWS = {
    "oslo-spring-202603": ("Europe/Oslo", "2026-03-27T00:00:00Z", "2026-03-31T00:00:00Z"),
    "oslo-fall-202610": ("Europe/Oslo", "2026-10-23T00:00:00Z", "2026-10-27T00:00:00Z"),
    "santiago-spring-202609": ("America/Santiago", "2026-09-04T00:00:00Z", "2026-09-08T00:00:00Z"),
    "santiago-fall-202604": ("America/Santiago", "2026-04-03T00:00:00Z", "2026-04-07T00:00:00Z"),
    "lord-howe-spring-202610": ("Australia/Lord_Howe", "2026-10-02T00:00:00Z", "2026-10-06T00:00:00Z"),
    "lord-howe-fall-202604": ("Australia/Lord_Howe", "2026-04-03T00:00:00Z", "2026-04-07T00:00:00Z"),
    "kolkata-202601": ("Asia/Kolkata", "2026-01-15T00:00:00Z", "2026-01-20T00:00:00Z"),
    "utc-oslowindow-202603": ("UTC", "2026-03-27T00:00:00Z", "2026-03-31T00:00:00Z"),
    "kiritimati-1994": ("Pacific/Kiritimati", "1994-12-28T00:00:00Z", "1995-01-03T00:00:00Z"),
}

EXPR_SLUGS = {
    "0 2 * * *": "daily-0200",
    "30 2 * * *": "daily-0230",
    "*/30 * * * *": "every30m",
    "@every 90m": "every90m",
    "0 0 * * *": "daily-0000",
    "30 23 * * *": "daily-2330",
    "45 9 * * *": "daily-0945",
    "0 12 * * *": "daily-1200",
    "0 6 * * *": "daily-0600",
    "30 12 * * *": "daily-1230",
    "45 1 * * *": "daily-0145",
}

# (window, expr, policy)
CORPUS: list[tuple[str, str, Policy]] = []

for _expr in ["0 2 * * *", "30 2 * * *", "*/30 * * * *"]:
    CORPUS += [
        ("oslo-spring-202603", _expr, SPRING_SKIP),
        ("oslo-spring-202603", _expr, SPRING_SHIFT),
    ]
CORPUS.append(("oslo-spring-202603", "@every 90m", DEFAULT))

for _expr in ["0 2 * * *", "30 2 * * *", "*/30 * * * *"]:
    CORPUS += [
        ("oslo-fall-202610", _expr, FALL_FIRST),
        ("oslo-fall-202610", _expr, FALL_BOTH),
    ]
CORPUS.append(("oslo-fall-202610", "@every 90m", DEFAULT))

for _expr in ["0 0 * * *", "*/30 * * * *"]:
    CORPUS += [
        ("santiago-spring-202609", _expr, SPRING_SKIP),
        ("santiago-spring-202609", _expr, SPRING_SHIFT),
    ]

for _expr in ["30 23 * * *", "*/30 * * * *"]:
    CORPUS += [
        ("santiago-fall-202604", _expr, FALL_FIRST),
        ("santiago-fall-202604", _expr, FALL_BOTH),
    ]

for _expr in ["0 2 * * *", "*/30 * * * *"]:
    CORPUS += [
        ("lord-howe-spring-202610", _expr, SPRING_SKIP),
        ("lord-howe-spring-202610", _expr, SPRING_SHIFT),
    ]

for _expr in ["45 1 * * *", "*/30 * * * *"]:
    CORPUS += [
        ("lord-howe-fall-202604", _expr, FALL_FIRST),
        ("lord-howe-fall-202604", _expr, FALL_BOTH),
    ]

CORPUS += [
    ("kolkata-202601", "45 9 * * *", DEFAULT),
    ("kolkata-202601", "0 12 * * *", DEFAULT),
    ("utc-oslowindow-202603", "0 2 * * *", DEFAULT),
    ("utc-oslowindow-202603", "*/30 * * * *", DEFAULT),
    ("kiritimati-1994", "0 6 * * *", DEFAULT),
    ("kiritimati-1994", "30 12 * * *", DEFAULT),
]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    here = Path(__file__).resolve()
    default_out = here.parents[2] / "internal" / "cronx" / "testdata" / "golden"
    ap.add_argument("--out", type=Path, default=default_out)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    written = []
    for window, expr, policy in CORPUS:
        tz_name, from_s, to_s = WINDOWS[window]
        from_utc = datetime.fromisoformat(from_s.replace("Z", "+00:00"))
        to_utc = datetime.fromisoformat(to_s.replace("Z", "+00:00"))
        slug = EXPR_SLUGS.get(expr.strip())
        if slug is None:
            raise RuntimeError(f"no slug for expression {expr!r}")
        fid = f"{window}-{slug}-{policy.tag}"
        fx = build_fixture(fid, expr.strip(), tz_name, policy, from_utc, to_utc, GENERATED_BY)
        path = args.out / f"{fid}.json"
        text = json.dumps(fx, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
        path.write_text(text, encoding="utf-8")
        written.append(path.name)

    print(f"wrote {len(written)} fixtures to {args.out}")
    for name in sorted(written):
        print(f"  {name}")


if __name__ == "__main__":
    main()
