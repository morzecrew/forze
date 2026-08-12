#!/usr/bin/env python3
"""The nightly DST matrix: flagship scenarios × fault profiles × a seed band no PR can afford.

The merge guard runs a handful of seeds on every build, which is the right size for a gate that
has to finish before a human waits on it: 8 dlock seeds and 12 HLC seeds, about a twentieth of
a second of searching. `just fuzz` widens that to 64 and 128, but it is `fuzz`-marked and so
excluded from the default suite — it runs when someone asks. Neither is a size at which a rare
interleaving turns up; the seeds a merge guard can afford are the ones a bug is least likely to
hide in.

This is the other half — the same scenarios, the same invariants, run overnight across a band
four orders of magnitude wider and across several fault profiles instead of one. Nothing here
is a new oracle; it is the existing one, given time.

Three modes, and they are deliberately split by what they need to import:

    # what cells exist — derived from the profiles, never hand-listed
    python .github/scripts/dst_nightly.py --matrix

    # run one cell (needs the dev environment: forze_dst, forze_mock, tests/)
    python .github/scripts/dst_nightly.py --cell dlock-storm --seeds 8192 --out cell.json

    # union the cells and gate (standard library only — cannot fail for want of an extra)
    python .github/scripts/dst_nightly.py --verdict results/ --expect dlock-storm,hlc

The verdict is where the gate lives, and it fails on four things:

1. a declared cell has no result file — the shard died, or the matrix and the declaration drifted;
2. a cell ran zero seeds — an empty band satisfies every other check while proving nothing;
3. any seed violated an invariant — printed with the tuple to append so it is re-checked forever;
4. a cell's declared reachability targets were not all driven — a green band that never raced is
   not evidence, and this is the check that says so.

Rule 2 is the one worth stating out loud. Every other check reads "no bad thing happened",
which an empty run satisfies perfectly; the run count is what separates "searched and found
nothing" from "did not search".
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

# ----------------------- #

HLC_CELL = "hlc"
"""The HLC scenario's single cell. It is register-based — no ports, so no surface for a
partition or a fault to act on — which is why it has no fault-profile axis. Its variation
comes from the schedule seed alone."""

DEFAULT_SEEDS = 65536
"""Seeds per cell when none is given.

Measured at ~0.85 ms a seed through the process pool (linear from 256 to 65,536), so a cell is
about a minute of work locally and a few minutes on a four-core runner — cheap for a job nobody
waits on. That is 1024× the `just fuzz` band and 8192× the merge guard's, which is the point: a
nightly running a band a PR could have run is a nightly that finds what the PR already found."""


# ....................... #


@dataclass(frozen=True)
class Cell:
    """One unit of nightly work: a scenario in one environment, run over its own seed band."""

    name: str
    scenario: str
    profile: str | None
    targets: tuple[str, ...]


# ....................... #


@dataclass(frozen=True)
class CellResult:
    """What one cell produced — the shape written to disk and read back by the verdict."""

    cell: str
    seeds: int
    violations: tuple[int, ...]
    reached: dict[str, int]
    targets: tuple[str, ...]
    behaviors: int
    wall_seconds: float

    # ....................... #

    @property
    def unreached(self) -> tuple[str, ...]:
        """Declared targets no run in this cell ever drove."""

        return tuple(sorted(t for t in self.targets if self.reached.get(t, 0) <= 0))


# ----------------------- #
# The cell declaration. Derived from the profiles rather than written out, so a new profile is a
# new nightly cell without anyone remembering to add one — the same reason the conformance
# ratchet derives its engines from the packages instead of trusting a list.


def declared_cells() -> tuple[Cell, ...]:
    """Every cell the nightly runs, derived from the flagship module's own declarations."""

    from tests.support.dst_flagship import DLOCK_PROFILES, HLC_TARGETS

    dlock = tuple(
        Cell(
            name=f"dlock-{profile.name}",
            scenario="dlock",
            profile=profile.name,
            targets=tuple(sorted(profile.targets)),
        )
        for profile in DLOCK_PROFILES.values()
    )

    return (*dlock, Cell(name=HLC_CELL, scenario="hlc", profile=None, targets=tuple(sorted(HLC_TARGETS))))


# ....................... #


def run_cell(name: str, *, seeds: int) -> CellResult:
    """Run one cell's whole band and fold it into a result."""

    from forze_dst.artifacts.sweep import parallel_sweep
    from tests.support.dst_flagship import dlock_target, run_hlc_seed

    cell = next((entry for entry in declared_cells() if entry.name == name), None)

    if cell is None:
        raise SystemExit(f"unknown cell {name!r}; known: {', '.join(c.name for c in declared_cells())}")

    if seeds <= 0:
        raise SystemExit(f"cell {name!r} was asked for {seeds} seeds; a band must not be empty")

    target = run_hlc_seed if cell.scenario == "hlc" else dlock_target(str(cell.profile))

    started = time.perf_counter()
    result = parallel_sweep(target, tuple(range(seeds)))

    return CellResult(
        cell=cell.name,
        seeds=result.runs,
        violations=tuple(result.violations),
        reached=dict(result.reached_runs),
        targets=cell.targets,
        behaviors=len(result.behaviors),
        wall_seconds=time.perf_counter() - started,
    )


# ----------------------- #
# The verdict. Standard library only — it reads JSON and nothing else, so the gate runs on a bare
# interpreter and cannot fail because an extra did not install.


def load_results(directory: Path) -> dict[str, CellResult]:
    """Read every cell result written under *directory*."""

    results: dict[str, CellResult] = {}

    for path in sorted(directory.rglob("*.json")):
        try:
            payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            # A truncated upload is a broken night, not a clean one — but the bare decode error
            # names no file, which is the one thing whoever reads this at 09:00 needs.
            raise SystemExit(f"cell result {path.name} could not be read: {error}") from error

        result = CellResult(
            cell=str(payload["cell"]),
            seeds=int(payload["seeds"]),
            violations=tuple(int(seed) for seed in payload.get("violations", ())),
            reached={str(k): int(v) for k, v in payload.get("reached", {}).items()},
            targets=tuple(str(t) for t in payload.get("targets", ())),
            behaviors=int(payload.get("behaviors", 0)),
            wall_seconds=float(payload.get("wall_seconds", 0.0)),
        )
        results[result.cell] = result

    return results


# ....................... #


def check_verdict(expected: tuple[str, ...], results: dict[str, CellResult]) -> list[str]:
    """The gate rules, as a list of violations (empty when the night was clean)."""

    violations: list[str] = []

    # Requiring nothing is the loop's own empty case, and it passes every rule below by never
    # entering them — the same vacuous green as a cell that ran zero seeds, one level up. Decide
    # it here rather than in the caller, so a second caller cannot inherit the hole.
    if not expected:
        return ["no cells were required to run, so nothing about tonight was checked"]

    for name in expected:
        result = results.get(name)

        if result is None:
            violations.append(
                f"{name}: no result — the shard never reported, so this cell did not run tonight",
            )
            continue

        # An empty band passes every check below it. Say so before anything else reads as green.
        if result.seeds <= 0:
            violations.append(f"{name}: ran 0 seeds — an empty band proves nothing")
            continue

        if result.violations:
            listed = ", ".join(str(seed) for seed in result.violations[:8])
            more = "" if len(result.violations) <= 8 else f" (+{len(result.violations) - 8} more)"
            violations.append(
                f"{name}: {len(result.violations)} seed(s) tripped an invariant: {listed}{more}"
                f" — append them to the scenario's *_REGRESSION_SEEDS so they are re-checked forever",
            )

        if result.unreached:
            violations.append(
                f"{name}: never reached {', '.join(result.unreached)} across {result.seeds} seeds"
                " — the band went green without driving the state it exists to drive",
            )

    unexpected = sorted(set(results) - set(expected))

    if unexpected:
        violations.append(
            f"results arrived for undeclared cell(s): {', '.join(unexpected)}"
            " — the matrix and the cell declaration disagree",
        )

    return violations


# ....................... #


def render(expected: tuple[str, ...], results: dict[str, CellResult]) -> str:
    """A markdown table of the night, for the workflow's job summary."""

    lines = [
        "| cell | seeds | violations | behaviours | targets reached | wall |",
        "| --- | --: | --: | --: | --- | --: |",
    ]

    for name in expected:
        result = results.get(name)

        if result is None:
            lines.append(f"| `{name}` | — | — | — | **no result** | — |")
            continue

        reached = ", ".join(
            f"{target} {result.reached.get(target, 0)}/{result.seeds}" for target in result.targets
        )
        violated = "0" if not result.violations else f"**{len(result.violations)}**"
        lines.append(
            f"| `{name}` | {result.seeds} | {violated} | {result.behaviors} |"
            f" {reached or '—'} | {result.wall_seconds:.0f}s |",
        )

    return "\n".join(lines)


# ----------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", action="store_true", help="print the declared cells as JSON")
    parser.add_argument("--cell", help="run this cell's band")
    parser.add_argument("--seeds", type=int, default=DEFAULT_SEEDS, help="seeds per cell")
    parser.add_argument("--out", type=Path, help="where to write the cell result")
    parser.add_argument("--verdict", type=Path, help="directory of cell results to gate")
    parser.add_argument("--expect", default="", help="comma-separated cells the verdict requires")
    args = parser.parse_args(argv)

    # `tests.support` is importable only with the repo root on the path, which pytest arranges and
    # a bare `python .github/scripts/...` does not.
    root = str(Path(__file__).resolve().parents[2])

    if root not in sys.path:
        sys.path.insert(0, root)

    if args.matrix:
        print(json.dumps([{"cell": cell.name} for cell in declared_cells()]))
        return 0

    if args.cell:
        result = run_cell(args.cell, seeds=args.seeds)
        payload = json.dumps(asdict(result), indent=2)

        if args.out:
            args.out.write_text(payload, encoding="utf-8")

        print(payload)
        return 0

    if args.verdict is None:
        parser.error("pass --matrix, --cell or --verdict")

    expected = tuple(name for name in args.expect.split(",") if name)
    results = load_results(args.verdict)
    violations = check_verdict(expected, results)

    print(f"### DST nightly matrix\n\n{render(expected, results)}")

    if violations:
        print(f"\nNightly DST matrix FAILED ({len(violations)} violation(s)):")

        for violation in violations:
            print(f"  - {violation}")

        return 1

    total = sum(result.seeds for result in results.values())
    print(f"\nNightly DST matrix passed: {len(expected)} cell(s), {total} seeds, no violations.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
