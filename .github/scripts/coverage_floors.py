#!/usr/bin/env python3
"""Per-package coverage floors: no thin package hides behind the global number.

The global ``fail_under`` gate is a single repo-wide floor, so a small new
package can ship at near-0% coverage while barely moving the total. This
checker closes that gap: it reads a ``coverage json`` report, aggregates
line+branch coverage per top-level ``forze*`` package under ``src/``, and
enforces a floor for every one of them.

Floors live in ``pyproject.toml``:

    [tool.coverage_floors]
    default = 80

    [tool.coverage_floors.exceptions]
    # package = floor, one entry per package that legitimately sits lower today

Every package gets the default floor unless it has an explicit exception entry
— so a brand-new package is gated automatically, which is the point. The
checker also fails when a ``src/`` package produced no coverage data at all
(nothing may be absent from enforcement) and when an exception entry names a
package that no longer exists (the table must not rot).

Calibration note: the floors gate the COMBINED unit+integration coverage that
CI measures (the ``coverage`` job in ``.github/workflows/ci.yml`` combines all
test shards before gating). A unit-only run reports much lower numbers for
integration-heavy packages — do not calibrate floors against it.

Usage (from the repo root, after ``coverage combine``/a coverage run):

    coverage json -o coverage.json
    python .github/scripts/coverage_floors.py
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

# ----------------------- #

_CONFIG_TABLE = "coverage_floors"
_PACKAGE_PREFIX = "forze"


@dataclass(frozen=True)
class PackageCoverage:
    """Aggregated line+branch coverage for one top-level package."""

    covered: int
    total: int

    @property
    def percent(self) -> float:
        if self.total == 0:
            return 100.0
        return 100.0 * self.covered / self.total


@dataclass(frozen=True)
class Floors:
    """Floor policy: a default plus an explicit per-package exceptions table."""

    default: float
    exceptions: dict[str, float]

    def floor_for(self, package: str) -> float:
        return self.exceptions.get(package, self.default)

    def kind_for(self, package: str) -> str:
        return "exception" if package in self.exceptions else "default"


# ----------------------- #


def load_floors(pyproject_path: Path) -> Floors:
    """Read the floor policy from ``[tool.coverage_floors]`` in pyproject.toml."""
    with pyproject_path.open("rb") as fh:
        config = tomllib.load(fh)
    try:
        table = config["tool"][_CONFIG_TABLE]
    except KeyError:
        raise SystemExit(
            f"error: [tool.{_CONFIG_TABLE}] table missing from {pyproject_path}"
        ) from None
    default = float(table["default"])
    exceptions = {name: float(value) for name, value in table.get("exceptions", {}).items()}
    return Floors(default=default, exceptions=exceptions)


def aggregate_packages(coverage_report: dict[str, object]) -> dict[str, PackageCoverage]:
    """Sum covered/total line+branch counts per top-level package under ``src/``.

    Matches coverage.py's own percent definition under ``branch = true``:
    ``(covered_lines + covered_branches) / (num_statements + num_branches)``.
    """
    files = coverage_report.get("files")
    if not isinstance(files, dict):
        raise SystemExit("error: coverage JSON has no 'files' section")

    covered: dict[str, int] = {}
    total: dict[str, int] = {}
    for path, entry in files.items():
        parts = Path(str(path)).as_posix().split("/")
        if "src" not in parts:
            continue
        src_index = parts.index("src")
        if src_index + 1 >= len(parts):
            continue
        package = parts[src_index + 1]
        summary = entry["summary"] if isinstance(entry, dict) else {}
        covered[package] = (
            covered.get(package, 0)
            + int(summary.get("covered_lines", 0))
            + int(summary.get("covered_branches", 0))
        )
        total[package] = (
            total.get(package, 0)
            + int(summary.get("num_statements", 0))
            + int(summary.get("num_branches", 0))
        )
    return {
        package: PackageCoverage(covered=covered[package], total=total[package])
        for package in covered
    }


def discover_src_packages(src_root: Path) -> set[str]:
    """Every top-level ``forze*`` package directory under ``src/``."""
    return {
        entry.name
        for entry in src_root.iterdir()
        if entry.is_dir() and entry.name.startswith(_PACKAGE_PREFIX)
    }


def check_floors(
    measured: dict[str, PackageCoverage],
    floors: Floors,
    src_packages: set[str],
) -> list[str]:
    """Return one violation message per failure; empty means the gate passes."""
    violations: list[str] = []

    for package in sorted(src_packages - measured.keys()):
        violations.append(
            f"{package}: no coverage data recorded — every src/ package must be "
            f"measured and gated (floor {floors.floor_for(package):.1f}%)"
        )

    for package in sorted(floors.exceptions.keys() - src_packages - measured.keys()):
        violations.append(
            f"{package}: stale exceptions entry — package not found under src/; "
            f"delete it from [tool.{_CONFIG_TABLE}.exceptions]"
        )

    for package in sorted(measured):
        coverage = measured[package]
        floor = floors.floor_for(package)
        if coverage.percent < floor:
            violations.append(
                f"{package}: coverage {coverage.percent:.1f}% is below its floor "
                f"{floor:.1f}% ({floors.kind_for(package)} floor)"
            )

    return violations


# ----------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "coverage_json",
        nargs="?",
        default="coverage.json",
        type=Path,
        help="path to a `coverage json` report (default: coverage.json)",
    )
    parser.add_argument(
        "--pyproject",
        default=Path("pyproject.toml"),
        type=Path,
        help="pyproject.toml holding [tool.coverage_floors] (default: pyproject.toml)",
    )
    parser.add_argument(
        "--src-root",
        default=Path("src"),
        type=Path,
        help="source root whose top-level forze* packages are gated (default: src)",
    )
    args = parser.parse_args(argv)

    floors = load_floors(args.pyproject)
    with args.coverage_json.open(encoding="utf-8") as fh:
        report = json.load(fh)
    measured = aggregate_packages(report)
    src_packages = discover_src_packages(args.src_root)

    violations = check_floors(measured, floors, src_packages)

    width = max((len(name) for name in measured), default=0)
    for package in sorted(measured, key=lambda name: measured[name].percent):
        coverage = measured[package]
        floor = floors.floor_for(package)
        marker = "FAIL" if coverage.percent < floor else "ok"
        print(
            f"{package:<{width}}  {coverage.percent:6.1f}%  "
            f"floor {floor:5.1f}% ({floors.kind_for(package)})  {marker}"
        )

    if violations:
        print(f"\nPer-package coverage floors FAILED ({len(violations)} violation(s)):")
        for violation in violations:
            print(f"  - {violation}")
        return 1

    print(f"\nPer-package coverage floors passed for {len(measured)} package(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
