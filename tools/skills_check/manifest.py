"""The coverage doctrine manifest, and the rules that keep it from rotting.

A manifest is a hand-maintained list, which is the shape most likely to drift away from
what it describes. Three properties stop that here, and each is enforced rather than
asked for:

**Every extra is classified.** An extra appears in exactly one of `subdivides`,
`whole-package` or `dependency-only`. One appearing in none fails the load — which is how
a newly added extra becomes impossible to ignore rather than silently absent from the
denominator.

**Every mapped unit is importable.** A row naming ``forze_kms.gcp`` fails if that module
does not exist, so a rename in ``src/`` breaks this file loudly instead of quietly dropping
a unit. Without it a plausible guess becomes a phantom unit that always reports covered
because nothing ever checks it.

**The doctrine map is total.** Every derived unit carries a doctrine and every doctrine row
names a derived unit, both directions. A unit with no doctrine is an error, not a default:
that is the mechanism by which a new plane cannot ship with zero corpus reach *and* zero
decision.
"""

from __future__ import annotations

import importlib.util
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

# ----------------------- #

MANIFEST_FILENAME = "coverage.toml"

Doctrine = Literal["D1", "D2", "D3", "D4"]

DOCTRINES: frozenset[str] = frozenset({"D1", "D2", "D3", "D4"})

PROVEN: frozenset[str] = frozenset({"D1", "D2"})
"""Doctrines whose unit must be reached by an import the gate resolves.

D1 and D2 differ in how much surrounding material is expected — a worked deps-module
block versus a bare anchor — never in whether the import is *verified*. Letting D2 pass on
unresolvable text would contradict the consumption rule the whole census rests on.
"""

NEEDS_RATIONALE: frozenset[str] = frozenset({"D3", "D4"})
NEEDS_TRIGGER: frozenset[str] = frozenset({"D4"})


@dataclass(frozen=True)
class Unit:
    """One census unit and the doctrine somebody assigned it."""

    name: str
    doctrine: Doctrine
    rationale: str = ""
    trigger: str = ""

    @property
    def must_be_proven(self) -> bool:
        return self.doctrine in PROVEN


@dataclass
class Manifest:
    """The parsed manifest, plus whatever refused to validate."""

    units: tuple[Unit, ...] = ()
    subdivides: dict[str, str] = field(default_factory=dict)
    violations: list[str] = field(default_factory=list)

    @property
    def proven(self) -> tuple[Unit, ...]:
        return tuple(unit for unit in self.units if unit.must_be_proven)


# ----------------------- #


def default_manifest_path() -> Path:
    return Path(__file__).with_name(MANIFEST_FILENAME)


def derive_units(packages: frozenset[str], subdivides: dict[str, str], merit: set[str]) -> set[str]:
    """The unit list: every wheel package, plus every boundary an extra draws inside one.

    Subdivision **adds**; it never replaces the root. A package can gain an uncovered
    submodule while its root stays green on some other submodule's import, which is the
    precise failure subdivision exists to catch — dropping the root would trade one blind
    spot for another.
    """
    return set(packages) | set(subdivides.values()) | merit


def load_manifest(path: Path, packages: frozenset[str], extras: frozenset[str]) -> Manifest:
    """Read the manifest and validate it against the two authoritative lists."""
    manifest = Manifest()

    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        manifest.violations.append(f"{path}: unreadable manifest ({type(error).__name__}: {error})")
        return manifest

    extras_table = raw.get("extras", {})
    subdivides: dict[str, str] = dict(extras_table.get("subdivides", {}))
    whole = list(extras_table.get("whole-package", {}).get("names", []))
    dependency_only = list(extras_table.get("dependency-only", {}).get("names", []))

    manifest.subdivides = subdivides
    manifest.violations.extend(_check_extras(path, extras, subdivides, whole, dependency_only))
    manifest.violations.extend(_check_importable(path, subdivides))

    merit = set(raw.get("extra-units", {}))
    declared = raw.get("units", {})
    manifest.units = tuple(_read_units(path, declared, manifest.violations))
    manifest.violations.extend(
        _check_totality(path, derive_units(packages, subdivides, merit), manifest.units)
    )

    return manifest


# ----------------------- #


def _check_extras(
    path: Path,
    extras: frozenset[str],
    subdivides: dict[str, str],
    whole: list[str],
    dependency_only: list[str],
) -> list[str]:
    """Rule 1 — every extra classified exactly once."""
    violations: list[str] = []
    seen: dict[str, list[str]] = {}

    for label, names in (
        ("subdivides", list(subdivides)),
        ("whole-package", whole),
        ("dependency-only", dependency_only),
    ):
        for name in names:
            seen.setdefault(name, []).append(label)

    for extra in sorted(extras - set(seen)):
        suggestion = f"forze_{extra.replace('-', '_')}"
        violations.append(
            f"{path}: extra `{extra}` is in no table — map it to a census unit under "
            f"[extras.subdivides] (perhaps `{suggestion}`, but check: the convention is a "
            f"suggestion, not the answer), or record it under [extras.whole-package] or "
            f"[extras.dependency-only]"
        )

    for name in sorted(set(seen) - extras):
        violations.append(f"{path}: `{name}` is not an extra in pyproject.toml")

    violations.extend(
        f"{path}: extra `{name}` appears in {len(tables)} tables ({', '.join(tables)}) — "
        "each extra is classified exactly once"
        for name, tables in sorted(seen.items())
        if len(tables) > 1
    )

    return violations


def _check_importable(path: Path, subdivides: dict[str, str]) -> list[str]:
    """Rule 2 — every mapped unit resolves to a real module."""
    violations: list[str] = []

    for extra, unit in sorted(subdivides.items()):
        try:
            found = importlib.util.find_spec(unit) is not None
        except (ImportError, ValueError):
            found = False

        if not found:
            violations.append(
                f"{path}: extra `{extra}` maps to `{unit}`, which does not import — "
                "a renamed module must break this file rather than silently drop a unit"
            )

    return violations


def _read_units(path: Path, declared: dict[str, object], violations: list[str]) -> list[Unit]:
    units: list[Unit] = []

    for name, body in sorted(declared.items()):
        if not isinstance(body, dict):
            violations.append(f"{path}: unit `{name}` is not a table")
            continue

        doctrine = str(body.get("doctrine", ""))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]

        if doctrine not in DOCTRINES:
            violations.append(
                f"{path}: unit `{name}` has doctrine `{doctrine or '(none)'}` — "
                f"one of {', '.join(sorted(DOCTRINES))}"
            )
            continue

        rationale = str(body.get("rationale", ""))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        trigger = str(body.get("trigger", ""))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]

        if doctrine in NEEDS_RATIONALE and not rationale:
            violations.append(f"{path}: `{name}` is {doctrine} and carries no rationale")

        if doctrine in NEEDS_TRIGGER and not trigger:
            violations.append(
                f"{path}: `{name}` is {doctrine} and carries no trigger — a deferral with "
                "no condition that ends it is an omission wearing a doctrine"
            )

        units.append(Unit(name=name, doctrine=doctrine, rationale=rationale, trigger=trigger))  # type: ignore[arg-type]

    return units


def _check_totality(path: Path, derived: set[str], units: tuple[Unit, ...]) -> list[str]:
    """The doctrine map covers the derived list exactly, in both directions."""
    named = {unit.name for unit in units}

    return [
        f"{path}: census unit `{name}` has no doctrine — assign one (a new package or a "
        "new extra boundary is a decision, not a default)"
        for name in sorted(derived - named)
    ] + [
        f"{path}: `{name}` carries a doctrine but is not a census unit"
        for name in sorted(named - derived)
    ]
