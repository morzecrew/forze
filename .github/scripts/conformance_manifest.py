#!/usr/bin/env python3
"""Conformance-leg ratchet: no plane ships an engine without a differential leg.

DST's oracle horizon is the mock port. Six differential legs now close that horizon
for real backends — but "a leg exists" was, until this checker, a fact recorded only
in prose. A plane could gain a backend, or lose a leg to a refactor, and CI would stay
green: the credentials plane shipped exactly that way, with a Postgres-only test and no
mock comparison, and nothing noticed.

This is the same machine as ``coverage_floors.py``, pointed at conformance instead of
coverage. It reads the code rather than a promise:

- every ``DepKey`` declared under ``contracts/`` is claimed by a manifested plane, a
  declared gap, or a categorised exemption — a new port cannot be silently unclaimed;
- every manifested ``(plane, engine)`` pair is backed by a test pytest can actually
  collect, carrying ``@pytest.mark.conformance(plane=…, engine=…)``;
- every plane's shared scenario still imports and resolves;
- **the ratchet proper**: the set of engines that *register* a plane's dep keys is
  derived from the integration packages themselves, and an engine that registers one
  without a leg fails the build. A new ``forze_<name>`` backend cannot merge as a
  "the route works" claim.

The manifest lives in ``pyproject.toml``:

    [tool.conformance_manifest.planes.counter]
    scenario = "tests.support.counter_conformance:COUNTER_BATTERY"
    dep_keys = ["counter", "counter_admin"]
    engines  = ["mock", "postgres", "redis", "mongo", "firestore"]

    [tool.conformance_manifest.gaps.queue]
    dep_keys = ["queue_command", "queue_query"]
    engines  = ["rabbitmq", "sqs"]
    reason   = "…"

    [tool.conformance_manifest.exemptions]
    cache = { kind = "single-engine", reason = "…" }

Collection proves a leg exists; it does not prove it ran. A leg whose engine never came up,
whose extra is missing, or whose whole suite is absent from CI's matrix skips quietly and
leaves the build green — which is how two of the four inference legs went un-run. So there
is a second, post-run mode: each shard writes its own outcome file and they are unioned at
the end, where a leg that passed nothing anywhere fails.

Usage (from the repo root):

    # before the run — is every manifested leg still there?
    pytest tests --collect-only -q -m conformance --conformance-census census.json
    python .github/scripts/conformance_manifest.py census.json

    # or let the checker run collection itself
    python .github/scripts/conformance_manifest.py --collect

    # after the run — did every manifested leg actually pass something?
    pytest <shard> --conformance-executed conformance-<shard>.json
    python .github/scripts/conformance_manifest.py --executed conformance-*.json
"""

from __future__ import annotations

import argparse
import ast
import importlib
import json
import pkgutil
import subprocess  # nosec B404 — fixed argv, no shell, used only to run pytest collection
import sys
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ----------------------- #

_CONFIG_TABLE = "conformance_manifest"
_PACKAGE_PREFIX = "forze_"
_CONTRACTS_PACKAGE = "forze.application.contracts"
_MOCK_ENGINE = "mock"

_SINGLE_ENGINE = "single-engine"
_NO_ENGINE_MATRIX = "no-engine-matrix"
_CONFIG_VALUE = "config-value"
_EXEMPTION_KINDS = frozenset({_SINGLE_ENGINE, _NO_ENGINE_MATRIX, _CONFIG_VALUE})

_MIN_REASON_LENGTH = 40

# Call shapes that *read* a dependency key rather than register one. A key referenced
# only through these is a consumer (every field-encrypting adapter resolves the keyring
# key, and that must not make fifteen packages look like keyring providers). Anything
# else counts as registration: the bias is deliberate, since over-reporting costs a
# visible waiver while under-reporting lets a new backend slip past the ratchet.
_CONSUMPTION_CALLS = frozenset(
    {
        "provide",
        "provide_routed",
        "exists",
        "resolve",
        "resolve_configurable",
        "_resolve_configurable",
    }
)


# ----------------------- #


@dataclass(frozen=True)
class Plane:
    """One conformance plane: a shared scenario, the keys it covers, the legs it needs."""

    name: str
    scenario: str
    dep_keys: tuple[str, ...]
    engines: tuple[str, ...]
    waivers: dict[str, str]
    """Engine → why this provider legitimately has no leg. Reviewed, printed, never silent."""


@dataclass(frozen=True)
class Gap:
    """A plane with providers but no leg yet — declared, so it cannot be mistaken for covered."""

    name: str
    dep_keys: tuple[str, ...]
    engines: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class Exemption:
    """A contract key with no differential to run, and the checked claim that says why."""

    kind: str
    reason: str


@dataclass(frozen=True)
class Manifest:
    planes: dict[str, Plane]
    gaps: dict[str, Gap]
    exemptions: dict[str, Exemption]
    engine_packages: dict[str, str]
    """Engine name → the distribution package that provides it (default ``forze_<engine>``)."""

    divergence_catalog: str | None

    def claims(self) -> dict[str, list[str]]:
        """Dep-key name → every manifest entry claiming it (>1 means the table contradicts itself)."""

        claimed: dict[str, list[str]] = {}

        for plane in self.planes.values():
            for key in plane.dep_keys:
                claimed.setdefault(key, []).append(f"plane {plane.name}")

        for gap in self.gaps.values():
            for key in gap.dep_keys:
                claimed.setdefault(key, []).append(f"gap {gap.name}")

        for key in self.exemptions:
            claimed.setdefault(key, []).append("exemption")

        return claimed

    def package_engines(self) -> dict[str, set[str]]:
        """Package → the engine names it can satisfy (``forze_inference`` serves two dialects)."""

        inverted: dict[str, set[str]] = {}

        for engine, package in self.engine_packages.items():
            inverted.setdefault(package, set()).add(engine)

        return inverted


@dataclass(frozen=True)
class Census:
    """What pytest could actually collect."""

    legs: dict[tuple[str, str], tuple[str, ...]]
    node_ids: frozenset[str]
    malformed: tuple[str, ...]


@dataclass(frozen=True)
class LegRun:
    """How one leg fared when it ran, summed over every shard that ran any of it."""

    passed: int = 0
    failed: int = 0
    skipped: int = 0
    error: int = 0

    def merged(self, other: LegRun) -> LegRun:
        return LegRun(
            passed=self.passed + other.passed,
            failed=self.failed + other.failed,
            skipped=self.skipped + other.skipped,
            error=self.error + other.error,
        )

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.skipped + self.error


@dataclass
class Report:
    violations: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


# ----------------------- #


def load_manifest(pyproject_path: Path) -> Manifest:
    """Read ``[tool.conformance_manifest]``; a missing table is a hard error, not a skip."""

    with pyproject_path.open("rb") as fh:
        config = tomllib.load(fh)

    try:
        table = config["tool"][_CONFIG_TABLE]
    except KeyError:
        raise SystemExit(
            f"error: [tool.{_CONFIG_TABLE}] table missing from {pyproject_path}"
        ) from None

    planes = {
        name: Plane(
            name=name,
            scenario=str(entry["scenario"]),
            dep_keys=tuple(entry.get("dep_keys", ())),
            engines=tuple(entry["engines"]),
            waivers={str(k): str(v) for k, v in entry.get("waivers", {}).items()},
        )
        for name, entry in table.get("planes", {}).items()
    }
    gaps = {
        name: Gap(
            name=name,
            dep_keys=tuple(entry.get("dep_keys", ())),
            engines=tuple(entry.get("engines", ())),
            reason=str(entry.get("reason", "")),
        )
        for name, entry in table.get("gaps", {}).items()
    }
    exemptions = {
        name: Exemption(kind=str(entry.get("kind", "")), reason=str(entry.get("reason", "")))
        for name, entry in table.get("exemptions", {}).items()
    }

    for group in table.get("exempt_groups", ()):
        exemption = Exemption(kind=str(group.get("kind", "")), reason=str(group.get("reason", "")))

        for name in group.get("keys", ()):
            exemptions[str(name)] = exemption

    engine_packages = {
        str(engine): str(entry["package"])
        for engine, entry in table.get("engines", {}).items()
    }

    catalog = table.get("divergence_catalog")

    return Manifest(
        planes=planes,
        gaps=gaps,
        exemptions=exemptions,
        engine_packages=engine_packages,
        divergence_catalog=str(catalog) if catalog is not None else None,
    )


def load_census(path: Path) -> Census:
    """Read the collection census written by ``--conformance-census``."""

    payload = json.loads(path.read_text(encoding="utf-8"))

    return Census(
        legs={
            (str(leg["plane"]), str(leg["engine"])): tuple(leg["node_ids"])
            for leg in payload["legs"]
        },
        node_ids=frozenset(payload["node_ids"]),
        malformed=tuple(payload.get("malformed", ())),
    )


def load_runs(paths: list[Path]) -> dict[tuple[str, str], LegRun]:
    """Union the per-shard execution censuses written by ``--conformance-executed``.

    Every shard sees only its own slice of the suite, so a leg is "run" if ANY shard ran
    it. Summing rather than picking a winner also keeps the numbers meaningful when a leg
    is split across shards, or when xdist wrote one file per worker.
    """

    runs: dict[tuple[str, str], LegRun] = {}
    files = [
        # A directory is the shape CI produces (one downloaded artifact folder), and taking
        # it directly keeps the workflow free of shell globbing.
        *(entry for path in paths if path.is_dir() for entry in sorted(path.glob("*.json"))),
        *(path for path in paths if not path.is_dir()),
    ]

    if not files:
        raise SystemExit(f"error: no execution census files found in {[str(p) for p in paths]}")

    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))

        for leg in payload["legs"]:
            key = (str(leg["plane"]), str(leg["engine"]))
            observed = LegRun(
                passed=int(leg.get("passed", 0)),
                failed=int(leg.get("failed", 0)),
                skipped=int(leg.get("skipped", 0)),
                error=int(leg.get("error", 0)),
            )
            runs[key] = runs.get(key, LegRun()).merged(observed)

    return runs


def check_legs_actually_ran(
    manifest: Manifest,
    runs: dict[tuple[str, str], LegRun],
    report: Report,
) -> None:
    """Every manifested leg passed at least one test somewhere.

    Collection proves a leg exists; only this proves it ran. The two failures it separates
    want different fixes, so they get different messages: a leg that ran and skipped
    everything usually means the engine never came up or an extra is missing, while a leg
    with no record at all usually means nothing in CI runs the directory it lives in —
    which is how two of the four inference legs sat un-run while every gate stayed green.

    Individual skips inside a leg are fine and deliberate: a check that cannot apply to an
    engine skips with a reason naming it. What must not happen is the whole leg vanishing.
    """

    for plane in sorted(manifest.planes.values(), key=lambda entry: entry.name):
        for engine in plane.engines:
            run = runs.get((plane.name, engine))

            if run is None:
                report.violations.append(
                    f"plane {plane.name!r} engine {engine!r}: no shard ran this leg — check "
                    "that the suite it lives in is part of CI's test matrix"
                )
                continue

            if run.passed == 0:
                report.violations.append(
                    f"plane {plane.name!r} engine {engine!r}: ran {run.total} test(s) and "
                    f"passed none ({run.skipped} skipped, {run.failed} failed, {run.error} "
                    "errored) — a leg that skips wholesale proves nothing"
                )
                continue

            if run.skipped:
                report.notes.append(
                    f"leg {plane.name}/{engine}: {run.passed} passed, {run.skipped} skipped"
                )


def collect_census(destination: Path, tests_root: Path) -> Census:
    """Run pytest collection ourselves, so the gate is one command locally."""

    command = [
        sys.executable,
        "-m",
        "pytest",
        str(tests_root),
        "--collect-only",
        "-q",
        "-p",
        "no:cacheprovider",
        f"--conformance-census={destination}",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)  # nosec B603

    if not destination.exists():
        raise SystemExit(
            "error: pytest collection produced no census — collection itself failed:\n"
            + completed.stdout[-4000:]
            + completed.stderr[-4000:]
        )

    return load_census(destination)


# ----------------------- #


def contract_dep_keys() -> dict[str, str]:
    """Every ``DepKey`` declared under ``contracts/``, mapped to its declaring module.

    Collected by import, like the mock-coverage guard: a key re-exported under another
    name is the same object, which a grep over source would double-count and a regex
    over multi-line declarations would miss outright.
    """

    from forze.application.contracts.deps import DepKey

    contracts = importlib.import_module(_CONTRACTS_PACKAGE)
    found: dict[str, str] = {}

    for module_info in pkgutil.walk_packages(contracts.__path__, f"{_CONTRACTS_PACKAGE}."):
        module = importlib.import_module(module_info.name)

        for attribute in dir(module):
            value = getattr(module, attribute, None)

            if isinstance(value, DepKey):
                found.setdefault(value.name, module_info.name)

    return found


def _registration_symbols(source_path: Path) -> set[str]:
    """Symbol names this file references anywhere other than a pure consumption call."""

    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    consumed: set[int] = set()
    referenced: set[tuple[str, int]] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            function = node.func
            called = (
                function.attr
                if isinstance(function, ast.Attribute)
                else function.id
                if isinstance(function, ast.Name)
                else None
            )

            if called in _CONSUMPTION_CALLS:
                arguments = [*node.args, *(keyword.value for keyword in node.keywords)]
                consumed.update(id(arg) for arg in arguments if isinstance(arg, ast.Name))

        if isinstance(node, ast.Name):
            referenced.add((node.id, id(node)))

    return {symbol for symbol, node_id in referenced if node_id not in consumed}


def _deps_modules(package: str) -> list[Any]:
    """A package's ``execution.deps`` modules, plus the modules its deps bases come from.

    The base classes matter: ``forze_s3`` and ``forze_gcs`` register nothing themselves —
    the three storage keys are bound once in the shared ``ObjectStorageDepsModule``. A
    census that only read each package's own files would report the storage plane as
    having no providers at all, and the ratchet would never fire for a new blob backend.
    """

    try:
        root = importlib.import_module(f"{package}.execution.deps")
    except ModuleNotFoundError:
        return []

    modules = [root]

    for module_info in pkgutil.walk_packages(root.__path__, f"{package}.execution.deps."):
        try:
            modules.append(importlib.import_module(module_info.name))
        except ModuleNotFoundError:  # pragma: no cover — an optional extra is absent
            continue

    inherited: list[Any] = []

    for module in list(modules):
        for attribute in dir(module):
            value = getattr(module, attribute, None)

            if not isinstance(value, type):
                continue

            for base in value.__mro__[1:]:
                base_module = sys.modules.get(base.__module__)

                if base_module is not None and base_module not in modules:
                    if base_module not in inherited:
                        inherited.append(base_module)

    return modules + inherited


def provider_census(src_root: Path) -> dict[str, set[str]]:
    """Dep-key name → the ``forze_*`` packages that REGISTER it.

    This is the fact the ratchet stands on, and it is derived from the integration
    packages rather than declared, so it cannot lag behind them.
    """

    from forze.application.contracts.deps import DepKey

    declared = set(contract_dep_keys())
    providers: dict[str, set[str]] = {}

    packages = sorted(
        entry.name
        for entry in src_root.iterdir()
        if entry.is_dir() and entry.name.startswith(_PACKAGE_PREFIX)
    )

    for package in packages:
        for module in _deps_modules(package):
            source = getattr(module, "__file__", None)

            if source is None or not source.endswith(".py"):
                continue

            try:
                symbols = _registration_symbols(Path(source))
            except (OSError, SyntaxError):  # pragma: no cover — unreadable file
                continue

            for symbol in symbols:
                value = getattr(module, symbol, None)

                if isinstance(value, DepKey) and value.name in declared:
                    providers.setdefault(value.name, set()).add(package)

    return providers


def resolve_scenario(reference: str) -> object:
    """Import ``module:attribute`` and return the attribute, raising a readable error."""

    module_name, separator, attribute = reference.partition(":")

    if not separator:
        raise ValueError(f"{reference!r} is not a 'module:attribute' reference")

    module = importlib.import_module(module_name)

    return getattr(module, attribute)


# ----------------------- #


def check_key_triage(
    manifest: Manifest,
    declared: dict[str, str],
    report: Report,
) -> None:
    """Every contract key is claimed exactly once, and nothing claims a key that is gone."""

    claims = manifest.claims()

    for key in sorted(set(declared) - set(claims)):
        report.violations.append(
            f"dep key {key!r} ({declared[key]}) is not in the conformance manifest — add it "
            "to a plane's dep_keys, to a declared gap, or to [exemptions] with a reason"
        )

    for key in sorted(set(claims) - set(declared)):
        report.violations.append(
            f"manifest claims dep key {key!r} ({', '.join(claims[key])}) but no such key is "
            "declared under contracts/ — delete the stale entry"
        )

    for key, owners in sorted(claims.items()):
        if len(owners) > 1:
            report.violations.append(
                f"dep key {key!r} is claimed more than once ({', '.join(owners)}) — one owner only"
            )


def check_exemptions(
    manifest: Manifest,
    providers: dict[str, set[str]],
    report: Report,
) -> None:
    """Exemptions are claims, and the checkable ones are checked.

    ``single-engine`` and ``no-engine-matrix`` both assert something about the world —
    that there is at most one, or no, backend implementing the key. A second backend
    landing turns that sentence false, and this is where it fails.
    """

    for key, exemption in sorted(manifest.exemptions.items()):
        if exemption.kind not in _EXEMPTION_KINDS:
            report.violations.append(
                f"exemption {key!r} has unknown kind {exemption.kind!r} — "
                f"use one of {', '.join(sorted(_EXEMPTION_KINDS))}"
            )

        if len(exemption.reason) < _MIN_REASON_LENGTH:
            report.violations.append(
                f"exemption {key!r} needs a real reason, not a label ({exemption.reason!r})"
            )

        implementers = sorted(providers.get(key, ()))

        if exemption.kind == _SINGLE_ENGINE and len(implementers) > 1:
            report.violations.append(
                f"exemption {key!r} claims a single engine, but {len(implementers)} packages "
                f"register it ({', '.join(implementers)}) — there is a differential to run now: "
                "promote it to a plane with a leg per engine, or to a declared gap"
            )

        if exemption.kind == _NO_ENGINE_MATRIX and implementers:
            report.violations.append(
                f"exemption {key!r} claims no backend implements it, but "
                f"{', '.join(implementers)} register it — the claim is stale"
            )


def check_gaps(manifest: Manifest, providers: dict[str, set[str]], report: Report) -> None:
    """A declared gap must state its real blast radius: every provider, listed."""

    package_engines = manifest.package_engines()

    for gap in sorted(manifest.gaps.values(), key=lambda entry: entry.name):
        if len(gap.reason) < _MIN_REASON_LENGTH:
            report.violations.append(f"gap {gap.name!r} needs a real reason for staying open")

        derived = _engines_for(gap.dep_keys, providers, package_engines)
        undeclared = sorted(derived - set(gap.engines))

        if undeclared:
            report.violations.append(
                f"gap {gap.name!r} does not list engine(s) {', '.join(undeclared)} that register "
                "its dep keys — a gap must name every backend it leaves uncovered"
            )

        report.notes.append(
            f"gap {gap.name}: {len(gap.engines)} engine(s) uncovered ({', '.join(gap.engines)})"
        )


def _engines_for(
    dep_keys: tuple[str, ...],
    providers: dict[str, set[str]],
    package_engines: dict[str, set[str]],
) -> set[str]:
    """The engine names that register any of *dep_keys*."""

    engines: set[str] = set()

    for key in dep_keys:
        for package in providers.get(key, ()):
            engines |= package_engines.get(package, {package.removeprefix(_PACKAGE_PREFIX)})

    return engines


def check_planes(
    manifest: Manifest,
    providers: dict[str, set[str]],
    census: Census,
    report: Report,
) -> None:
    """The heart of it: scenarios resolve, legs are collectable, providers all have legs."""

    package_engines = manifest.package_engines()

    for plane in sorted(manifest.planes.values(), key=lambda entry: entry.name):
        try:
            resolve_scenario(plane.scenario)
        except (ImportError, AttributeError, ValueError) as error:
            report.violations.append(
                f"plane {plane.name!r}: scenario {plane.scenario!r} does not resolve ({error}) — "
                "the manifest points at code that moved or was deleted"
            )

        for engine in plane.engines:
            if (plane.name, engine) not in census.legs:
                report.violations.append(
                    f"plane {plane.name!r} declares engine {engine!r} but no collected test "
                    f"carries @pytest.mark.conformance(plane={plane.name!r}, engine={engine!r})"
                )

        derived = _engines_for(plane.dep_keys, providers, package_engines)
        missing = sorted(derived - set(plane.engines) - set(plane.waivers))

        if missing:
            report.violations.append(
                f"plane {plane.name!r}: engine(s) {', '.join(missing)} register this plane's dep "
                "keys but run no conformance leg — add the leg, or waive it in "
                f"[tool.{_CONFIG_TABLE}.planes.{plane.name}.waivers] with a reason"
            )

        for engine, reason in sorted(plane.waivers.items()):
            if len(reason) < _MIN_REASON_LENGTH:
                report.violations.append(
                    f"plane {plane.name!r}: waiver for {engine!r} needs a real reason"
                )

            if engine not in derived:
                report.violations.append(
                    f"plane {plane.name!r}: waiver for {engine!r} is stale — that package no "
                    "longer registers this plane's dep keys"
                )

            report.notes.append(f"plane {plane.name}: {engine} waived — {reason}")


def check_census_is_declared(manifest: Manifest, census: Census, report: Report) -> None:
    """The reverse direction: a marker naming a plane or engine the manifest never declared."""

    for node_id in census.malformed:
        report.violations.append(
            f"{node_id}: @pytest.mark.conformance needs both plane= and engine= keywords"
        )

    for (plane_name, engine), node_ids in sorted(census.legs.items()):
        plane = manifest.planes.get(plane_name)

        if plane is None:
            report.violations.append(
                f"{node_ids[0]}: marked as conformance plane {plane_name!r}, which the manifest "
                "does not declare — add the plane or fix the marker"
            )
            continue

        if engine not in plane.engines:
            report.violations.append(
                f"{node_ids[0]}: marked as engine {engine!r} for plane {plane_name!r}, which "
                f"declares {', '.join(plane.engines)} — a leg nothing requires is a leg nothing "
                "protects"
            )


def check_divergence_probes(manifest: Manifest, census: Census, report: Report) -> None:
    """Every catalogued divergence points at a test that exists.

    The catalog is the artifact that keeps a differential honest — it says which
    differences are known and expected. Left as prose it rots into folklore, so each row
    names the probe asserting it, and the link is resolved here against real collection.
    """

    if manifest.divergence_catalog is None:
        return

    try:
        catalog = resolve_scenario(manifest.divergence_catalog)
    except (ImportError, AttributeError, ValueError) as error:
        report.violations.append(
            f"divergence catalog {manifest.divergence_catalog!r} does not resolve ({error})"
        )
        return

    if not isinstance(catalog, dict):
        report.violations.append(
            f"divergence catalog {manifest.divergence_catalog!r} must be a plane → rows mapping"
        )
        return

    for plane_name, rows in sorted(catalog.items()):
        if plane_name not in manifest.planes:
            report.violations.append(
                f"divergence catalog names plane {plane_name!r}, which the manifest does not declare"
            )

        for row in rows:
            probe = getattr(row, "probe", None)

            if not isinstance(probe, str) or not probe:
                report.violations.append(
                    f"divergence {plane_name}/{getattr(row, 'name', '?')!r} has no probe — a "
                    "catalogued divergence must name the test that asserts it"
                )
                continue

            if probe not in census.node_ids:
                report.violations.append(
                    f"divergence {plane_name}/{getattr(row, 'name', '?')!r} names probe {probe!r}, "
                    "which pytest does not collect — the link is dead"
                )


# ----------------------- #


def run_checks(
    manifest: Manifest,
    declared: dict[str, str],
    providers: dict[str, set[str]],
    census: Census,
) -> Report:
    report = Report()

    check_key_triage(manifest, declared, report)
    check_exemptions(manifest, providers, report)
    check_gaps(manifest, providers, report)
    check_planes(manifest, providers, census, report)
    check_census_is_declared(manifest, census, report)
    check_divergence_probes(manifest, census, report)

    return report


def _check_executed(manifest: Manifest, paths: list[Path]) -> Report:
    """The post-run half of the gate: did every manifested leg actually run?"""

    report = Report()
    runs = load_runs(paths)

    check_legs_actually_ran(manifest, runs, report)

    width = max((len(plane) for plane, _ in runs), default=0)

    for (plane, engine), run in sorted(runs.items()):
        print(
            f"{plane:<{width}}  {engine:<12}  {run.passed:4d} passed  "
            f"{run.skipped:3d} skipped  {run.failed:3d} failed"
        )

    return report


def _report(report: Report, *, banner: str, summary: str = "") -> int:
    for note in report.notes:
        print(f"  note: {note}")

    if report.violations:
        print(f"\n{banner} FAILED ({len(report.violations)} violation(s)):")

        for violation in report.violations:
            print(f"  - {violation}")

        return 1

    print(f"\n{summary or f'{banner} passed.'}")

    return 0


def _render(manifest: Manifest, census: Census) -> None:
    width = max((len(name) for name in manifest.planes), default=0)

    for plane in sorted(manifest.planes.values(), key=lambda entry: entry.name):
        legs = [
            f"{engine}{'' if (plane.name, engine) in census.legs else '(MISSING)'}"
            for engine in plane.engines
        ]
        print(f"{plane.name:<{width}}  {len(plane.engines):2d} engine(s)  {', '.join(legs)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "census_json",
        nargs="?",
        type=Path,
        help="census written by `pytest --conformance-census PATH` (omit with --collect)",
    )
    parser.add_argument(
        "--collect",
        action="store_true",
        help="run pytest collection here instead of consuming a prepared census",
    )
    parser.add_argument(
        "--pyproject",
        default=Path("pyproject.toml"),
        type=Path,
        help=f"pyproject.toml holding [tool.{_CONFIG_TABLE}] (default: pyproject.toml)",
    )
    parser.add_argument(
        "--src-root",
        default=Path("src"),
        type=Path,
        help="source root whose forze_* packages are censused for providers (default: src)",
    )
    parser.add_argument(
        "--tests-root",
        default=Path("tests"),
        type=Path,
        help="test root to collect when --collect is given (default: tests)",
    )
    parser.add_argument(
        "--executed",
        nargs="+",
        type=Path,
        metavar="PATH",
        help=(
            "per-shard files written by `pytest --conformance-executed PATH`. Checks that "
            "every manifested leg actually ran and passed somewhere, instead of skipping "
            "quietly. Runs on its own — no imports, so it works on a runner without extras."
        ),
    )
    args = parser.parse_args(argv)

    # Scenarios live in tests/support as often as in src/, and `tests` is only importable
    # when the repo root is on the path — which it is under pytest and is not under a bare
    # `python .github/scripts/...`. Put it there rather than making every caller remember.
    repository_root = str(args.pyproject.resolve().parent)

    if repository_root not in sys.path:
        sys.path.insert(0, repository_root)

    manifest = load_manifest(args.pyproject)

    if args.executed:
        return _report(_check_executed(manifest, args.executed), banner="Conformance execution")

    declared = contract_dep_keys()
    providers = provider_census(args.src_root)

    if args.collect:
        with tempfile.TemporaryDirectory() as directory:
            census = collect_census(Path(directory) / "census.json", args.tests_root)
    elif args.census_json is not None:
        census = load_census(args.census_json)
    else:
        parser.error("pass a census file or --collect")

    report = run_checks(manifest, declared, providers, census)

    _render(manifest, census)

    summary = (
        f"Conformance manifest passed: {len(manifest.planes)} plane(s), "
        f"{len(census.legs)} collected leg(s), {len(declared)} contract key(s) triaged."
    )
    return _report(report, banner="Conformance manifest", summary=summary)


if __name__ == "__main__":
    sys.exit(main())
