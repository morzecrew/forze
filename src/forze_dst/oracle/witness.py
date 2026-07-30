"""Falsifiability witnesses — the demonstration that a green result could have been red.

The doctrine: **a clean verdict may cover an invariant only if the harness has demonstrated it
could catch that invariant failing.** The demonstration is a *witness* — a recorded, replayable
perturbed run in which the invariant fires (schema-identical to a regression corpus's killing
entry, because that is exactly what it is). Invariants genuinely outside the simulation's
horizon — enforced below the port, effecting outside the process, wall-clock-bound, or spanning
runs — are excluded by an audited :class:`HorizonDeclaration` naming what covers them instead.
Everything else is **unaccounted**, and the accounting says so out loud.

Per-invariant status is therefore three-valued (:class:`InvariantStatus`), the verdict line's
oracle-set clause becomes *countable* ("… for the K witnessed invariants, M declared
out-of-horizon"), and declarations are audited in reverse: :func:`mine_witnesses` probes declared
invariants too, and one it *can* witness is a wrong declaration — self-report is verified, never
trusted.
"""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, final

import attrs

from forze_dst.oracle.invariants import name_of

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from forze_dst.artifacts.corpus import RegressionEntry
    from forze_dst.config import SimulationConfig
    from forze_dst.harness import Simulation
    from forze_dst.misuse import MisuseMutant
    from forze_dst.scenario import Scenario

# NOTE: everything from ``forze_dst.artifacts`` / ``faults`` / ``scheduler`` is imported
# lazily inside the functions that use it. This module is re-exported by the ``oracle``
# package ``__init__``, which ``faults`` (via the recorder) triggers mid-import — a
# module-level import back into that chain would be circular.

# ----------------------- #


class InvariantStatus(Enum):
    """What licenses a clean verdict to cover an invariant."""

    WITNESSED = "witnessed"
    """The registry holds a live witness: a replayable perturbed run where the invariant fired,
    minted against the current operation-catalog fingerprint."""

    DECLARED = "declared"
    """An audited :class:`HorizonDeclaration` places it outside the simulation's horizon and
    names what covers it instead. Verified in reverse by the miner — never taken on trust."""

    UNACCOUNTED = "unaccounted"
    """Neither. A clean sweep says nothing about this invariant, and the gate fails on it."""


# ....................... #


class HorizonClass(Enum):
    """Why an invariant is genuinely outside the simulation's horizon."""

    BELOW_PORT = "below_port"
    """Truth maintained by machinery beneath the port the mock does not run — a trigger, a
    unique index, a TTL, a generated column."""

    EXTERNAL_EFFECT = "external_effect"
    """The effect leaves the process (an external charge, a sent email) — no port read can
    observe it inside the simulation."""

    REAL_TIME = "real_time"
    """A wall-clock property virtual time cannot falsify (true elapsed-time SLAs, cross-process
    clock behavior)."""

    CROSS_RUN = "cross_run"
    """Spans multiple simulation lifetimes (long-horizon retention, migration-order properties)."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class HorizonDeclaration:
    """An audited out-of-horizon exclusion: why, and — mandatorily — what covers it instead.

    The pointer is required because an exclusion without a replacement is how documented-but-
    unchecked guarantees are born; a declaration that cannot name its integration test (or other
    covering check) is not ready to be one.
    """

    invariant: str
    """The declared invariant name (see :func:`~forze_dst.oracle.invariants.name_of`)."""

    horizon: HorizonClass

    covered_by: str
    """What covers the invariant instead — an integration-test id, a conformance battery, a
    monitoring check. Free text for now; must be non-empty."""

    notes: str = ""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.covered_by.strip():
            raise ValueError(
                f"{self.invariant}: a horizon declaration must name what covers the "
                "invariant instead (covered_by)"
            )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class InvariantWitness:
    """One witness: the invariant it certifies, and the replayable entry that fires it.

    The entry is the regression-corpus schema verbatim — seed, exploration knobs, and the
    catalog fingerprint at find time. Fingerprint drift invalidates a witness loudly, exactly
    like a killing seed: a changed catalog means the replay proves nothing until re-mined.
    """

    invariant: str
    entry: RegressionEntry

    # ....................... #

    def to_json(self) -> str:
        """Render as a single JSON-Lines record (the entry embedded verbatim)."""

        return json.dumps({"invariant": self.invariant, "entry": json.loads(self.entry.to_json())})

    # ....................... #

    @classmethod
    def from_json(cls, line: str) -> InvariantWitness:
        """Parse one JSON-Lines record."""

        from forze_dst.artifacts.corpus import RegressionEntry

        data = json.loads(line)
        return cls(
            invariant=str(data["invariant"]),
            entry=RegressionEntry.from_json(json.dumps(data["entry"])),
        )


# ....................... #


def save_witnesses(path: str | Path, witnesses: Iterable[InvariantWitness]) -> None:
    """Write *witnesses* as a JSONL registry (creating parent dirs), one record per line."""

    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text("".join(witness.to_json() + "\n" for witness in witnesses), encoding="utf-8")


def load_witnesses(path: str | Path) -> tuple[InvariantWitness, ...]:
    """Load a JSONL witness registry; an absent file is an empty registry (no error)."""

    file = Path(path)

    if not file.exists():
        return ()

    return tuple(
        InvariantWitness.from_json(line)
        for line in file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )


# ....................... #


class InvariantAccountingError(RuntimeError):
    """The accounting gate: unaccounted invariants, drifted witnesses, or wrong declarations."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class InvariantAccounting:
    """Per-invariant statuses for one simulation — the countable form of the verdict's scope.

    Built by :func:`account_invariants`; entirely static (names + registries + the current
    catalog fingerprint), so it is checkable before any seed runs.
    """

    statuses: tuple[tuple[str, InvariantStatus], ...]
    """``(invariant, status)`` per declared invariant, in declaration order."""

    declarations: tuple[HorizonDeclaration, ...] = ()

    drifted: tuple[str, ...] = ()
    """Invariants whose only witnesses were minted against a *different* catalog fingerprint —
    the replay can no longer be trusted; re-mine (the killing-seed rule, never auto-quarantine)."""

    conflicts: tuple[str, ...] = ()
    """Invariants both declared out-of-horizon **and** live-witnessed — the reverse audit's
    verdict that the declaration is wrong (the harness demonstrably can catch it)."""

    # ....................... #

    @property
    def witnessed(self) -> tuple[str, ...]:
        return tuple(name for name, status in self.statuses if status is InvariantStatus.WITNESSED)

    @property
    def declared(self) -> tuple[str, ...]:
        return tuple(name for name, status in self.statuses if status is InvariantStatus.DECLARED)

    @property
    def unaccounted(self) -> tuple[str, ...]:
        return tuple(
            name for name, status in self.statuses if status is InvariantStatus.UNACCOUNTED
        )

    # ....................... #

    @property
    def problems(self) -> tuple[str, ...]:
        """Human one-liners for everything the gate would fail on (empty == accounted)."""

        out: list[str] = []

        if self.unaccounted:
            out.append(
                f"unaccounted invariant(s): {', '.join(self.unaccounted)} — no witness "
                "demonstrates the harness could catch them failing, and no horizon declaration "
                "excludes them (mine a witness or declare, with a covering check)"
            )

        if self.drifted:
            out.append(
                f"drifted witness(es): {', '.join(self.drifted)} — minted against a different "
                "operation catalog; re-mine before trusting the replay"
            )

        if self.conflicts:
            out.append(
                f"wrong declaration(s): {', '.join(self.conflicts)} — declared out-of-horizon "
                "yet a live witness fires them; drop the declaration"
            )

        return tuple(out)

    # ....................... #

    def require_accounted(self) -> None:
        """Raise :class:`InvariantAccountingError` unless every invariant is accounted for."""

        if self.problems:
            raise InvariantAccountingError(
                "invariant accounting failed:\n" + "\n".join(f"  • {p}" for p in self.problems)
            )


# ....................... #


def account_invariants(
    names: Sequence[str],
    *,
    witnesses: Sequence[InvariantWitness],
    declarations: Sequence[HorizonDeclaration],
    fingerprint: str,
) -> InvariantAccounting:
    """Compute per-invariant statuses for the declared invariant *names*.

    A witness is *live* only when its entry's fingerprint equals the current *fingerprint*; a
    stale one lands the invariant in :attr:`InvariantAccounting.drifted` (and, unless declared,
    UNACCOUNTED). An invariant both live-witnessed and declared is a conflict — the declaration
    said "cannot be witnessed" and the registry proves otherwise.
    """

    live: set[str] = set()
    stale: set[str] = set()

    for witness in witnesses:
        if witness.entry.registry_fingerprint == fingerprint:
            live.add(witness.invariant)
        else:
            stale.add(witness.invariant)

    declared = {declaration.invariant for declaration in declarations}

    statuses: list[tuple[str, InvariantStatus]] = []
    for name in names:
        if name in live:
            statuses.append((name, InvariantStatus.WITNESSED))
        elif name in declared:
            statuses.append((name, InvariantStatus.DECLARED))
        else:
            statuses.append((name, InvariantStatus.UNACCOUNTED))

    return InvariantAccounting(
        statuses=tuple(statuses),
        declarations=tuple(declarations),
        # Only a *lost* capability drifts: a stale witness with no live replacement.
        drifted=tuple(sorted((stale - live) & set(names))),
        conflicts=tuple(sorted(live & declared & set(names))),
    )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class Perturbation:
    """One probe of the miner's repertoire: a label and the perturbed config to run."""

    label: str
    config: SimulationConfig


# ....................... #


def default_repertoire(base: SimulationConfig) -> tuple[Perturbation, ...]:
    """The v1 perturbation repertoire — adverse schedules, transient faults, crash placement.

    All existing machinery; deliberately absent is silent write suppression (fabricating a typed
    return value for a swallowed call is unfaithful — ``FaultRule.at_call`` crash-after
    approximates the omission class without fabrication). Deterministic-position probes are the
    caller's to add: ``Perturbation(label="crash:call-2", config=attrs.evolve(base,
    faults=FaultPolicy(rules=(FaultRule(op="update", crash=1.0, at_call=2),))))``.
    """

    from forze_dst.faults import CrashPolicy, FaultPolicy, FaultRule
    from forze_dst.scheduler import PCTScheduler, RandomScheduler

    return (
        Perturbation(
            label="schedule:random", config=attrs.evolve(base, scheduler=RandomScheduler())
        ),
        *(
            Perturbation(
                label=f"schedule:pct-d{depth}",
                config=attrs.evolve(base, scheduler=PCTScheduler(depth=depth)),
            )
            for depth in (2, 3, 4)
        ),
        Perturbation(
            label="fault:transient",
            config=attrs.evolve(
                base, faults=FaultPolicy(rules=(FaultRule(error=0.1, timeout=0.05),))
            ),
        ),
        Perturbation(
            label="fault:crash",
            config=attrs.evolve(base, crash=CrashPolicy(probability=0.05)),
        ),
    )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class WitnessMining:
    """The miner's outcome. A non-empty :attr:`unwitnessed` does **not** auto-classify: it
    demands either a richer repertoire or a reviewed declaration — never a silent default."""

    witnesses: tuple[InvariantWitness, ...]
    unwitnessed: tuple[str, ...]

    wrong_declarations: tuple[str, ...]
    """Declared-out-of-horizon invariants the miner witnessed anyway — each one is a wrong
    declaration (the reverse audit); fail on any before trusting the accounting."""

    probes_run: int


# ....................... #


def mine_witnesses(
    sim: Simulation,
    base: SimulationConfig,
    *,
    scenario: Scenario | None = None,
    targets: Sequence[str] | None = None,
    repertoire: Sequence[Perturbation] | None = None,
) -> WitnessMining:
    """Search the perturbation *repertoire* for a replayable run per invariant where it fires.

    Offline by design (the corpus-mining posture): budgeted at ``len(repertoire) × len(seeds)``
    runs, stopping early once every target is witnessed. *targets* defaults to **all** of the
    simulation's declared invariant names — including DECLARED ones, deliberately: witnessing a
    declared invariant is the reverse audit catching a wrong declaration. Each found witness
    embeds the full perturbed config (via the failure-bundle serializer), so
    :func:`replay_witnesses` reproduces the exact environment, not the current defaults.
    """

    from forze_dst.artifacts.corpus import entry_from_report
    from forze_dst.artifacts.serialize import config_to_dict

    names = tuple(targets) if targets is not None else tuple(name_of(i) for i in sim.invariants)
    probes = tuple(repertoire) if repertoire is not None else default_repertoire(base)
    declared = {declaration.invariant for declaration in sim.horizon}

    found: dict[str, InvariantWitness] = {}
    probes_run = 0

    for probe in probes:
        if all(name in found for name in names):
            break

        report = sim.run(probe.config, scenario=scenario)
        probes_run += 1

        if report is None:
            continue

        entry = entry_from_report(
            report,
            explore={"perturbation": probe.label, "config": config_to_dict(probe.config)},
        )

        for fired in sorted({violation.invariant for violation in report.violations}):
            if fired in names and fired not in found:
                found[fired] = InvariantWitness(invariant=fired, entry=entry)

    return WitnessMining(
        witnesses=tuple(found.values()),
        unwitnessed=tuple(name for name in names if name not in found),
        wrong_declarations=tuple(sorted(set(found) & declared)),
        probes_run=probes_run,
    )


# ....................... #


def replay_witnesses(sim: Simulation, *, scenario: Scenario | None = None) -> None:
    """The per-build replay tier: every registered witness must still fire its invariant.

    The smoke-tier discipline, applied to witnesses: a fingerprint mismatch fails loud (the
    catalog moved — re-mine, never silently quarantine); a witness that replays clean means a
    drifted catalog or a weakened oracle, both loud. Cost is one run per witness.
    """

    from forze_dst.artifacts.serialize import config_from_dict

    fingerprint = sim.fingerprint()
    failures: list[str] = []

    for witness in sim.witnesses:
        entry = witness.entry

        if entry.registry_fingerprint != fingerprint:
            failures.append(
                f"{witness.invariant}: witness fingerprint drifted — re-mine the witness"
            )
            continue

        snapshot = (entry.explore or {}).get("config")
        if snapshot is None:
            failures.append(
                f"{witness.invariant}: witness carries no config snapshot — re-mine it with "
                "mine_witnesses (the snapshot is what makes the replay exact)"
            )
            continue

        config = attrs.evolve(config_from_dict(dict(snapshot)), seeds=[entry.seed])
        report = sim.run(config, scenario=scenario)
        fired = set[str]() if report is None else {v.invariant for v in report.violations}

        if witness.invariant not in fired:
            failures.append(
                f"{witness.invariant}: witness seed {entry.seed} no longer fires the invariant "
                "— a weakened oracle or changed behavior; re-mine before trusting green"
            )

    if failures:
        raise InvariantAccountingError(
            "witness replay failed:\n" + "\n".join(f"  • {f}" for f in failures)
        )


# ....................... #


def witnesses_from_mutants(mutants: Iterable[MisuseMutant]) -> tuple[InvariantWitness, ...]:
    """Convert a misuse corpus's killing entries into witnesses — the corpus was the first
    witness registry all along (each killing entry proves the harness catches that invariant
    failing). One witness per distinct expected-invariant name; first mutant wins.

    These witnesses carry the corpus's bare exploration knobs, not the full config snapshot the
    miner embeds — so they account (:func:`account_invariants`) but :func:`replay_witnesses`
    refuses them. That is deliberate, not a gap: the corpus smoke tier *is* their replay tier
    (it re-runs every killing seed per build under the recorded knobs, with the same
    fingerprint-drift discipline)."""

    out: dict[str, InvariantWitness] = {}

    for mutant in mutants:
        for name in mutant.expected_invariants:
            out.setdefault(name, InvariantWitness(invariant=name, entry=mutant.killing))

    return tuple(out.values())
