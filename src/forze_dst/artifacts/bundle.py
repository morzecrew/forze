"""Failure artifact bundle — a found bug as one self-contained, replayable file.

The end of the loop, made portable. A :class:`~forze_dst.oracle.ViolationReport` reproduces only
in the process that found it (it carries live objects); a :class:`FailureBundle` is the durable
form — the seed, the *full* :class:`SimulationConfig` that produced it (faults, latency,
partitions, crash, scheduler — via :mod:`forze_dst.artifacts.serialize`), the minimized workload for the eye,
the registry fingerprint, and the ``module:attr`` of the app under test — as plain JSON. Hand the
file to anyone (or a CI artifact store) and :func:`replay_bundle` re-runs the exact configuration at
that seed, so the bug reproduces on another machine, another day, from one command.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, cast, final

import attrs

from forze_dst.artifacts.serialize import config_from_dict, config_to_dict
from forze_dst.artifacts.sweep import _load_simulation  # pyright: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from forze_dst.config import SimulationConfig
    from forze_dst.oracle import ViolationReport

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class FailureBundle:
    """A reproducible counterexample as portable JSON — seed + full config + context."""

    seed: int
    schedule_seed: int | None
    target: str | None
    """The ``module:attr`` import string of the Simulation the bug was found against."""
    config: dict[str, Any]
    """The full run configuration (:func:`~forze_dst.artifacts.serialize.config_to_dict`)."""
    workload: tuple[tuple[str, str], ...] = ()
    """The minimized workload as ``(op, repr(arg))`` pairs — for reading; reproduction comes from
    the seed + config, not from re-injecting these."""
    registry_fingerprint: str | None = None
    invariants: tuple[str, ...] = ()
    """Names of the invariants the seed violated."""

    # ....................... #

    def to_json(self) -> str:
        """Render the bundle as a single pretty JSON document."""

        return json.dumps(
            {
                "seed": self.seed,
                "schedule_seed": self.schedule_seed,
                "target": self.target,
                "config": self.config,
                "workload": [list(pair) for pair in self.workload],
                "registry_fingerprint": self.registry_fingerprint,
                "invariants": list(self.invariants),
            },
            indent=2,
        )

    # ....................... #

    @classmethod
    def from_json(cls, text: str) -> FailureBundle:
        """Parse a bundle from :meth:`to_json` output (tolerant of missing optional keys)."""

        data = json.loads(text)

        return cls(
            seed=int(data["seed"]),
            schedule_seed=data.get("schedule_seed"),
            target=data.get("target"),
            config=data["config"],
            workload=tuple((pair[0], pair[1]) for pair in data.get("workload", ())),
            registry_fingerprint=data.get("registry_fingerprint"),
            invariants=tuple(data.get("invariants", ())),
        )

    # ....................... #

    def save(self, path: str | Path) -> None:
        """Write the bundle to *path* (creating parent dirs as needed)."""

        file = Path(path)
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(self.to_json(), encoding="utf-8")

    # ....................... #

    @classmethod
    def load(cls, path: str | Path) -> FailureBundle:
        """Read a bundle from *path*."""

        return cls.from_json(Path(path).read_text(encoding="utf-8"))


# ....................... #


def bundle_from_report(
    report: ViolationReport,
    config: SimulationConfig,
    *,
    target: str | None = None,
) -> FailureBundle:
    """Capture a violating *report* (+ the *config* it was found under) as a portable bundle.

    *target* is the ``module:attr`` of the Simulation, needed for :func:`replay_bundle` to re-import
    it; omit it only when the replay will supply its own ``load``.
    """

    return FailureBundle(
        seed=report.seed,
        schedule_seed=report.schedule_seed,
        target=target,
        config=config_to_dict(config),
        workload=tuple(
            (str(op), repr(arg))
            for op, arg in cast("tuple[tuple[Any, Any], ...]", report.workload)
        ),
        registry_fingerprint=report.registry_fingerprint,
        invariants=tuple(sorted({v.invariant for v in report.violations})),
    )


def replay_bundle(
    bundle: FailureBundle,
    *,
    load: Callable[[str], Any] = _load_simulation,
) -> ViolationReport | None:
    """Re-run *bundle* at its seed under its saved configuration; return the reproduced report.

    Resolves the Simulation (via *load*, defaulting to a ``module:attr`` importer), rebuilds the
    full config, pins it to the bundle's single seed, and explores — the auto-derived scenario, so
    no externally-supplied workload is needed (the seed + config reproduce it). A faithful bundle
    returns a violation again; *load* is injectable so a test can hand back a Simulation directly.
    """

    from forze_dst.harness import Simulation

    if bundle.target is None:
        raise ValueError("bundle has no target; pass load=... that ignores it")

    sim = load(bundle.target)
    if not isinstance(sim, Simulation):
        raise TypeError(f"{bundle.target!r} did not load a forze_dst.Simulation")

    config = attrs.evolve(config_from_dict(bundle.config), seeds=[bundle.seed])
    return sim.run(config)
