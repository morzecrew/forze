"""The assertion helpers — run DST inside a normal pytest test, fail with the counterexample."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import attrs

from forze_dst.artifacts import FailureBundle, bundle_from_report, config_from_dict
from forze_dst.config import SimulationConfig
from forze_dst.engines.cases import OperationCase
from forze_dst.harness import Simulation
from forze_dst.oracle import ViolationReport
from forze_dst.scenario import Scenario
from forze_dst.testing._options import DstOptions, active

# ----------------------- #


def _resolve_config(
    config: SimulationConfig | None, options: DstOptions | None
) -> SimulationConfig:
    """The config to actually run: the given one (or ``thorough()``), with any ``--dst-seeds``
    override applied. Pure, so the override logic is unit-testable on its own."""

    cfg = config if config is not None else SimulationConfig.thorough()

    if options is not None and options.seeds is not None:
        cfg = attrs.evolve(cfg, seeds=range(options.seeds))

    return cfg


# ....................... #


def assert_no_violation(
    sim: Simulation,
    config: SimulationConfig | None = None,
    *,
    scenario: Scenario | None = None,
    cases: Sequence[OperationCase] | None = None,
) -> None:
    """Sweep *sim* and fail the test if any seed violates an invariant.

    Defaults to :meth:`SimulationConfig.thorough` when no *config* is given. On a violation it
    raises ``AssertionError`` carrying the minimized, reproducible counterexample
    (``report.format()``), so a failing DST test reads like any other pytest failure — with the
    seed to reproduce it. A clean sweep returns ``None`` and the test passes.

    When the :mod:`forze_dst.testing.plugin` is enabled, ``--dst-seeds=N`` (or ini ``dst_seeds``)
    overrides the seed count for every sweep, so one test runs quick locally (``--dst-seeds=16``)
    and exhaustive in CI (``--dst-seeds=2000``) with no change to the test itself.

    *scenario* / *cases* are passed through to :meth:`Simulation.run` as usual (``cases`` for the
    ``OP_CASE`` strategy; ``scenario`` for the scenario strategies, auto-derived if omitted).
    """

    options = active()
    cfg = _resolve_config(config, options)
    report = sim.run(cfg, scenario=scenario, cases=cases)

    if report is not None:
        if options is not None and options.save_bundle is not None:
            _save_bundle(report, cfg, options.save_bundle)

        raise AssertionError("DST found a violation:\n" + report.format())


# ....................... #


def _save_bundle(report: ViolationReport, config: SimulationConfig, directory: str) -> None:
    """Drop a portable :class:`~forze_dst.artifacts.FailureBundle` for *report* into *directory*."""

    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    bundle_from_report(report, config).save(out / f"dst-seed-{report.seed}.json")


# ....................... #


def assert_no_regressions(sim: Simulation, *, bundles: str | Path) -> None:
    """Replay saved :class:`~forze_dst.artifacts.FailureBundle` s against *sim* — fail if any still
    violates.

    *bundles* is a directory of ``.json`` bundles (as ``--dst-save-bundle`` writes) or a single
    bundle file. Each is replayed at its seed under its own saved config (the bundle is
    self-contained, so the exact environment that found the bug is reproduced — not the current
    defaults). Lock a bundle into your repo and this turns it into a permanent regression test:
    the day the bug comes back, the seed reproduces it.

    A bundle whose registry fingerprint no longer matches *sim* is reported as a drift warning
    (the catalog moved, so the seed may not exercise the original path) rather than a pass.

    **Self-containment.** A bundle reproduces via seed + config → the *auto-derived* scenario, so
    only strategies that regenerate their workload from the seed are faithfully replayable
    (``SCENARIO`` / ``HYPOTHESIS`` / ``DPOR`` / crash). An ``OP_CASE`` bundle needs the ``cases=``
    a bundle cannot carry — it is reported as a **failure** (not self-replayable), never crashing
    the batch or silently passing. A bug originally found under a *custom* ``scenario=`` is likewise
    not captured: replay re-derives a different scenario, so its non-reproduction here is not a
    trustworthy pass — replay such a bundle manually against its original scenario instead.
    """

    paths = _bundle_paths(bundles)

    if not paths:
        return

    failures: list[str] = []

    for path in paths:
        bundle = FailureBundle.load(path)
        drifted = (
            bundle.registry_fingerprint is not None
            and bundle.registry_fingerprint != sim.fingerprint()
        )
        replay = attrs.evolve(config_from_dict(bundle.config), seeds=[bundle.seed])

        if not _is_self_replayable(replay):
            # An OP_CASE bundle reproduces only from a caller-supplied ``cases=`` that the bundle
            # never stored; a bare replay raises ``ValueError`` in dispatch. Report it (rather than
            # letting the exception abort every remaining bundle) so it can't masquerade as a pass.
            failures.append(
                f"seed {bundle.seed} ({path.name}) is not a self-contained regression bundle: "
                f"its {replay.strategy.value!r} strategy reproduces only from an externally-supplied "
                "workload (cases/scenario) a bundle cannot carry. Replay it manually with the "
                "original cases=/scenario=, or regenerate it under a seed-reproducible strategy."
            )
            continue

        try:
            report = sim.run(replay)
        except Exception as e:  # a malformed/unreplayable bundle must not abort the whole batch
            failures.append(f"seed {bundle.seed} ({path.name}) could not be replayed: {e}")
            continue

        if report is not None:
            failures.append(f"seed {bundle.seed} ({path.name}) still violates:\n{report.format()}")
        elif drifted:
            failures.append(
                f"seed {bundle.seed} ({path.name}) no longer reproduces, but the registry "
                "fingerprint drifted — the catalog changed, so this is not a trustworthy pass."
            )

    if failures:
        raise AssertionError(
            f"{len(failures)} of {len(paths)} regression bundle(s) failed:\n\n"
            + "\n\n".join(failures)
        )


# ....................... #


def _is_self_replayable(config: SimulationConfig) -> bool:
    """Whether a bundle under *config* can reproduce from seed + config alone.

    True for every strategy that regenerates its workload from the seed (``SCENARIO`` /
    ``HYPOTHESIS`` / ``DPOR``, and crash runs). False for ``OP_CASE``, whose workload is the
    caller's ``cases=`` — never stored in a bundle, so dispatch would raise on replay.
    """

    from forze_dst.config import Strategy

    return config.strategy is not Strategy.OP_CASE


# ....................... #


def _bundle_paths(bundles: str | Path) -> list[Path]:
    """Resolve *bundles* to a sorted list of bundle files (a directory's ``*.json``, or one file)."""

    path = Path(bundles)

    if path.is_dir():
        return sorted(path.glob("*.json"))

    return [path] if path.is_file() else []
