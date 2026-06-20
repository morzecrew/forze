"""The assertion helper — run DST inside a normal pytest test, fail with the counterexample."""

from __future__ import annotations

from typing import Sequence

import attrs

from forze_dst.config import SimulationConfig
from forze_dst.engines.cases import OperationCase
from forze_dst.harness import Simulation
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

    cfg = _resolve_config(config, active())
    report = sim.run(cfg, scenario=scenario, cases=cases)

    if report is not None:
        raise AssertionError("DST found a violation:\n" + report.format())
