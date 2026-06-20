"""Artifacts — turn a found counterexample into durable, portable, runnable-at-scale form.

Submodules: ``corpus`` (a JSON-Lines regression-seed corpus re-run every build), ``bundle`` +
``serialize`` (a self-contained replay file carrying the seed *and* the full config that produced
it), and ``sweep`` (fan a seed sweep across a process pool). This ``__init__`` re-exports the public
API so ``from forze_dst.artifacts import FailureBundle`` works; submodules are also importable by path.
"""

from __future__ import annotations

from forze_dst.artifacts.bundle import (
    FailureBundle,
    bundle_from_report,
    replay_bundle,
)
from forze_dst.artifacts.corpus import (
    RegressionEntry,
    append_regression,
    entry_from_report,
    load_regressions,
)
from forze_dst.artifacts.serialize import config_from_dict, config_to_dict
from forze_dst.artifacts.sweep import (
    SeedOutcome,
    SimulationSeedRunner,
    SweepResult,
    parallel_sweep,
    sweep,
)

__all__ = [
    "FailureBundle",
    "bundle_from_report",
    "replay_bundle",
    "RegressionEntry",
    "append_regression",
    "entry_from_report",
    "load_regressions",
    "config_from_dict",
    "config_to_dict",
    "SeedOutcome",
    "SimulationSeedRunner",
    "SweepResult",
    "parallel_sweep",
    "sweep",
]
