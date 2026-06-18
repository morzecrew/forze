"""Unified, seed-driven configuration for a DST run — the single source of nondeterminism.

One master ``seed`` (each value in :attr:`SimulationConfig.seeds`) drives every nondeterminism
stream — schedule, faults, entropy, inputs. The harness derives an independent sub-seed per
stream via :func:`~forze.base.primitives.derive_seed` (stable + order-insensitive), so the
streams vary independently yet a single seed reproduces the whole run. This is the canonical
DST contract: ``(system, seed) -> deterministic execution``.

Nested by design (decision D1): richer concerns — fault policy, latency profile, cluster
topology — attach as their own sub-objects added by later work-streams (S2/S6); a simple run
stays ``SimulationConfig(seeds=range(64))``.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Sequence

import attrs

from forze_dst.faults import FaultPolicy
from forze_dst.latency import LatencyProfile
from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #


class Strategy(StrEnum):
    """How the workload is generated and explored."""

    OP_CASE = "op_case"
    """Seeded weighted workload of operation cases (``explore``)."""

    SCENARIO = "scenario"
    """Generative arrange→act scenario, greedy-minimized (``explore_scenario``)."""

    HYPOTHESIS = "hypothesis"
    """Scenario driven + shrunk by Hypothesis (``explore_scenario_hypothesis``)."""

    DPOR = "dpor"
    """Systematic interleaving search over one fixed workload (``explore_scenario_dpor``)."""


# ....................... #


class SchedulerKind(StrEnum):
    """Which interleaving strategy drives the loop (orthogonal to :class:`Strategy`)."""

    FIFO = "fifo"
    """Deterministic ready-queue order — one interleaving (no perturbation)."""

    RANDOM = "random"
    """Seeded ready-queue shuffle each tick — explores random interleavings."""

    PCT = "pct"
    """Probabilistic Concurrency Testing (priority + change points) — depth-d guarantees."""


# ....................... #


@attrs.define(frozen=True, kw_only=True)
class SimulationConfig:
    """The full spec for a DST exploration; one seed drives every stream.

    The DPOR strategy ignores :attr:`scheduler` (it drives a systematic scheduler itself);
    HYPOTHESIS treats :attr:`act_count` as the max act length.
    """

    seeds: Sequence[int] = attrs.field(default=range(32))
    """The master seeds to sweep; each derives its own independent sub-streams."""

    strategy: Strategy = Strategy.SCENARIO
    """Workload generation + exploration strategy."""

    scheduler: SchedulerKind = SchedulerKind.RANDOM
    """Interleaving strategy (ignored by DPOR)."""

    concurrency: int = 4
    """Max concurrent act operations."""

    epoch: datetime = DEFAULT_EPOCH
    """Virtual-clock start instant."""

    # Strategy knobs.
    count: int = 50
    """OP_CASE: workload size."""

    act_count: int = 20
    """SCENARIO/DPOR: act-call count; HYPOTHESIS: max act length."""

    max_examples: int = 200
    """HYPOTHESIS: examples to try."""

    max_runs: int = 500
    """DPOR: interleavings to explore."""

    dpor_seed: int = 0
    """DPOR: the single seed whose workload is fixed and re-interleaved."""

    # Scheduler knobs (PCT).
    pct_depth: int = 3
    """PCT: bug depth the search targets."""

    pct_steps: int = 50
    """PCT: scheduling steps over which change points are placed."""

    # Nondeterminism streams (compiled per-run from sub-seeds derived from the master seed).
    faults: FaultPolicy | None = None
    """Declarative, seeded fault injection over the port seam (error / timeout / crash).
    Compiled with ``derive_seed(seed, "fault")`` — no caller-supplied RNG. ``cluster``
    (ClusterConfig) is added by S6."""

    latency: LatencyProfile | None = None
    """Declarative, seeded simulated-I/O latency (per-route distributions). Compiled with
    ``derive_seed(seed, "latency")``. Overrides ``Simulation.latency`` (the raw-callable escape
    hatch) when set."""

    # ....................... #

    @property
    def perturb(self) -> bool:
        """Whether interleavings are perturbed (any scheduler other than deterministic FIFO)."""

        return self.scheduler is not SchedulerKind.FIFO
