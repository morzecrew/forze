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
from typing import Any, Sequence

import attrs

from forze.base.exceptions import exc
from forze_dst.faults import CrashPolicy, FaultPolicy
from forze_dst.latency import LatencyProfile
from forze_dst.scheduler import PCTScheduler, RandomScheduler, SchedulerSpec
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


@attrs.define(frozen=True, kw_only=True)
class Partition:
    """One network-partition window: nodes cut off from the shared infrastructure.

    During ``[start, end)`` of virtual time, every node in :attr:`isolated` is cut from the
    gated port surfaces (its calls raise a retryable *unreachable* error — modeling a network
    split where the broker/store lives on the other side); the rest of the cluster proceeds.
    The window heals at ``end``. A set of nodes is split off as a unit; per-window :attr:`loss`
    makes the cut a *lossy/flaky link* (a fraction of calls drop) rather than a clean break, and
    different windows can give different node groups different loss in overlapping time — an
    asymmetric split.
    """

    start: float
    end: float
    isolated: frozenset[int]

    loss: float = 1.0
    """Probability (``0 < loss <= 1``) that a gated call from an isolated node drops during the
    window. ``1.0`` (default) is a clean cut — every call drops, the classic partition. A value
    below ``1`` is a *lossy link*: each call drops with this probability (seeded per node), so
    some calls slip through and a flaky link is modeled, not just a hard split."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not 0.0 < self.loss <= 1.0:
            raise exc.configuration("Partition.loss must be in (0, 1]")


# ....................... #


@attrs.define(frozen=True, kw_only=True)
class PartitionSchedule:
    """A seeded set of :class:`Partition` windows + which port surfaces a split cuts."""

    windows: tuple[Partition, ...] = ()
    surfaces: frozenset[str] = frozenset()
    """Port surfaces a partition makes unreachable (e.g. ``queue_command``, ``dlock``,
    ``document_command``). Empty ⇒ every surface is cut (a total split)."""

    # ....................... #

    def isolated_at(self, node_id: int, at: float) -> bool:
        """Whether *node_id* is cut off from the cluster at virtual time *at*."""

        return any(
            window.start <= at < window.end and node_id in window.isolated
            for window in self.windows
        )

    # ....................... #

    def loss_at(self, node_id: int, at: float) -> float:
        """The drop probability for *node_id*'s gated calls at virtual time *at*.

        The strongest (max) loss across the windows isolating the node right now, or ``0.0`` when
        the node is fully connected. A clean cut (``loss=1.0``) returns ``1.0`` → every gated call
        drops, exactly as before; a lossy window returns its fractional probability.
        """

        return max(
            (
                window.loss
                for window in self.windows
                if window.start <= at < window.end and node_id in window.isolated
            ),
            default=0.0,
        )

    # ....................... #

    def gates(self, surface: str | None) -> bool:
        """Whether a partition cuts the given *surface* (all surfaces when none are named)."""

        return not self.surfaces or surface in self.surfaces


# ....................... #


@attrs.define(frozen=True, kw_only=True)
class ClusterConfig:
    """Topology for a multi-runtime (distributed) DST run — N nodes over one shared store."""

    nodes: int = 3
    """How many real ``ExecutionRuntime`` nodes run concurrently over the shared ``MockState``."""

    partitions: PartitionSchedule | None = None
    """The network partitions to inject (``None`` ⇒ a fully-connected cluster)."""


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

    scheduler: SchedulerSpec = attrs.field(factory=RandomScheduler)
    """Interleaving strategy — a :class:`~forze_dst.scheduler.SchedulerSpec` variant
    (``FIFOScheduler()`` / ``RandomScheduler()`` / ``PCTScheduler(depth, steps)``); each carries
    only its own params. Ignored by DPOR (it drives its own systematic reorderer)."""

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

    dpor_prune: bool = True
    """DPOR: skip expanding an interleaving whose observed effect order matches one already
    explored. On (the default, the fast mode) the search is complete only up to that
    observed-effect equivalence — a heuristic that can prune a subtree hiding a violation, since
    state a run never recorded is invisible to the signature. Off, every branch point of every
    explored interleaving is expanded: exhaustive over the schedule tree of the fixed workload
    whenever the tree fits within :attr:`max_runs`."""

    # Coverage-guided exploration (``Simulation.coverage``).
    coverage_plateau: int = 8
    """Stop a coverage-guided sweep after this many consecutive seeds add no new behavioral
    coverage (the exploration has saturated). ``0`` disables early-stop (sweep every seed).
    Lets a sweep right-size itself instead of guessing a fixed seed count."""

    # Coverage-guided mutation (``Simulation.coverage_guided``).
    guided_budget: int = 256
    """Total runs a coverage-guided mutation sweep may spend before stopping (it also stops early
    on the first invariant violation). The guided run is one seed-derived lineage rooted at the
    first value of :attr:`seeds`; ``count`` sizes the initial workload and bounds growth."""

    # Value-level trace capture (``read_your_writes`` / value invariants).
    capture_values: bool = False
    """Capture redaction-applied call **values** (write payloads + read results) onto the trace,
    so value-level invariants can assert on *what* was written/read, not just which key. Off by
    default — the trace stays id-only (matching production). Sim data is synthetic, and fields a
    spec declares sensitive (``encryption.encrypted``/``.searchable``) are masked to
    ``"<redacted>"`` even when captured."""

    # Reachability ("sometimes") assertions (``Simulation.coverage`` / ``Cluster``).
    reachability_targets: frozenset[str] = frozenset()
    """States the sweep must reach at least once (``forze_dst.oracle.reachability.reached`` labels). A
    target no seed ever hits is a *reachability failure* — false confidence, the dangerous
    interleaving never fired. ``coverage()`` folds these across the sweep into
    :attr:`~forze_dst.oracle.coverage.CoverageStats.reachability`. Empty disables the check."""

    # Nondeterminism streams (compiled per-run from sub-seeds derived from the master seed).
    faults: FaultPolicy | None = None
    """Declarative, seeded fault injection over the port seam (error / timeout / crash).
    Compiled with ``derive_seed(seed, "fault")`` — no caller-supplied RNG. ``cluster``
    (ClusterConfig) is added by S6."""

    latency: LatencyProfile | None = None
    """Declarative, seeded simulated-I/O latency (per-route distributions). Compiled with
    ``derive_seed(seed, "latency")``. Overrides ``Simulation.latency`` (the raw-callable escape
    hatch) when set."""

    # Runtime / crash-restart.
    runtime: bool = False
    """Drive the workload inside the real ``ExecutionRuntime.scope()`` — lifecycle startup runs
    before the workload, graceful drain + shutdown after — instead of a bare ``ExecutionContext``
    (decision D4: keep both; bare is the default, lighter, with no background-task interference).
    The crash/restart scenario always restarts under the runtime regardless of this flag."""

    cluster: ClusterConfig | None = None
    """Topology for a multi-runtime (distributed) run driven by :class:`~forze_dst.Cluster`:
    N nodes over one shared store, with optional network partitions. One master seed still
    drives the whole cluster (each node derives independent fault sub-seeds). ``None`` for a
    single-process run."""

    crash: CrashPolicy | None = None
    """When set, ``run()`` executes the crash → restart → recovery scenario instead of the plain
    workload: the workload runs under a seeded :class:`~forze_dst.faults.CrashPolicy` (compiled
    with ``derive_seed(seed, "crash")``), the process *dies* at a matched port boundary (no
    graceful shutdown), then a fresh runtime restarts over the SAME persisted store and the
    ``Simulation.recover`` pass runs before the invariants are checked. Finds recovery bugs —
    lost after-commit work, partial non-transactional writes. Uses the scenario machinery
    (arrange → act), auto-deriving the scenario when none is passed."""

    # ....................... #

    @property
    def perturb(self) -> bool:
        """Whether interleavings are perturbed (any scheduler other than deterministic FIFO)."""

        return self.scheduler.perturb

    # ....................... #
    # Presets — intent-named intensity tiers over the ~20 knobs. Each scales the *search*
    # (seeds, scheduler, concurrency, workload size); environment (faults/latency/crash) stays
    # explicit since the right policy is app-specific. Every field is still overridable —
    # ``SimulationConfig.thorough(seeds=range(1000), concurrency=16)``.

    @classmethod
    def quick(cls, **overrides: Any) -> "SimulationConfig":
        """A fast inner-loop sweep — a handful of seeds under the default shuffle, small
        workloads. Runs in seconds; the config to reach for while iterating."""

        overrides.setdefault("seeds", range(16))
        overrides.setdefault("act_count", 8)
        overrides.setdefault("count", 16)
        return cls(**overrides)

    @classmethod
    def thorough(cls, **overrides: Any) -> "SimulationConfig":
        """A pre-merge sweep — a broad seed range under PCT (biased toward deep interleavings),
        higher concurrency, and longer workloads. The 'before I ship' run."""

        overrides.setdefault("seeds", range(256))
        overrides.setdefault("scheduler", PCTScheduler(depth=3))
        overrides.setdefault("concurrency", 8)
        overrides.setdefault("act_count", 24)
        return cls(**overrides)

    @classmethod
    def nightly(cls, **overrides: Any) -> "SimulationConfig":
        """A heavy overnight sweep — thousands of seeds under deeper PCT, sized for
        :func:`~forze_dst.artifacts.parallel_sweep`. Add a fault / latency policy for a
        hostile-environment run."""

        overrides.setdefault("seeds", range(2048))
        overrides.setdefault("scheduler", PCTScheduler(depth=4))
        overrides.setdefault("concurrency", 12)
        overrides.setdefault("act_count", 32)
        return cls(**overrides)

    @classmethod
    def reproduce(cls, seed: int, **overrides: Any) -> "SimulationConfig":
        """A single-seed debug run — sweep exactly *seed*. Reach for it with a seed from a
        :class:`~forze_dst.ViolationReport` to re-drive just that timeline."""

        overrides.setdefault("seeds", (seed,))
        return cls(**overrides)
