"""Multi-runtime distributed DST — the capstone: N real runtimes over one shared store.

A :class:`Cluster` runs *N* real :class:`~forze.application.execution.runtime.ExecutionRuntime`
nodes concurrently on the deterministic loop, each over its own scope but sharing one
``MockState`` — the in-memory stand-in for the shared broker/store. One master seed drives the
whole cluster: each node derives an independent fault sub-seed, the schedule perturbs the
interleaving of every node's steps, and a :class:`~forze_dst.config.PartitionSchedule` cuts
node-groups off from the shared infrastructure for virtual-time windows (a network split,
modeled at the seam as the gated surfaces becoming *unreachable* — a retryable error — so a
correct retry/outbox flow survives and heals while a naive one loses work).

Distributed invariants — mutual exclusion (no split brain), exactly-once-effect, no lost
update, linearizability — are the ordinary :mod:`forze_dst.oracle.invariants`, asserted over the
folded multi-node history. On a violation the cluster minimizes by **dropping nodes** (the
classic "it already breaks with two") and returns a reproducible :class:`ViolationReport`.
"""

from __future__ import annotations

import asyncio
import random
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Awaitable, Callable, Sequence, cast, final

import attrs

from forze.application.execution import (
    DepsModule,
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
)
from forze.application.contracts.interception import (
    PortCall,
    PortInterceptor,
    PortNext,
)
from forze.application.execution.interception import LatencyModel
from forze.base.exceptions import exc
from forze.base.primitives import derive_seed, monotonic
from forze_dst.config import (
    ClusterConfig,
    PartitionSchedule,
    SchedulerKind,
    SimulationConfig,
)
from forze_dst.faults import SimulatedCrash, compile_fault_policy
from forze_dst.engines.projection import fold_runtime_trace
from forze_dst.oracle.invariants import Invariant, check
from forze_dst.latency import compile_latency
from forze_dst.oracle import ViolationReport, minimize
from forze_dst.oracle.recorder import History, Recorder, bind_recorder, record_event
from forze_dst.runtime import run_simulation
from forze_dst.scheduler import pct_scheduler_factory

# ----------------------- #

NodeWorkload = Callable[[int, ExecutionContext], Awaitable[None]]
"""A node's work, given its id and its own execution context (over the shared store)."""

ClusterDeps = Callable[[Any], "DepsModule | Sequence[DepsModule]"]
"""Build a node's deps from the shared store — typically ``lambda state: MockDepsModule(state=state)``.
The store object is opaque to the cluster (the app supplies both it and this factory), so this
package stays free of any adapter/substrate dependency, exactly like ``Simulation.deps``."""

Hook = Callable[[ExecutionContext], Awaitable[None]]


# ....................... #


@final
@attrs.define(kw_only=True)
class _PartitionInterceptor:
    """Cut a node off from the gated surfaces while it is on the wrong side of a split.

    During an isolation window the node's calls to a gated surface raise a retryable
    ``exc.infrastructure`` (``code="dst.partition"``) — modeling the broker/store being
    unreachable across the split — and are recorded on the report's injected-environment
    timeline. A correct retry/outbox flow keeps the work pending and delivers it once the
    partition heals; a fire-and-forget flow loses it. A window's ``loss`` below ``1.0`` makes
    the link *lossy* rather than fully cut: each gated call drops with that seeded probability,
    so some slip through (a flaky link the clean-cut model can't express).
    """

    node_id: int
    """The node's id."""

    schedule: PartitionSchedule
    """The partition schedule."""

    rng: random.Random
    """Per-node, seed-derived RNG that rolls lossy-link drops (unused for a clean ``loss=1.0``
    cut, so a hard partition stays byte-identical to the no-RNG behavior)."""

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> Any:
        if self.schedule.gates(call.surface):
            loss = self.schedule.loss_at(self.node_id, monotonic())

            # loss == 1.0 → a clean cut: always drop, never touch the RNG (replay-stable with the
            # group-based model). 0 < loss < 1 → a lossy link: roll the seeded RNG per call.
            if loss >= 1.0 or (loss > 0.0 and self.rng.random() < loss):
                await asyncio.sleep(
                    0
                )  # yield so the unreachable failure interleaves at the boundary
                record_event(
                    "partition",
                    at=monotonic(),
                    node=self.node_id,
                    surface=call.surface,
                    route=call.route,
                    op=call.op,
                    loss=loss,
                )

                raise exc.infrastructure(
                    f"node {self.node_id} partitioned from {call.surface}",
                    code="dst.partition",
                )

        return await nxt(call)


# ....................... #


@final
@attrs.define(kw_only=True)
class Cluster:
    """Drive N real runtimes over one shared store under partitions + faults; check invariants."""

    deps: ClusterDeps
    """Builds a node's deps from the shared ``MockState`` (fresh store per run)."""

    node: NodeWorkload
    """The per-node workload — ``async (node_id, ctx) -> None`` — run concurrently on every node."""

    invariants: Sequence[Invariant] = attrs.field(factory=tuple)
    """Distributed invariants checked over the folded multi-node history."""

    state_factory: Callable[[], Any]
    """Builds the shared store for a run — typically ``MockState`` — created once and shared by
    every node (the app provides it, so this package needs no substrate import)."""

    setup: Hook | None = None
    """Optional - seed shared state once (over a clean context) before the nodes start."""

    observe: Hook | None = None
    """Optional - record domain facts once (over a clean context) after all nodes finish."""

    fingerprint: str | None = None
    """Optional registry fingerprint stamped on the report (the cluster drives ctx ports, not a
    single operation registry, so it is supplied rather than derived)."""

    # ....................... #

    def run(self, config: SimulationConfig) -> ViolationReport | None:
        """Sweep ``config.seeds``; on the first violating seed, minimize nodes and report.

        ``config.cluster`` supplies the topology (node count + partitions); ``config.faults`` /
        ``config.latency`` / scheduler / seeds drive the rest — one master seed for the whole
        cluster. Returns the first violating seed's reproducible :class:`ViolationReport`.
        """

        cluster = config.cluster or ClusterConfig()
        node_ids = list(range(cluster.nodes))

        for seed in config.seeds:
            report = self._attempt(node_ids, seed=seed, config=config)

            if report is not None:
                return report

        return None

    # ....................... #

    def histories(self, config: SimulationConfig) -> list[History]:
        """Run *every* seed (no short-circuit on violation) and return all folded histories.

        :meth:`run` stops at the first violating seed — right for finding a bug, wrong for a
        cross-sweep *reachability* check, which can only conclude a state was never reached after
        the whole sweep. Feed the result to
        :func:`~forze_dst.oracle.reachability.assess_reachability` to prove the dangerous interleavings
        (partition isolated a contender, crash mid-flush, …) actually fired across the sweep.
        """

        cluster = config.cluster or ClusterConfig()
        node_ids = list(range(cluster.nodes))

        return [self._run(node_ids, seed=seed, config=config) for seed in config.seeds]

    # ....................... #

    def _attempt(
        self,
        node_ids: Sequence[int],
        *,
        seed: int,
        config: SimulationConfig,
    ) -> ViolationReport | None:
        def run(ids: Sequence[int]) -> History:
            return self._run(ids, seed=seed, config=config)

        if not check(run(node_ids), self.invariants):
            return None

        # Minimize by dropping nodes — the smallest cluster that still breaks (often two).
        minimal = minimize(
            list(node_ids), lambda subset: bool(check(run(subset), self.invariants))
        )
        history = run(minimal)

        return ViolationReport(
            seed=seed,
            schedule_seed=(derive_seed(seed, "schedule") if config.perturb else None),
            violations=tuple(check(history, self.invariants)),
            workload=tuple(("node", node_id) for node_id in minimal),
            history=history,
            registry_fingerprint=self.fingerprint,
        )

    # ....................... #

    def _run(
        self,
        node_ids: Sequence[int],
        *,
        seed: int,
        config: SimulationConfig,
    ) -> History:
        recorder = Recorder(seed=seed)

        async def driver() -> None:
            state = self.state_factory()

            if self.setup is not None:
                async with self._context(state, -1, seed, config, clean=True) as ctx:
                    await self.setup(ctx)

            async def run_node(node_id: int) -> None:
                async with self._context(state, node_id, seed, config) as ctx:
                    try:
                        await self.node(node_id, ctx)

                    except SimulatedCrash:
                        record_event(
                            "crash", node=node_id
                        )  # the node died; cluster proceeds

                    except Exception as error:  # noqa: BLE001 — one node's failure must not abort the cluster
                        # Record it so an invariant can catch a node that stopped on an
                        # unexpected error (e.g. a bug in a port call outside the op trace),
                        # rather than leaving the cluster history looking clean.
                        record_event(
                            "node_error", node=node_id, error=type(error).__name__
                        )
                    finally:
                        fold_runtime_trace(ctx)

            await asyncio.gather(*(run_node(node_id) for node_id in node_ids))

            if self.observe is not None:
                async with self._context(state, -2, seed, config, clean=True) as ctx:
                    await self.observe(ctx)
                    fold_runtime_trace(ctx)

        scheduler, schedule_seed = self._interleaving(seed, config)

        with bind_recorder(recorder):
            run_simulation(
                driver,
                seed=derive_seed(seed, "entropy"),
                schedule_seed=schedule_seed,
                scheduler=scheduler,
                epoch=config.epoch,
                latency=self._latency(seed, config),
            )

        return recorder.history

    # ....................... #

    @asynccontextmanager
    async def _context(
        self,
        state: Any,
        node_id: int,
        seed: int,
        config: SimulationConfig,
        *,
        clean: bool = False,
    ) -> AsyncGenerator[ExecutionContext]:
        """A node's context — a real ``ExecutionRuntime`` scope over the shared store.

        Unless *clean* (setup/observe run with no injected environment), the node's resolved
        ports are wrapped with its partition interceptor and a fault interceptor seeded from a
        per-node sub-seed (so each node's fault stream is independent yet reproducible).
        """

        produced = self.deps(state)
        modules: tuple[DepsModule, ...] = (
            tuple(produced)
            if isinstance(produced, (list, tuple))
            else (cast("DepsModule", produced),)
        )

        registry = DepsRegistry.from_modules(*modules).with_tracing(
            runtime=True, capture_values=config.capture_values
        )

        if not clean:
            interceptors: list[PortInterceptor] = []
            cluster = config.cluster or ClusterConfig()

            if cluster.partitions is not None:
                part_seed = derive_seed(
                    derive_seed(seed, "partition"), f"node-{node_id}"
                )
                interceptors.append(
                    _PartitionInterceptor(
                        node_id=node_id,
                        schedule=cluster.partitions,
                        rng=random.Random(part_seed),  # nosec B311 - seeded sim partition loss
                    )
                )

            if config.faults is not None:
                node_seed = derive_seed(derive_seed(seed, "fault"), f"node-{node_id}")
                interceptors.append(
                    compile_fault_policy(
                        config.faults,
                        random.Random(node_seed),  # nosec B311 - seeded sim faults
                    )
                )

            if interceptors:
                registry = registry.with_interceptors(*interceptors)

        runtime = ExecutionRuntime(deps=registry.freeze())

        async with runtime.scope():
            yield runtime.get_context()

    # ....................... #

    @staticmethod
    def _interleaving(
        seed: int,
        config: SimulationConfig,
    ) -> tuple[object | None, int | None]:
        """The (scheduler, schedule_seed) for a run, mirroring the single-process harness."""

        if config.scheduler is SchedulerKind.PCT:
            factory = pct_scheduler_factory(
                depth=config.pct_depth, steps=config.pct_steps
            )
            return factory(derive_seed(seed, "schedule")), None

        return None, (derive_seed(seed, "schedule") if config.perturb else None)

    # ....................... #

    @staticmethod
    def _latency(seed: int, config: SimulationConfig) -> LatencyModel | None:
        """The run's compiled latency model (from the seed-derived latency RNG), if declared."""

        if config.latency is None:
            return None

        return compile_latency(
            config.latency,
            random.Random(derive_seed(seed, "latency")),  # nosec B311 - seeded sim latency
        )
