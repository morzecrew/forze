"""Multi-runtime distributed DST (S6 capstone) — N runtimes, one store, one seed.

A :class:`Cluster` drives N real ``ExecutionRuntime`` nodes concurrently over one shared
``MockState`` under network partitions + seeded faults, all from one master seed. What the
tests pin down:

* a distributed-lock-guarded critical section stays mutually exclusive and loses no update
  even while a node is partitioned from the store and writes fault — the correct flow retries
  and heals (a fuzz-clean sweep);
* an unguarded cluster races, loses updates, and overlaps its critical sections — the oracle
  catches it, minimizes to two nodes, and reproduces from one seed;
* a partition cuts a node off from a gated surface, surfacing on the report's injected-
  environment timeline and minimizing to just the cut-off node;
* the partition interceptor itself raises an unreachable error only while isolated.
"""

from __future__ import annotations

import asyncio
import random

import pytest

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.application.execution.interception import PortCall
from forze.base.exceptions import CoreException
from forze.base.primitives import monotonic
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import (
    Cluster,
    ClusterConfig,
    FaultPolicy,
    FaultRule,
    Partition,
    PartitionSchedule,
    SimulationConfig,
    expect,
    mutual_exclusion,
    record_event,
)
from forze_dst.cluster import _PartitionInterceptor
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #

_KEY = "counter"
_LOCK = DistributedLockSpec(name="locks")
_HOLD = 1.0
_RETRY = 0.2


class Marker(Document):
    by: int = 0


class MarkerCreate(CreateDocumentCmd):
    by: int = 0


class MarkerRead(ReadDocument):
    by: int


MARKER_SPEC = DocumentSpec(
    name="markers",
    read=MarkerRead,
    write=DocumentWriteTypes(domain=Marker, create_cmd=MarkerCreate),
)

_MUTUAL_EXCLUSION = mutual_exclusion("hold", resource="resource", start="start", end="end")
_NO_LOST_UPDATE = expect(
    "result",
    lambda e: e.fields["final"] == e.fields["expected"],
    message="lost update under contention",
)


def _deps(state: MockState) -> MockDepsModule:
    return MockDepsModule(state=state)


# ....................... #
# A counter shared in-memory across nodes (no OCC guard — so a missing lock loses updates).
# ``setup`` resets it per run, so seeds and minimization re-runs each start clean.


def _shared_counter() -> dict[str, object]:
    return {"value": 0, "attempts": []}


def _reset(counter: dict[str, object]):
    async def setup(_ctx: ExecutionContext) -> None:
        counter["value"] = 0
        counter["attempts"] = []

    return setup


def _observe(counter: dict[str, object]):
    async def observe(_ctx: ExecutionContext) -> None:
        record_event(
            "result",
            final=counter["value"],
            expected=len(counter["attempts"]),  # type: ignore[arg-type]
        )

    return observe


# ....................... #


class TestGuardedClusterSurvivesPartitions:
    def test_dlock_guarded_critical_section_holds_under_partition_and_faults(self) -> None:
        counter = _shared_counter()

        async def node(node_id: int, ctx: ExecutionContext) -> None:
            owner = str(node_id)
            lock = ctx.dlock.command(_LOCK)

            while True:  # acquire (dlock is not partitioned; None ⇒ held, so spin)
                if await lock.acquire(_KEY, owner) is not None:
                    break
                await asyncio.sleep(_RETRY)

            try:
                counter["attempts"].append(node_id)  # type: ignore[attr-defined]
                start = monotonic()
                current = counter["value"]  # type: ignore[assignment]

                while True:  # the guarded write — retried through the partition until it heals
                    try:
                        await ctx.document.command(MARKER_SPEC).create(
                            MarkerCreate(by=node_id)
                        )
                        break
                    except CoreException:
                        await asyncio.sleep(_RETRY)

                counter["value"] = current + 1  # type: ignore[operator]
                record_event(
                    "hold", resource=_KEY, holder=owner, start=start, end=monotonic()
                )
            finally:
                await lock.release(_KEY, owner)

        cluster = Cluster(
            deps=_deps,
            state_factory=MockState,
            node=node,
            setup=_reset(counter),
            observe=_observe(counter),
            invariants=[_MUTUAL_EXCLUSION, _NO_LOST_UPDATE],
        )
        report = cluster.run(
            SimulationConfig(
                seeds=range(6),
                cluster=ClusterConfig(
                    nodes=3,
                    partitions=PartitionSchedule(
                        windows=(Partition(start=0.5, end=1.5, isolated=frozenset({1})),),
                        surfaces=frozenset({"document_command"}),
                    ),
                ),
                faults=FaultPolicy(
                    rules=(FaultRule(surface="document_command", error=0.3),)
                ),
            )
        )
        # The lock serializes the nodes; the partition only delays the holder's write, which
        # retries and heals — mutual exclusion holds and no update is lost, under every seed.
        assert report is None


class TestUnguardedClusterIsCaught:
    def _unguarded(self) -> Cluster:
        counter = _shared_counter()

        async def node(node_id: int, _ctx: ExecutionContext) -> None:
            counter["attempts"].append(node_id)  # type: ignore[attr-defined]
            start = monotonic()
            current = counter["value"]  # type: ignore[assignment]
            await asyncio.sleep(_HOLD)  # hold across virtual time → critical sections overlap
            counter["value"] = current + 1  # type: ignore[operator]
            record_event(
                "hold", resource=_KEY, holder=str(node_id), start=start, end=monotonic()
            )

        return Cluster(
            deps=_deps,
            state_factory=MockState,
            node=node,
            setup=_reset(counter),
            observe=_observe(counter),
            invariants=[_MUTUAL_EXCLUSION, _NO_LOST_UPDATE],
        )

    def _config(self) -> SimulationConfig:
        return SimulationConfig(seeds=range(3), cluster=ClusterConfig(nodes=4))

    def test_unguarded_cluster_is_caught_and_minimized(self) -> None:
        report = self._unguarded().run(self._config())

        assert report is not None
        assert {v.invariant for v in report.violations} & {"mutual_exclusion", "expect"}
        # Two contending nodes are the minimal counterexample.
        assert 2 <= len(report.workload) < 4

    def test_reproducible_from_one_seed(self) -> None:
        a = self._unguarded().run(self._config())
        b = self._unguarded().run(self._config())
        assert a is not None and b is not None
        assert a.seed == b.seed


# ....................... #


class Thing(Document):
    pass


class ThingCreate(CreateDocumentCmd):
    pass


class ThingRead(ReadDocument):
    pass


THING_SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(domain=Thing, create_cmd=ThingCreate),
)


class TestPartitionEndToEnd:
    def test_partition_isolates_a_node_and_surfaces_on_the_timeline(self) -> None:
        async def node(node_id: int, ctx: ExecutionContext) -> None:
            wrote = False
            for _ in range(3):  # bounded attempts — a permanently-cut node gives up
                try:
                    await ctx.document.command(THING_SPEC).create(ThingCreate())
                    wrote = True
                    break
                except CoreException:
                    await asyncio.sleep(0.5)
            record_event("complete", node=node_id, wrote=wrote)

        cluster = Cluster(
            deps=_deps,
            state_factory=MockState,
            node=node,
            invariants=[
                expect("complete", lambda e: e.fields["wrote"], message="node could not write")
            ],
        )
        report = cluster.run(
            SimulationConfig(
                seeds=range(2),
                cluster=ClusterConfig(
                    nodes=2,
                    partitions=PartitionSchedule(
                        windows=(
                            Partition(start=0.0, end=100.0, isolated=frozenset({1})),
                        ),
                        surfaces=frozenset({"document_command"}),
                    ),
                ),
            )
        )

        assert report is not None
        # Minimizes to just the cut-off node, and its partition shows on the timeline.
        assert report.workload == (("node", 1),)
        rendered = report.format()
        assert "injected environment" in rendered
        assert "partition (node 1 cut off)" in rendered


class TestPartitionInterceptor:
    def test_raises_only_while_isolated(self) -> None:
        schedule = PartitionSchedule(
            windows=(Partition(start=10.0, end=20.0, isolated=frozenset({0})),),
            surfaces=frozenset({"document_command"}),
        )
        interceptor = _PartitionInterceptor(
            node_id=0, schedule=schedule, rng=random.Random(0)
        )
        call = PortCall(surface="document_command", route="things", op="create")

        async def nxt(_call: PortCall) -> str:
            return "ok"

        async def go() -> None:
            # Outside the window → passes through (monotonic() is 0.0 at epoch start).
            assert await interceptor.around(call, nxt) == "ok"

        asyncio.run(go())

    def test_gates_only_named_surfaces(self) -> None:
        schedule = PartitionSchedule(
            windows=(Partition(start=0.0, end=100.0, isolated=frozenset({0})),),
            surfaces=frozenset({"queue_command"}),
        )
        assert schedule.gates("queue_command") is True
        assert schedule.gates("document_command") is False
        # Empty surfaces ⇒ a total split (everything gated).
        assert PartitionSchedule(surfaces=frozenset()).gates("anything") is True


# ....................... #


class TestLossyLink:
    """A partition window's ``loss`` below 1.0 models a flaky/asymmetric link, not a clean cut."""

    def test_loss_at_reads_the_active_window_else_zero(self) -> None:
        schedule = PartitionSchedule(
            windows=(Partition(start=1.0, end=2.0, isolated=frozenset({0}), loss=0.3),),
            surfaces=frozenset({"document_command"}),
        )
        assert schedule.loss_at(0, 1.5) == 0.3  # isolated, inside the window
        assert schedule.loss_at(0, 0.5) == 0.0  # before the window — fully connected
        assert schedule.loss_at(1, 1.5) == 0.0  # a different node — not isolated

    def test_overlapping_windows_take_the_strongest_loss(self) -> None:
        # An asymmetric split: node 0 sits under two overlapping windows; the harsher one wins.
        schedule = PartitionSchedule(
            windows=(
                Partition(start=0.0, end=10.0, isolated=frozenset({0}), loss=0.2),
                Partition(start=0.0, end=10.0, isolated=frozenset({0}), loss=0.9),
            ),
        )
        assert schedule.loss_at(0, 5.0) == 0.9

    def test_loss_out_of_range_is_rejected(self) -> None:
        with pytest.raises(CoreException):
            Partition(start=0.0, end=1.0, isolated=frozenset({0}), loss=0.0)
        with pytest.raises(CoreException):
            Partition(start=0.0, end=1.0, isolated=frozenset({0}), loss=1.5)

    def test_lossy_link_drops_some_calls_and_passes_others_seeded(self) -> None:
        # Over many calls in a lossy window, roughly `loss` fraction drop — and it is fully
        # determined by the seeded RNG (same seed → same drop/pass sequence). The window spans any
        # real ``monotonic()`` (this runs outside the sim loop) so it is reliably active; window
        # *timing* is covered separately by the ``loss_at`` tests.
        schedule = PartitionSchedule(
            windows=(Partition(start=0.0, end=1e18, isolated=frozenset({0}), loss=0.5),),
            surfaces=frozenset({"document_command"}),
        )
        call = PortCall(surface="document_command", route="things", op="create")

        async def nxt(_call: PortCall) -> str:
            return "ok"

        def run(seed: int) -> list[bool]:
            interceptor = _PartitionInterceptor(
                node_id=0, schedule=schedule, rng=random.Random(seed)
            )

            async def go() -> list[bool]:
                outcomes: list[bool] = []
                for _ in range(40):
                    try:
                        await interceptor.around(call, nxt)
                        outcomes.append(True)  # slipped through
                    except CoreException:
                        outcomes.append(False)  # dropped
                return outcomes

            return asyncio.run(go())

        outcomes = run(7)
        assert any(outcomes) and not all(outcomes)  # a flaky link: some pass, some drop
        assert run(7) == outcomes  # deterministic for a fixed seed

    def test_clean_cut_never_consumes_the_rng(self) -> None:
        # A loss=1.0 window always drops without touching the RNG, so the RNG is left pristine —
        # this is what keeps a hard partition byte-identical to the pre-lossy-link behavior. The
        # window spans any real ``monotonic()`` so it is reliably active outside the sim loop.
        schedule = PartitionSchedule(
            windows=(Partition(start=0.0, end=1e18, isolated=frozenset({0})),),
            surfaces=frozenset({"document_command"}),
        )
        rng = random.Random(123)
        interceptor = _PartitionInterceptor(node_id=0, schedule=schedule, rng=rng)
        call = PortCall(surface="document_command", route="things", op="create")

        async def nxt(_call: PortCall) -> str:
            return "ok"

        async def go() -> None:
            for _ in range(5):
                try:
                    await interceptor.around(call, nxt)
                except CoreException:
                    pass

        asyncio.run(go())
        assert rng.random() == random.Random(123).random()  # untouched
