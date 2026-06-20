"""Flagship DST: Forze passes its own simulation (E2 — "prove it").

The strongest claim a DST harness can make is not "I explored N seeds" but "I drove my
own distributed machinery through the dangerous interleavings and it held". A green
invariant over a fault that never bit is *false confidence*; so each scenario here pairs a
safety invariant (must *always* hold) with a **reachability target** (must *sometimes* be
reached) — and asserts both. Only when the reachability target fires do we know the
invariant was tested against the hard case, not a quiet run.

Two of Forze's own distributed primitives, under simulation:

* **distributed lock** — a lock-guarded critical section over N runtimes sharing one store,
  under a network partition + seeded write faults: mutual exclusion holds and no update is
  lost, *and* reachability proves a contender actually spun on the held lock while the
  partition isolated a node mid-write (the proven-dangerous interleaving). The unguarded
  variant is caught + minimized + reproduced.
* **hybrid logical clock** — replicas exchanging stamps under perturbed interleaving: every
  replica's clock is monotonic and every merged event's HLC strictly exceeds its cause,
  *and* reachability proves a replica merged a remote stamp that was ahead of its own. A
  broken clock that ignores the remote stamp is caught by the causality invariant.
"""

from __future__ import annotations

import asyncio

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp, HybridLogicalClock, monotonic
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import (
    Cluster,
    ClusterConfig,
    FaultPolicy,
    FaultRule,
    Partition,
    PartitionSchedule,
    SimulationConfig,
    assess_reachability,
    check,
    expect,
    monotonic_per,
    mutual_exclusion,
    reached,
    reached_labels,
    record_event,
    run_recorded,
    sometimes,
)
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #

_KEY = "counter"
_LOCK = DistributedLockSpec(name="locks")
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
_DLOCK_INVARIANTS = (_MUTUAL_EXCLUSION, _NO_LOST_UPDATE)

# The reachability targets that make the green result meaningful: a contender actually
# raced on the held lock, and the partition actually struck during the guarded write.
_DLOCK_TARGETS = frozenset({"lock-contended", "write-retried"})


def _deps(state: MockState) -> MockDepsModule:
    return MockDepsModule(state=state)


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


def _guarded_cluster(counter: dict[str, object]) -> Cluster:
    """A dlock-guarded critical section that marks the dangerous states it passes through."""

    async def node(node_id: int, ctx: ExecutionContext) -> None:
        owner = str(node_id)
        lock = ctx.dlock.command(_LOCK)

        while True:  # acquire (dlock is not partitioned; None ⇒ held by a peer, so spin)
            if await lock.acquire(_KEY, owner) is not None:
                break
            reached("lock-contended")  # a peer held the lock → genuine contention
            await asyncio.sleep(_RETRY)

        try:
            counter["attempts"].append(node_id)  # type: ignore[attr-defined]
            start = monotonic()
            current = counter["value"]  # type: ignore[assignment]

            while True:  # the guarded write — retried through the partition until it heals
                try:
                    await ctx.document.command(MARKER_SPEC).create(MarkerCreate(by=node_id))
                    break
                except CoreException:
                    reached("write-retried")  # infra failure (partition/fault) struck mid-section
                    await asyncio.sleep(_RETRY)

            counter["value"] = current + 1  # type: ignore[operator]
            record_event("hold", resource=_KEY, holder=owner, start=start, end=monotonic())
        finally:
            await lock.release(_KEY, owner)

    return Cluster(
        deps=_deps,
        state_factory=MockState,
        node=node,
        setup=_reset(counter),
        observe=_observe(counter),
        invariants=_DLOCK_INVARIANTS,
    )


def _dlock_config(seeds: range, *, isolated: frozenset[int] = frozenset({1})) -> SimulationConfig:
    return SimulationConfig(
        seeds=seeds,
        cluster=ClusterConfig(
            nodes=3,
            partitions=PartitionSchedule(
                windows=(Partition(start=0.5, end=1.5, isolated=isolated),),
                surfaces=frozenset({"document_command"}),
            ),
        ),
        faults=FaultPolicy(rules=(FaultRule(surface="document_command", error=0.3),)),
        reachability_targets=_DLOCK_TARGETS,
    )


class TestDistributedLockProven:
    def test_holds_AND_the_dangerous_interleaving_actually_fired(self) -> None:
        counter = _shared_counter()
        cluster = _guarded_cluster(counter)
        config = _dlock_config(range(8))

        histories = cluster.histories(config)

        # Safety: no run violated mutual exclusion or lost an update.
        assert all(not check(history, _DLOCK_INVARIANTS) for history in histories)

        # Liveness of the *test*: the hard states were actually reached across the sweep —
        # so the green safety result was tested against contention + a mid-section partition,
        # not a quiet interleaving where the faults never bit.
        reachability = assess_reachability(histories, _DLOCK_TARGETS)
        assert reachability.satisfied, reachability.format()

        # And the partition genuinely isolated a node mid-run (recorded on the timeline).
        assert sometimes(histories, lambda h: bool(h.of_kind("partition")))

    def test_unguarded_variant_is_caught_minimized_and_reproduced(self) -> None:
        counter = _shared_counter()

        async def node(node_id: int, _ctx: ExecutionContext) -> None:
            counter["attempts"].append(node_id)  # type: ignore[attr-defined]
            start = monotonic()
            current = counter["value"]  # type: ignore[assignment]
            await asyncio.sleep(1.0)  # hold across virtual time → critical sections overlap
            counter["value"] = current + 1  # type: ignore[operator]
            record_event(
                "hold", resource=_KEY, holder=str(node_id), start=start, end=monotonic()
            )

        cluster = Cluster(
            deps=_deps,
            state_factory=MockState,
            node=node,
            setup=_reset(counter),
            observe=_observe(counter),
            invariants=_DLOCK_INVARIANTS,
        )
        config = SimulationConfig(seeds=range(3), cluster=ClusterConfig(nodes=4))

        report = cluster.run(config)
        assert report is not None
        assert {v.invariant for v in report.violations} & {"mutual_exclusion", "expect"}
        assert 2 <= len(report.workload) < 4  # two contenders are the minimal counterexample

        # Reproducible: same seed, same minimal counterexample.
        again = cluster.run(config)
        assert again is not None and again.seed == report.seed

    def test_single_node_cannot_reach_contention(self) -> None:
        # A one-node cluster can never contend — the reachability check must report it as a
        # *failure* (false confidence: a passing safety result that never exercised the race).
        counter = _shared_counter()
        cluster = _guarded_cluster(counter)
        config = SimulationConfig(
            seeds=range(3),
            cluster=ClusterConfig(nodes=1),
            reachability_targets=_DLOCK_TARGETS,
        )

        reachability = assess_reachability(cluster.histories(config), _DLOCK_TARGETS)
        assert not reachability.satisfied
        assert "lock-contended" in reachability.unreached


# ....................... #
# Hybrid logical clock: causal monotonicity across replicas under perturbed interleaving.

_REPLICAS = 3
_ROUNDS = 3

_HLC_MONOTONIC = monotonic_per("hlc", "value", actor="actor")
_HLC_CAUSAL = expect(
    "causal",
    lambda e: e.fields["effect"] > e.fields["cause"],
    message="merged HLC did not strictly exceed the cause it reacted to",
)
_HLC_INVARIANTS = (_HLC_MONOTONIC, _HLC_CAUSAL)


def _hlc_scenario(*, causal: bool):
    """Replicas exchange stamps; each merge issues an HLC that must exceed the cause.

    With ``causal`` the merge uses ``HybridLogicalClock.update`` (correct); without it the
    replica ignores the remote stamp and just ticks locally (the bug a wall-clock or a
    naive Lamport-without-merge would have) — caught by the causality invariant.
    """

    inboxes: dict[int, list[tuple[int, int]]] = {r: [] for r in range(_REPLICAS)}
    clocks = {r: HybridLogicalClock() for r in range(_REPLICAS)}

    async def replica(rid: int) -> None:
        clock = clocks[rid]

        for _ in range(_ROUNDS):
            # Asymmetric local progress (higher ids tick faster) so the replicas' logical
            # clocks genuinely diverge — virtual time doesn't advance on sleep(0), so without
            # this every clock would tick in lockstep and no stamp would ever be *ahead*.
            for _ in range(rid + 1):
                issued = clock.now()
                record_event("hlc", actor=rid, value=issued.pack())
            for other in range(_REPLICAS):
                if other != rid:
                    inboxes[other].append((rid, clock.last.pack()))

            await asyncio.sleep(0)  # yield → the scheduler interleaves the replicas

            pending, inboxes[rid] = inboxes[rid], []
            for _src, remote_packed in pending:
                before = clock.last.pack()
                if remote_packed > before:
                    reached("hlc-merged-ahead")  # the merge path actually exercised causality

                if causal:
                    merged = clock.update(HlcTimestamp.unpack(remote_packed))
                else:
                    merged = clock.now()  # BROKEN: drops the remote stamp

                record_event("hlc", actor=rid, value=merged.pack())
                record_event("causal", cause=remote_packed, effect=merged.pack())

            await asyncio.sleep(0)

    async def scenario() -> None:
        await asyncio.gather(*(replica(rid) for rid in range(_REPLICAS)))

    return scenario


class TestHybridLogicalClockProven:
    def test_causal_monotonicity_holds_AND_a_merge_ahead_actually_fired(self) -> None:
        histories = [
            run_recorded(_hlc_scenario(causal=True), seed=seed, schedule_seed=seed)
            for seed in range(12)
        ]

        # Safety: every replica's clock is monotonic and every merge exceeds its cause.
        assert all(not check(history, _HLC_INVARIANTS) for history in histories)

        # The causality path was actually exercised — a replica merged a remote stamp that
        # was ahead of its own, so the monotonicity result is not vacuous.
        assert sometimes(histories, lambda h: "hlc-merged-ahead" in reached_labels(h))

    def test_broken_clock_that_drops_the_remote_stamp_is_caught(self) -> None:
        histories = [
            run_recorded(_hlc_scenario(causal=False), seed=seed, schedule_seed=seed)
            for seed in range(12)
        ]

        # At least one interleaving must surface a merged event whose HLC did not exceed
        # the cause it reacted to — the causality invariant catches the dropped stamp.
        violating = [history for history in histories if check(history, (_HLC_CAUSAL,))]
        assert violating, "the broken clock should violate causality under some interleaving"
