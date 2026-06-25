"""Flagship DST scenarios as reusable, picklable seed targets — the substance behind P5's sweeps.

The two scenarios that *prove Forze passes its own simulation* (a distributed lock under a network
partition + write faults, and a hybrid logical clock under perturbed interleaving) live here as
plain top-level functions so they can be:

* driven point-in-time as fast smoke tests (``test_distributed_prove.py`` — kept),
* swept **wide** through :func:`~forze_dst.artifacts.sweep.parallel_sweep` for a nightly fuzz
  (B.1), and
* run as a **fast corpus** every build, a merge guard that re-checks a small band plus any seed that
  ever found a bug (B.2).

``run_dlock_seed`` / ``run_hlc_seed`` are top-level functions returning a picklable
:class:`~forze_dst.artifacts.sweep.SeedOutcome`, so a process-pool worker can run one seed by
importing this module (a closure could not cross the process boundary). Each builds its own fresh
state per call, so distinct seeds share nothing — the inter-seed parallelism the sweep relies on.
"""

from __future__ import annotations

import asyncio
from typing import Sequence

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp, HybridLogicalClock, monotonic
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import Cluster, SimulationConfig
from forze_dst.artifacts.sweep import SeedOutcome
from forze_dst.cluster import ClusterConfig, Partition, PartitionSchedule
from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.invariants import check, expect, monotonic_per, mutual_exclusion
from forze_dst.markers import reached, record_event
from forze_dst.oracle import behavioral_coverage, reached_labels, run_recorded
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #
# Seed bands. FAST runs every build (the merge guard); WIDE is the fuzz-marked nightly sweep. Append
# any seed that ever found a bug to the *_REGRESSION_SEEDS tuple so it is re-checked forever.

DLOCK_FAST = range(8)
DLOCK_WIDE = range(64)
DLOCK_REGRESSION_SEEDS: tuple[int, ...] = ()

HLC_FAST = range(12)
HLC_WIDE = range(128)
HLC_REGRESSION_SEEDS: tuple[int, ...] = ()


def dlock_corpus_seeds() -> tuple[int, ...]:
    """The fast merge-guard band for the dlock scenario plus every regression seed."""

    return tuple(DLOCK_FAST) + DLOCK_REGRESSION_SEEDS


def hlc_corpus_seeds() -> tuple[int, ...]:
    """The fast merge-guard band for the HLC scenario plus every regression seed."""

    return tuple(HLC_FAST) + HLC_REGRESSION_SEEDS


# ----------------------- #
# Distributed lock under partition + faults.

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
DLOCK_INVARIANTS = (_MUTUAL_EXCLUSION, _NO_LOST_UPDATE)

# The reachability targets that make a green result meaningful (a contender actually raced on the
# held lock, and the partition struck during the guarded write).
DLOCK_TARGETS = frozenset({"lock-contended", "write-retried"})


def _deps(state: MockState) -> MockDepsModule:
    return MockDepsModule(state=state)


def shared_counter() -> dict[str, object]:
    return {"value": 0, "attempts": []}


def _reset(counter: dict[str, object]):  # type: ignore[no-untyped-def]
    async def setup(_ctx: ExecutionContext) -> None:
        counter["value"] = 0
        counter["attempts"] = []

    return setup


def _observe(counter: dict[str, object]):  # type: ignore[no-untyped-def]
    async def observe(_ctx: ExecutionContext) -> None:
        record_event(
            "result",
            final=counter["value"],
            expected=len(counter["attempts"]),  # type: ignore[arg-type]
        )

    return observe


def guarded_cluster(counter: dict[str, object]) -> Cluster:
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
        invariants=DLOCK_INVARIANTS,
    )


def dlock_config(seeds: Sequence[int], *, isolated: frozenset[int] = frozenset({1})) -> SimulationConfig:
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
        reachability_targets=DLOCK_TARGETS,
    )


def run_dlock_seed(seed: int) -> SeedOutcome:
    """Run the guarded-dlock scenario at one *seed* and report its outcome (picklable for the pool)."""

    counter = shared_counter()
    cluster = guarded_cluster(counter)
    histories = cluster.histories(dlock_config([seed]))

    behaviors = frozenset[tuple[object, ...]]().union(
        *(behavioral_coverage(history) for history in histories)
    )
    reached = frozenset[str]().union(*(reached_labels(history) for history in histories))
    violated = any(bool(check(history, DLOCK_INVARIANTS)) for history in histories)

    return SeedOutcome(seed=seed, violated=violated, behaviors=behaviors, reached=reached)


# ----------------------- #
# Hybrid logical clock: causal monotonicity across replicas under perturbed interleaving.

_REPLICAS = 3
_ROUNDS = 3

_HLC_MONOTONIC = monotonic_per("hlc", "value", actor="actor")
_HLC_CAUSAL = expect(
    "causal",
    lambda e: e.fields["effect"] > e.fields["cause"],
    message="merged HLC did not strictly exceed the cause it reacted to",
)
HLC_INVARIANTS = (_HLC_MONOTONIC, _HLC_CAUSAL)

# The reachability target that makes a green HLC result meaningful: a merge actually carried a remote
# stamp ahead of local time (the causal path the invariants guard was genuinely exercised).
HLC_TARGETS = frozenset({"hlc-merged-ahead"})


def hlc_scenario(*, causal: bool):  # type: ignore[no-untyped-def]
    """Replicas exchange stamps; each merge issues an HLC that must exceed the cause.

    With ``causal`` the merge uses ``HybridLogicalClock.update`` (correct); without it the replica
    ignores the remote stamp and ticks locally (the bug a naive Lamport-without-merge would have).
    """

    inboxes: dict[int, list[tuple[int, int]]] = {r: [] for r in range(_REPLICAS)}
    clocks = {r: HybridLogicalClock() for r in range(_REPLICAS)}

    async def replica(rid: int) -> None:
        clock = clocks[rid]

        for _ in range(_ROUNDS):
            for _ in range(rid + 1):  # asymmetric local progress so clocks genuinely diverge
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


def run_hlc_seed(seed: int) -> SeedOutcome:
    """Run the causal-HLC scenario at one *seed* and report its outcome (picklable for the pool)."""

    history = run_recorded(hlc_scenario(causal=True), seed=seed, schedule_seed=seed)

    return SeedOutcome(
        seed=seed,
        violated=bool(check(history, HLC_INVARIANTS)),
        behaviors=behavioral_coverage(history),
        reached=reached_labels(history),
    )
