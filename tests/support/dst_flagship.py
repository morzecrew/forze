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
from collections.abc import Callable, Mapping, Sequence
from functools import partial
from types import MappingProxyType
from typing import final

import attrs

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp, HybridLogicalClock, monotonic
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import Cluster, SimulationConfig
from forze_dst.artifacts.sweep import SeedOutcome
from forze_dst.cluster import ClusterConfig, Partition, PartitionSchedule
from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.invariants import check, expect, monotonic_per, mutual_exclusion
from forze_dst.markers import reached, record_event
from forze_dst.oracle import Behavior, behavioral_coverage, reached_labels, run_recorded
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

CONTENDED = "lock-contended"
WRITE_RETRIED = "write-retried"


# ....................... #
# The fault-profile axis — the environment the same critical section must survive.


@final
@attrs.define(frozen=True, kw_only=True)
class FaultProfile:
    """One environment for the dlock scenario: same workload, different infrastructure hostility.

    A profile carries its own :attr:`targets` rather than sharing :data:`DLOCK_TARGETS`, because
    which dangerous states are *reachable at all* is a property of the environment: a profile that
    injects no errors and cuts no link can never drive ``write-retried``, so a shared target set
    would fail every run of it. Coupling the two keeps both halves honest — the targets say what
    this environment is supposed to provoke, and the sweep fails if it did not.
    """

    name: str
    """Cell name, used in the nightly matrix and in artifact filenames."""

    targets: frozenset[str]
    """The reachability labels this environment must drive. Never empty — see below."""

    isolated: frozenset[int] = frozenset()
    """Nodes cut from ``document_command`` during :attr:`window`."""

    window: tuple[float, float] | None = None
    """The partition's ``(start, end)`` in virtual seconds, or ``None`` for no partition."""

    loss: float = 1.0
    """Drop probability for an isolated node's gated calls — ``1.0`` is a clean cut, below that a
    flaky link where some calls slip through."""

    error: float = 0.0
    """Probability that a ``document_command`` call fails outright, partition or not."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # An empty target set passes `reachability(...).satisfied` vacuously, which would make this
        # profile's whole band green without proving it ever raced. Refuse it at declaration.
        if not self.targets:
            raise exc.configuration(f"Fault profile {self.name!r} declares no reachability targets")

        if (self.window is None) != (not self.isolated):
            raise exc.configuration(
                f"Fault profile {self.name!r} must declare a window and isolated nodes together",
            )

        if self.window is not None and self.window[0] >= self.window[1]:
            raise exc.configuration(f"Fault profile {self.name!r} has an empty partition window")

        if not 0.0 <= self.error <= 1.0:
            raise exc.configuration(f"Fault profile {self.name!r} has an error rate outside [0, 1]")

        # `Partition` refuses this too, but only when `partitions()` is finally called — so
        # without it here one bad field is caught at declaration and another at first use.
        if not 0.0 < self.loss <= 1.0:
            raise exc.configuration(f"Fault profile {self.name!r} has a link loss outside (0, 1]")

    # ....................... #

    def partitions(self) -> PartitionSchedule | None:
        """The partition schedule this profile describes, or ``None`` when it cuts nothing."""

        if self.window is None:
            return None

        start, end = self.window

        return PartitionSchedule(
            windows=(Partition(start=start, end=end, isolated=self.isolated, loss=self.loss),),
            surfaces=frozenset({"document_command"}),
        )


# ....................... #

DLOCK_PROFILES: Mapping[str, FaultProfile] = MappingProxyType(
    {
        profile.name: profile
        for profile in (
            # The historical shape — a clean cut of one node plus a moderate error rate. Every
            # other profile is a deliberate departure from exactly one of its axes.
            FaultProfile(
                name="baseline",
                isolated=frozenset({1}),
                window=(0.5, 1.5),
                error=0.3,
                targets=DLOCK_TARGETS,
            ),
            # No partition, no injected errors: the lock alone against pure contention. A violation
            # here is the mutual-exclusion logic itself, with no infrastructure noise to blame — the
            # one profile whose failure has a single possible cause. `write-retried` is
            # unreachable by construction, so it is not declared.
            FaultProfile(
                name="contention",
                targets=frozenset({CONTENDED}),
            ),
            # A lossy link rather than a clean break: some calls slip through mid-partition, so the
            # retry loop sees interleaved success and failure instead of a solid outage block.
            FaultProfile(
                name="flaky-link",
                isolated=frozenset({1}),
                window=(0.3, 1.8),
                loss=0.5,
                error=0.1,
                targets=DLOCK_TARGETS,
            ),
            # Two of three nodes cut, for most of the run, with heavy errors on top: the harshest
            # environment the scenario is expected to survive.
            FaultProfile(
                name="storm",
                isolated=frozenset({0, 2}),
                window=(0.2, 2.0),
                error=0.7,
                targets=DLOCK_TARGETS,
            ),
        )
    },
)
"""Every environment the nightly runs the dlock scenario in, by name."""


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


def dlock_config(seeds: Sequence[int], *, profile: str = "baseline") -> SimulationConfig:
    """The simulation config for a seed band under the named environment."""

    chosen = DLOCK_PROFILES[profile]
    rules = (FaultRule(surface="document_command", error=chosen.error),) if chosen.error else ()

    return SimulationConfig(
        seeds=seeds,
        cluster=ClusterConfig(nodes=3, partitions=chosen.partitions()),
        faults=FaultPolicy(rules=rules),
        reachability_targets=chosen.targets,
    )


def run_dlock_seed(seed: int, profile: str = "baseline") -> SeedOutcome:
    """Run the guarded-dlock scenario at one *seed* and report its outcome (picklable for the pool)."""

    counter = shared_counter()
    cluster = guarded_cluster(counter)
    histories = cluster.histories(dlock_config([seed], profile=profile))

    behaviors = frozenset[Behavior]().union(
        *(behavioral_coverage(history) for history in histories)
    )
    reached = frozenset[str]().union(*(reached_labels(history) for history in histories))
    violated = any(bool(check(history, DLOCK_INVARIANTS)) for history in histories)

    return SeedOutcome(seed=seed, violated=violated, behaviors=behaviors, reached=reached)


def dlock_target(profile: str) -> Callable[[int], SeedOutcome]:
    """A one-argument seed target bound to *profile*, picklable for the process pool.

    ``partial`` of a module-level function pickles by reference, so a worker rebuilds it by
    importing this module — the same reason the seed runners are top-level functions and not
    closures.
    """

    if profile not in DLOCK_PROFILES:
        raise exc.configuration(f"Unknown dlock fault profile {profile!r}")

    return partial(run_dlock_seed, profile=profile)


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
