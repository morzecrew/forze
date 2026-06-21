"""DST P6: multi-node simulation — N replicas over one shared in-memory backend.

Replicas run concurrently on the deterministic loop, sharing a single ``MockState`` (the
in-memory stand-in for a shared broker/store), and contend on a distributed lock around
a critical-section increment. A correct lock serializes them — mutual exclusion holds
and no update is lost — across every explored seed/interleaving. Skipping the lock makes
the holds overlap and updates race; the oracle catches it and minimizes to two replicas.
"""

from __future__ import annotations

import asyncio
from typing import Sequence

from forze.application.contracts.dlock import DistributedLockSpec
from forze.base.primitives import monotonic

from forze_dst.invariants import expect, mutual_exclusion
from forze_dst.oracle import explore
from forze_dst.oracle.recorder import record_event
from forze_mock.adapters import MockDistributedLockAdapter, MockState

# ----------------------- #

_KEY = "counter"
_LOCK_SPEC = DistributedLockSpec(name="locks")
_HOLD = 1.0  # virtual seconds the critical section spans (so holds are real intervals)
_RETRY = 0.05

_MUTUAL_EXCLUSION = mutual_exclusion(
    "hold", resource="resource", start="start", end="end"
)
_NO_LOST_UPDATE = expect(
    "result",
    lambda event: event.fields["final"] == event.fields["expected"],
    message="lost update under contention",
)


def _build_cluster(*, use_lock: bool):
    def build(replicas: Sequence[int]):
        async def scenario() -> None:
            state = MockState()
            lock = MockDistributedLockAdapter(
                spec=_LOCK_SPEC, state=state, namespace="locks"
            )
            counter = {"value": 0}

            async def critical_section(owner: str) -> None:
                start = monotonic()
                current = counter["value"]
                await asyncio.sleep(_HOLD)  # hold the section across virtual time
                counter["value"] = current + 1
                record_event(
                    "hold", resource=_KEY, holder=owner, start=start, end=monotonic()
                )

            async def replica(owner: str) -> None:
                if use_lock:
                    while await lock.acquire(_KEY, owner) is None:
                        await asyncio.sleep(_RETRY)  # spin until the lock is free
                    try:
                        await critical_section(owner)
                    finally:
                        await lock.release(_KEY, owner)
                else:
                    await critical_section(owner)

            await asyncio.gather(*(replica(str(r)) for r in replicas))
            record_event("result", final=counter["value"], expected=len(replicas))

        return scenario

    return build


_INVARIANTS = [_MUTUAL_EXCLUSION, _NO_LOST_UPDATE]


class TestMultiNode:
    def test_correct_dlock_preserves_mutual_exclusion(self) -> None:
        report = explore(
            _build_cluster(use_lock=True),
            list(range(5)),
            _INVARIANTS,
            seeds=range(20),
        )
        assert report is None  # serialized by the lock — no violation under any seed

    def test_unlocked_cluster_is_caught_and_minimized(self) -> None:
        report = explore(
            _build_cluster(use_lock=False),
            list(range(5)),
            _INVARIANTS,
            seeds=range(3),
        )
        assert report is not None
        # Concurrent critical sections overlap and lose updates.
        assert {v.invariant for v in report.violations} & {"mutual_exclusion", "expect"}
        # Two contending replicas are the minimal counterexample.
        assert 2 <= len(report.workload) < 5
