"""Criticality-based prioritized load shedding on the bulkhead wait queue."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    BulkheadStrategy,
)
from forze.application.execution.context.criticality import (
    Criticality,
    bind_criticality,
)
from forze.application.execution.resilience.state import AdaptiveBulkheadState
from forze.base.exceptions import CoreException

# ----------------------- #


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _state(clock: _Clock | None = None, **kw: object) -> AdaptiveBulkheadState:
    params: dict[str, object] = {
        "latency_threshold": 0.1,
        "min_concurrency": 1,
        "max_concurrency": 1,
        "max_queue": 1,
        "backoff_ratio": 0.5,
        "increase_step": 1.0,
        "cooldown": 1.0,
        "prioritized": True,
    }

    if clock is not None:
        params["clock"] = clock

    params.update(kw)

    return AdaptiveBulkheadState(**params)  # type: ignore[arg-type]


async def _park(state: AdaptiveBulkheadState, tier: Criticality) -> None:
    with bind_criticality(tier):
        await state.acquire()


# ----------------------- #


class TestPriorityAdmissionDisplacement:
    async def test_higher_criticality_displaces_lowest_waiter(self) -> None:
        state = _state()
        await state.acquire()  # hold the only slot (NORMAL)

        low = asyncio.create_task(_park(state, Criticality.BEST_EFFORT))
        await asyncio.sleep(0)

        # Queue is full: an equal-tier arrival cannot displace, a higher one can.
        with bind_criticality(Criticality.BEST_EFFORT):
            assert state.can_admit() is False

        high = asyncio.create_task(_park(state, Criticality.CRITICAL))
        await asyncio.sleep(0)

        with pytest.raises(CoreException) as ei:
            await low

        assert ei.value.code == "bulkhead_queue_shed"

        # The critical request now holds the slot once the holder releases.
        state.release()
        await high
        assert state.in_use == 1
        state.release()

    async def test_can_admit_rejects_equal_or_lower_when_full(self) -> None:
        state = _state()
        await state.acquire()

        normal = asyncio.create_task(_park(state, Criticality.NORMAL))
        await asyncio.sleep(0)

        # A NORMAL arrival cannot displace an equal-tier waiter.
        with bind_criticality(Criticality.NORMAL):
            assert state.can_admit() is False

        # A DEGRADED (lower) arrival likewise cannot.
        with bind_criticality(Criticality.DEGRADED):
            assert state.can_admit() is False

        # A CRITICAL (higher) arrival can.
        with bind_criticality(Criticality.CRITICAL):
            assert state.can_admit() is True

        state.release()
        await normal
        state.release()

    async def test_lowest_of_several_is_displaced(self) -> None:
        state = _state(max_queue=3)
        await state.acquire()

        degraded = asyncio.create_task(_park(state, Criticality.DEGRADED))
        await asyncio.sleep(0)
        best = asyncio.create_task(_park(state, Criticality.BEST_EFFORT))
        await asyncio.sleep(0)
        normal = asyncio.create_task(_park(state, Criticality.NORMAL))
        await asyncio.sleep(0)
        # Queue is now full (3); a CRITICAL arrival sheds the BEST_EFFORT one.
        high = asyncio.create_task(_park(state, Criticality.CRITICAL))
        await asyncio.sleep(0)

        with pytest.raises(CoreException) as ei:
            await best

        assert ei.value.code == "bulkhead_queue_shed"
        assert not degraded.done() and not normal.done() and not high.done()

        for _ in range(4):
            state.release()
            await asyncio.sleep(0)

        await asyncio.gather(degraded, normal, high)


class TestCriticalityScaledSojourn:
    async def test_low_tier_shed_first_under_congestion(self) -> None:
        clock = _Clock()
        state = _state(
            clock,
            max_queue=3,
            queue_target_s=0.01,
            queue_interval_s=0.1,
        )
        await state.acquire()  # hold the slot

        low = asyncio.create_task(_park(state, Criticality.BEST_EFFORT))
        await asyncio.sleep(0)

        clock.now += 0.2  # sustained congestion (queue non-empty > interval)
        high = asyncio.create_task(_park(state, Criticality.CRITICAL))
        await asyncio.sleep(0)

        clock.now += 0.005  # low's sojourn >> target*0.25; high's << target*2.0
        state.release()

        # The low-criticality waiter breaches its tightened allowance and sheds.
        with pytest.raises(CoreException) as ei:
            await low

        assert ei.value.code == "bulkhead_queue_shed"

        # The critical waiter keeps its grace and is granted the slot.
        await high
        assert state.in_use == 1
        state.release()


class TestBehaviorPreservedWhenUniform:
    async def test_all_equal_criticality_is_plain_fifo(self) -> None:
        clock = _Clock()
        state = _state(clock, max_queue=3, queue_target_s=None)
        await state.acquire()
        order: list[str] = []

        async def waiter(name: str) -> None:
            await _park(state, Criticality.NORMAL)
            order.append(name)
            state.release()

        a = asyncio.create_task(waiter("a"))
        await asyncio.sleep(0)
        b = asyncio.create_task(waiter("b"))
        await asyncio.sleep(0)

        clock.now += 0.05
        state.release()
        await asyncio.gather(a, b)

        assert order == ["a", "b"]  # unchanged FIFO when no tier differs

    def test_strategy_requires_queue_for_prioritized(self) -> None:
        for factory in (
            lambda: BulkheadStrategy(max_concurrency=2, prioritized=True),
            lambda: AdaptiveBulkheadStrategy(
                latency_threshold=timedelta(seconds=1),
                max_concurrency=2,
                prioritized=True,
            ),
        ):
            with pytest.raises(CoreException):
                factory()
