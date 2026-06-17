"""Adaptive (AIMD) bulkhead: controller schedule, admission, executor wiring."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    BulkheadStrategy,
    ResiliencePolicy,
)
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.state import AdaptiveBulkheadState
from forze.application.execution.resilience.store import InMemoryLatencyDigestStore
from forze.base.exceptions import CoreException, ExceptionKind, exc

# ----------------------- #


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _state(**kw: object) -> AdaptiveBulkheadState:
    params: dict[str, object] = {
        "latency_threshold": 0.1,
        "min_concurrency": 1,
        "max_concurrency": 10,
        "max_queue": 0,
        "backoff_ratio": 0.5,
        "increase_step": 1.0,
        "cooldown": 1.0,
    }
    params.update(kw)
    return AdaptiveBulkheadState(**params)  # type: ignore[arg-type]


def _strategy(**kw: object) -> AdaptiveBulkheadStrategy:
    params: dict[str, object] = {
        "latency_threshold": timedelta(milliseconds=100),
        "max_concurrency": 2,
    }
    params.update(kw)
    return AdaptiveBulkheadStrategy(**params)  # type: ignore[arg-type]


# ----------------------- #


class TestAimdController:
    def test_starts_at_max_and_is_capped(self) -> None:
        state = _state(max_concurrency=4)

        assert state.limit == 4.0
        state.on_complete(0.01, now=100.0)
        assert state.limit == 4.0  # ceiling clamp

    def test_breach_decreases_multiplicatively_to_floor(self) -> None:
        state = _state(max_concurrency=8, min_concurrency=2, backoff_ratio=0.5)

        assert state.on_complete(1.0, now=100.0) is True
        assert state.limit == 4.0

        assert state.on_complete(1.0, now=102.0) is True
        assert state.limit == 2.0

        assert state.on_complete(1.0, now=104.0) is True
        assert state.limit == 2.0  # floor clamp

    def test_cooldown_coalesces_burst_of_breaches(self) -> None:
        state = _state(max_concurrency=8, backoff_ratio=0.5, cooldown=1.0)

        assert state.on_complete(1.0, now=100.0) is True
        # A burst of slow completions within the cooldown backs off once.
        assert state.on_complete(1.0, now=100.1) is False
        assert state.on_complete(1.0, now=100.9) is False
        assert state.limit == 4.0

        assert state.on_complete(1.0, now=101.5) is True
        assert state.limit == 2.0

    def test_additive_recovery_one_slot_per_limit_successes(self) -> None:
        state = _state(max_concurrency=10, backoff_ratio=0.5)
        state.on_complete(1.0, now=100.0)  # 10 -> 5

        for _ in range(5):
            state.on_complete(0.01, now=102.0)

        # 5 in-budget completions at limit ~5 recover ~one slot.
        assert 5.9 <= state.limit <= 6.1

    async def test_shrink_gates_admission_without_eviction(self) -> None:
        state = _state(max_concurrency=2, min_concurrency=1, backoff_ratio=0.5)

        await state.acquire()
        await state.acquire()
        state.on_complete(1.0, now=100.0)  # limit 2 -> 1 with 2 in flight

        assert state.in_use == 2  # nothing evicted
        assert state.can_admit() is False

        state.release()
        assert state.can_admit() is False  # still at the new limit (1 in use)

        state.release()
        assert state.can_admit() is True

    async def test_fifo_waiters_wake_on_release(self) -> None:
        state = _state(max_concurrency=1, max_queue=2)

        await state.acquire()
        order: list[str] = []

        async def waiter(name: str) -> None:
            await state.acquire()
            order.append(name)
            state.release()

        a = asyncio.create_task(waiter("a"))
        await asyncio.sleep(0)
        b = asyncio.create_task(waiter("b"))
        await asyncio.sleep(0)

        state.release()
        await asyncio.gather(a, b)

        assert order == ["a", "b"]


class TestStrategyValidation:
    def test_rejects_invalid_params(self) -> None:
        for kw in (
            {"latency_threshold": timedelta(0)},
            {"latency_quantile": 0.0},
            {"latency_quantile": 1.0},
            {"min_concurrency": 0},
            {"max_concurrency": 1, "min_concurrency": 2},
            {"backoff_ratio": 1.0},
            {"increase_step": 0.0},
            {"max_queue": -1},
        ):
            with pytest.raises(CoreException):
                _strategy(**kw)

    def test_policy_rejects_both_bulkhead_kinds(self) -> None:
        with pytest.raises(CoreException) as ei:
            ResiliencePolicy(
                name="p",
                strategies=(BulkheadStrategy(max_concurrency=1), _strategy()),
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION


class TestExecutorIntegration:
    def _executor(
        self, strat: AdaptiveBulkheadStrategy, clock: _Clock
    ) -> InProcessResilienceExecutor:
        return InProcessResilienceExecutor(
            policies={"p": ResiliencePolicy(name="p", strategies=(strat,))},
            clock=clock,
        )

    async def test_rejects_beyond_limit_and_queue(self) -> None:
        executor = self._executor(_strategy(max_concurrency=1, max_queue=0), _Clock())
        started = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> str:
            started.set()
            await release.wait()
            return "ok"

        task = asyncio.create_task(executor.run(holder, policy="p"))
        await started.wait()

        with pytest.raises(CoreException) as ei:
            await executor.run(holder, policy="p")

        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
        release.set()
        assert await task == "ok"

    async def test_slow_call_backs_off_limit(self) -> None:
        clock = _Clock()
        executor = self._executor(
            _strategy(max_concurrency=8, backoff_ratio=0.5), clock
        )

        async def slow() -> str:
            clock.now += 1.0  # observed latency 1s > 100ms threshold
            return "ok"

        assert await executor.run(slow, policy="p") == "ok"

        ((_, _, limit),) = executor.adaptive_bulkhead_limits()
        assert limit == 4.0

    async def test_fast_failure_leaves_limit_untouched(self) -> None:
        clock = _Clock()
        executor = self._executor(_strategy(max_concurrency=8), clock)

        async def boom() -> str:
            raise exc.conflict("nope")

        with pytest.raises(CoreException):
            await executor.run(boom, policy="p")

        ((_, _, limit),) = executor.adaptive_bulkhead_limits()
        assert limit == 8.0


class TestExpiredWaiterDrop:
    async def test_expired_waiter_failed_at_wake_instead_of_granted(self) -> None:
        from forze.application.execution.context.deadline import bind_deadline

        state = _state(max_concurrency=1, max_queue=2)
        await state.acquire()  # hold the only slot

        async def park_with_deadline() -> None:
            with bind_deadline(0.01):
                await state.acquire()

        waiter = asyncio.create_task(park_with_deadline())
        await asyncio.sleep(0.03)  # let the parked waiter's budget expire

        state.release()  # wake path runs: expired waiter must be dropped

        with pytest.raises(CoreException) as ei:
            await waiter

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"
        assert state.in_use == 0  # slot was returned, not granted to the dead waiter

    async def test_waiter_with_budget_left_is_granted(self) -> None:
        from forze.application.execution.context.deadline import bind_deadline

        state = _state(max_concurrency=1, max_queue=2)
        await state.acquire()

        async def park_with_deadline() -> None:
            with bind_deadline(30.0):
                await state.acquire()
                state.release()

        waiter = asyncio.create_task(park_with_deadline())
        await asyncio.sleep(0)
        state.release()

        await waiter  # granted normally


class TestQueueManagement:
    """CoDel sojourn shedding + adaptive LIFO on the unified wait queue."""

    def _q_state(self, clock: _Clock, **kw: object) -> AdaptiveBulkheadState:
        params: dict[str, object] = {
            "max_concurrency": 1,
            "max_queue": 3,
            "clock": clock,
            "queue_target_s": 0.005,
            "queue_interval_s": 0.1,
        }
        params.update(kw)
        return _state(**params)

    async def test_codel_sheds_stale_waiter_under_congestion(self) -> None:
        clock = _Clock()
        state = self._q_state(clock)
        await state.acquire()  # hold the slot

        stale = asyncio.create_task(state.acquire())
        await asyncio.sleep(0)

        clock.now += 0.2  # congested (queue non-empty > interval); sojourn 0.2 > target
        fresh = asyncio.create_task(state.acquire())
        await asyncio.sleep(0)

        state.release()

        with pytest.raises(CoreException) as ei:
            await stale

        assert ei.value.code == "bulkhead_queue_shed"

        await fresh  # the fresh waiter got the slot instead
        assert state.in_use == 1
        state.release()

    async def test_recently_empty_queue_tolerates_interval_sojourn(self) -> None:
        clock = _Clock()
        state = self._q_state(clock)
        await state.acquire()

        waiter = asyncio.create_task(state.acquire())
        await asyncio.sleep(0)

        clock.now += 0.05  # below the interval: not congested, generous allowance
        state.release()

        await waiter  # granted, not shed
        state.release()

    async def test_adaptive_lifo_serves_newest_first_under_congestion(self) -> None:
        clock = _Clock()
        state = self._q_state(clock, queue_target_s=None, queue_adaptive_lifo=True)
        await state.acquire()
        order: list[str] = []

        async def waiter(name: str) -> None:
            await state.acquire()
            order.append(name)
            state.release()

        a = asyncio.create_task(waiter("a"))
        await asyncio.sleep(0)
        b = asyncio.create_task(waiter("b"))
        await asyncio.sleep(0)

        clock.now += 0.2  # sustained congestion
        state.release()
        await asyncio.gather(a, b)

        assert order == ["b", "a"]  # newest first while congested

    async def test_fifo_preserved_without_congestion(self) -> None:
        clock = _Clock()
        state = self._q_state(clock, queue_target_s=None, queue_adaptive_lifo=True)
        await state.acquire()
        order: list[str] = []

        async def waiter(name: str) -> None:
            await state.acquire()
            order.append(name)
            state.release()

        a = asyncio.create_task(waiter("a"))
        await asyncio.sleep(0)
        b = asyncio.create_task(waiter("b"))
        await asyncio.sleep(0)

        clock.now += 0.05  # queue young: not congested
        state.release()
        await asyncio.gather(a, b)

        assert order == ["a", "b"]

    def test_queue_knob_validation(self) -> None:
        for kw in (
            {"queue_target": timedelta(seconds=0), "max_queue": 1},
            {"queue_target": timedelta(seconds=1), "max_queue": 1},  # >= interval
            {"queue_target": timedelta(milliseconds=5)},  # max_queue == 0
            {"queue_adaptive_lifo": True},  # max_queue == 0
        ):
            with pytest.raises(CoreException):
                _strategy(**kw)

            with pytest.raises(CoreException):
                BulkheadStrategy(max_concurrency=1, **kw)  # type: ignore[arg-type]

    async def test_fixed_strategy_threads_queue_knobs(self) -> None:
        executor = InProcessResilienceExecutor(
            policies={
                "p": ResiliencePolicy(
                    name="p",
                    strategies=(
                        BulkheadStrategy(
                            max_concurrency=2,
                            max_queue=4,
                            queue_target=timedelta(milliseconds=5),
                            queue_adaptive_lifo=True,
                        ),
                    ),
                )
            },
        )

        async def fn() -> str:
            return "ok"

        assert await executor.run(fn, policy="p") == "ok"

        ((_, state),) = executor._bulkheads.items()  # noqa: SLF001
        assert state.queue_target_s == 0.005
        assert state.queue_adaptive_lifo is True
        assert state.limit == 2.0


class TestQuantileSignal:
    """Percentile-windowed breach via the latency digest store + AIMD math.

    The store owns the windowed-P² estimator (so the signal can be process-local
    or fleet-shared); these drive ``store.observe -> on_complete -> store.reset``
    exactly as the executor does, asserting the original integrated behavior.
    """

    _KEY = ("p", None)

    def _q_state(self, **kw: object) -> AdaptiveBulkheadState:
        params: dict[str, object] = {
            "latency_threshold": 0.1,
            "latency_quantile": 0.95,
            "max_concurrency": 8,
            "backoff_ratio": 0.5,
            "cooldown": 1.0,
        }
        params.update(kw)
        return _state(**params)

    def _q_strat(self) -> AdaptiveBulkheadStrategy:
        return AdaptiveBulkheadStrategy(
            latency_threshold=timedelta(milliseconds=100),
            max_concurrency=8,
            latency_quantile=0.95,
            backoff_ratio=0.5,
            cooldown=timedelta(seconds=1),
        )

    async def _drive(
        self,
        store: InMemoryLatencyDigestStore,
        strat: AdaptiveBulkheadStrategy,
        state: AdaptiveBulkheadState,
        latency: float,
        now: float,
    ) -> bool:
        quantile = await store.observe(self._KEY, latency, strat)
        decreased = state.on_complete(latency, now, quantile)

        if decreased:
            await store.reset(self._KEY, strat)

        return decreased

    async def test_single_outlier_does_not_back_off(self) -> None:
        state, store, strat = (
            self._q_state(),
            InMemoryLatencyDigestStore(),
            self._q_strat(),
        )

        for _ in range(30):
            assert await self._drive(store, strat, state, 0.01, 100.0) is False

        # One 10s outlier: a per-sample signal would halve the limit here.
        assert await self._drive(store, strat, state, 10.0, 100.0) is False
        assert state.limit == 8.0

    async def test_shifted_distribution_backs_off(self) -> None:
        state, store, strat = (
            self._q_state(),
            InMemoryLatencyDigestStore(),
            self._q_strat(),
        )

        decreases = 0

        for _ in range(10):
            decreases += await self._drive(store, strat, state, 1.0, 100.0)

        # The estimator warms at five samples, breaches once (cooldown
        # coalesces the rest of the burst).
        assert decreases == 1
        assert state.limit == 4.0

    async def test_backoff_opens_fresh_measurement_epoch(self) -> None:
        state, store, strat = (
            self._q_state(),
            InMemoryLatencyDigestStore(),
            self._q_strat(),
        )

        for _ in range(5):
            await self._drive(store, strat, state, 1.0, 100.0)

        assert state.limit == 4.0  # first breach

        # Past the cooldown, the reset estimator is still warming: four more
        # slow completions cannot re-breach...
        for _ in range(4):
            assert await self._drive(store, strat, state, 1.0, 102.0) is False

        assert state.limit == 4.0

        # ...the fifth defines the new epoch's estimate and breaches again.
        assert await self._drive(store, strat, state, 1.0, 102.0) is True
        assert state.limit == 2.0

    async def test_recovery_after_backoff_increases_additively(self) -> None:
        state, store, strat = (
            self._q_state(),
            InMemoryLatencyDigestStore(),
            self._q_strat(),
        )

        for _ in range(5):
            await self._drive(store, strat, state, 1.0, 100.0)

        assert state.limit == 4.0

        # The new concurrency is healthy: fast completions recover the limit
        # without the stale slow history vetoing the increase.
        for _ in range(8):
            await self._drive(store, strat, state, 0.01, 102.0)

        assert state.limit > 4.0

    async def test_warming_estimator_holds_the_limit(self) -> None:
        state, store, strat = (
            self._q_state(),
            InMemoryLatencyDigestStore(),
            self._q_strat(),
        )

        for _ in range(4):
            assert await self._drive(store, strat, state, 5.0, 100.0) is False

        assert state.limit == 8.0  # no signal yet: neither breach nor growth

    async def test_executor_threads_latency_quantile(self) -> None:
        clock = _Clock()
        executor = InProcessResilienceExecutor(
            policies={
                "p": ResiliencePolicy(
                    name="p",
                    strategies=(_strategy(max_concurrency=8, latency_quantile=0.95),),
                )
            },
            clock=clock,
        )

        async def slow() -> str:
            clock.now += 1.0  # one slow sample: per-sample mode would back off
            return "ok"

        assert await executor.run(slow, policy="p") == "ok"

        ((_, _, limit),) = executor.adaptive_bulkhead_limits()
        assert limit == 8.0  # per-sample mode would have halved it
