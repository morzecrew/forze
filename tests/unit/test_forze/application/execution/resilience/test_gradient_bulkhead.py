"""Delay-based (Gradient2) bulkhead: controller delegation, validation, wiring."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    BulkheadStrategy,
    GradientBulkheadStrategy,
    ResiliencePolicy,
)
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.limiter import Gradient2Limiter
from forze.application.execution.resilience.state import AdaptiveBulkheadState
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _gradient_state(**kw: object) -> AdaptiveBulkheadState:
    params: dict[str, object] = {
        "latency_threshold": float("inf"),
        "min_concurrency": 1,
        "max_concurrency": 50,
        "max_queue": 0,
        "backoff_ratio": 0.5,
        "increase_step": 1.0,
        "cooldown": 0.0,
        "limiter": Gradient2Limiter(
            initial_limit=50, max_limit=50, min_limit=1, long_window=50
        ),
    }
    params.update(kw)

    return AdaptiveBulkheadState(**params)  # type: ignore[arg-type]


def _strategy(**kw: object) -> GradientBulkheadStrategy:
    params: dict[str, object] = {"max_concurrency": 8}
    params.update(kw)

    return GradientBulkheadStrategy(**params)  # type: ignore[arg-type]


# ----------------------- #


class TestControllerDelegation:
    def test_latency_inflation_contracts_the_limit(self) -> None:
        state = _gradient_state()

        # Warm the baseline at a low latency, fully loaded.
        for _ in range(200):
            state.on_complete(0.01, now=100.0, inflight=50)

        warmed = state.limit

        # Latency inflates 10x: the gradient controller contracts the limit.
        for _ in range(50):
            state.on_complete(0.1, now=100.0, inflight=int(state.limit))

        assert state.limit < warmed
        assert state.limit >= 1  # never below the floor

    def test_no_load_guard_holds_limit(self) -> None:
        state = _gradient_state()

        # Healthy latency but trivial concurrency: nothing to probe with.
        for _ in range(200):
            state.on_complete(0.01, now=100.0, inflight=1)

        assert state.limit == 50

    def test_aimd_fields_are_inert_in_gradient_mode(self) -> None:
        # latency_threshold/backoff/cooldown are ignored when a limiter is set.
        state = _gradient_state(latency_threshold=0.001, cooldown=999.0)

        for _ in range(100):
            state.on_complete(0.01, now=100.0, inflight=50)

        # No AIMD breach/cooldown interaction — the limiter alone drives it.
        assert state.limit == 50


class TestStrategyValidation:
    def test_rejects_invalid_params(self) -> None:
        for kw in (
            {"min_concurrency": 0},
            {"max_concurrency": 1, "min_concurrency": 2},
            {"max_queue": -1},
            {"rtt_tolerance": 0.5},
            {"smoothing": 0.0},
            {"smoothing": 1.5},
            {"long_window": 0},
            {"headroom": -1.0},
            {"queue_adaptive_lifo": True},  # needs max_queue >= 1
            {"queue_target": timedelta(milliseconds=5)},  # needs max_queue >= 1
        ):
            with pytest.raises(CoreException):
                _strategy(**kw)

    def test_accepts_queue_management_with_queue(self) -> None:
        strat = _strategy(
            max_queue=4,
            queue_target=timedelta(milliseconds=5),
            queue_adaptive_lifo=True,
            prioritized=True,
        )

        assert strat.max_queue == 4


class TestPolicyExclusion:
    def test_cannot_combine_with_other_bulkhead_kinds(self) -> None:
        pairs = (
            (BulkheadStrategy(max_concurrency=1), _strategy()),
            (
                AdaptiveBulkheadStrategy(
                    latency_threshold=timedelta(milliseconds=100), max_concurrency=2
                ),
                _strategy(),
            ),
        )

        for a, b in pairs:
            with pytest.raises(CoreException) as ei:
                ResiliencePolicy(name="p", strategies=(a, b))

            assert ei.value.kind is ExceptionKind.CONFIGURATION


class TestExecutorWiring:
    def _executor(self, strat: GradientBulkheadStrategy) -> InProcessResilienceExecutor:
        return InProcessResilienceExecutor(
            policies={"p": ResiliencePolicy(name="p", strategies=(strat,))},
            clock=_Clock(),
        )

    async def test_admits_and_rejects_beyond_limit_and_queue(self) -> None:
        executor = self._executor(_strategy(max_concurrency=1, max_queue=0))
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

    async def test_limit_is_observable(self) -> None:
        executor = self._executor(_strategy(max_concurrency=8))

        async def ok() -> str:
            return "ok"

        assert await executor.run(ok, policy="p") == "ok"

        ((policy, _, limit),) = executor.adaptive_bulkhead_limits()
        assert policy == "p"
        assert limit == 8.0  # starts at max_concurrency
