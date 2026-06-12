"""Tail-based hedging: P²-driven delay, primary-only sampling, clamps, gauge."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    HedgeStrategy,
    ResiliencePolicy,
    TimeoutStrategy,
)
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.state import HedgeDelayState
from forze.base.exceptions import CoreException

# ----------------------- #


def _strategy(**kw: object) -> HedgeStrategy:
    params: dict[str, object] = {
        "delay": timedelta(milliseconds=200),
        "max_attempts": 2,
        "adaptive_delay_quantile": 0.95,
    }
    params.update(kw)
    return HedgeStrategy(**params)  # type: ignore[arg-type]


def _executor(hedge: HedgeStrategy) -> InProcessResilienceExecutor:
    pol = ResiliencePolicy(
        name="h",
        strategies=(TimeoutStrategy(timeout=timedelta(seconds=30)),),
        hedge=hedge,
    )
    return InProcessResilienceExecutor(policies={"h": pol})


class _Fn:
    """Hedged callable whose nth attempt sleeps then returns."""

    def __init__(self, behaviors: list[tuple[float, str]]) -> None:
        self.behaviors = behaviors
        self.started = 0

    def __call__(self) -> Awaitable[str]:
        idx = self.started
        self.started += 1
        delay, outcome = self.behaviors[min(idx, len(self.behaviors) - 1)]

        async def run() -> str:
            await asyncio.sleep(delay)
            return outcome

        return run()


# ----------------------- #


class TestStrategyValidation:
    def test_rejects_invalid_params(self) -> None:
        for kw in (
            {"adaptive_delay_quantile": 0.0},
            {"adaptive_delay_quantile": 1.0},
            {"adaptive_delay_quantile": None, "delay_min": timedelta(seconds=1)},
            {"adaptive_delay_quantile": None, "delay_max": timedelta(seconds=1)},
            {"delay_min": timedelta(0)},
            {"delay_max": timedelta(0)},
            {
                "delay_min": timedelta(seconds=2),
                "delay_max": timedelta(seconds=1),
            },
        ):
            with pytest.raises(CoreException):
                _strategy(**kw)

    def test_fixed_strategy_unchanged(self) -> None:
        strat = HedgeStrategy(delay=timedelta(milliseconds=50), max_attempts=2)
        assert strat.adaptive_delay_quantile is None


class TestHedgeDelayState:
    def test_fixed_fallback_until_warmed(self) -> None:
        state = HedgeDelayState(quantile=0.95, fixed_delay=0.2)

        for x in (0.01, 0.01, 0.01, 0.01):
            state.observe(x)

        assert state.delay() == 0.2  # four samples: estimator still undefined

        state.observe(0.01)
        assert state.delay() == pytest.approx(0.01)

    def test_floor_and_cap_clamp_estimate(self) -> None:
        state = HedgeDelayState(
            quantile=0.95, fixed_delay=0.2, floor=0.05, cap=0.5
        )

        for _ in range(20):
            state.observe(0.001)  # estimate collapses below the floor

        assert state.delay() == 0.05

        for _ in range(200):
            state.observe(10.0)  # estimate blows past the cap

        assert state.delay() == 0.5

    def test_clamps_do_not_touch_fixed_fallback(self) -> None:
        state = HedgeDelayState(quantile=0.95, fixed_delay=0.2, floor=0.5)

        assert state.delay() == 0.2  # unwarmed: fixed value served verbatim


class TestExecutorIntegration:
    async def test_adaptive_delay_learns_from_primary_latencies(self) -> None:
        ex = _executor(_strategy(delay=timedelta(seconds=60)))

        # Warm the estimator with fast primaries. The fixed delay (60s) would
        # never hedge inside this test.
        for _ in range(10):
            fn = _Fn([(0.0, "fast")])
            assert await ex.run_hedged(fn, policy="h", route="r") == "fast"

        ((_, _, delay),) = ex.hedge_delays()
        assert delay < 0.05  # learned ~p95 of the fast primaries

        # A slow primary now gets hedged at the learned delay, not at 60s.
        fn = _Fn([(5.0, "slow-primary"), (0.0, "hedge")])
        result = await ex.run_hedged(fn, policy="h", route="r")

        assert result == "hedge"
        assert fn.started == 2

    async def test_fixed_delay_used_before_warmup(self) -> None:
        ex = _executor(_strategy(delay=timedelta(milliseconds=20)))

        # First call: estimator empty -> fixed 20ms delay drives the hedge.
        fn = _Fn([(0.5, "slow"), (0.0, "hedge")])
        result = await ex.run_hedged(fn, policy="h", route="r")

        assert result == "hedge"
        assert fn.started == 2

    async def test_cancelled_primary_contributes_censored_sample(self) -> None:
        ex = _executor(_strategy(delay=timedelta(milliseconds=20)))

        fn = _Fn([(0.5, "slow"), (0.0, "hedge")])
        assert await ex.run_hedged(fn, policy="h", route="r") == "hedge"

        ((key, state),) = ex._hedge_delays.items()  # noqa: SLF001
        assert key == ("h", "r")
        # The cancelled primary was recorded at >= the hedge delay.
        assert state._estimator._old.count == 1  # noqa: SLF001

    async def test_caller_cancellation_records_no_sample(self) -> None:
        # A caller cancel can land at any elapsed time — recording it would
        # feed arbitrary garbage into the quantile (only hedge wins are
        # legitimate censoring points).
        ex = _executor(_strategy(delay=timedelta(seconds=60)))

        fn = _Fn([(5.0, "slow")])
        task = asyncio.create_task(ex.run_hedged(fn, policy="h", route="r"))
        await asyncio.sleep(0.01)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        ((_, state),) = ex._hedge_delays.items()  # noqa: SLF001
        assert state._estimator._old.count == 0  # noqa: SLF001

    async def test_routes_keep_separate_estimators(self) -> None:
        ex = _executor(_strategy())

        for route in ("a", "b"):
            fn = _Fn([(0.0, "ok")])
            await ex.run_hedged(fn, policy="h", route=route)

        keys = {key for key, _ in ex._hedge_delays.items()}  # noqa: SLF001
        assert keys == {("h", "a"), ("h", "b")}

    async def test_fixed_strategy_keeps_no_state(self) -> None:
        ex = _executor(HedgeStrategy(delay=timedelta(milliseconds=50), max_attempts=2))

        fn = _Fn([(0.0, "ok")])
        assert await ex.run_hedged(fn, policy="h", route="r") == "ok"

        assert not ex._hedge_delays  # noqa: SLF001
        assert list(ex.hedge_delays()) == []
