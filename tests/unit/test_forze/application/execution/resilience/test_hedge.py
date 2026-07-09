"""Tests for the hedging executor (run_hedged): staggered attempts, first wins."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    HedgeStrategy,
    ResiliencePolicy,
    RetryBudget,
    TimeoutStrategy,
)
from forze.base.exceptions import CoreException, exc
from forze.application.execution.resilience import InProcessResilienceExecutor

# ----------------------- #


def _policy(
    *,
    delay: float = 0.05,
    max_attempts: int = 3,
    budget: RetryBudget | None = None,
) -> ResiliencePolicy:
    return ResiliencePolicy(
        name="h",
        strategies=(TimeoutStrategy(timeout=timedelta(seconds=10)),),
        hedge=HedgeStrategy(
            delay=timedelta(seconds=delay),
            max_attempts=max_attempts,
            budget=budget,
        ),
    )


class _Fn:
    """Builds a hedged callable whose nth attempt sleeps then returns/raises."""

    def __init__(self, behaviors: list[tuple[float, object]]) -> None:
        self.behaviors = behaviors
        self.started = 0
        self.cancelled = 0

    def __call__(self) -> Awaitable[object]:
        idx = self.started
        self.started += 1
        delay, outcome = self.behaviors[min(idx, len(self.behaviors) - 1)]

        async def run() -> object:
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                self.cancelled += 1
                raise

            if isinstance(outcome, BaseException):
                raise outcome

            return outcome

        return run()


def _exec(pol: ResiliencePolicy) -> InProcessResilienceExecutor:
    return InProcessResilienceExecutor(policies={pol.name: pol})


class TestHedge:
    async def test_fast_primary_wins_without_hedging(self) -> None:
        fn = _Fn([(0.0, "primary")])
        ex = _exec(_policy(delay=0.1))

        result = await ex.run_hedged(fn, policy="h", route="r")

        assert result == "primary"
        assert fn.started == 1  # completed before the hedge delay -> no second copy

    async def test_slow_primary_hedge_wins_and_cancels_loser(self) -> None:
        fn = _Fn([(0.5, "primary"), (0.01, "hedge")])
        ex = _exec(_policy(delay=0.05))

        result = await ex.run_hedged(fn, policy="h", route="r")

        assert result == "hedge"
        assert fn.started == 2
        assert fn.cancelled == 1  # the slow primary was cancelled

    async def test_fast_failure_does_not_hedge(self) -> None:
        # Hedging fires on slowness, not failure — a fast failure just propagates.
        boom = exc.infrastructure("boom")
        fn = _Fn([(0.0, boom)])
        ex = _exec(_policy(delay=0.1))

        with pytest.raises(CoreException):
            await ex.run_hedged(fn, policy="h", route="r")

        assert fn.started == 1

    async def test_all_attempts_fail_raises_last(self) -> None:
        fn = _Fn(
            [
                (0.4, exc.infrastructure("e1")),
                (0.4, exc.infrastructure("e2")),
                (0.4, exc.infrastructure("e3")),
            ]
        )
        ex = _exec(_policy(delay=0.02, max_attempts=3))

        with pytest.raises(CoreException):
            await ex.run_hedged(fn, policy="h", route="r")

        assert fn.started == 3  # both hedges fired (all slow), then all failed

    async def test_budget_caps_hedging(self) -> None:
        # min_throughput=0 + ratio<1 -> first hedge try_spend fails, so no extra copy.
        fn = _Fn([(0.3, "primary")])
        ex = _exec(_policy(delay=0.02, budget=RetryBudget(ratio=0.5, min_throughput=0)))

        result = await ex.run_hedged(fn, policy="h", route="r")

        assert result == "primary"
        assert fn.started == 1  # budget blocked the hedge

    async def test_max_attempts_caps_spawns(self) -> None:
        fn = _Fn([(0.5, "p"), (0.5, "a2"), (0.01, "a3")])
        ex = _exec(_policy(delay=0.02, max_attempts=2))

        result = await ex.run_hedged(fn, policy="h", route="r")

        # max_attempts=2 -> only primary + one hedge spawn; a3 never fires despite the
        # delay elapsing again. Both in-flight are equally slow, so the primary wins.
        assert fn.started == 2
        assert result == "p"

    async def test_unknown_policy_raises(self) -> None:
        ex = _exec(_policy())
        with pytest.raises(CoreException):
            await ex.run_hedged(_Fn([(0.0, "x")]), policy="missing")

    async def test_policy_without_hedge_raises(self) -> None:
        pol = ResiliencePolicy(
            name="nohedge",
            strategies=(TimeoutStrategy(timeout=timedelta(seconds=1)),),
        )
        ex = _exec(pol)
        with pytest.raises(CoreException):
            await ex.run_hedged(_Fn([(0.0, "x")]), policy="nohedge")


class _SinkSpy:
    """Captures every resilience event op the executor emits to its metrics sink."""

    def __init__(self) -> None:
        self.events: list[str] = []

    def __call__(self, op: str, policy: str, route: str | None) -> None:
        self.events.append(op)


class TestHedgeMetrics:
    """The hedge win counters count only the *hedged* population, so the effectiveness ratio
    ``hedge_won / (hedge_won + hedge_primary_won)`` is meaningful (regression for the fast-primary
    over-count that pulled the ratio toward 0)."""

    async def test_fast_primary_emits_no_win_or_attempt_event(self) -> None:
        # The primary beats the delay, so no hedge is ever fired — this call is not part of the
        # hedged population and must emit neither win nor attempt.
        fn = _Fn([(0.0, "primary")])
        ex = _exec(_policy(delay=0.1))
        spy = _SinkSpy()
        ex.set_metrics_sink(spy)

        assert await ex.run_hedged(fn, policy="h", route="r") == "primary"
        assert "hedge_attempt" not in spy.events
        assert "hedge_primary_won" not in spy.events
        assert "hedge_won" not in spy.events

    async def test_hedge_fires_then_primary_wins_emits_one_primary_won(self) -> None:
        # A hedge fires (primary slow past the delay), then the primary still wins the race: exactly
        # one hedge_attempt and one hedge_primary_won — this call IS in the hedged population.
        fn = _Fn([(0.08, "primary"), (0.5, "hedge")])
        ex = _exec(_policy(delay=0.03, max_attempts=2))
        spy = _SinkSpy()
        ex.set_metrics_sink(spy)

        assert await ex.run_hedged(fn, policy="h", route="r") == "primary"
        assert spy.events.count("hedge_attempt") == 1
        assert spy.events.count("hedge_primary_won") == 1
        assert "hedge_won" not in spy.events

    async def test_hedge_wins_emits_hedge_won_not_primary_won(self) -> None:
        fn = _Fn([(0.5, "primary"), (0.01, "hedge")])
        ex = _exec(_policy(delay=0.03))
        spy = _SinkSpy()
        ex.set_metrics_sink(spy)

        assert await ex.run_hedged(fn, policy="h", route="r") == "hedge"
        assert spy.events.count("hedge_won") == 1
        assert "hedge_primary_won" not in spy.events


class TestPassthrough:
    async def test_run_hedged_runs_once(self) -> None:
        from forze_mock.adapters.resilience import PassthroughResilienceExecutor

        calls = 0

        async def fn() -> str:
            nonlocal calls
            calls += 1
            return "ok"

        result = await PassthroughResilienceExecutor().run_hedged(fn, policy="x")

        assert result == "ok"
        assert calls == 1
