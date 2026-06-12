"""Adaptive client throttling (SRE book): probabilistic shedding by accept ratio."""

from __future__ import annotations

import random
from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    AdaptiveThrottleStrategy,
    CircuitBreakerStrategy,
    ResiliencePolicy,
)
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.state import AdaptiveThrottleState
from forze.base.exceptions import CoreException, ExceptionKind, exc

# ----------------------- #


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _strategy(**kw: object) -> AdaptiveThrottleStrategy:
    params: dict[str, object] = {
        "k": 2.0,
        "window": timedelta(minutes=2),
        "min_throughput": 5,
    }
    params.update(kw)
    return AdaptiveThrottleStrategy(**params)  # type: ignore[arg-type]


def _executor(
    strat: AdaptiveThrottleStrategy,
    clock: _Clock | None = None,
) -> InProcessResilienceExecutor:
    return InProcessResilienceExecutor(
        policies={"p": ResiliencePolicy(name="p", strategies=(strat,))},
        clock=clock if clock is not None else _Clock(),
        rng=random.Random(1),
    )


# ----------------------- #


class TestStrategyValidation:
    def test_rejects_invalid_params(self) -> None:
        for kw in (
            {"k": 0.5},
            {"window": timedelta(0)},
            {"min_throughput": 0},
        ):
            with pytest.raises(CoreException):
                _strategy(**kw)

    def test_defaults_are_the_published_ones(self) -> None:
        strat = AdaptiveThrottleStrategy()

        assert strat.k == 2.0
        assert strat.window == timedelta(minutes=2)
        assert strat.min_throughput == 10

    def test_policy_rejects_throttle_with_breaker(self) -> None:
        with pytest.raises(CoreException) as ei:
            ResiliencePolicy(
                name="p",
                strategies=(
                    CircuitBreakerStrategy(
                        failure_ratio=0.5,
                        sampling_window=timedelta(seconds=60),
                        min_throughput=5,
                        break_duration=timedelta(seconds=10),
                        half_open_max_calls=1,
                    ),
                    _strategy(),
                ),
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION


class TestThrottleState:
    def _state(self, **kw: object) -> AdaptiveThrottleState:
        params: dict[str, object] = {"k": 2.0, "window": 120.0, "min_throughput": 5}
        params.update(kw)
        return AdaptiveThrottleState(**params)  # type: ignore[arg-type]

    def test_no_shedding_below_min_throughput(self) -> None:
        state = self._state()

        for _ in range(4):  # all failing, but volume is trivial
            state.record_request(100.0)

        assert state.reject_probability(100.0) == 0.0

    def test_probability_formula(self) -> None:
        state = self._state(min_throughput=1)

        for _ in range(100):
            state.record_request(100.0)

        for _ in range(40):
            state.record_accept(100.0)

        # (100 - 2*40) / 101
        assert state.reject_probability(100.0) == pytest.approx(20 / 101)

    def test_healthy_traffic_never_sheds(self) -> None:
        state = self._state(min_throughput=1)

        for _ in range(50):
            state.record_request(100.0)
            state.record_accept(100.0)

        # requests - 2*accepts < 0 -> clamped to zero.
        assert state.reject_probability(100.0) == 0.0

    def test_window_roll_resets_counters(self) -> None:
        state = self._state(window=10.0, min_throughput=1)

        for _ in range(20):
            state.record_request(100.0)  # 20 requests, 0 accepts

        assert state.reject_probability(100.0) > 0.9

        # One window later the slate is clean: trivial volume, no shedding.
        assert state.reject_probability(111.0) == 0.0
        assert state.requests == 0


class TestExecutorIntegration:
    async def test_healthy_traffic_passes_untouched(self) -> None:
        ex = _executor(_strategy())

        async def ok() -> str:
            return "ok"

        for _ in range(50):
            assert await ex.run(ok, policy="p", route="r") == "ok"

        ((_, state),) = ex._throttles.items()  # noqa: SLF001
        assert state.requests == 50
        assert state.accepts == 50

    async def test_failing_downstream_gets_shed_proportionally(self) -> None:
        ex = _executor(_strategy(min_throughput=5))
        events: list[str] = []
        ex.set_metrics_sink(lambda event, _pol, _route: events.append(event))

        async def boom() -> str:
            raise exc.infrastructure("down")

        outcomes: list[str] = []

        for _ in range(60):
            try:
                await ex.run(boom, policy="p", route="r")

            except CoreException as error:
                outcomes.append(error.code)

        shed = outcomes.count("adaptive_throttle")
        sent = len(outcomes) - shed

        # Proportional, not binary: most calls shed locally, but a probe
        # stream keeps reaching the downstream (unlike an open breaker).
        assert shed > 30
        assert sent > 0
        assert events.count("throttle_reject") == shed

    async def test_shed_failure_is_retryable_throttled(self) -> None:
        ex = _executor(_strategy(min_throughput=1))

        async def boom() -> str:
            raise exc.infrastructure("down")

        shed_error: CoreException | None = None

        for _ in range(30):
            try:
                await ex.run(boom, policy="p")

            except CoreException as error:
                if error.code == "adaptive_throttle":
                    shed_error = error
                    break

        assert shed_error is not None
        assert shed_error.kind is ExceptionKind.THROTTLED

    async def test_domain_failures_count_as_accepts(self) -> None:
        # A downstream rejecting inputs is doing its job, not buckling.
        ex = _executor(_strategy(min_throughput=1))

        async def nope() -> str:
            raise exc.domain("not allowed")

        for _ in range(50):
            with pytest.raises(CoreException) as ei:
                await ex.run(nope, policy="p")

            assert ei.value.code != "adaptive_throttle"

        ((_, state),) = ex._throttles.items()  # noqa: SLF001
        assert state.accepts == 50

    async def test_recovery_after_window_roll(self) -> None:
        clock = _Clock()
        ex = _executor(_strategy(window=timedelta(seconds=10)), clock=clock)

        async def boom() -> str:
            raise exc.infrastructure("down")

        for _ in range(30):
            try:
                await ex.run(boom, policy="p")

            except CoreException:
                pass

        async def ok() -> str:
            return "ok"

        clock.now += 11.0  # the window rolls: counters reset, shedding stops

        for _ in range(20):
            assert await ex.run(ok, policy="p") == "ok"
