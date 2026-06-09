"""Behavioral tests for the in-process resilience executor."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    BackoffStrategy,
    BulkheadStrategy,
    CircuitBreakerStrategy,
    FallbackStrategy,
    ResiliencePolicy,
    RetryBudget,
    RetryStrategy,
    TimeoutStrategy,
)
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.tracing import bind_active_deps
from forze.base.exceptions import CoreException, ExceptionKind, exc

# ----------------------- #


async def _no_sleep(_delay: float) -> None:
    return None


class _Clock:
    """Manually advanced monotonic clock."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _backoff() -> BackoffStrategy:
    return BackoffStrategy(
        base=timedelta(milliseconds=1),
        max=timedelta(milliseconds=10),
        jitter="none",
    )


def _retry_policy(**kw: object) -> ResiliencePolicy:
    params: dict[str, object] = {
        "max_attempts": 3,
        "backoff": _backoff(),
        "retry_on": frozenset({ExceptionKind.INFRASTRUCTURE}),
    }
    params.update(kw)
    return ResiliencePolicy(
        name="p",
        strategies=(RetryStrategy(**params),),  # type: ignore[arg-type]
    )


def _executor(policy: ResiliencePolicy, **kw: object) -> InProcessResilienceExecutor:
    return InProcessResilienceExecutor(
        policies={policy.name: policy},
        sleep=_no_sleep,
        **kw,  # type: ignore[arg-type]
    )


class _Counter:
    def __init__(self) -> None:
        self.calls = 0


# ....................... #


class TestRetry:
    async def test_succeeds_after_transient(self) -> None:
        executor = _executor(_retry_policy())
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            if counter.calls < 3:
                raise exc.infrastructure("transient")
            return "ok"

        assert await executor.run(fn, policy="p") == "ok"
        assert counter.calls == 3

    async def test_exhausts_and_raises(self) -> None:
        executor = _executor(_retry_policy(max_attempts=2))
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            raise exc.infrastructure("down")

        with pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p")

        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
        assert counter.calls == 2

    async def test_non_retryable_not_retried(self) -> None:
        executor = _executor(_retry_policy())
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            raise exc.conflict("nope")

        with pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p")

        assert ei.value.kind is ExceptionKind.CONFLICT
        assert counter.calls == 1

    async def test_retry_on_narrowing_excludes_other_retryable(self) -> None:
        # Only CONCURRENCY is retried; an INFRASTRUCTURE error must propagate.
        executor = _executor(
            _retry_policy(retry_on=frozenset({ExceptionKind.CONCURRENCY})),
        )
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            raise exc.infrastructure("down")

        with pytest.raises(CoreException):
            await executor.run(fn, policy="p")

        assert counter.calls == 1


# ....................... #


class TestBudget:
    async def test_budget_caps_retries(self) -> None:
        policy = _retry_policy(
            max_attempts=5,
            budget=RetryBudget(ratio=1.0, min_throughput=0),
        )
        executor = _executor(policy)
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            raise exc.infrastructure("down")

        with pytest.raises(CoreException):
            await executor.run(fn, policy="p")

        # ratio=1.0 earns one token per call; only a single retry is permitted.
        assert counter.calls == 2

    async def test_without_budget_uses_all_attempts(self) -> None:
        executor = _executor(_retry_policy(max_attempts=5))
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            raise exc.infrastructure("down")

        with pytest.raises(CoreException):
            await executor.run(fn, policy="p")

        assert counter.calls == 5


# ....................... #


class TestTimeout:
    async def test_timeout_fires(self) -> None:
        policy = ResiliencePolicy(
            name="p",
            strategies=(TimeoutStrategy(timeout=timedelta(milliseconds=10)),),
        )
        executor = _executor(policy)

        async def fn() -> str:
            await asyncio.sleep(5)
            return "never"

        with pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p")

        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE

    async def test_timeout_inside_retry_retries_each_attempt(self) -> None:
        policy = ResiliencePolicy(
            name="p",
            strategies=(
                RetryStrategy(
                    max_attempts=3,
                    backoff=_backoff(),
                    retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
                ),
                TimeoutStrategy(timeout=timedelta(milliseconds=10)),
            ),
        )
        executor = _executor(policy)
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            await asyncio.sleep(5)
            return "never"

        with pytest.raises(CoreException):
            await executor.run(fn, policy="p")

        assert counter.calls == 3


# ....................... #


def _breaker_policy(**kw: object) -> ResiliencePolicy:
    params: dict[str, object] = {
        "failure_ratio": 1.0,
        "sampling_window": timedelta(seconds=100),
        "min_throughput": 2,
        "break_duration": timedelta(seconds=10),
        "half_open_max_calls": 1,
    }
    params.update(kw)
    return ResiliencePolicy(
        name="p",
        strategies=(CircuitBreakerStrategy(**params),),  # type: ignore[arg-type]
    )


class TestCircuitBreaker:
    async def test_opens_and_fast_fails(self) -> None:
        clock = _Clock()
        executor = _executor(_breaker_policy(), clock=clock)
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            raise exc.infrastructure("down")

        # Two failures reach min_throughput and trip the breaker open.
        for _ in range(2):
            with pytest.raises(CoreException):
                await executor.run(fn, policy="p")

        # Third call is rejected without invoking fn.
        with pytest.raises(CoreException, match="Circuit breaker open"):
            await executor.run(fn, policy="p")

        assert counter.calls == 2

    async def test_half_open_recovers_and_closes(self) -> None:
        clock = _Clock()
        executor = _executor(
            _breaker_policy(min_throughput=1),
            clock=clock,
        )

        async def boom() -> str:
            raise exc.infrastructure("down")

        async def ok() -> str:
            return "ok"

        # Trip open at t=0.
        with pytest.raises(CoreException):
            await executor.run(boom, policy="p")

        # Still open before break_duration elapses.
        with pytest.raises(CoreException, match="Circuit breaker open"):
            await executor.run(ok, policy="p")

        # Advance past break_duration -> half-open probe succeeds -> closed.
        clock.now = 10.0
        assert await executor.run(ok, policy="p") == "ok"
        assert await executor.run(ok, policy="p") == "ok"

    async def test_state_persists_across_scopes(self) -> None:
        # One executor instance models the process singleton; breaker state must
        # survive across independent run() calls (each modelling a request scope).
        clock = _Clock()
        executor = _executor(_breaker_policy(), clock=clock)

        async def boom() -> str:
            raise exc.infrastructure("down")

        for _ in range(2):
            with pytest.raises(CoreException):
                await executor.run(boom, policy="p")

        # A later, separate scope still sees the open breaker.
        async def unexpected() -> str:  # pragma: no cover - must not run
            raise AssertionError("breaker should have short-circuited")

        with pytest.raises(CoreException, match="Circuit breaker open"):
            await executor.run(unexpected, policy="p")


# ....................... #


class TestBulkhead:
    async def test_rejects_when_full(self) -> None:
        policy = ResiliencePolicy(
            name="p",
            strategies=(BulkheadStrategy(max_concurrency=1, max_queue=0),),
        )
        executor = _executor(policy)
        started = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> str:
            started.set()
            await release.wait()
            return "held"

        async def quick() -> str:  # pragma: no cover - must not run
            raise AssertionError("should be rejected before running")

        task = asyncio.create_task(executor.run(holder, policy="p"))
        await started.wait()

        try:
            with pytest.raises(CoreException, match="Bulkhead full"):
                await executor.run(quick, policy="p")
        finally:
            release.set()

        assert await task == "held"


# ....................... #


class TestFallbackAndConfig:
    async def test_fallback_invoked_on_failure(self) -> None:
        policy = ResiliencePolicy(
            name="p",
            strategies=(FallbackStrategy(), _retry_policy().strategies[0]),
        )
        executor = _executor(policy)

        async def fn() -> str:
            raise exc.infrastructure("down")

        async def fallback(_error: BaseException) -> str:
            return "fallback"

        assert await executor.run(fn, policy="p", fallback=fallback) == "fallback"

    async def test_fallback_without_marker_is_config_error(self) -> None:
        executor = _executor(_retry_policy())

        async def fn() -> str:
            return "ok"

        async def fallback(_error: BaseException) -> str:
            return "fallback"

        with pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p", fallback=fallback)

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    async def test_unknown_policy_is_config_error(self) -> None:
        executor = _executor(_retry_policy())

        async def fn() -> str:
            return "ok"

        with pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="missing")

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestTracing:
    async def test_emits_resilience_events(self, traced_deps: object) -> None:
        bind_active_deps(traced_deps)  # type: ignore[arg-type]
        executor = _executor(_retry_policy())
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            if counter.calls < 2:
                raise exc.infrastructure("transient")
            return "ok"

        assert await executor.run(fn, policy="p") == "ok"

        trace = traced_deps.runtime_trace()  # type: ignore[attr-defined]
        assert trace is not None
        ops = {(e.domain, e.op) for e in trace.events}
        assert ("resilience", "retry_attempt") in ops
