"""Behavioral tests for the in-process resilience executor."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

import attrs
import pytest

from forze.application.contracts.resilience import (
    BackoffStrategy,
    BulkheadStrategy,
    CircuitBreakerStrategy,
    FallbackStrategy,
    RateLimitStrategy,
    ResiliencePolicy,
    RetryBudget,
    RetryStrategy,
    TimeoutStrategy,
)
from forze.application.execution.context.deadline import bind_deadline
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

    async def test_throttled_downstream_does_not_open_breaker(self) -> None:
        # A downstream returning 429 is backpressure, not a health failure: it must not
        # trip the breaker, however many times it happens.
        clock = _Clock()
        executor = _executor(_breaker_policy(min_throughput=1), clock=clock)

        async def throttled() -> str:
            raise exc.throttled("downstream 429", code="rate_limited")

        for _ in range(5):
            with pytest.raises(CoreException):
                await executor.run(throttled, policy="p")

        # The breaker is still closed: a following call reaches fn (not fast-failed).
        counter = _Counter()

        async def ok() -> str:
            counter.calls += 1
            return "ok"

        assert await executor.run(ok, policy="p") == "ok"
        assert counter.calls == 1

    async def test_concurrency_conflict_does_not_open_breaker(self) -> None:
        # OCC contention is not a downstream-health signal either.
        clock = _Clock()
        executor = _executor(_breaker_policy(min_throughput=1), clock=clock)

        async def conflict() -> str:
            raise exc.concurrency("revision mismatch")

        for _ in range(5):
            with pytest.raises(CoreException):
                await executor.run(conflict, policy="p")

        counter = _Counter()

        async def ok() -> str:
            counter.calls += 1
            return "ok"

        assert await executor.run(ok, policy="p") == "ok"
        assert counter.calls == 1

    async def test_timeout_counts_as_a_failure(self) -> None:
        # A timeout means the downstream did not respond — a breaker failure, even though
        # a deadline timeout is not retryable.
        clock = _Clock()
        executor = _executor(_breaker_policy(min_throughput=1), clock=clock)

        async def slow() -> str:
            raise exc.timeout("no response", code="deadline_exceeded")

        with pytest.raises(CoreException):
            await executor.run(slow, policy="p")

        # The breaker opened: a following call is fast-failed without running.
        async def unexpected() -> str:  # pragma: no cover - must not run
            raise AssertionError("breaker should have short-circuited")

        with pytest.raises(CoreException, match="Circuit breaker open"):
            await executor.run(unexpected, policy="p")


# ....................... #


class TestRunStream:
    """Streams under a policy: breaker gate at acquisition + outcome recording."""

    async def test_mid_stream_failures_open_breaker_for_unary_sibling(self) -> None:
        clock = _Clock()
        executor = _executor(_breaker_policy(), clock=clock)

        async def broken() -> AsyncGenerator[int]:
            yield 1
            raise exc.infrastructure("died mid-stream")

        # Two mid-stream failures reach min_throughput and trip the breaker.
        for _ in range(2):
            with pytest.raises(CoreException):
                async for _ in executor.run_stream(broken, policy="p"):
                    pass

        # The stream failures were recorded, so a unary call under the same
        # (policy, route) is fast-failed without running.
        async def unexpected() -> str:  # pragma: no cover - must not run
            raise AssertionError("breaker should have short-circuited")

        with pytest.raises(CoreException, match="Circuit breaker open"):
            await executor.run(unexpected, policy="p")

    async def test_open_breaker_rejects_stream_acquisition(self) -> None:
        clock = _Clock()
        executor = _executor(_breaker_policy(min_throughput=1), clock=clock)

        async def boom() -> str:
            raise exc.infrastructure("down")

        with pytest.raises(CoreException):
            await executor.run(boom, policy="p")

        started = _Counter()

        async def stream() -> AsyncGenerator[int]:
            started.calls += 1
            yield 1

        # The unary failure opened the breaker: opening a stream against the
        # known-dead backend is rejected before the port stream ever starts.
        with pytest.raises(CoreException, match="Circuit breaker open"):
            async for _ in executor.run_stream(stream, policy="p"):
                pass

        assert started.calls == 0

    async def test_forced_open_wildcard_rejects_stream_acquisition(self) -> None:
        executor = _executor(_breaker_policy())
        await executor.force_open("p")

        async def stream() -> AsyncGenerator[int]:
            yield 1  # pragma: no cover - must not run

        # The route=None kill-switch is a policy-wide wildcard: it sheds
        # streams on every route under the policy, same as unary calls.
        with pytest.raises(CoreException, match="force-opened"):
            async for _ in executor.run_stream(stream, policy="p", route="r"):
                pass

    async def test_caller_caused_mid_stream_error_does_not_trip_breaker(self) -> None:
        clock = _Clock()
        executor = _executor(_breaker_policy(min_throughput=1), clock=clock)

        async def rejected() -> AsyncGenerator[int]:
            yield 1
            raise exc.conflict("caller-caused")

        for _ in range(5):
            with pytest.raises(CoreException):
                async for _ in executor.run_stream(rejected, policy="p"):
                    pass

        # The downstream answered fine every time: the breaker is still closed.
        counter = _Counter()

        async def ok() -> str:
            counter.calls += 1
            return "ok"

        assert await executor.run(ok, policy="p") == "ok"
        assert counter.calls == 1

    async def test_throttled_mid_stream_is_neutral(self) -> None:
        # Backpressure mid-stream is not a health signal — same as unary.
        clock = _Clock()
        executor = _executor(_breaker_policy(min_throughput=1), clock=clock)

        async def throttled() -> AsyncGenerator[int]:
            yield 1
            raise exc.throttled("downstream 429", code="rate_limited")

        for _ in range(5):
            with pytest.raises(CoreException):
                async for _ in executor.run_stream(throttled, policy="p"):
                    pass

        counter = _Counter()

        async def ok() -> str:
            counter.calls += 1
            return "ok"

        assert await executor.run(ok, policy="p") == "ok"
        assert counter.calls == 1

    async def test_clean_completion_closes_half_open_breaker(self) -> None:
        clock = _Clock()
        executor = _executor(_breaker_policy(min_throughput=1), clock=clock)

        async def boom() -> str:
            raise exc.infrastructure("down")

        # Trip open at t=0, then advance past break_duration -> half-open.
        with pytest.raises(CoreException):
            await executor.run(boom, policy="p")

        clock.now = 10.0

        async def stream() -> AsyncGenerator[int]:
            yield 1
            yield 2

        # A cleanly exhausted stream is the probe success that closes the breaker.
        assert [i async for i in executor.run_stream(stream, policy="p")] == [1, 2]

        counter = _Counter()

        async def ok() -> str:
            counter.calls += 1
            return "ok"

        assert await executor.run(ok, policy="p") == "ok"
        assert counter.calls == 1

    async def test_consumer_close_records_success_immediately(self) -> None:
        clock = _Clock()
        executor = _executor(_breaker_policy(min_throughput=1), clock=clock)

        async def boom() -> str:
            raise exc.infrastructure("down")

        with pytest.raises(CoreException):
            await executor.run(boom, policy="p")

        clock.now = 10.0

        async def endless() -> AsyncGenerator[int]:
            i = 0
            while True:
                yield i
                i += 1

        # The half-open probe is a long-lived stream the consumer abandons
        # cleanly: the close records a success right away (not at GC time),
        # so the breaker is closed again for unary traffic.
        stream = executor.run_stream(endless, policy="p")
        assert await anext(stream) == 0
        await stream.aclose()

        counter = _Counter()

        async def ok() -> str:
            counter.calls += 1
            return "ok"

        assert await executor.run(ok, policy="p") == "ok"
        assert counter.calls == 1

    async def test_unknown_policy_rejected(self) -> None:
        executor = _executor(_breaker_policy())

        async def stream() -> AsyncGenerator[int]:
            yield 1  # pragma: no cover - must not run

        with pytest.raises(CoreException, match="Unknown resilience policy"):
            async for _ in executor.run_stream(stream, policy="nope"):
                pass


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


def _rate_limit_policy(
    name: str = "p",
    *,
    permits: int = 2,
    per_seconds: float = 1.0,
    burst: int | None = None,
) -> ResiliencePolicy:
    return ResiliencePolicy(
        name=name,
        strategies=(
            RateLimitStrategy(
                permits=permits,
                per=timedelta(seconds=per_seconds),
                burst=burst,
            ),
        ),
    )


async def _ok() -> str:
    return "ok"


class TestRateLimit:
    async def test_burst_consumed_then_rejects_with_throttled(self) -> None:
        clock = _Clock()
        executor = _executor(_rate_limit_policy(permits=2), clock=clock)

        # The bucket starts full: exactly `capacity` calls pass at t=0.
        assert await executor.run(_ok, policy="p") == "ok"
        assert await executor.run(_ok, policy="p") == "ok"

        with pytest.raises(CoreException) as ei:
            await executor.run(_ok, policy="p")

        assert ei.value.kind is ExceptionKind.THROTTLED
        assert ei.value.code == "rate_limited"
        assert ei.value.details == {"policy": "p", "route": None}

    async def test_rejection_does_not_run_fn(self) -> None:
        clock = _Clock()
        executor = _executor(_rate_limit_policy(permits=1), clock=clock)
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            return "ok"

        assert await executor.run(fn, policy="p") == "ok"

        with pytest.raises(CoreException):
            await executor.run(fn, policy="p")

        assert counter.calls == 1

    async def test_refill_over_time_grants_more_permits(self) -> None:
        clock = _Clock()
        executor = _executor(
            _rate_limit_policy(permits=2, per_seconds=1.0),
            clock=clock,
        )

        for _ in range(2):
            assert await executor.run(_ok, policy="p") == "ok"

        with pytest.raises(CoreException):
            await executor.run(_ok, policy="p")

        # Half the window refills one token (rate = 2/s).
        clock.now = 0.5
        assert await executor.run(_ok, policy="p") == "ok"

        with pytest.raises(CoreException):
            await executor.run(_ok, policy="p")

    async def test_sustained_rate_honored(self) -> None:
        clock = _Clock()
        executor = _executor(
            _rate_limit_policy(permits=2, per_seconds=1.0),
            clock=clock,
        )

        # Drain the initial burst.
        for _ in range(2):
            await executor.run(_ok, policy="p")

        # Each elapsed second grants exactly `permits` more calls.
        granted = 0
        for second in (1.0, 2.0, 3.0):
            clock.now = second
            for _ in range(2):
                assert await executor.run(_ok, policy="p") == "ok"
                granted += 1

            with pytest.raises(CoreException):
                await executor.run(_ok, policy="p")

        assert granted == 6

    async def test_burst_caps_saved_up_capacity(self) -> None:
        clock = _Clock()
        executor = _executor(
            _rate_limit_policy(permits=1, per_seconds=1.0, burst=3),
            clock=clock,
        )

        # Bucket starts at burst capacity.
        for _ in range(3):
            assert await executor.run(_ok, policy="p") == "ok"

        with pytest.raises(CoreException):
            await executor.run(_ok, policy="p")

        # A long idle period refills to at most `burst` tokens.
        clock.now = 100.0
        for _ in range(3):
            assert await executor.run(_ok, policy="p") == "ok"

        with pytest.raises(CoreException):
            await executor.run(_ok, policy="p")

    async def test_state_isolated_per_route(self) -> None:
        clock = _Clock()
        executor = _executor(_rate_limit_policy(permits=1), clock=clock)

        assert await executor.run(_ok, policy="p", route="a") == "ok"

        with pytest.raises(CoreException):
            await executor.run(_ok, policy="p", route="a")

        # Another route under the same policy has its own bucket.
        assert await executor.run(_ok, policy="p", route="b") == "ok"

        with pytest.raises(CoreException) as ei:
            await executor.run(_ok, policy="p", route="b")

        assert ei.value.details == {"policy": "p", "route": "b"}

    async def test_state_isolated_per_policy(self) -> None:
        clock = _Clock()
        pol_a = _rate_limit_policy("a", permits=1)
        pol_b = _rate_limit_policy("b", permits=1)
        executor = InProcessResilienceExecutor(
            policies={"a": pol_a, "b": pol_b},
            sleep=_no_sleep,
            clock=clock,
        )

        assert await executor.run(_ok, policy="a") == "ok"

        with pytest.raises(CoreException):
            await executor.run(_ok, policy="a")

        assert await executor.run(_ok, policy="b") == "ok"

    async def test_rate_limit_rejects_before_bulkhead_admits(self) -> None:
        # RateLimit composes outermost: with the bucket empty, the call is
        # throttled without ever touching the (full) bulkhead queue.
        clock = _Clock()
        policy = ResiliencePolicy(
            name="p",
            strategies=(
                RateLimitStrategy(permits=1, per=timedelta(seconds=1)),
                BulkheadStrategy(max_concurrency=1, max_queue=0),
            ),
        )
        executor = _executor(policy, clock=clock)
        started = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> str:
            started.set()
            await release.wait()
            return "held"

        task = asyncio.create_task(executor.run(holder, policy="p"))
        await started.wait()

        try:
            # Bucket is empty (the holder spent the only token) and the
            # bulkhead is full — the rate limit must reject first.
            with pytest.raises(CoreException) as ei:
                await executor.run(_ok, policy="p")

            assert ei.value.kind is ExceptionKind.THROTTLED

            # With a refilled bucket, the same call now reaches the bulkhead.
            clock.now = 5.0
            with pytest.raises(CoreException, match="Bulkhead full"):
                await executor.run(_ok, policy="p")
        finally:
            release.set()

        assert await task == "held"

    async def test_rate_limited_call_plus_retry_waits_and_succeeds(self) -> None:
        # The composition story: a rate-limited call raises THROTTLED, and a
        # retry-with-backoff policy *around* it waits the limit out. Sleeping
        # advances the controlled clock, refilling the bucket.
        clock = _Clock()

        async def sleeping(delay: float) -> None:
            clock.now += delay

        limited = _rate_limit_policy("limited", permits=1, per_seconds=1.0)
        patient = ResiliencePolicy(
            name="patient",
            strategies=(
                RetryStrategy(
                    max_attempts=3,
                    backoff=BackoffStrategy(
                        base=timedelta(seconds=1),
                        max=timedelta(seconds=2),
                        jitter="none",
                    ),
                    retry_on=frozenset({ExceptionKind.THROTTLED}),
                ),
            ),
        )
        executor = InProcessResilienceExecutor(
            policies={"limited": limited, "patient": patient},
            sleep=sleeping,
            clock=clock,
        )
        counter = _Counter()

        async def limited_call() -> str:
            counter.calls += 1
            return await executor.run(_ok, policy="limited")

        # Drain the bucket so the first attempt is throttled.
        assert await executor.run(_ok, policy="limited") == "ok"

        assert await executor.run(limited_call, policy="patient") == "ok"
        # Attempt 1 throttled; the 1s backoff refills one token; attempt 2 wins.
        assert counter.calls == 2


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

    async def test_emits_rate_limit_reject_event(self, traced_deps: object) -> None:
        bind_active_deps(traced_deps)  # type: ignore[arg-type]
        clock = _Clock()
        executor = _executor(_rate_limit_policy(permits=1), clock=clock)

        assert await executor.run(_ok, policy="p", route="r") == "ok"

        with pytest.raises(CoreException):
            await executor.run(_ok, policy="p", route="r")

        trace = traced_deps.runtime_trace()  # type: ignore[attr-defined]
        assert trace is not None
        events = [e for e in trace.events if e.op == "rate_limit_reject"]
        assert len(events) == 1
        assert events[0].domain == "resilience"
        assert events[0].route == "r"
        assert events[0].phase == "p"


# ....................... #


class TestDeadline:
    """Invocation-deadline integration (see ``context.deadline``)."""

    async def test_expired_deadline_rejects_before_call(self) -> None:
        executor = _executor(_retry_policy())
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            return "ok"

        with bind_deadline(0.0), pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"
        assert counter.calls == 0

    async def test_deadline_bounds_whole_call(self) -> None:
        executor = _executor(_retry_policy())

        async def fn() -> str:
            await asyncio.Event().wait()
            return "ok"

        with bind_deadline(0.05), pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"

    async def test_retry_abandons_backoff_past_deadline(self) -> None:
        # Backoff far larger than the remaining budget: the retry loop must
        # surface the real error instead of sleeping into the deadline.
        policy = _retry_policy(
            backoff=BackoffStrategy(
                base=timedelta(seconds=60),
                max=timedelta(seconds=60),
                jitter="none",
            ),
        )
        executor = _executor(policy)
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            raise exc.infrastructure("down")

        with bind_deadline(5.0), pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p")

        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
        assert counter.calls == 1

    async def test_no_deadline_leaves_behavior_unchanged(self) -> None:
        executor = _executor(_retry_policy())
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            if counter.calls < 3:
                raise exc.infrastructure("transient")
            return "ok"

        assert await executor.run(fn, policy="p") == "ok"
        assert counter.calls == 3


# ....................... #


class _FlakyBreakerStore:
    """Breaker store stub that can raise on ``admit`` and/or ``record``."""

    def __init__(self, *, fail_admit: bool = False, fail_record: bool = False) -> None:
        self.fail_admit = fail_admit
        self.fail_record = fail_record

    async def admit(self, key: object, strat: object) -> tuple[bool, None]:
        if self.fail_admit:
            raise ConnectionError("breaker store down")

        return (True, None)

    async def record(self, key: object, strat: object, ok: bool) -> None:
        if self.fail_record:
            raise ConnectionError("breaker store down")

        return None


class _FlakyRateLimitStore:
    """Rate-limit store stub whose ``try_acquire`` raises."""

    def __init__(self, *, fail: bool = True) -> None:
        self.fail = fail

    async def try_acquire(self, key: object, strat: object) -> bool:
        if self.fail:
            raise ConnectionError("rate-limit store down")

        return True


class TestStoreFailureFailsOpen:
    """A distributed breaker / rate-limit store outage must not fail live traffic."""

    async def test_breaker_admit_failure_fails_open_by_default(self) -> None:
        executor = _executor(
            _breaker_policy(), breaker_store=_FlakyBreakerStore(fail_admit=True)
        )
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            return "ok"

        # Store down on admit -> fail open -> the call still runs.
        assert await executor.run(fn, policy="p") == "ok"
        assert counter.calls == 1

    async def test_breaker_admit_failure_fails_closed_when_configured(self) -> None:
        executor = _executor(
            attrs.evolve(_breaker_policy(), fail_open_on_store_error=False),
            breaker_store=_FlakyBreakerStore(fail_admit=True),
        )
        counter = _Counter()

        async def fn() -> str:  # pragma: no cover - must not run
            counter.calls += 1
            return "ok"

        with pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p")

        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
        assert ei.value.code == "resilience_store_unavailable"
        assert counter.calls == 0

    async def test_breaker_record_failure_does_not_mask_domain_error(self) -> None:
        executor = _executor(
            _breaker_policy(), breaker_store=_FlakyBreakerStore(fail_record=True)
        )

        async def fn() -> str:
            raise exc.validation("bad input")

        # record() blows up, but the caller must still see the domain error.
        with pytest.raises(CoreException) as ei:
            await executor.run(fn, policy="p")

        assert ei.value.kind is ExceptionKind.VALIDATION

    async def test_breaker_record_failure_preserves_success(self) -> None:
        executor = _executor(
            _breaker_policy(), breaker_store=_FlakyBreakerStore(fail_record=True)
        )

        # A record() failure on the success path must not fail the call.
        assert await executor.run(_ok, policy="p") == "ok"

    async def test_rate_limit_store_failure_fails_open_by_default(self) -> None:
        executor = _executor(
            _rate_limit_policy(), rate_limit_store=_FlakyRateLimitStore()
        )
        counter = _Counter()

        async def fn() -> str:
            counter.calls += 1
            return "ok"

        assert await executor.run(fn, policy="p") == "ok"
        assert counter.calls == 1

    async def test_rate_limit_store_failure_fails_closed_when_configured(self) -> None:
        executor = _executor(
            attrs.evolve(_rate_limit_policy(), fail_open_on_store_error=False),
            rate_limit_store=_FlakyRateLimitStore(),
        )

        with pytest.raises(CoreException) as ei:
            await executor.run(_ok, policy="p")

        assert ei.value.kind is ExceptionKind.INFRASTRUCTURE
        assert ei.value.code == "resilience_store_unavailable"

    async def test_store_error_is_surfaced_as_metric(self) -> None:
        executor = _executor(
            _breaker_policy(), breaker_store=_FlakyBreakerStore(fail_admit=True)
        )
        events: list[str] = []
        executor.set_metrics_sink(lambda event, _pol, _route: events.append(event))

        assert await executor.run(_ok, policy="p") == "ok"
        assert "breaker_store_error" in events
