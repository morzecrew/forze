"""`instrument_resilience` exports resilience events as always-on OTel metrics.

The metrics sink is independent of the runtime-tracing gate: every test here
runs **without** binding traced deps, proving production processes with
tracing off still report retries, rejections, breaker state, and bulkhead
queue depth.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.resilience import (
    BackoffStrategy,
    BulkheadStrategy,
    CircuitBreakerStrategy,
    RateLimitStrategy,
    ResiliencePolicy,
    RetryStrategy,
)
from forze.application.execution.observability import (
    BREAKER_STATE_GAUGE,
    BULKHEAD_QUEUE_GAUGE,
    RESILIENCE_EVENTS_COUNTER,
    instrument_resilience,
)
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.base.exceptions import CoreException, ExceptionKind, exc

# ----------------------- #


async def _no_sleep(_delay: float) -> None:
    return None


def _executor(*policies: ResiliencePolicy) -> InProcessResilienceExecutor:
    return InProcessResilienceExecutor(
        policies={p.name: p for p in policies},
        sleep=_no_sleep,
    )


def _meter() -> tuple[Any, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    return MeterProvider(metric_readers=[reader]).get_meter("test"), reader


def _points(reader: InMemoryMetricReader, name: str) -> list[tuple[dict[str, Any], Any]]:
    data = reader.get_metrics_data()
    out: list[tuple[dict[str, Any], Any]] = []

    if data is None:
        return out

    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    for dp in metric.data.data_points:
                        out.append((dict(dp.attributes), dp))

    return out


def _retry_policy() -> ResiliencePolicy:
    return ResiliencePolicy(
        name="p",
        strategies=(
            RetryStrategy(
                max_attempts=3,
                backoff=BackoffStrategy(
                    base=timedelta(milliseconds=1),
                    max=timedelta(milliseconds=10),
                    jitter="none",
                ),
                retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
            ),
        ),
    )


# ----------------------- #


class TestResilienceMetrics:
    async def test_retry_attempts_counted_without_tracing(self) -> None:
        meter, reader = _meter()
        executor = instrument_resilience(_executor(_retry_policy()), meter=meter)
        calls = 0

        async def fn() -> str:
            nonlocal calls
            calls += 1

            if calls < 3:
                raise exc.infrastructure("transient")

            return "ok"

        assert await executor.run(fn, policy="p") == "ok"

        points = {
            labels["forze.event"]: point.value
            for labels, point in _points(reader, RESILIENCE_EVENTS_COUNTER)
        }
        assert points["retry_attempt"] == 2

    async def test_rate_limit_rejection_counted_with_route(self) -> None:
        meter, reader = _meter()
        policy = ResiliencePolicy(
            name="p",
            strategies=(
                RateLimitStrategy(
                    permits=1, per=timedelta(seconds=100), burst=1
                ),
            ),
        )
        executor = instrument_resilience(_executor(policy), meter=meter)

        async def fn() -> str:
            return "ok"

        assert await executor.run(fn, policy="p", route="r") == "ok"

        with pytest.raises(CoreException):
            await executor.run(fn, policy="p", route="r")

        ((labels, point),) = [
            (labels, point)
            for labels, point in _points(reader, RESILIENCE_EVENTS_COUNTER)
            if labels["forze.event"] == "rate_limit_reject"
        ]
        assert point.value == 1
        assert labels["forze.policy"] == "p"
        assert labels["forze.route"] == "r"

    async def test_breaker_state_gauge_tracks_open(self) -> None:
        meter, reader = _meter()
        policy = ResiliencePolicy(
            name="p",
            strategies=(
                CircuitBreakerStrategy(
                    failure_ratio=1.0,
                    sampling_window=timedelta(seconds=100),
                    min_throughput=2,
                    break_duration=timedelta(seconds=10),
                    half_open_max_calls=1,
                ),
            ),
        )
        executor = instrument_resilience(_executor(policy), meter=meter)

        async def boom() -> str:
            raise exc.infrastructure("down")

        for _ in range(2):
            with pytest.raises(CoreException):
                await executor.run(boom, policy="p")

        ((labels, point),) = _points(reader, BREAKER_STATE_GAUGE)
        assert point.value == 2  # open
        assert labels["forze.policy"] == "p"

    async def test_bulkhead_queue_depth_observed(self) -> None:
        meter, reader = _meter()
        policy = ResiliencePolicy(
            name="p",
            strategies=(BulkheadStrategy(max_concurrency=1, max_queue=2),),
        )
        executor = instrument_resilience(_executor(policy), meter=meter)

        started = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> str:
            started.set()
            await release.wait()
            return "ok"

        async def fast() -> str:
            return "ok"

        holder_task = asyncio.create_task(executor.run(holder, policy="p"))
        await started.wait()
        waiter_task = asyncio.create_task(executor.run(fast, policy="p"))
        await asyncio.sleep(0)

        ((labels, point),) = _points(reader, BULKHEAD_QUEUE_GAUGE)
        assert point.value == 1  # one call queued behind the held semaphore
        assert labels["forze.policy"] == "p"

        release.set()
        assert await holder_task == "ok"
        assert await waiter_task == "ok"

    async def test_uninstrumented_executor_emits_nothing(self) -> None:
        _meter_obj, reader = _meter()
        executor = _executor(_retry_policy())
        calls = 0

        async def fn() -> str:
            nonlocal calls
            calls += 1

            if calls < 2:
                raise exc.infrastructure("transient")

            return "ok"

        assert await executor.run(fn, policy="p") == "ok"
        assert _points(reader, RESILIENCE_EVENTS_COUNTER) == []
