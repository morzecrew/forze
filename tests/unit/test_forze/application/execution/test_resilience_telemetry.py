"""`instrument_resilience` exports resilience events as always-on OTel metrics.

The metrics sink is independent of the runtime-tracing gate: every test here
runs **without** binding traced deps, proving production processes with
tracing off still report retries, rejections, breaker state, and bulkhead
queue depth.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from functools import partial
from typing import Any

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    BackoffStrategy,
    BulkheadStrategy,
    CircuitBreakerStrategy,
    GradientBulkheadStrategy,
    HedgeStrategy,
    RateLimitStrategy,
    ResiliencePolicy,
    RetryStrategy,
    TimeoutStrategy,
)
from forze.application.execution.observability import (
    BREAKER_STATE_GAUGE,
    BULKHEAD_IN_USE_GAUGE,
    BULKHEAD_LIMIT_GAUGE,
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


    async def test_adaptive_bulkhead_limit_observed(self) -> None:
        meter, reader = _meter()
        policy = ResiliencePolicy(
            name="p",
            strategies=(
                AdaptiveBulkheadStrategy(
                    latency_threshold=timedelta(milliseconds=100),
                    max_concurrency=8,
                ),
            ),
        )
        executor = instrument_resilience(_executor(policy), meter=meter)

        async def fn() -> str:
            return "ok"

        assert await executor.run(fn, policy="p") == "ok"

        ((labels, point),) = _points(reader, BULKHEAD_LIMIT_GAUGE)
        assert point.value == 8.0
        assert labels["forze.policy"] == "p"

    async def test_adaptive_hedge_delay_observed(self) -> None:
        from forze.application.execution.observability import HEDGE_DELAY_GAUGE

        meter, reader = _meter()
        policy = ResiliencePolicy(
            name="p",
            strategies=(TimeoutStrategy(timeout=timedelta(seconds=10)),),
            hedge=HedgeStrategy(
                delay=timedelta(milliseconds=250),
                max_attempts=2,
                adaptive_delay_quantile=0.95,
            ),
        )
        executor = instrument_resilience(_executor(policy), meter=meter)

        async def fn() -> str:
            return "ok"

        assert await executor.run_hedged(fn, policy="p", route="r") == "ok"

        ((labels, point),) = _points(reader, HEDGE_DELAY_GAUGE)
        assert point.value == pytest.approx(0.25)  # unwarmed: fixed fallback
        assert labels["forze.policy"] == "p"
        assert labels["forze.route"] == "r"


# ----------------------- #

_BULKHEAD_STRATEGIES = [
    pytest.param(BulkheadStrategy(max_concurrency=3), id="fixed"),
    pytest.param(
        AdaptiveBulkheadStrategy(
            latency_threshold=timedelta(seconds=10),
            max_concurrency=3,
        ),
        id="aimd",
    ),
    pytest.param(GradientBulkheadStrategy(max_concurrency=3), id="gradient2"),
]


class TestBulkheadOccupancyMetrics:
    @pytest.mark.parametrize("strategy", _BULKHEAD_STRATEGIES)
    async def test_in_use_and_limit_observed_while_slots_held(
        self,
        strategy: Any,
    ) -> None:
        meter, reader = _meter()
        policy = ResiliencePolicy(name="p", strategies=(strategy,))
        executor = instrument_resilience(_executor(policy), meter=meter)

        started = [asyncio.Event(), asyncio.Event()]
        release = asyncio.Event()

        async def holder(i: int) -> str:
            started[i].set()
            await release.wait()
            return "ok"

        tasks = [
            asyncio.create_task(executor.run(partial(holder, i), policy="p", route="r"))
            for i in range(2)
        ]

        for event in started:
            await asyncio.wait_for(event.wait(), 5)

        assert list(executor.bulkhead_occupancy()) == [("p", "r", 2, 3.0)]

        ((labels, point),) = _points(reader, BULKHEAD_IN_USE_GAUGE)
        assert point.value == 2  # two calls hold a slot
        assert labels == {"forze.policy": "p", "forze.route": "r"}

        ((labels, point),) = _points(reader, BULKHEAD_LIMIT_GAUGE)
        assert point.value == 3.0  # a fixed cap reports like a dynamic limit
        assert labels == {"forze.policy": "p", "forze.route": "r"}

        release.set()
        assert await asyncio.gather(*tasks) == ["ok", "ok"]

        ((_, point),) = _points(reader, BULKHEAD_IN_USE_GAUGE)
        assert point.value == 0

    async def test_uninstrumented_bulkhead_registers_nothing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from opentelemetry import metrics

        meter, reader = _meter()
        # Any registration on the global meter would land in this reader.
        monkeypatch.setattr(metrics, "get_meter", lambda *_a, **_k: meter)

        policy = ResiliencePolicy(
            name="p",
            strategies=(BulkheadStrategy(max_concurrency=1),),
        )
        executor = _executor(policy)
        started = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> str:
            started.set()
            await release.wait()
            return "ok"

        task = asyncio.create_task(executor.run(holder, policy="p"))
        await asyncio.wait_for(started.wait(), 5)

        for name in (BULKHEAD_IN_USE_GAUGE, BULKHEAD_LIMIT_GAUGE, BULKHEAD_QUEUE_GAUGE):
            assert _points(reader, name) == []

        release.set()
        assert await task == "ok"


class TestScrapeDuringAdmission:
    """The SDK's exporter thread runs gauge callbacks while the event loop admits calls.

    Each admission marks its state most-recently-used, reordering the executor's LRU map, so
    a callback resumed after one must not be iterating that map live.
    """

    async def _fn(self) -> str:
        return "ok"

    @pytest.mark.parametrize("accessor", ["bulkhead_queue_depths", "bulkhead_occupancy"])
    async def test_bulkhead_scrape_survives_admission(self, accessor: str) -> None:
        policy = ResiliencePolicy(
            name="p",
            strategies=(BulkheadStrategy(max_concurrency=1),),
        )
        executor = _executor(policy)

        for route in ("a", "b"):
            assert await executor.run(self._fn, policy="p", route=route) == "ok"

        scrape = getattr(executor, accessor)()
        first = next(scrape)
        assert await executor.run(self._fn, policy="p", route="a") == "ok"

        assert [first[:2], *(row[:2] for row in scrape)] == [("p", "a"), ("p", "b")]

    async def test_hedge_scrape_survives_admission(self) -> None:
        policy = ResiliencePolicy(
            name="p",
            strategies=(TimeoutStrategy(timeout=timedelta(seconds=10)),),
            hedge=HedgeStrategy(
                delay=timedelta(milliseconds=250),
                max_attempts=2,
                adaptive_delay_quantile=0.95,
            ),
        )
        executor = _executor(policy)

        for route in ("a", "b"):
            assert await executor.run_hedged(self._fn, policy="p", route=route) == "ok"

        scrape = executor.hedge_delays()
        first = next(scrape)
        assert await executor.run_hedged(self._fn, policy="p", route="a") == "ok"

        assert [first[:2], *(row[:2] for row in scrape)] == [("p", "a"), ("p", "b")]
