"""OpenTelemetry spans + metrics for durable execution.

# covers: DurableTelemetry.run_span
# covers: DurableTelemetry.record_run
# covers: DurableTelemetry.record_recovered
# covers: DurableTelemetry.record_fire
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, exc
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DURABLE_RECOVERED_COUNTER,
    DURABLE_RUN_DURATION_HISTOGRAM,
    DURABLE_RUNS_COUNTER,
    DURABLE_SCHEDULE_FIRES_COUNTER,
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableScheduler,
    DurableTelemetry,
)
from forze_mock import MockDepsModule

# ----------------------- #

UTC = timezone.utc


def _otel() -> tuple[Any, Any, InMemorySpanExporter, InMemoryMetricReader]:
    exporter = InMemorySpanExporter()
    tp = TracerProvider()
    tp.add_span_processor(SimpleSpanProcessor(exporter))

    reader = InMemoryMetricReader()
    mp = MeterProvider(metric_readers=[reader])

    return tp.get_tracer("test"), mp.get_meter("test"), exporter, reader


def _points(reader: InMemoryMetricReader, name: str) -> list[tuple[dict[str, Any], Any]]:
    data = reader.get_metrics_data()
    out: list[tuple[dict[str, Any], Any]] = []
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name == name:
                    for dp in metric.data.data_points:
                        out.append((dict(dp.attributes), dp))
    return out


def _registry(fail: bool = False, *, forward: bool = False) -> DurableFunctionRegistry:
    registry = DurableFunctionRegistry()

    async def handler(ctx: ExecutionContext, input_json: dict | None) -> dict:
        if forward:
            raise exc.infrastructure("pivot", code="saga.forward_incomplete")
        if fail:
            raise exc.internal("boom")
        return {"ok": True}

    registry.register("fn", handler)
    return registry


# ....................... #


class TestRunTelemetry:
    async def test_success_emits_span_and_metrics(self) -> None:
        tracer, meter, exporter, reader = _otel()
        ctx = context_from_modules(MockDepsModule())
        runner = DurableFunctionRunner(
            registry=_registry(),
            telemetry=DurableTelemetry.create(tracer=tracer, meter=meter),
        )

        record = await runner.run_now(ctx, "fn", {"n": 1})

        (span,) = exporter.get_finished_spans()
        assert span.name == "durable.run"
        assert span.attributes["forze.durable.name"] == "fn"
        assert span.attributes["forze.durable.run_id"] == record.run_id
        assert span.status.status_code is not StatusCode.ERROR

        ((labels, point),) = _points(reader, DURABLE_RUNS_COUNTER)
        assert labels["forze.durable.outcome"] == "completed"
        assert point.value == 1
        ((_, hist),) = _points(reader, DURABLE_RUN_DURATION_HISTOGRAM)
        assert hist.count == 1

    async def test_failure_marks_span_error_and_counts_failed(self) -> None:
        tracer, meter, exporter, reader = _otel()
        ctx = context_from_modules(MockDepsModule())
        runner = DurableFunctionRunner(
            registry=_registry(fail=True),
            telemetry=DurableTelemetry.create(tracer=tracer, meter=meter),
        )

        with pytest.raises(CoreException, match="boom"):
            await runner.run_now(ctx, "fn")

        (span,) = exporter.get_finished_spans()
        assert span.status.status_code is StatusCode.ERROR
        assert any(e.name == "exception" for e in span.events)

        ((labels, _),) = _points(reader, DURABLE_RUNS_COUNTER)
        assert labels["forze.durable.outcome"] == "failed"

    async def test_forward_incomplete_outcome_label(self) -> None:
        tracer, meter, _exporter, reader = _otel()
        ctx = context_from_modules(MockDepsModule())
        runner = DurableFunctionRunner(
            registry=_registry(forward=True),
            telemetry=DurableTelemetry.create(tracer=tracer, meter=meter),
        )

        with pytest.raises(CoreException):
            await runner.run_now(ctx, "fn")

        ((labels, _),) = _points(reader, DURABLE_RUNS_COUNTER)
        assert labels["forze.durable.outcome"] == "forward_incomplete"

    async def test_recover_counts_reclaimed_runs(self) -> None:
        tracer, meter, _exporter, reader = _otel()
        ctx = context_from_modules(MockDepsModule())
        runner = DurableFunctionRunner(
            registry=_registry(),
            telemetry=DurableTelemetry.create(tracer=tracer, meter=meter),
        )

        await runner.enqueue(ctx, "fn")
        assert await runner.recover(ctx) == 1

        ((_, point),) = _points(reader, DURABLE_RECOVERED_COUNTER)
        assert point.value == 1


class TestScheduleTelemetry:
    async def test_fire_is_counted(self) -> None:
        tracer, meter, _exporter, reader = _otel()
        ctx = context_from_modules(MockDepsModule())
        scheduler = DurableScheduler(
            telemetry=DurableTelemetry.create(tracer=tracer, meter=meter)
        )

        await scheduler.put(
            ctx, "s", "fn", "* * * * *", now=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)
        )
        fired = await scheduler.tick(ctx, now=datetime(2026, 1, 1, 0, 1, 5, tzinfo=UTC))
        assert fired == 1

        ((labels, point),) = _points(reader, DURABLE_SCHEDULE_FIRES_COUNTER)
        assert labels["forze.durable.name"] == "fn"
        assert point.value == 1
