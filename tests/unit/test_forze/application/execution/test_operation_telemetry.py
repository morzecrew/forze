"""`instrument_operations` exports an OpenTelemetry span + metrics per operation."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import attrs
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.execution import Handler
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.context.invocation import InvocationMetadata
from forze.application.execution.observability import (
    DURATION_HISTOGRAM,
    OPERATIONS_COUNTER,
    instrument_operations,
)
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry

from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


@attrs.define(slots=True)
class _Echo(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return args


@attrs.define(slots=True)
class _Boom(Handler[None, None]):
    async def __call__(self, _args: None) -> None:
        raise RuntimeError("boom")


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


def _instrumented(handlers: dict[str, Any], tracer: Any, meter: Any, *, query=()):
    reg = OperationRegistry(handlers=handlers)
    for op in query:
        reg = reg.bind(op).as_query().finish()
    return instrument_operations(reg, tracer=tracer, meter=meter).freeze()


# ....................... #


class TestInstrumentOperations:
    async def test_success_emits_span_and_metrics(self) -> None:
        tracer, meter, exporter, reader = _otel()
        reg = _instrumented({"do": lambda _c: _Echo()}, tracer, meter)
        ctx = context_from_modules(MockDepsModule())

        tenant_id, principal_id = uuid4(), uuid4()
        with ctx.inv_ctx.bind(
            metadata=InvocationMetadata(
                execution_id=uuid4(), correlation_id=uuid4()
            ),
            authn=AuthnIdentity(principal_id=principal_id),
            tenant=TenantIdentity(tenant_id=tenant_id),
        ):
            result = await run_operation(reg, "do", "hi", ctx)
        assert result == "hi"

        (span,) = exporter.get_finished_spans()
        assert span.name == "do"
        assert span.attributes["forze.operation"] == "do"
        assert span.attributes["forze.operation.kind"] == "command"
        assert span.attributes["forze.execution_id"]  # metadata flows through
        assert span.attributes["forze.tenant_id"] == str(tenant_id)
        assert span.attributes["forze.principal_id"] == str(principal_id)
        assert span.status.status_code is not StatusCode.ERROR

        ((labels, point),) = _points(reader, OPERATIONS_COUNTER)
        assert labels["forze.outcome"] == "success"
        assert point.value == 1
        ((_, hist),) = _points(reader, DURATION_HISTOGRAM)
        assert hist.count == 1

    async def test_failure_marks_span_error_and_reraises(self) -> None:
        tracer, meter, exporter, reader = _otel()
        reg = _instrumented({"boom": lambda _c: _Boom()}, tracer, meter)
        ctx = context_from_modules(MockDepsModule())

        with pytest.raises(RuntimeError, match="boom"):
            await run_operation(reg, "boom", None, ctx)

        (span,) = exporter.get_finished_spans()
        assert span.status.status_code is StatusCode.ERROR
        assert any(e.name == "exception" for e in span.events)  # record_exception

        ((labels, _),) = _points(reader, OPERATIONS_COUNTER)
        assert labels["forze.outcome"] == "error"

    async def test_query_op_kind_attribute(self) -> None:
        tracer, meter, exporter, _ = _otel()
        reg = _instrumented({"q": lambda _c: _Echo()}, tracer, meter, query=("q",))
        ctx = context_from_modules(MockDepsModule())

        await run_operation(reg, "q", "x", ctx)

        (span,) = exporter.get_finished_spans()
        assert span.attributes["forze.operation.kind"] == "query"

    async def test_instruments_every_operation(self) -> None:
        tracer, meter, exporter, _ = _otel()
        reg = _instrumented(
            {"a": lambda _c: _Echo(), "b": lambda _c: _Echo()}, tracer, meter
        )
        ctx = context_from_modules(MockDepsModule())

        await run_operation(reg, "a", "x", ctx)
        await run_operation(reg, "b", "y", ctx)

        names = {s.name for s in exporter.get_finished_spans()}
        assert names == {"a", "b"}
