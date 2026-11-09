"""`instrument_operations` exports an OpenTelemetry span + metrics per operation."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import attrs
import pytest
from opentelemetry import trace
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
from forze.base.exceptions import CoreException, exc
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


@attrs.define(slots=True)
class _Raise(Handler[None, None]):
    error: CoreException

    async def __call__(self, _args: None) -> None:
        raise self.error


@attrs.define(slots=True)
class _Throw(Handler[None, None]):
    error: BaseException

    async def __call__(self, _args: None) -> None:
        raise self.error


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
                execution_id=uuid4(), correlation_id=uuid4(), surface="mcp"
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
        assert span.attributes["forze.invocation_surface"] == "mcp"
        assert span.attributes["forze.tenant_id"] == str(tenant_id)
        assert span.attributes["forze.principal_id"] == str(principal_id)
        assert span.status.status_code is not StatusCode.ERROR

        ((labels, point),) = _points(reader, OPERATIONS_COUNTER)
        assert labels["forze.outcome"] == "success"
        # A log field and a span attribute only: a metric label would change shipped series.
        assert "forze.invocation_surface" not in labels
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

    @pytest.mark.parametrize(
        ("error", "outcome", "status", "exception_events"),
        [
            (exc.not_found("No such note"), "failed", StatusCode.UNSET, 0),
            # A 503, but a state an operator chose, not a fault: no error-rate noise.
            (exc.unavailable("Switched off"), "failed", StatusCode.UNSET, 0),
            (exc.internal("Wiring broke"), "error", StatusCode.ERROR, 1),
        ],
        ids=["client-kind", "unavailable", "server-kind"],
    )
    async def test_only_a_server_error_marks_the_span(
        self,
        error: CoreException,
        outcome: str,
        status: StatusCode,
        exception_events: int,
    ) -> None:
        tracer, meter, exporter, reader = _otel()
        reg = _instrumented({"op": lambda _c: _Raise(error)}, tracer, meter)
        ctx = context_from_modules(MockDepsModule())

        with pytest.raises(CoreException):
            await run_operation(reg, "op", None, ctx)

        (span,) = exporter.get_finished_spans()
        assert span.status.status_code is status
        assert sum(e.name == "exception" for e in span.events) == exception_events

        ((labels, _),) = _points(reader, OPERATIONS_COUNTER)
        assert labels["forze.outcome"] == outcome

    async def test_query_op_kind_attribute(self) -> None:
        tracer, meter, exporter, _ = _otel()
        reg = _instrumented({"q": lambda _c: _Echo()}, tracer, meter, query=("q",))
        ctx = context_from_modules(MockDepsModule())

        await run_operation(reg, "q", "x", ctx)

        (span,) = exporter.get_finished_spans()
        assert span.attributes["forze.operation.kind"] == "query"
        # No boundary bound metadata, and the span still says so.
        assert span.attributes["forze.invocation_surface"] == "unknown"

    async def test_span_attributes_are_exactly_the_bound_ids(self) -> None:
        tracer, meter, exporter, _ = _otel()
        reg = _instrumented({"do": lambda _c: _Echo()}, tracer, meter)
        ctx = context_from_modules(MockDepsModule())

        metadata = InvocationMetadata(
            execution_id=uuid4(),
            correlation_id=uuid4(),
            causation_id=uuid4(),
            surface="mcp",
        )
        tenant_id, principal_id = uuid4(), uuid4()
        with ctx.inv_ctx.bind(
            metadata=metadata,
            authn=AuthnIdentity(principal_id=principal_id),
            tenant=TenantIdentity(tenant_id=tenant_id),
        ):
            await run_operation(reg, "do", "x", ctx)

        await run_operation(reg, "do", "x", ctx)  # nothing bound

        bound, bare = exporter.get_finished_spans()
        assert dict(bound.attributes) == {
            "forze.operation": "do",
            "forze.operation.kind": "command",
            "forze.invocation_surface": "mcp",
            "forze.execution_id": str(metadata.execution_id),
            "forze.correlation_id": str(metadata.correlation_id),
            "forze.causation_id": str(metadata.causation_id),
            "forze.tenant_id": str(tenant_id),
            "forze.principal_id": str(principal_id),
        }
        assert dict(bare.attributes) == {
            "forze.operation": "do",
            "forze.operation.kind": "command",
            "forze.invocation_surface": "unknown",
        }

    @pytest.mark.parametrize(
        ("error", "outcome"),
        [
            (None, "success"),
            (exc.not_found("No such note"), "failed"),
            (RuntimeError("boom"), "error"),
            (asyncio.CancelledError(), "error"),
        ],
        ids=["success", "failed", "error", "cancelled"],
    )
    @pytest.mark.parametrize("kind", ["command", "query"])
    async def test_metric_labels_per_kind_and_outcome(
        self,
        error: BaseException | None,
        outcome: str,
        kind: str,
    ) -> None:
        tracer, meter, exporter, reader = _otel()
        handler = (lambda _c: _Echo()) if error is None else (lambda _c: _Throw(error))
        query = ("op",) if kind == "query" else ()
        reg = _instrumented({"op": handler}, tracer, meter, query=query)
        ctx = context_from_modules(MockDepsModule())

        if error is None:
            await run_operation(reg, "op", "x", ctx)

        else:
            with pytest.raises(type(error)):
                await run_operation(reg, "op", None, ctx)

        expected = {
            "forze.operation": "op",
            "forze.operation.kind": kind,
            "forze.outcome": outcome,
        }
        ((labels, point),) = _points(reader, OPERATIONS_COUNTER)
        assert labels == expected
        assert point.value == 1
        ((hist_labels, hist),) = _points(reader, DURATION_HISTOGRAM)
        assert hist_labels == expected
        assert hist.count == 1

        (span,) = exporter.get_finished_spans()
        assert span.attributes["forze.operation.kind"] == kind
        error_status = outcome == "error"
        assert (span.status.status_code is StatusCode.ERROR) is error_status

    async def test_span_is_current_inside_the_operation_only(self) -> None:
        tracer, meter, exporter, reader = _otel()
        seen: list[Any] = []

        @attrs.define(slots=True)
        class _Peek(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                seen.append(trace.get_current_span())

        reg = _instrumented({"op": lambda _c: _Peek()}, tracer, meter)
        ctx = context_from_modules(MockDepsModule())

        with tracer.start_as_current_span("outer") as outer:
            await run_operation(reg, "op", None, ctx)
            assert trace.get_current_span() is outer  # detached on the way out

        op_span, outer_span = exporter.get_finished_spans()
        assert seen[0].get_span_context() == op_span.context
        assert op_span.parent is not None
        assert op_span.parent.span_id == outer_span.context.span_id

        # Recorded while the operation's span is current: the exemplar points at it.
        ((_, point),) = _points(reader, OPERATIONS_COUNTER)
        assert [e.span_id for e in point.exemplars] == [op_span.context.span_id]

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
