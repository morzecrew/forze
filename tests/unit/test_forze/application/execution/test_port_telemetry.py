"""`with_otel_port_spans` emits a per-port OpenTelemetry CLIENT span under the operation span."""

from __future__ import annotations

from typing import Any, AsyncIterator

import attrs
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind, StatusCode

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.observability import instrument_operations
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.tracing.otel_port_proxy import wrap_port_otel_spans
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

# ----------------------- #


def _tracer_and_exporter() -> tuple[Any, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


# ....................... #
# Proxy-level: wrap_port_otel_spans directly (fast, no execution harness).


class _FakePort:
    def __init__(self) -> None:
        self.label = "not-a-method"  # a non-callable attribute

    async def get(self, pk: str) -> dict[str, str]:
        return {"id": pk}

    def stat(self) -> int:  # a sync method
        return 7

    async def stream(self) -> AsyncIterator[int]:
        yield 1
        yield 2

    async def boom(self) -> None:
        raise RuntimeError("infra down")

    async def missing(self) -> None:
        raise exc.not_found("nope")  # a 4xx-kind domain failure

    async def dead(self) -> None:
        raise exc.infrastructure("connection lost")  # a 5xx-kind transport failure


def _wrapped(tracer: Any, **over: Any) -> Any:
    return wrap_port_otel_spans(
        _FakePort(),
        tracer=tracer,
        domain=over.get("domain", "document"),
        surface=over.get("surface", "document_query"),
        route=over.get("route", "orders"),
        phase=over.get("phase", "query"),
    )


class TestOtelSpanPortProxy:
    async def test_call_nests_a_client_span_under_the_current_span(self) -> None:
        tracer, exporter = _tracer_and_exporter()
        port = _wrapped(tracer)

        with tracer.start_as_current_span("op") as op_span:
            assert await port.get("x") == {"id": "x"}
            op_id = op_span.get_span_context().span_id

        spans = {s.name: s for s in exporter.get_finished_spans()}
        child = spans["document_query.get"]
        assert child.parent is not None and child.parent.span_id == op_id
        assert child.kind == SpanKind.CLIENT
        assert child.attributes["forze.port.domain"] == "document"
        assert child.attributes["forze.port.surface"] == "document_query"
        assert child.attributes["forze.port.route"] == "orders"
        assert child.attributes["forze.port.phase"] == "query"
        assert child.attributes["forze.port.op"] == "get"

    async def test_optional_attributes_are_omitted_when_absent(self) -> None:
        tracer, exporter = _tracer_and_exporter()
        port = _wrapped(tracer, route=None, phase=None)

        await port.get("x")

        (span,) = exporter.get_finished_spans()
        assert "forze.port.route" not in span.attributes
        assert "forze.port.phase" not in span.attributes

    async def test_infra_exception_marks_error_and_records(self) -> None:
        tracer, exporter = _tracer_and_exporter()
        port = _wrapped(tracer)

        with pytest.raises(RuntimeError, match="infra down"):
            await port.boom()

        (span,) = exporter.get_finished_spans()
        assert span.status.status_code is StatusCode.ERROR
        assert any(event.name == "exception" for event in span.events)

    async def test_domain_core_exception_leaves_the_span_clean(self) -> None:
        # A 4xx-kind domain failure (not-found) the caller may handle must not paint the client span red.
        tracer, exporter = _tracer_and_exporter()
        port = _wrapped(tracer)

        with pytest.raises(exc):
            await port.missing()

        (span,) = exporter.get_finished_spans()
        assert span.status.status_code is not StatusCode.ERROR
        assert not any(event.name == "exception" for event in span.events)

    async def test_infrastructure_core_exception_marks_error(self) -> None:
        # A 5xx-kind CoreException (lost connection, adapter bug) IS a transport failure — the client
        # span exists to surface exactly this, so it sets ERROR (unlike a 4xx domain failure).
        tracer, exporter = _tracer_and_exporter()
        port = _wrapped(tracer)

        with pytest.raises(exc):
            await port.dead()

        (span,) = exporter.get_finished_spans()
        assert span.status.status_code is StatusCode.ERROR
        assert any(event.name == "exception" for event in span.events)

    async def test_sync_method_is_spanned(self) -> None:
        tracer, exporter = _tracer_and_exporter()
        port = _wrapped(tracer)

        assert port.stat() == 7

        (span,) = exporter.get_finished_spans()
        assert span.name == "document_query.stat" and span.kind == SpanKind.CLIENT

    async def test_async_generator_passes_through_unspanned(self) -> None:
        # Streaming methods are deliberately not spanned (a current-context span across yields leaks
        # and corrupts the ambient span on early break); items still flow, no span is emitted.
        tracer, exporter = _tracer_and_exporter()
        port = _wrapped(tracer)

        with tracer.start_as_current_span("op"):
            assert [item async for item in port.stream()] == [1, 2]

        names = {s.name for s in exporter.get_finished_spans()}
        assert "document_query.stream" not in names

    async def test_async_generator_early_break_does_not_corrupt_context(self) -> None:
        # Regression guard for the OTel-generator contextvar bug: breaking out of a stream early must
        # leave the current span as the op span (not a leaked child) and raise nothing.
        from opentelemetry import trace as otel_trace

        tracer, _ = _tracer_and_exporter()
        port = _wrapped(tracer)

        with tracer.start_as_current_span("op") as op_span:
            async for _ in port.stream():
                break  # abandon the stream early
            assert otel_trace.get_current_span() is op_span

    async def test_non_callable_attribute_passes_through(self) -> None:
        tracer, exporter = _tracer_and_exporter()
        port = _wrapped(tracer)

        assert port.label == "not-a-method"
        assert exporter.get_finished_spans() == ()


# ....................... #
# End-to-end wiring: with_otel_port_spans → resolution → real port calls under the op span.


class Cell(Document):
    value: int = 0


class CellCreate(CreateDocumentCmd):
    value: int = 0


class CellRead(ReadDocument):
    value: int


class CellUpdate(BaseDTO):
    value: int | None = None


CELL = DocumentSpec(
    name="cells",
    read=CellRead,
    write=DocumentWriteTypes(domain=Cell, create_cmd=CellCreate, update_cmd=CellUpdate),
)


@attrs.define(slots=True, kw_only=True)
class _CreateAndGet(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        row = await self.ctx.document.command(CELL).create(CellCreate(value=1))
        await self.ctx.document.query(CELL).get(row.id)


def _context(tracer: Any | None) -> ExecutionContext:
    registry = DepsRegistry.from_modules(MockDepsModule())
    if tracer is not None:
        registry = registry.with_otel_port_spans(tracer=tracer)
    return ExecutionContext(deps=registry.freeze().resolve())


class TestPortSpanWiring:
    async def test_port_calls_emit_child_spans_under_the_op_span(self) -> None:
        tracer, exporter = _tracer_and_exporter()
        registry = instrument_operations(
            OperationRegistry(handlers={"do": lambda ctx: _CreateAndGet(ctx=ctx)}),
            tracer=tracer,
        ).freeze()
        ctx = _context(tracer)

        await run_operation(registry, "do", None, ctx)

        spans = {s.name: s for s in exporter.get_finished_spans()}
        op = spans["do"]
        assert "document_command.create" in spans and "document_query.get" in spans

        op_id = op.get_span_context().span_id
        for name in ("document_command.create", "document_query.get"):
            child = spans[name]
            assert child.parent is not None and child.parent.span_id == op_id
            assert child.attributes["forze.port.route"] == "cells"

    async def test_off_by_default_emits_no_port_spans(self) -> None:
        tracer, exporter = _tracer_and_exporter()
        registry = instrument_operations(
            OperationRegistry(handlers={"do": lambda ctx: _CreateAndGet(ctx=ctx)}),
            tracer=tracer,
        ).freeze()
        ctx = _context(None)  # no with_otel_port_spans

        await run_operation(registry, "do", None, ctx)

        names = {s.name for s in exporter.get_finished_spans()}
        assert names == {"do"}  # only the op span; ports stay bare (zero cost)

    async def test_port_spans_without_an_op_span(self) -> None:
        # Port spans don't require instrument_operations — they nest under whatever span is current
        # (here none, so they are roots). Proves the two opt-ins are independent.
        tracer, exporter = _tracer_and_exporter()
        registry = OperationRegistry(
            handlers={"do": lambda ctx: _CreateAndGet(ctx=ctx)}
        ).freeze()
        ctx = _context(tracer)

        await run_operation(registry, "do", None, ctx)

        names = {s.name for s in exporter.get_finished_spans()}
        assert "document_command.create" in names and "document_query.get" in names
        assert "do" not in names  # no op span was instrumented
