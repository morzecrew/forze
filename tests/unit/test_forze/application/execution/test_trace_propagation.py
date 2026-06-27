"""W3C trace-context propagation helpers — capture the active span, rebuild a remote-parent context."""

from __future__ import annotations

from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from forze.application.execution.tracing.propagation import (
    context_from_traceparent,
    current_traceparent,
)

# ----------------------- #


def _tracer() -> tuple[Any, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


class TestTracePropagationHelpers:
    def test_current_traceparent_is_none_without_an_active_span(self) -> None:
        assert current_traceparent() is None

    def test_current_traceparent_encodes_the_active_span(self) -> None:
        tracer, _ = _tracer()
        with tracer.start_as_current_span("publish") as span:
            tp = current_traceparent()
            sc = span.get_span_context()

        assert tp is not None
        # W3C format: 00-<32hex trace>-<16hex span>-<2hex flags>
        parts = tp.split("-")
        assert parts[0] == "00"
        assert parts[1] == format(sc.trace_id, "032x")
        assert parts[2] == format(sc.span_id, "016x")

    def test_context_from_traceparent_makes_work_a_child_of_the_origin(self) -> None:
        tracer, _ = _tracer()

        with tracer.start_as_current_span("publish") as publish:
            tp = current_traceparent()
            publish_sc = publish.get_span_context()

        assert tp is not None
        token = otel_context.attach(context_from_traceparent(tp))
        try:
            with tracer.start_as_current_span("consume") as consume:
                consume_sc = consume.get_span_context()
                parent = consume.parent
        finally:
            otel_context.detach(token)

        assert consume_sc.trace_id == publish_sc.trace_id  # same distributed trace
        assert parent is not None and parent.span_id == publish_sc.span_id

    def test_round_trip_survives_only_a_traceparent_string(self) -> None:
        # The async hop persists/forwards just the one string; rebuilding from it alone must link.
        tracer, _ = _tracer()
        with tracer.start_as_current_span("origin") as origin:
            tp = current_traceparent()
            origin_trace = origin.get_span_context().trace_id

        assert tp is not None
        rebuilt = context_from_traceparent(tp)
        parent_span = trace.get_current_span(rebuilt)
        assert parent_span.get_span_context().trace_id == origin_trace
