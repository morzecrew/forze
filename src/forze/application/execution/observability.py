"""OpenTelemetry instrumentation for Forze operations.

``instrument_operations`` wraps every operation in a registry with a span + metrics, using
the existing per-operation ``wrap`` middleware seam — no engine changes. OpenTelemetry is a
core dependency (the logging layer already uses it), so this is built in, not an optional
extra. Emits via the global OpenTelemetry providers — configure the SDK + exporter in your
app.
"""

from __future__ import annotations

from time import perf_counter
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics, trace
from opentelemetry.trace import Status, StatusCode

from forze.application.contracts.execution import (
    Middleware,
    MiddlewareFactory,
    MiddlewareStep,
)

from .operations.registry import OperationRegistry

if TYPE_CHECKING:
    from opentelemetry.metrics import Counter, Histogram, Meter
    from opentelemetry.trace import Tracer

    from .context import ExecutionContext

# ----------------------- #

_TELEMETRY_STEP_ID = "otel.telemetry"

_TELEMETRY_PRIORITY = -1_000_000
"""Lowest priority → outermost wrap, so the span measures the whole operation."""

OPERATIONS_COUNTER = "forze.operations"
DURATION_HISTOGRAM = "forze.operation.duration"


def instrument_operations(
    registry: OperationRegistry,
    *,
    tracer: Tracer | None = None,
    meter: Meter | None = None,
) -> OperationRegistry:
    """Instrument every operation in *registry* with an OpenTelemetry span + metrics.

    Returns a new registry (call before ``.freeze()``). Each operation runs inside a span
    named by its key (attributes: kind, execution/correlation/causation ids, tenant,
    principal), and records a request counter (``forze.operations``) and a duration
    histogram (``forze.operation.duration``, ms), labelled by operation / kind / outcome.

    Emits via the global OTel providers unless *tracer* / *meter* are supplied — configure
    the OTel SDK + exporter in your app. Pair with ``configure_logging(otel_config=...)`` to
    correlate logs to the active span.
    """

    tracer = tracer or trace.get_tracer("forze")
    meter = meter or metrics.get_meter("forze")

    counter = meter.create_counter(
        OPERATIONS_COUNTER,
        unit="1",
        description="Count of Forze operations executed.",
    )
    duration = meter.create_histogram(
        DURATION_HISTOGRAM,
        unit="ms",
        description="Duration of Forze operations in milliseconds.",
    )

    for op in registry.operation_keys():
        step = MiddlewareStep(
            id=_TELEMETRY_STEP_ID,
            priority=_TELEMETRY_PRIORITY,
            factory=_telemetry_factory(str(op), tracer, counter, duration),
        )
        registry = registry.bind(op).bind_outer().wrap(step).finish(deep=True)

    return registry


# ....................... #


def _telemetry_factory(
    op_name: str,
    tracer: Tracer,
    counter: Counter,
    duration: Histogram,
) -> MiddlewareFactory:
    def factory(ctx: ExecutionContext) -> Middleware[Any, Any]:
        async def middleware(
            next: Any,  # noqa: A002 — matches the Middleware protocol parameter name
            args: Any,
        ) -> Any:
            kind = "query" if ctx.inv_ctx.is_read_only() else "command"
            labels = {"forze.operation": op_name, "forze.operation.kind": kind}

            start = perf_counter()
            outcome = "success"

            with tracer.start_as_current_span(
                op_name, attributes=_span_attributes(ctx, op_name, kind)
            ) as span:
                try:
                    return await next(args)

                except BaseException as exc:
                    outcome = "error"
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR))
                    raise

                finally:
                    elapsed_ms = (perf_counter() - start) * 1000.0
                    out = {**labels, "forze.outcome": outcome}
                    counter.add(1, out)
                    duration.record(elapsed_ms, out)

        return middleware

    return factory


# ....................... #


def _span_attributes(
    ctx: ExecutionContext, op_name: str, kind: str
) -> dict[str, str]:
    attributes: dict[str, str] = {
        "forze.operation": op_name,
        "forze.operation.kind": kind,
    }

    metadata = ctx.inv_ctx.get_metadata()
    if metadata is not None:
        attributes["forze.execution_id"] = str(metadata.execution_id)
        attributes["forze.correlation_id"] = str(metadata.correlation_id)
        if metadata.causation_id is not None:
            attributes["forze.causation_id"] = str(metadata.causation_id)

    tenant = ctx.inv_ctx.get_tenant()
    if tenant is not None:
        attributes["forze.tenant_id"] = str(tenant.tenant_id)

    authn = ctx.inv_ctx.get_authn()
    if authn is not None:
        attributes["forze.principal_id"] = str(authn.principal_id)

    return attributes
