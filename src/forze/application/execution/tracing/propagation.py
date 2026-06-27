"""W3C trace-context propagation helpers — carry distributed-trace context across an async gap.

OpenTelemetry auto-instrumentation propagates trace context across *synchronous* transports it patches
(an instrumented HTTP client/server), but it cannot reach Forze's custom outbox→broker→inbox envelope:
the relay runs in a *different* context than the publishing operation, so the consume side has no live
span to parent to. These two helpers bridge that: capture the active span as a ``traceparent`` string
at the point work is initiated (outbox staging, inside the publishing op's span), persist/forward it,
then rebuild an OpenTelemetry context from it on the consume side so the consume span links to its
cause.

Always uses the **W3C** ``traceparent`` format (via :class:`TraceContextTextMapPropagator` directly,
not the global propagator) — the context rides a single string column/header, so it is W3C regardless
of an app's configured global propagator, and a B3/Jaeger-only app still gets messaging trace linkage.
W3C ``tracestate`` (vendor/sampling state) is **not** carried — only the parent identity in
``traceparent`` (whose flags byte still conveys the sampling decision); the synchronous HTTP path,
which uses the global propagator, carries both.

``opentelemetry`` is a core dependency already imported eagerly elsewhere in ``forze.application``
(logging, operation/port instrumentation), so importing it here is free; the inbox integration still
imports these helpers lazily, the outbox enricher at module scope (both fine).
"""

from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

if TYPE_CHECKING:
    from opentelemetry.context import Context

# ----------------------- #

_TRACEPARENT = "traceparent"
_PROPAGATOR = TraceContextTextMapPropagator()


def current_traceparent() -> str | None:
    """The active span's W3C ``traceparent``, or ``None`` when no valid span is active.

    Capture this where the work is *initiated* (e.g. outbox staging, inside the publishing operation's
    span) so it can be replayed later from a different context (the relay's background loop). Cheap: a
    no-op when no span is recording.
    """

    if not trace.get_current_span().get_span_context().is_valid:
        return None

    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier)
    return carrier.get(_TRACEPARENT)


def context_from_traceparent(traceparent: str) -> "Context":
    """An OpenTelemetry context whose remote parent is *traceparent*.

    Attach it (``opentelemetry.context.attach`` / detach in a ``finally``) around consume-side work so
    the work's spans become children of the originating (publish-side) span — stitching the async flow
    into one distributed trace.
    """

    return _PROPAGATOR.extract({_TRACEPARENT: traceparent})
