"""Per-port OpenTelemetry client spans — each configurable port call as a child span.

A hexagonal app talks to every external system through a configurable port, so wrapping the port seam
gives a complete picture of outbound I/O *for free*: under the operation span (from
:func:`~forze.application.execution.observability.instrument_operations`) each document / cache / queue
/ search / http call becomes a child ``CLIENT`` span carrying the port's domain, surface, route, and
phase. This is the production observability counterpart to the dev :class:`~.port_proxy.TracingPortProxy`
(which records the id-only DST buffer); it is opt-in (``DepsRegistry.with_otel_port_spans``) and emits
through the OpenTelemetry tracer, so an uninstrumented app pays nothing.

The wrap sits *inside* the resilience port policy, so a retried call yields one span per attempt (a
rejected call — breaker open, bulkhead full — never reaches the port, so it emits no span). Error
status follows the exception's *kind*: a genuine failure (any non-``CoreException``, or an
infrastructure / internal / configuration ``CoreException`` — a 5xx kind) sets ``ERROR``, while a
client-class domain ``CoreException`` (not-found, conflict, precondition — a 4xx kind the caller may
handle) leaves the span clean, exactly as a 404 does not red an HTTP client span. Streaming
(async-generator) methods are passed through un-spanned — see :meth:`OtelSpanPortProxy` for why.

This module imports ``opentelemetry`` at module scope, so it must stay lazily imported (it is, from
:func:`~forze.application.execution.deps.port_instrumentation.maybe_wrap_otel_spans`, only when the
feature is enabled) — importing it eagerly would pull OTel into an uninstrumented app's import path.
"""

from functools import wraps
from typing import Any, cast

import attrs
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer

from forze.base.exceptions import CoreException, http_status_for_kind

from ..port_proxy_base import PortProxy

# ----------------------- #


@attrs.define(slots=True)
class OtelSpanPortProxy(PortProxy):
    """Wrap a port so each call opens an OpenTelemetry ``CLIENT`` span under the current (op) span."""

    tracer: Tracer
    """The OpenTelemetry tracer the spans are emitted through."""

    domain: str
    """The port's contract family (e.g. ``document``, ``cache``, ``queue``)."""

    surface: str
    """The port's dependency surface (e.g. ``document_command``, ``cache_query``) — the span name stem."""

    route: str | None
    """The port's route / spec name (a higher-cardinality attribute, never the span name)."""

    phase: str | None
    """The port's phase (``query`` / ``command``) when applicable."""

    # ....................... #

    def _attributes(self, op: str) -> dict[str, str]:
        attributes = {
            "forze.port.domain": self.domain,
            "forze.port.surface": self.surface,
            "forze.port.op": op,
        }

        if self.route is not None:
            attributes["forze.port.route"] = self.route

        if self.phase is not None:
            attributes["forze.port.phase"] = self.phase

        return attributes

    # ....................... #

    @staticmethod
    def _on_error(span: Span, exc: BaseException) -> None:
        """Mark a span ``ERROR`` for a genuine failure; leave a *client-class* domain failure clean.

        A client span exists to surface outbound-I/O failures, so an infrastructure/internal/config
        ``CoreException`` (a 5xx kind — a lost connection, a timeout, an adapter bug) sets ``ERROR``,
        as does any non-``CoreException``. A domain ``CoreException`` whose kind maps to a 4xx status
        (not-found, conflict, precondition, validation) is an *expected* result the caller may handle —
        like a 404 on an HTTP client span, it does not paint the span red.
        """

        if isinstance(exc, CoreException) and http_status_for_kind(exc.kind) < 500:
            return

        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR))

    # ....................... #

    def _wrap_async(self, name: str, attr: Any) -> Any:
        @wraps(attr)
        async def traced(*args: Any, **kwargs: Any) -> Any:
            with self.tracer.start_as_current_span(
                f"{self.surface}.{name}",
                kind=SpanKind.CLIENT,
                attributes=self._attributes(name),
                # We classify the exception ourselves (an expected CoreException must not mark the
                # client span an error), so disable OTel's auto record/status-on-exception.
                record_exception=False,
                set_status_on_exception=False,
            ) as span:
                try:
                    return await attr(*args, **kwargs)

                except BaseException as exc:
                    self._on_error(span, exc)
                    raise

        return traced

    # ....................... #

    def _wrap_sync(self, name: str, attr: Any) -> Any:
        @wraps(attr)
        def traced(*args: Any, **kwargs: Any) -> Any:
            with self.tracer.start_as_current_span(
                f"{self.surface}.{name}",
                kind=SpanKind.CLIENT,
                attributes=self._attributes(name),
                # We classify the exception ourselves (an expected CoreException must not mark the
                # client span an error), so disable OTel's auto record/status-on-exception.
                record_exception=False,
                set_status_on_exception=False,
            ) as span:
                try:
                    return attr(*args, **kwargs)

                except BaseException as exc:
                    self._on_error(span, exc)
                    raise

        return traced

    # Async-generator methods (``find_cursor`` / ``find_stream`` / ``consume`` / ``subscribe``) are
    # deliberately **not** spanned (the base passthrough is kept): a single current-context span held
    # open across ``yield`` points leaks and corrupts the ambient span when a consumer breaks early
    # (the idiomatic case for a stream) — and a lone span over a long-lived consume loop is the wrong
    # shape anyway. Per-message spans belong in the consumer, not the port wrap. The dev
    # :class:`~.port_proxy.TracingPortProxy` makes the same choice.


# ....................... #


def wrap_port_otel_spans[T](
    inner: T,
    *,
    tracer: Tracer,
    domain: str,
    surface: str,
    route: str | None,
    phase: str | None,
) -> T:
    """Return *inner* wrapped so each call emits an OpenTelemetry client span."""

    return cast(
        T,
        OtelSpanPortProxy(
            inner=inner,
            tracer=tracer,
            domain=domain,
            surface=surface,
            route=route,
            phase=phase,
        ),
    )
