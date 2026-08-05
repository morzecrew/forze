"""Probe routes bound to the runtime's drain state, and the optional pull metrics endpoint."""

from __future__ import annotations

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING

from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse

from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics.export import MetricReader
    from prometheus_client import CollectorRegistry

# ----------------------- #

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# ....................... #


def attach_readiness_route(
    router: APIRouter,
    runtime: ExecutionRuntime,
    *,
    path: str = "/readyz",
) -> APIRouter:
    """Attach a readiness probe reflecting the runtime's scope state.

    ``200`` while a scope is active and not draining; ``503`` otherwise —
    ``draining`` once shutdown flipped the drain gate (point your load
    balancer's readiness check here so routing stops before the drain window),
    ``unavailable`` before the scope exists. Excluded from the OpenAPI schema.
    """

    @router.get(path, include_in_schema=False)
    async def readyz() -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        if runtime.ready:
            return JSONResponse({"status": "ready"})

        status = "draining" if runtime.draining else "unavailable"

        return JSONResponse({"status": status}, status_code=503)

    return router


# ....................... #


def attach_liveness_route(
    router: APIRouter,
    *,
    path: str = "/livez",
) -> APIRouter:
    """Attach a liveness probe: ``200 {"status": "alive"}``, unconditionally.

    Reaching the handler *is* the check — it answers only if the event loop is still
    scheduling work, which is the one question a liveness probe should ask.

    Deliberately blind to drain, and that is the whole point of separating it from
    :func:`attach_readiness_route`: a draining pod is alive but not ready. Wire liveness
    here and readiness at ``/readyz``, and a slow drain stops receiving traffic instead of
    being killed and restarted mid-drain — which is what happens when one endpoint is
    forced to answer both questions.

    Takes no runtime: there is no state it could consult that would make the answer more
    true. Excluded from the OpenAPI schema.
    """

    @router.get(path, include_in_schema=False)
    async def livez() -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        return JSONResponse({"status": "alive"})

    return router


# ....................... #


def attach_metrics_route(
    router: APIRouter,
    reader: MetricReader,
    *,
    path: str = "/metrics",
) -> APIRouter:
    """Attach a Prometheus **pull** endpoint over an already-registered metric reader.

    *reader* must be a ``PrometheusMetricReader`` that is already registered on the meter
    provider — pass it to ``bootstrap_telemetry(extra_metric_readers=[...])`` or to your own
    ``MeterProvider``. It is checked, not merely documented: hand this route a
    ``PeriodicExportingMetricReader`` and the page renders perfectly and stays empty
    forever, which is the kind of failure nobody notices until an incident.

    **Single-process deployments only.** Under a pre-fork server (gunicorn/uvicorn with
    workers) the scrape is answered by whichever worker happened to accept the connection,
    so consecutive scrapes sample different processes: per-process cumulative counters
    interleave, ``rate()`` sees phantom resets, and gauges flap between workers. Nothing
    inside this endpoint can fix that. Multi-worker deployments push over OTLP instead,
    where each process stays a distinct series by ``service.instance.id``.

    ``prometheus_client`` is deliberately *not* part of the ``observability`` extra — the
    blessed push path has no use for it. Install ``opentelemetry-exporter-prometheus``
    yourself if you want this route.

    The registry is not configurable, deliberately: ``PrometheusMetricReader`` registers
    its collector into ``prometheus_client``'s global one at construction, so rendering
    anything else would serve a perfectly healthy, permanently empty page — the exact
    failure the reader check above exists to prevent.

    :raises CoreException: ``configuration`` — the Prometheus exporter is not installed, or
        *reader* is not a ``PrometheusMetricReader``.
    """

    generate_latest, registry = _prometheus_renderer(reader)

    @router.get(path, include_in_schema=False)
    async def metrics() -> Response:  # pyright: ignore[reportUnusedFunction]
        # Off the event loop: rendering the registry is what drives collection, and that
        # runs every observable callback in the process — pool stats, keyring stats, L1
        # stats, bulkhead depths. On the loop, each scrape would stall every other
        # coroutine for as long as the whole collection takes.
        rendered = await asyncio.to_thread(generate_latest, registry)

        return Response(content=rendered, media_type=PROMETHEUS_CONTENT_TYPE)

    return router


# ....................... #


def _prometheus_renderer(
    reader: MetricReader,
) -> tuple[Callable[[CollectorRegistry], bytes], CollectorRegistry]:
    try:
        from opentelemetry.exporter.prometheus import PrometheusMetricReader
        from prometheus_client import REGISTRY, generate_latest

    except ImportError as error:
        raise exc.configuration(
            "The pull metrics route needs the Prometheus exporter "
            "(pip install opentelemetry-exporter-prometheus); the blessed OTLP push path "
            "does not require it"
        ) from error

    if not isinstance(reader, PrometheusMetricReader):
        raise exc.configuration(
            f"attach_metrics_route needs a PrometheusMetricReader, got "
            f"{type(reader).__name__}: any other reader exports elsewhere and leaves this "
            f"endpoint permanently empty"
        )

    return generate_latest, REGISTRY
