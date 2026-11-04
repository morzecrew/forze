"""Probe routes bound to the runtime's drain state, and the optional pull metrics endpoint."""

from __future__ import annotations

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import asyncio
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse

from forze.application.contracts.deps import DepKey
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.application.execution.background import evaluate_readiness
from forze.base.exceptions import CoreException, exc
from forze.base.logging import Logger

from ._snapshots import MetricsSnapshots, absorb_abandoned_failure, render_gate

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics.export import MetricReader

# ----------------------- #

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

_logger = Logger("fastapi.errors")

# ....................... #


def attach_readiness_route(
    router: APIRouter,
    runtime: ExecutionRuntime | Callable[[], ExecutionRuntime | None],
    *,
    path: str = "/readyz",
    probes: Mapping[str, DepKey[Any]] | None = None,
    probe_timeout: timedelta = timedelta(seconds=2),
) -> APIRouter:
    """Attach a readiness probe reflecting the runtime's scope state, and optionally its deps.

    ``200`` once the scope's lifecycle startup finished and while it is not draining;
    ``503`` otherwise — ``draining`` from the moment a drain is announced
    (:meth:`ExecutionRuntime.begin_draining`, or shutdown closing the drain gate: point your
    load balancer's readiness check here so routing stops before work is refused),
    ``unavailable`` before the scope exists or while its startup is still running. Excluded
    from the OpenAPI schema. The answer comes from
    :func:`~forze.application.execution.background.evaluate_readiness`, which the stdlib
    probe listener uses too, so both answer the same from the same probes.

    *runtime* may also be a zero-argument callable returning the runtime, or ``None`` while
    there is none yet, asked on every request. An app that builds its runtime inside the
    FastAPI lifespan can then attach the probe in its factory, before that runtime exists —
    ``runtime_lifespan(build=...)`` provides one::

        lifespan = runtime_lifespan(build=build_my_runtime)
        attach_readiness_route(router, lifespan.runtime)

    ``None`` answers ``unavailable``, as a runtime with no open scope does; so does a
    callable that raises, whose exception is logged and never served to the probe.

    The scope's state answers "is this process *willing* to take traffic?". *probes* adds the
    other half — "can it *reach* what it needs?" — because an un-drained process whose
    database is gone still answers ``ready`` and still 500s every request the load balancer
    keeps sending it. Name the clients that matter and each is resolved from the deps
    container and asked ``health()``, the signature every forze client port already
    declares; the response then carries a per-dependency breakdown::

        {"status": "degraded",
         "checks": {"postgres": {"ok": true,  "detail": "ok"},
                    "redis":    {"ok": false, "detail": "timed out after 2.0s"}}}

    Any failed check makes the whole probe ``503 degraded``. No *probes* (the default)
    answers from the scope's state alone, with no ``checks`` key at all.

    Not auto-discovered: there is no common base client port to enumerate — every
    integration satisfies ``health()`` structurally — and a sweep of every registered
    dependency would probe things whose reachability readiness does not depend on. The app
    already had to register these keys, so it can name the ones it cannot serve without.

    **Keep the endpoint internal.** ``detail`` is the client's own summary, and a failing
    driver's is its exception text — which routinely names the host, the port and the user
    it failed to authenticate as. That detail is the breakdown's whole value to whoever is
    holding the pager, and it is not something to serve to the public internet: put this
    route where ``/metrics`` goes. (Exceptions *this* route catches are reported by type
    only; there is no summary behind them worth the same risk.) Every request also runs
    every probe, so the probe period is a real query rate against each dependency.

    :param probes: Probe name (the key in ``checks``) to the deps key its client is
        registered under, plain; a key registered only per route, or a client scoped per
        tenant, is reported as such.
    :param probe_timeout: Budget for **each** probe, not for the sweep.

    :raises CoreException: ``configuration`` — *probes* is set and *probe_timeout* is
        not positive.
    """

    if probes and probe_timeout <= timedelta(0):
        # Every probe would report a timeout it never waited for, so the endpoint answers
        # a permanent `degraded` that looks exactly like every dependency being down.
        raise exc.configuration(
            f"attach_readiness_route needs a positive probe_timeout, got {probe_timeout}"
        )

    @router.get(path, include_in_schema=False)
    async def readyz() -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        current: ExecutionRuntime | None
        ctx: ExecutionContext | None

        # By type, not by callability: a runtime (or a stand-in for one) is never asked to
        # provide itself.
        if isinstance(runtime, ExecutionRuntime) or not callable(runtime):
            current = runtime

        else:
            try:
                current = runtime()

            except Exception:
                # The text may name the very settings the app could not load: log it only.
                _logger.exception("Readiness route's runtime provider failed")
                current = None

        try:
            ctx = current.get_context() if current is not None else None

        except CoreException:
            ctx = None

        status, body = await evaluate_readiness(ctx, probes, check_timeout=probe_timeout)

        return JSONResponse(body, status_code=status)

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
    snapshots: MetricsSnapshots | None = None,
) -> APIRouter:
    """Attach a Prometheus **pull** endpoint over an already-registered metric reader.

    *reader* must be a ``PrometheusMetricReader`` that is already registered on the meter
    provider — pass it to ``bootstrap_telemetry(extra_metric_readers=[...])`` or to your own
    ``MeterProvider``. It is checked, not merely documented: hand this route a
    ``PeriodicExportingMetricReader`` and the page renders perfectly and stays empty
    forever, which is the kind of failure nobody notices until an incident.

    **Without** *snapshots*, **single-process deployments only.** Under a pre-fork server
    (gunicorn/uvicorn with workers) the scrape is answered by whichever worker happened to
    accept the connection, so consecutive scrapes sample different processes: per-process
    cumulative counters interleave, ``rate()`` sees phantom resets, and gauges flap between
    workers. Nothing inside this endpoint alone can fix that.

    **With** *snapshots*, the route answers for every worker of the group: each worker's
    :meth:`MetricsSnapshots.writer_step` leaves a snapshot in the shared directory, and
    whichever worker takes the scrape serves the union of the fresh ones, each series
    labelled ``service_instance_id``, plus a ``forze_metrics_snapshot_age_seconds`` sample per
    worker. The route then renders nothing itself; it answers ``503`` when no snapshot can be
    served. Deployments across hosts push over OTLP instead.

    ``prometheus_client`` is deliberately *not* part of the ``observability`` extra — the
    blessed push path has no use for it. Install ``opentelemetry-exporter-prometheus``
    yourself if you want this route.

    The registry is taken **from the reader**, not from a parameter and not from the global
    default: ``PrometheusMetricReader(registry=...)`` is a supported configuration, and a
    route rendering any other registry would serve a perfectly healthy, permanently empty
    page — the same failure the reader check above exists to prevent.

    :param snapshots: Serve the union of every worker's snapshot from this directory
        instead of rendering this process alone.

    :raises CoreException: ``configuration`` — the Prometheus exporter is not installed, or
        *reader* is not a ``PrometheusMetricReader``.
    """

    # One render gate per reader, shared with the snapshot writer and any other plain route
    # on it: two collections in flight on one reader steal each other's data.
    gate = render_gate(reader)

    if snapshots is not None:
        union = snapshots.union
        union_in_flight: asyncio.Future[tuple[int, bytes]] | None = None

        @router.get(path, include_in_schema=False)
        async def metrics_union() -> Response:  # pyright: ignore[reportUnusedFunction]
            nonlocal union_in_flight

            # Off the loop: it reads every worker's file. It renders nothing, so a scrape
            # runs no observable callback and never races the writer for the collector.
            # Concurrent scrapes share one read, as renders are shared, so a flood of
            # scrapes costs one pass over the directory, not one each.
            if union_in_flight is None or union_in_flight.done():
                union_in_flight = asyncio.ensure_future(asyncio.to_thread(union))
                union_in_flight.add_done_callback(absorb_abandoned_failure)

            status, body = await asyncio.shield(union_in_flight)

            return Response(content=body, status_code=status, media_type=PROMETHEUS_CONTENT_TYPE)

        return router

    @router.get(path, include_in_schema=False)
    async def metrics() -> Response:  # pyright: ignore[reportUnusedFunction]
        # Never two renders at once, and never on the loop: see RenderGate. A scrape that
        # finds a render in flight on this reader, its own or the snapshot writer's, waits
        # for that one; an abandoned render still finishes before anything new begins.
        rendered = await gate.run()

        return Response(content=rendered, media_type=PROMETHEUS_CONTENT_TYPE)

    return router
