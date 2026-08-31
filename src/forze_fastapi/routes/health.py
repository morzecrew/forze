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
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze.base.logging import Logger

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics.export import MetricReader
    from prometheus_client import CollectorRegistry

# ----------------------- #

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

_logger = Logger("fastapi.probes")

# ....................... #


def attach_readiness_route(
    router: APIRouter,
    runtime: ExecutionRuntime,
    *,
    path: str = "/readyz",
    probes: Mapping[str, DepKey[Any]] | None = None,
    probe_timeout: timedelta = timedelta(seconds=2),
) -> APIRouter:
    """Attach a readiness probe reflecting the runtime's scope state, and optionally its deps.

    ``200`` while a scope is active and not draining; ``503`` otherwise —
    ``draining`` once shutdown flipped the drain gate (point your load
    balancer's readiness check here so routing stops before the drain window),
    ``unavailable`` before the scope exists. Excluded from the OpenAPI schema.

    The drain gate answers "is this process *willing* to take traffic?". *probes* adds the
    other half — "can it *reach* what it needs?" — because an un-drained process whose
    database is gone still answers ``ready`` and still 500s every request the load balancer
    keeps sending it. Name the clients that matter and each is resolved from the deps
    container and asked ``health()``, the signature every forze client port already
    declares; the response then carries a per-dependency breakdown::

        {"status": "degraded",
         "checks": {"postgres": {"ok": true,  "detail": "ok"},
                    "redis":    {"ok": false, "detail": "timed out after 2.0s"}}}

    Any failed check makes the whole probe ``503 degraded``. No *probes* (the default)
    keeps the drain-gate-only behaviour, with no ``checks`` key at all.

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
        registered under.
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
        if not runtime.ready:
            status = "draining" if runtime.draining else "unavailable"

            return JSONResponse({"status": status}, status_code=503)

        if not probes:
            return JSONResponse({"status": "ready"})

        try:
            checks = await _probe_dependencies(runtime, probes, probe_timeout)

        except CoreException:
            # The scope was reset between the readiness check above and the deps read.
            # Narrow, but a probe must answer rather than 500 on the way down.
            return JSONResponse({"status": "unavailable"}, status_code=503)

        healthy = all(check["ok"] for check in checks.values())

        return JSONResponse(
            {"status": "ready" if healthy else "degraded", "checks": checks},
            status_code=200 if healthy else 503,
        )

    return router


# ....................... #


async def _probe_dependencies(
    runtime: ExecutionRuntime,
    probes: Mapping[str, DepKey[Any]],
    timeout: timedelta,
) -> dict[str, dict[str, Any]]:
    """Ask every named client ``health()`` concurrently, one timeout each."""

    seconds = timeout.total_seconds()
    deps = runtime.get_context().deps

    async def probe(name: str, key: DepKey[Any]) -> tuple[str, dict[str, Any]]:
        # Every failure below becomes a *reported* one. A probe that raises turns readiness
        # into a 500 — indistinguishable from an unrelated bug — and takes the breakdown
        # with it, in exactly the situation the breakdown exists for.
        try:
            if not deps.exists(key):
                return name, {"ok": False, "detail": "not registered"}

            # Per probe, deliberately, not one deadline around the sweep. The two look
            # equivalent — the probes run concurrently, so the wall clock is the same — but
            # a sweep-wide timeout cancels *every* probe when one dependency's driver
            # retries past the deadline, and answers 503 with `checks: {}`. Per probe, a
            # hanging dependency is one failed check with a name on it.
            budget = asyncio.timeout(seconds)

            try:
                async with budget:
                    detail, ok = await deps.provide(key).health()

            except TimeoutError:
                # Only *our* budget is reported as ours. A driver with its own deadline
                # raises `TimeoutError` too, and calling that "timed out after 2.0s" names
                # a deadline that never elapsed — it goes to the handler below instead,
                # which reports it by type like any other client failure.
                if not budget.expired():
                    raise

                return name, {"ok": False, "detail": f"timed out after {seconds}s"}

            return name, {"ok": bool(ok), "detail": str(detail)}

        except Exception as error:
            _logger.warning("Readiness probe %s failed", name, exc_info=True)

            # The type, not the message: a driver's connection error routinely carries the
            # DSN, and this body is served to anything that can reach the probe.
            return name, {"ok": False, "detail": type(error).__name__}

    return dict(await asyncio.gather(*(probe(name, key) for name, key in probes.items())))


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

    The registry is taken **from the reader**, not from a parameter and not from the global
    default: ``PrometheusMetricReader(registry=...)`` is a supported configuration, and a
    route rendering any other registry would serve a perfectly healthy, permanently empty
    page — the same failure the reader check above exists to prevent.

    :raises CoreException: ``configuration`` — the Prometheus exporter is not installed, or
        *reader* is not a ``PrometheusMetricReader``.
    """

    generate_latest, registry = _prometheus_renderer(reader)
    scrape = asyncio.Lock()
    in_flight: asyncio.Future[bytes] | None = None

    @router.get(path, include_in_schema=False)
    async def metrics() -> Response:  # pyright: ignore[reportUnusedFunction]
        nonlocal in_flight

        # Never two renders at once. The exporter's collector drains a plain
        # unsynchronized deque: two in flight each trigger a collection, then race to pop
        # each other's data, and one of them answers 200 with an empty or partial body.
        # On the event loop this was serialized by accident; off it, on purpose.
        #
        # A lock alone would not survive cancellation, which is exactly when this happens:
        # Prometheus times out a slow scrape, the client disconnects, the handler is
        # cancelled — and the *thread* carries on, because nothing can cancel one. The
        # lock would release and the retry would render straight into the running
        # collection. So the render is a shared future rather than a critical section: a
        # scrape that finds one in flight waits for that one instead of starting another,
        # and an abandoned render still finishes before anything new begins.
        async with scrape:
            if in_flight is None or in_flight.done():
                # Off the loop, because rendering *is* collection: it runs every
                # observable callback in the process — pool stats, keyring stats, L1
                # stats, bulkhead depths — and on the loop each scrape would stall every
                # other coroutine, including the liveness probe.
                in_flight = asyncio.ensure_future(asyncio.to_thread(generate_latest, registry))
                in_flight.add_done_callback(_absorb_abandoned_failure)

            pending = in_flight

        rendered = await asyncio.shield(pending)

        return Response(content=rendered, media_type=PROMETHEUS_CONTENT_TYPE)

    return router


# ....................... #


def _absorb_abandoned_failure(render: asyncio.Future[bytes]) -> None:
    """Retrieve a failed render's exception so asyncio does not report it as a leak.

    A render outlives the scrape that started it — the client disconnects, the handler is
    cancelled, the thread keeps going. If that render then raises (an observable callback
    reading a half-disposed client, say) nobody is left to receive it, and asyncio logs
    ``Task exception was never retrieved`` with a full traceback at ERROR. That fires
    precisely during an incident, where it reads as a framework fault rather than as the
    scrape timeout it actually is.

    Retrieving here only clears the leak warning: a scrape still awaiting this render
    receives the exception exactly as before and answers 500 for it.
    """

    if not render.cancelled():
        render.exception()


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

    # The reader keeps the registry it registered its collector into — the global one by
    # default, a caller-supplied one when ``PrometheusMetricReader(registry=...)`` was
    # used. Reading it back is what keeps the route and the reader from ever disagreeing;
    # the exporter exposes no public accessor, and the fallback covers older releases that
    # predate the constructor argument.
    return generate_latest, getattr(reader, "_registry", REGISTRY)
