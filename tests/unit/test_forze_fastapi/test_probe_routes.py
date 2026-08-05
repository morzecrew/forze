"""Liveness route and the documented-with-warnings pull metrics route."""

from __future__ import annotations

import asyncio
import gc
import sys
import threading
import time
from contextlib import suppress
from typing import Any

import pytest
from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from prometheus_client import CollectorRegistry
from starlette.testclient import TestClient

from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_fastapi.routes import (
    attach_liveness_route,
    attach_metrics_route,
    attach_readiness_route,
)

# ----------------------- #


def _client(router: APIRouter) -> TestClient:
    app = FastAPI()
    app.include_router(router)

    return TestClient(app)


# ----------------------- #


class TestLivenessRoute:
    def test_alive_without_a_scope(self) -> None:
        router = APIRouter()
        attach_liveness_route(router)

        response = _client(router).get("/livez")

        assert response.status_code == 200
        assert response.json() == {"status": "alive"}

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_draining_pod_is_alive_but_not_ready(self) -> None:
        """The whole reason liveness is a separate endpoint.

        Served from one path, a draining pod looks dead and gets killed mid-drain.
        """

        runtime = ExecutionRuntime()
        router = APIRouter()
        attach_liveness_route(router)
        attach_readiness_route(router, runtime)
        client = _client(router)

        async with runtime.scope():
            await runtime.get_context().drain_gate.drain(0.0)

            assert client.get("/readyz").status_code == 503
            assert client.get("/livez").status_code == 200

    # ....................... #

    def test_path_is_configurable_and_stays_out_of_the_schema(self) -> None:
        router = APIRouter()
        attach_liveness_route(router, path="/healthz")
        app = FastAPI()
        app.include_router(router)

        assert TestClient(app).get("/healthz").status_code == 200
        assert "/healthz" not in app.openapi()["paths"]


# ----------------------- #


class TestMetricsRoute:
    def test_renders_the_metrics_the_reader_collected(self) -> None:
        # ``PrometheusMetricReader`` registers its collector into prometheus_client's
        # global registry on construction, and the route renders exactly that — there is
        # no seam between them to point somewhere else, on purpose.
        reader = PrometheusMetricReader()
        provider = MeterProvider(metric_readers=[reader])

        try:
            provider.get_meter("forze").create_counter("forze.test.pull").add(2)

            router = APIRouter()
            attach_metrics_route(router, reader)

            response = _client(router).get("/metrics")

            assert response.status_code == 200
            assert "forze_test_pull" in response.text

        finally:
            # Unregisters the collector from the global registry, which is also what keeps
            # the provider's own atexit hook from failing on an already-clean registry.
            provider.shutdown()

    # ....................... #

    def test_a_reader_with_its_own_registry_still_serves_its_metrics(self) -> None:
        """``PrometheusMetricReader(registry=...)`` is a supported configuration.

        The route takes the registry *from the reader*, so the two cannot disagree.
        Rendering the global default instead would answer 200 with an empty body for
        every reader that was pointed somewhere else.
        """

        registry = CollectorRegistry()
        reader = PrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])

        try:
            provider.get_meter("forze").create_counter("forze.test.scoped").add(3)

            router = APIRouter()
            attach_metrics_route(router, reader)

            response = _client(router).get("/metrics")

            assert response.status_code == 200
            assert "forze_test_scoped" in response.text

        finally:
            provider.shutdown()

    # ....................... #

    @pytest.mark.asyncio
    async def test_concurrent_scrapes_never_render_at_the_same_time(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The exporter's collector drains an unsynchronized deque.

        Two renders in flight each trigger a collection and then race to pop each other's
        data — one of them answers ``200`` with an empty body. Rendering used to be
        serialized by accident, because it happened on the event loop; moving it to a
        thread means the endpoint has to serialize it on purpose.
        """

        import prometheus_client

        live = 0
        overlapped = False

        def _slow_render(_registry: object) -> bytes:
            nonlocal live, overlapped

            live += 1
            overlapped = overlapped or live > 1
            time.sleep(0.05)
            live -= 1

            return b"# rendered\n"

        # Patched before the route is attached: the renderer is resolved at attach time.
        monkeypatch.setattr(prometheus_client, "generate_latest", _slow_render)

        registry = CollectorRegistry()
        reader = PrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])

        try:
            router = APIRouter()
            attach_metrics_route(router, reader)
            app = FastAPI()
            app.include_router(router)

            transport = ASGITransport(app=app)

            async with AsyncClient(transport=transport, base_url="http://probe") as client:
                responses = await asyncio.gather(*(client.get("/metrics") for _ in range(4)))

            assert all(response.status_code == 200 for response in responses)
            assert not overlapped, "two scrapes rendered the same registry concurrently"

        finally:
            provider.shutdown()

    # ....................... #

    @pytest.mark.asyncio
    async def test_an_abandoned_scrape_still_blocks_the_next_one(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Cancellation is exactly when a second scrape shows up.

        Prometheus times out a slow scrape, the client disconnects, the handler is
        cancelled — and the render thread carries on, because nothing can cancel one. A
        plain lock would release here and let the retry render straight into the
        collection still in progress, which is the race the serialization exists to stop.
        """

        import prometheus_client

        live = 0
        overlapped = False
        # A threading.Event, because it is set from the render thread: asyncio's is
        # not safe to set from outside the loop.
        started = threading.Event()

        def _slow_render(_registry: object) -> bytes:
            nonlocal live, overlapped

            live += 1
            overlapped = overlapped or live > 1
            started.set()
            time.sleep(0.3)
            live -= 1

            return b"# rendered\n"

        monkeypatch.setattr(prometheus_client, "generate_latest", _slow_render)

        registry = CollectorRegistry()
        reader = PrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])

        try:
            router = APIRouter()
            attach_metrics_route(router, reader)
            app = FastAPI()
            app.include_router(router)

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://probe"
            ) as client:
                abandoned = asyncio.ensure_future(client.get("/metrics"))

                assert await asyncio.to_thread(started.wait, 5.0)
                abandoned.cancel()

                with suppress(asyncio.CancelledError):
                    await abandoned

                # The retry arrives while the abandoned render is still running.
                response = await client.get("/metrics")

            assert response.status_code == 200
            assert not overlapped, "a cancelled scrape let the next one render concurrently"

        finally:
            provider.shutdown()

    # ....................... #

    @pytest.mark.asyncio
    async def test_an_abandoned_render_that_fails_is_not_reported_as_a_leak(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The two failures compound: a scrape times out, then the render raises.

        Nobody is left to receive that exception, so asyncio logs "Task exception was
        never retrieved" with a traceback at ERROR — during an incident, reading as a
        framework fault rather than as the scrape timeout it is.
        """

        import prometheus_client

        started = threading.Event()
        renders = 0

        def _render(_registry: object) -> bytes:
            nonlocal renders

            renders += 1
            started.set()

            if renders == 1:
                time.sleep(0.2)

                raise RuntimeError("collector callback exploded")

            return b"# rendered\n"

        monkeypatch.setattr(prometheus_client, "generate_latest", _render)

        reported: list[dict[str, Any]] = []
        asyncio.get_running_loop().set_exception_handler(
            lambda _loop, context: reported.append(context)
        )

        registry = CollectorRegistry()
        reader = PrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])

        try:
            router = APIRouter()
            attach_metrics_route(router, reader)
            app = FastAPI()
            app.include_router(router)

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://probe"
            ) as client:
                abandoned = asyncio.ensure_future(client.get("/metrics"))

                assert await asyncio.to_thread(started.wait, 5.0)
                abandoned.cancel()

                with suppress(asyncio.CancelledError):
                    await abandoned

                await asyncio.sleep(0.4)  # the orphaned render fails here

                # A later scrape replaces the route's reference to the failed render,
                # which is what finally makes it collectable — and, unhandled, is when
                # asyncio would report it.
                assert (await client.get("/metrics")).status_code == 200

            gc.collect()
            await asyncio.sleep(0)

            leaked = [c for c in reported if "never retrieved" in str(c.get("message", ""))]

            assert not leaked, f"asyncio reported the abandoned render as a leak: {leaked}"

        finally:
            asyncio.get_running_loop().set_exception_handler(None)
            provider.shutdown()

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_render_failure_still_reaches_a_waiting_scrape(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Draining the exception must not swallow it for whoever is still listening."""

        import prometheus_client

        def _failing_render(_registry: object) -> bytes:
            raise RuntimeError("collector callback exploded")

        monkeypatch.setattr(prometheus_client, "generate_latest", _failing_render)

        registry = CollectorRegistry()
        reader = PrometheusMetricReader(registry=registry)
        provider = MeterProvider(metric_readers=[reader])

        try:
            router = APIRouter()
            attach_metrics_route(router, reader)
            app = FastAPI()
            app.include_router(router)

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://probe"
            ) as client:
                with pytest.raises(RuntimeError, match="exploded"):
                    await client.get("/metrics")

        finally:
            provider.shutdown()

    # ....................... #

    def test_refuses_a_reader_that_would_render_an_empty_page_forever(self) -> None:
        router = APIRouter()

        with pytest.raises(CoreException) as caught:
            attach_metrics_route(router, InMemoryMetricReader())

        assert caught.value.kind == ExceptionKind.CONFIGURATION
        assert "PrometheusMetricReader" in str(caught.value)

    # ....................... #

    def test_a_missing_prometheus_exporter_names_what_to_install(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The package is deliberately outside the ``observability`` extra.

        Someone reaching for this route has not installed it yet by definition, so the
        error has to say which package — and say that the push path does not need it.
        """

        # ``None`` in sys.modules is the documented way to make an import raise.
        monkeypatch.setitem(sys.modules, "opentelemetry.exporter.prometheus", None)

        router = APIRouter()

        with pytest.raises(CoreException) as caught:
            attach_metrics_route(router, InMemoryMetricReader())

        assert caught.value.kind == ExceptionKind.CONFIGURATION
        assert "opentelemetry-exporter-prometheus" in str(caught.value)
