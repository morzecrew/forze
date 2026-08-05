"""Liveness route and the documented-with-warnings pull metrics route."""

from __future__ import annotations

import pytest
from fastapi import APIRouter, FastAPI
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
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

    def test_refuses_a_reader_that_would_render_an_empty_page_forever(self) -> None:
        router = APIRouter()

        with pytest.raises(CoreException) as caught:
            attach_metrics_route(router, InMemoryMetricReader())

        assert caught.value.kind == ExceptionKind.CONFIGURATION
        assert "PrometheusMetricReader" in str(caught.value)
