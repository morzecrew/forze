"""Readiness route reflects the runtime's scope/drain state."""

from __future__ import annotations

import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.execution.runtime import ExecutionRuntime
from forze_fastapi.routes import attach_readiness_route

# ----------------------- #


def _client(runtime: ExecutionRuntime) -> TestClient:
    router = APIRouter()
    attach_readiness_route(router, runtime)
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestReadinessRoute:
    def test_unavailable_outside_scope(self) -> None:
        response = _client(ExecutionRuntime()).get("/readyz")

        assert response.status_code == 503
        assert response.json() == {"status": "unavailable"}

    @pytest.mark.asyncio
    async def test_ready_then_draining(self) -> None:
        runtime = ExecutionRuntime()
        client = _client(runtime)

        async with runtime.scope():
            assert client.get("/readyz").json() == {"status": "ready"}

            await runtime.get_context().drain_gate.drain(0.0)

            response = client.get("/readyz")
            assert response.status_code == 503
            assert response.json() == {"status": "draining"}
