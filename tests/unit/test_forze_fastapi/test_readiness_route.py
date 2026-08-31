"""Readiness route reflects the runtime's scope/drain state."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, cast

import attrs
import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.contracts.deps import DepKey, Deps
from forze.application.execution import DepsRegistry
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, exc
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


# ----------------------- #


@attrs.define(slots=True)
class _FakeClient:
    """Minimal stand-in for the ``health()`` every forze client port declares."""

    detail: str = "ok"
    ok: bool = True
    hang: bool = False
    raises: Exception | None = None

    calls: int = attrs.field(default=0, init=False)

    async def health(self) -> tuple[str, bool]:
        self.calls += 1

        if self.raises is not None:
            raise self.raises

        if self.hang:
            await asyncio.sleep(3600)

        return self.detail, self.ok


def _probed(
    clients: dict[str, _FakeClient],
    *,
    extra: dict[str, DepKey[Any]] | None = None,
    timeout: timedelta = timedelta(seconds=5),
) -> tuple[ExecutionRuntime, TestClient]:
    keys = {name: DepKey[Any](name) for name in clients}
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(
            Deps.plain({keys[name]: client for name, client in clients.items()})
        ).freeze()
    )

    router = APIRouter()
    attach_readiness_route(
        router,
        runtime,
        probes={**keys, **(extra or {})},
        probe_timeout=timeout,
    )
    app = FastAPI()
    app.include_router(router)

    return runtime, TestClient(app)


class TestReadinessProbes:
    @pytest.mark.asyncio
    async def test_all_healthy(self) -> None:
        runtime, client = _probed({"postgres": _FakeClient()})

        async with runtime.scope():
            response = client.get("/readyz")

        assert response.status_code == 200
        assert response.json() == {
            "status": "ready",
            "checks": {"postgres": {"ok": True, "detail": "ok"}},
        }

    @pytest.mark.asyncio
    async def test_one_unhealthy_degrades_the_whole_probe(self) -> None:
        runtime, client = _probed(
            {
                "postgres": _FakeClient(),
                "redis": _FakeClient(detail="connection refused", ok=False),
            }
        )

        async with runtime.scope():
            response = client.get("/readyz")

        assert response.status_code == 503
        assert response.json() == {
            "status": "degraded",
            "checks": {
                "postgres": {"ok": True, "detail": "ok"},
                "redis": {"ok": False, "detail": "connection refused"},
            },
        }

    @pytest.mark.asyncio
    async def test_a_hanging_dependency_leaves_the_others_reported(self) -> None:
        # The regression a sweep-wide timeout causes: one dependency retrying past the
        # deadline cancels every probe, and the 503 comes back with `checks: {}` — no
        # breakdown, in exactly the situation the breakdown exists for.
        runtime, client = _probed(
            {"postgres": _FakeClient(), "redis": _FakeClient(hang=True)},
            timeout=timedelta(seconds=0.05),
        )

        async with runtime.scope():
            response = client.get("/readyz")

        assert response.status_code == 503
        assert response.json() == {
            "status": "degraded",
            "checks": {
                "postgres": {"ok": True, "detail": "ok"},
                "redis": {"ok": False, "detail": "timed out after 0.05s"},
            },
        }

    @pytest.mark.asyncio
    async def test_a_raising_probe_is_reported_not_propagated(self) -> None:
        # An escaping exception would answer 500 — indistinguishable from an unrelated
        # bug — and lose the breakdown. The type, not the message: driver errors carry
        # the DSN, and this body is served to anything that can reach the probe.
        runtime, client = _probed(
            {"postgres": _FakeClient(raises=ValueError("dsn=postgres://user:hunter2@db"))}
        )

        async with runtime.scope():
            response = client.get("/readyz")

        assert response.status_code == 503
        assert response.json() == {
            "status": "degraded",
            "checks": {"postgres": {"ok": False, "detail": "ValueError"}},
        }

    @pytest.mark.asyncio
    async def test_unregistered_key_names_itself(self) -> None:
        runtime, client = _probed(
            {"postgres": _FakeClient()},
            extra={"redis": DepKey[Any]("never_registered")},
        )

        async with runtime.scope():
            response = client.get("/readyz")

        assert response.status_code == 503
        assert response.json()["checks"]["redis"] == {"ok": False, "detail": "not registered"}

    @pytest.mark.asyncio
    async def test_draining_answers_before_any_probe_runs(self) -> None:
        postgres = _FakeClient()
        runtime, client = _probed({"postgres": postgres})

        async with runtime.scope():
            await runtime.get_context().drain_gate.drain(0.0)

            response = client.get("/readyz")

        assert response.status_code == 503
        assert response.json() == {"status": "draining"}
        assert postgres.calls == 0

    def test_outside_scope_answers_before_any_probe_runs(self) -> None:
        postgres = _FakeClient()
        _, client = _probed({"postgres": postgres})

        response = client.get("/readyz")

        assert response.status_code == 503
        assert response.json() == {"status": "unavailable"}
        assert postgres.calls == 0

    @pytest.mark.asyncio
    async def test_empty_probes_keeps_the_drain_gate_only_behaviour(self) -> None:
        runtime = ExecutionRuntime()
        router = APIRouter()
        attach_readiness_route(router, runtime, probes={})
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        async with runtime.scope():
            response = client.get("/readyz")

        assert response.status_code == 200
        assert response.json() == {"status": "ready"}

    def test_a_non_positive_probe_timeout_is_refused_at_attach_time(self) -> None:
        # Every probe would report a timeout it never waited for: a permanent `degraded`
        # indistinguishable from every dependency being down.
        with pytest.raises(CoreException) as error:
            attach_readiness_route(
                APIRouter(),
                ExecutionRuntime(),
                probes={"postgres": DepKey[Any]("postgres_client")},
                probe_timeout=timedelta(0),
            )

        assert "positive probe_timeout" in str(error.value)

    def test_a_scope_lost_mid_probe_answers_unavailable(self) -> None:
        # Narrow race: the scope is reset between the readiness check and the deps read.
        # A probe must answer on the way down rather than 500.
        @attrs.define(slots=True)
        class _TornDownRuntime:
            ready: bool = True
            draining: bool = False

            def get_context(self) -> Any:
                raise exc.internal("no context")

        router = APIRouter()
        attach_readiness_route(
            router,
            cast(ExecutionRuntime, _TornDownRuntime()),
            probes={"postgres": DepKey[Any]("postgres_client")},
        )
        app = FastAPI()
        app.include_router(router)

        response = TestClient(app).get("/readyz")

        assert response.status_code == 503
        assert response.json() == {"status": "unavailable"}
