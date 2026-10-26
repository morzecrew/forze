"""Readiness route reflects the runtime's scope/drain state."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any, cast
from unittest.mock import MagicMock

import attrs
import httpx
import pytest
import structlog.testing
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.contracts.deps import DepKey, Deps
from forze.application.contracts.tenancy.routed_client_base import RoutedTenantClientBase
from forze.application.execution import DepsRegistry, build_runtime
from forze.application.execution.background import probe_listener_step
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze_fastapi.routes import attach_readiness_route
from tests.support.ports import free_port

# ----------------------- #


def _client(runtime: ExecutionRuntime | Callable[[], ExecutionRuntime | None]) -> TestClient:
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


class TestReadinessRuntimeProvider:
    """The route attached before the runtime exists, as an app factory does it."""

    def test_no_runtime_yet_answers_unavailable(self) -> None:
        response = _client(lambda: None).get("/readyz")

        assert response.status_code == 503
        assert response.json() == {"status": "unavailable"}

    @pytest.mark.asyncio
    async def test_asked_on_every_request(self) -> None:
        holder: dict[str, ExecutionRuntime] = {}
        client = _client(lambda: holder.get("runtime"))

        assert client.get("/readyz").json() == {"status": "unavailable"}

        runtime = holder["runtime"] = ExecutionRuntime()

        # Built, but its scope not open yet.
        assert client.get("/readyz").json() == {"status": "unavailable"}

        async with runtime.scope():
            response = client.get("/readyz")
            assert response.status_code == 200
            assert response.json() == {"status": "ready"}

            runtime.begin_draining()

            response = client.get("/readyz")
            assert response.status_code == 503
            assert response.json() == {"status": "draining"}

    def test_a_raising_provider_answers_unavailable_and_keeps_its_message_out(self) -> None:
        # The provider's failure is usually the app's own settings or secrets failing to
        # load, and its text says which: logged for the operator, not served to the probe.
        def provider() -> ExecutionRuntime:
            raise RuntimeError("dsn=postgres://user:fixture-7chr@db")

        with structlog.testing.capture_logs() as logs:
            response = _client(provider).get("/readyz")

        assert response.status_code == 503
        assert response.json() == {"status": "unavailable"}
        assert "fixture-7chr" not in response.text
        assert any("runtime provider failed" in str(one.get("event")) for one in logs)


# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class _RoutedClient(RoutedTenantClientBase[Any]):
    """Reaches its backend per tenant, as the routed Redis and Postgres clients do."""

    async def initialize_client(self, tenant_id: Any, creds: Any) -> Any:  # pragma: no cover
        raise NotImplementedError

    async def health(self) -> tuple[str, bool]:
        await self._get_client()

        return "ok", True  # pragma: no cover - no tenant to route by


@attrs.define(slots=True)
class _TenantAwareClient:
    """Refuses every call made with no tenant bound."""

    tenant_aware: bool = True

    async def health(self) -> tuple[str, bool]:  # pragma: no cover - refused before
        raise AssertionError("probed with no tenant bound")


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
            {"postgres": _FakeClient(raises=ValueError("dsn=postgres://user:fixture-7chr@db"))}
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
    async def test_a_key_registered_only_per_route_says_so(self) -> None:
        """Not "not registered": it is registered, but names no single client to probe."""

        key = DepKey[Any]("routed_client")
        runtime = ExecutionRuntime(
            deps=DepsRegistry.from_deps(Deps.routed({key: {"a": _FakeClient()}})).freeze()
        )
        router = APIRouter()
        attach_readiness_route(router, runtime, probes={"pg": key})
        app = FastAPI()
        app.include_router(router)

        async with runtime.scope():
            response = TestClient(app).get("/readyz")

        assert response.status_code == 503
        assert response.json()["checks"]["pg"] == {
            "ok": False,
            "detail": "registered per route only",
        }

    @pytest.mark.asyncio
    async def test_a_client_routed_per_tenant_says_so(self) -> None:
        """A probe runs with no tenant bound, so such a client has none to route by."""

        key = DepKey[Any]("routed_client")
        routed = _RoutedClient(secrets=MagicMock(), secret_ref_for_tenant={}, tenant_provider=lambda: None)
        runtime = ExecutionRuntime(deps=DepsRegistry.from_deps(Deps.plain({key: routed})).freeze())
        router = APIRouter()
        attach_readiness_route(router, runtime, probes={"redis": key})
        app = FastAPI()
        app.include_router(router)

        async with runtime.scope():
            response = TestClient(app).get("/readyz")

        assert response.status_code == 503
        assert response.json()["checks"]["redis"] == {"ok": False, "detail": "routed per tenant"}

    @pytest.mark.asyncio
    async def test_a_tenant_aware_client_says_so(self) -> None:
        """Not "routed per tenant": it needs a tenant bound, not one to route by."""

        key = DepKey[Any]("tenant_aware_client")
        runtime = ExecutionRuntime(
            deps=DepsRegistry.from_deps(Deps.plain({key: _TenantAwareClient()})).freeze()
        )
        router = APIRouter()
        attach_readiness_route(router, runtime, probes={"store": key})
        app = FastAPI()
        app.include_router(router)

        async with runtime.scope():
            response = TestClient(app).get("/readyz")

        assert response.status_code == 503
        assert response.json()["checks"]["store"] == {"ok": False, "detail": "tenant-aware"}

    # ....................... #

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

    @pytest.mark.asyncio
    async def test_a_client_that_times_itself_out_is_not_reported_as_the_route_budget(
        self,
    ) -> None:
        # A driver with its own deadline raises `TimeoutError` long before ours elapses.
        # Reporting that as "timed out after 5.0s" names a deadline that never expired.
        runtime, client = _probed(
            {"postgres": _FakeClient(raises=TimeoutError("driver gave up after 0.5s"))},
            timeout=timedelta(seconds=5),
        )

        async with runtime.scope():
            response = client.get("/readyz")

        assert response.status_code == 503
        assert response.json()["checks"]["postgres"] == {
            "ok": False,
            "detail": "TimeoutError",
        }


# ----------------------- #


_PARITY_CASES: dict[
    str, tuple[dict[str, _FakeClient], dict[str, DepKey[Any]], str | None, str]
] = {
    "healthy": ({"postgres": _FakeClient()}, {}, None, "ready"),
    "one unhealthy": (
        {"postgres": _FakeClient(), "redis": _FakeClient(detail="refused", ok=False)},
        {},
        None,
        "degraded",
    ),
    "one hanging": (
        {"postgres": _FakeClient(), "redis": _FakeClient(hang=True)},
        {},
        None,
        "degraded",
    ),
    "one raising": ({"postgres": _FakeClient(raises=ValueError("dsn"))}, {}, None, "degraded"),
    "one unregistered": (
        {"postgres": _FakeClient()},
        {"redis": DepKey[Any]("nope")},
        None,
        "degraded",
    ),
    "drain announced": ({"postgres": _FakeClient()}, {}, "announce", "draining"),
    "gate closed": ({"postgres": _FakeClient()}, {}, "close", "draining"),
}


class TestListenerAndRouteAgree:
    """The stdlib listener and the FastAPI route share one evaluator: same probes, same answer."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("case", list(_PARITY_CASES))
    async def test_the_same_probes_give_the_same_answer(self, case: str) -> None:
        clients, extra, action, expected = _PARITY_CASES[case]
        keys: dict[str, DepKey[Any]] = {name: DepKey[Any](name) for name in clients}
        probes = {**keys, **extra}
        timeout = timedelta(seconds=0.05)
        port = free_port()

        runtime = build_runtime(
            deps=Deps.plain({keys[name]: client for name, client in clients.items()}),
            lifecycle_steps=probe_listener_step(
                host="127.0.0.1", port=port, checks=probes, check_timeout=timeout
            ),
            drain_timeout=timedelta(0),
        )
        router = APIRouter()
        attach_readiness_route(router, runtime, probes=probes, probe_timeout=timeout)
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        async with runtime.scope():
            if action == "announce":
                runtime.begin_draining()

            elif action == "close":
                await runtime.get_context().drain_gate.drain(0.0)

            route = client.get("/readyz")

            async with httpx.AsyncClient(timeout=5.0) as http:
                listener = await http.get(f"http://127.0.0.1:{port}/readyz")

        assert route.json()["status"] == expected
        assert (listener.status_code, listener.json()) == (route.status_code, route.json())
