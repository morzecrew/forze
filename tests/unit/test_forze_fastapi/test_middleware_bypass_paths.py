"""A probe path listed in ``bypass_paths`` is served without the middleware running."""

from __future__ import annotations

import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient
from starlette.websockets import WebSocket, WebSocketDisconnect

from forze.application.contracts.authn import AuthnSpec
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.logging import DEFAULT_HEALTH_PATHS
from forze_fastapi.middlewares import InvocationMetadataMiddleware, SecurityContextMiddleware
from forze_fastapi.middlewares.raw_websocket import WS_POLICY_VIOLATION
from forze_fastapi.routes import attach_liveness_route
from forze_fastapi.security import AuthnRequirement, HeaderTokenAuthn

# ----------------------- #

_TOKEN_SPEC = AuthnSpec(name="auth", enabled_methods=frozenset({"token"}))


def _no_scope() -> ExecutionContext:
    """The context factory outside a runtime scope — exactly what a cold pod has."""

    return ExecutionRuntime().get_context()


def _app(*, bypass: frozenset[str] = frozenset()) -> FastAPI:
    router = APIRouter()
    attach_liveness_route(router)

    @router.get("/livez/sub")
    async def livez_sub() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        return {"status": "neighbour"}

    @router.get("/orders")
    async def orders() -> dict[str, str]:  # pyright: ignore[reportUnusedFunction]
        return {"ok": "yes"}

    @router.websocket("/livez")
    async def livez_socket(websocket: WebSocket) -> None:  # pyright: ignore[reportUnusedFunction]
        await websocket.accept()
        await websocket.send_text("reached")

    app = FastAPI()
    app.include_router(router)

    app.add_middleware(InvocationMetadataMiddleware, ctx_dep=_no_scope, bypass_paths=bypass)
    app.add_middleware(
        SecurityContextMiddleware,
        ctx_dep=_no_scope,
        authn=AuthnRequirement(
            ingress=(HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),)
        ),
        when_multiple_credentials="first_in_order",
        bypass_paths=bypass,
    )

    return app


def _refused(app: FastAPI, path: str) -> CoreException:
    """The error a still-governed path raises with no scope open."""

    with pytest.raises(CoreException) as error:
        TestClient(app).get(path)

    return error.value


def _served(app: FastAPI, path: str) -> int:
    """The status a real server would return, exceptions shaped rather than re-raised."""

    return TestClient(app, raise_server_exceptions=False).get(path).status_code


class TestBypassPaths:
    def test_liveness_answers_before_the_scope_opens(self) -> None:
        # The window a liveness probe exists to observe: the process is up, the runtime
        # scope is not. Both middlewares resolve the execution context on every request,
        # so in front of the probe path they are what stops it answering.
        response = TestClient(_app(bypass=DEFAULT_HEALTH_PATHS)).get("/livez")

        assert response.status_code == 200
        assert response.json() == {"status": "alive"}

    def test_without_the_bypass_the_same_probe_answers_500(self) -> None:
        # The defect the field exists for, at the status a probe would actually see —
        # the middleware raises above Starlette's exception handling, so the failure is
        # a server error, not a shaped one. Asserting the kind as well: a probe failing
        # for some *other* reason would prove nothing about the bypass.
        assert _served(_app(), "/livez") == 500
        assert "context" in str(_refused(_app(), "/livez")).lower()

    def test_an_unlisted_path_stays_governed(self) -> None:
        assert _refused(_app(bypass=DEFAULT_HEALTH_PATHS), "/orders")

    def test_a_path_under_a_bypassed_one_is_not_bypassed(self) -> None:
        # Exact paths, never prefixes — a prefix is one refactor away from an
        # ungoverned hole.
        assert _refused(_app(bypass=frozenset({"/livez"})), "/livez/sub")

    def test_a_bypassed_path_does_not_open_a_websocket_hole(self) -> None:
        # `bypass_paths` is HTTP-only. A websocket scope at the same path still answers
        # to the websocket gate, which refuses it unless `allowed_websocket_paths`
        # names it — otherwise listing a probe path would admit unauthenticated,
        # tenant-free duplex traffic at that path.
        client = TestClient(_app(bypass=DEFAULT_HEALTH_PATHS))

        with (
            pytest.raises(WebSocketDisconnect) as refusal,
            client.websocket_connect("/livez"),
        ):
            pass  # pragma: no cover - the connect itself is what is refused

        # The reason, not only the code: FastAPI closes a *validation* failure with
        # 1008 too, so asserting the code alone passes even when the scope reached
        # routing — the exact hole this test denies.
        assert refusal.value.code == WS_POLICY_VIOLATION
        assert "raw websocket ingress is disabled" in refusal.value.reason
