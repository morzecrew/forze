"""Fail-closed websocket scopes — governed middlewares refuse raw websocket ingress.

# covers: forze_fastapi.middlewares.raw_websocket, SecurityContextMiddleware /
#         InvocationMetadataMiddleware (websocket refusal + allow_raw_websockets opt-out)

The middlewares resolve identity, tenancy, and the invocation envelope for HTTP scopes
only. A raw ``@app.websocket`` route on the same app previously slipped straight through
both middlewares and ran unauthenticated. Now a ``websocket`` scope is refused at the
ASGI level (policy-violation close before accept) unless the middleware was built with
``allow_raw_websockets=True`` — while ``lifespan`` scopes keep passing through.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI, WebSocket
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from forze.application.contracts.authn import AuthnSpec
from forze.application.execution import ExecutionContext
from forze_fastapi.middlewares import (
    InvocationMetadataMiddleware,
    SecurityContextMiddleware,
)
from forze_fastapi.middlewares.raw_websocket import WS_POLICY_VIOLATION
from forze_fastapi.security import AuthnRequirement, HeaderTokenAuthn
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #

_AUTHN = AuthnRequirement(
    ingress=(
        HeaderTokenAuthn(
            authn_spec=AuthnSpec(name="main", enabled_methods=frozenset({"token"})),
            header_name="Authorization",
        ),
    )
)


def _execution_ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=MockState())())


class _Inner:
    """A terminal ASGI app that records whether it was reached."""

    def __init__(self) -> None:
        self.called = False

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        self.called = True


async def _receive() -> dict[str, Any]:  # pragma: no cover - never pulled on refusal
    return {"type": "websocket.connect"}


def _collector(into: list[dict[str, Any]]) -> Any:
    async def send(message: dict[str, Any]) -> None:
        into.append(message)

    return send


def _security(inner: _Inner, **overrides: Any) -> SecurityContextMiddleware:
    return SecurityContextMiddleware(
        inner,
        _AUTHN,
        "first_in_order",
        ctx_dep=_execution_ctx,
        **overrides,
    )


# ----------------------- #


class TestWebsocketScopeRefusal:
    @pytest.mark.parametrize("allow", [False, True])
    async def test_security_middleware_refuses_unless_opted_in(self, allow: bool) -> None:
        inner = _Inner()
        sent: list[dict[str, Any]] = []

        middleware = _security(inner, allow_raw_websockets=allow)
        await middleware({"type": "websocket"}, _receive, _collector(sent))

        assert inner.called is allow
        if not allow:
            assert sent == [
                {
                    "type": "websocket.close",
                    "code": WS_POLICY_VIOLATION,
                    "reason": (
                        "raw websocket ingress is disabled (allow_raw_websockets=False)"
                    ),
                }
            ]

    @pytest.mark.parametrize("allow", [False, True])
    async def test_invocation_middleware_refuses_unless_opted_in(self, allow: bool) -> None:
        inner = _Inner()
        sent: list[dict[str, Any]] = []

        middleware = InvocationMetadataMiddleware(
            inner, ctx_dep=_execution_ctx, allow_raw_websockets=allow
        )
        await middleware({"type": "websocket"}, _receive, _collector(sent))

        assert inner.called is allow
        if not allow:
            assert sent and sent[0]["type"] == "websocket.close"
            assert sent[0]["code"] == WS_POLICY_VIOLATION

    async def test_allowlisted_path_passes_through_others_still_refused(self) -> None:
        for middleware_for in (
            lambda inner: _security(inner, allowed_websocket_paths={"/realtime/ws"}),
            lambda inner: InvocationMetadataMiddleware(
                inner, ctx_dep=_execution_ctx, allowed_websocket_paths={"/realtime/ws"}
            ),
        ):
            inner = _Inner()
            middleware = middleware_for(inner)

            # the governed route's exact path passes through (it resolves identity itself)
            await middleware(
                {"type": "websocket", "path": "/realtime/ws"}, _receive, _collector([])
            )
            assert inner.called

            # every other websocket path stays fail-closed — and so does a prefix
            for path in ("/realtime/ws/extra", "/other"):
                other = _Inner()
                sent: list[dict[str, Any]] = []
                await middleware_for(other)(
                    {"type": "websocket", "path": path}, _receive, _collector(sent)
                )
                assert not other.called
                assert sent and sent[0]["type"] == "websocket.close"

    async def test_lifespan_scopes_still_pass_through(self) -> None:
        for middleware_for in (
            lambda inner: _security(inner),
            lambda inner: InvocationMetadataMiddleware(inner, ctx_dep=_execution_ctx),
        ):
            inner = _Inner()
            await middleware_for(inner)({"type": "lifespan"}, _receive, _collector([]))
            assert inner.called  # only websocket fails closed; lifespan is untouched


# ----------------------- #


class TestAllowlistCheck:
    """The startup reconciliation: an allowlisted path must serve the governed route."""

    def _governed_app(self, *, prefix: str = "", allow: str | None = None) -> FastAPI:
        from fastapi import APIRouter

        from forze.application.integrations.realtime import (
            InMemoryMailboxCursors,
            InMemoryRealtimeMailbox,
        )
        from forze_fastapi.realtime import WsConnection, attach_realtime_ws_route

        async def _resolve(connect: Any) -> WsConnection:  # pragma: no cover - unused
            raise NotImplementedError

        router = APIRouter(prefix=prefix)
        attach_realtime_ws_route(
            router,
            ctx_dep=_execution_ctx,
            resolve=_resolve,
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
            cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        )

        app = FastAPI()
        app.include_router(router)

        if allow is not None:
            app.add_middleware(
                InvocationMetadataMiddleware,  # type: ignore[arg-type]
                ctx_dep=_execution_ctx,
                allowed_websocket_paths={allow},
            )

        return app

    def test_matching_governed_route_passes(self) -> None:
        from forze_fastapi.middlewares import check_websocket_allowlist

        check_websocket_allowlist(self._governed_app(allow="/realtime/ws"))

    def test_no_allowlist_is_a_noop(self) -> None:
        from forze_fastapi.middlewares import check_websocket_allowlist

        check_websocket_allowlist(self._governed_app())

    def test_router_prefix_mismatch_fails_the_boot(self) -> None:
        from forze.base.exceptions import CoreException
        from forze_fastapi.middlewares import check_websocket_allowlist

        # the documented route-local path does not exist once the router is
        # mounted under /api — a silent 1008 on every connect becomes a loud boot error
        app = self._governed_app(prefix="/api", allow="/realtime/ws")

        with pytest.raises(CoreException) as caught:
            check_websocket_allowlist(app)

        assert "full mounted path" in str(caught.value)

        # the corrected full path verifies
        check_websocket_allowlist(self._governed_app(prefix="/api", allow="/api/realtime/ws"))

    def test_foreign_route_at_an_allowlisted_path_fails_the_boot(self) -> None:
        from forze.base.exceptions import CoreException
        from forze_fastapi.middlewares import check_websocket_allowlist

        app = FastAPI()

        @app.websocket("/realtime/ws")
        async def rogue(websocket: WebSocket) -> None:  # pragma: no cover - never runs
            await websocket.accept()

        app.add_middleware(
            InvocationMetadataMiddleware,  # type: ignore[arg-type]
            ctx_dep=_execution_ctx,
            allowed_websocket_paths={"/realtime/ws"},
        )

        with pytest.raises(CoreException) as caught:
            check_websocket_allowlist(app)

        assert "not a governed realtime route" in str(caught.value)

    def test_runtime_lifespan_runs_the_check_at_startup(self) -> None:
        from forze.application.execution import DepsRegistry, ExecutionRuntime
        from forze.base.exceptions import CoreException
        from forze_fastapi import runtime_lifespan

        runtime = ExecutionRuntime(
            deps=DepsRegistry.from_modules(MockDepsModule(state=MockState())).freeze()
        )
        app = self._governed_app(prefix="/api", allow="/realtime/ws")
        app.router.lifespan_context = runtime_lifespan(runtime)

        with pytest.raises(CoreException):
            with TestClient(app):  # entering runs the lifespan
                pass  # pragma: no cover - startup refuses


def test_socket_teardown_error_predicate() -> None:
    from forze_fastapi.realtime.ws import _is_socket_teardown_error  # pyright: ignore[reportPrivateUsage]

    assert _is_socket_teardown_error(
        RuntimeError('Cannot call "send" once a close message has been sent.')
    )
    assert _is_socket_teardown_error(
        RuntimeError('Cannot call "receive" once a disconnect message has been received.')
    )
    # anything else (hub, presence, app logic) must stay observable
    assert not _is_socket_teardown_error(RuntimeError("presence store exploded"))


def test_raw_websocket_route_handshake_is_rejected_end_to_end() -> None:
    app = FastAPI()

    @app.websocket("/ws")
    async def ws(websocket: WebSocket) -> None:  # pragma: no cover - unreachable
        await websocket.accept()

    app.add_middleware(
        SecurityContextMiddleware,  # type: ignore[arg-type]
        authn=_AUTHN,
        when_multiple_credentials="first_in_order",
        ctx_dep=_execution_ctx,
    )

    client = TestClient(app)

    with pytest.raises(WebSocketDisconnect) as caught:
        with client.websocket_connect("/ws"):
            pass  # pragma: no cover - the handshake never completes

    assert caught.value.code == WS_POLICY_VIOLATION

    # HTTP traffic on the same app is unaffected by the websocket refusal.
    assert client.get("/nonexistent").status_code == 404
