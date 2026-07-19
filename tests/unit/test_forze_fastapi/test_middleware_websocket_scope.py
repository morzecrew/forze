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

    async def test_lifespan_scopes_still_pass_through(self) -> None:
        for middleware_for in (
            lambda inner: _security(inner),
            lambda inner: InvocationMetadataMiddleware(inner, ctx_dep=_execution_ctx),
        ):
            inner = _Inner()
            await middleware_for(inner)({"type": "lifespan"}, _receive, _collector([]))
            assert inner.called  # only websocket fails closed; lifespan is untouched


# ----------------------- #


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
