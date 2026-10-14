"""Draining the hub: SSE streams end and WebSocket tails close with 1012, over the window.

# covers: forze_fastapi.realtime.lifecycle (realtime_sse_drain_lifecycle_step),
#         forze_fastapi.realtime.hub (SseSubscription.end),
#         forze_fastapi.realtime.sse / ws (refusals while draining, an ended live tail)

Each app holds a real runtime scope whose plan carries the hub's drain step, driven by
``runtime.drain()`` — the same path SIGTERM takes under ``serve_uvicorn``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketDisconnect

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import ExecutionContext, build_runtime
from forze.application.execution.runtime import ExecutionRuntime
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
)
from forze_fastapi import runtime_lifespan
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.realtime import (
    RealtimeSseHub,
    WsConnect,
    WsConnection,
    attach_realtime_sse_route,
    attach_realtime_ws_route,
    realtime_sse_drain_lifecycle_step,
)
from forze_fastapi.realtime.ws import WS_RECONNECT_CLOSE

# ----------------------- #

_PRINCIPAL = uuid4()


def _runtime(hub: RealtimeSseHub, *, window: float = 0.3) -> ExecutionRuntime:
    return build_runtime(
        lifecycle_steps=realtime_sse_drain_lifecycle_step(
            hub, window=timedelta(seconds=window), tail=timedelta(seconds=0.5)
        ),
        drain_timeout=timedelta(0),
    )


class _BindIdentity:
    """Stands in for the security middleware: binds the principal on the scope's context."""

    def __init__(self, app: ASGIApp, *, runtime: ExecutionRuntime) -> None:
        self.app = app
        self.runtime = runtime

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        ctx = self.runtime.get_context()

        with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=_PRINCIPAL)):
            await self.app(scope, receive, send)


def _grant_and_announce(runtime: ExecutionRuntime) -> Any:
    async def authorize(
        _ctx: ExecutionContext, _principal: str, _tenant: UUID | None, requested: frozenset[str]
    ) -> frozenset[str]:
        runtime.begin_draining()  # the announcement lands while this connect is under way
        return requested

    return authorize


@asynccontextmanager
async def _sse(
    hub: RealtimeSseHub, runtime: ExecutionRuntime, **attach: Any
) -> AsyncIterator[httpx.AsyncClient]:
    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=runtime.get_context,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        hub=hub,
        **attach,
    )
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.add_middleware(_BindIdentity, runtime=runtime)  # type: ignore[arg-type]

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


async def _until(check: Any, *, within: float = 2.0) -> None:
    for _ in range(int(within / 0.01)):  # bounded
        if check():
            return

        await asyncio.sleep(0.01)

    raise AssertionError("condition not reached")


# ----------------------- #


class TestSseDrain:
    async def test_the_drain_ends_open_streams_spread_over_the_window(self) -> None:
        hub = RealtimeSseHub()
        runtime = _runtime(hub, window=0.4)
        clock = asyncio.get_running_loop()
        ended_at: list[float] = []

        async with runtime.scope(), _sse(hub, runtime) as client:

            async def stream() -> httpx.Response:
                response = await client.get("/realtime/sse")  # returns once the stream ends
                ended_at.append(clock.time())
                return response

            streams = [asyncio.ensure_future(stream()) for _ in range(4)]
            await _until(lambda: hub.subscribers == 4)

            await runtime.drain(timedelta(seconds=5))
            responses = await asyncio.wait_for(asyncio.gather(*streams), timeout=5)

        assert [response.status_code for response in responses] == [200] * 4
        assert hub.subscribers == 0
        # Four ends over 0.4 s start 0.1 s apart: spread, not all at once.
        assert max(ended_at) - min(ended_at) >= 0.2

    # ....................... #

    async def test_a_new_stream_while_draining_gets_503(self) -> None:
        hub = RealtimeSseHub()
        runtime = _runtime(hub)

        async with runtime.scope(), _sse(hub, runtime) as client:
            runtime.begin_draining()
            response = await client.get("/realtime/sse")

        assert response.status_code == 503
        assert response.headers[ERROR_CODE_HEADER] == "realtime_draining"
        assert hub.subscribers == 0

    # ....................... #

    async def test_a_stream_the_drain_overtook_mid_connect_gets_503(self) -> None:
        hub = RealtimeSseHub()
        runtime = _runtime(hub)

        async with (
            runtime.scope(),
            _sse(hub, runtime, authorize_topics=_grant_and_announce(runtime)) as client,
        ):
            response = await client.get("/realtime/sse", params={"topics": "lobby"})

        assert response.status_code == 503
        assert hub.subscribers == 0


# ----------------------- #


def _ws(hub: RealtimeSseHub, *, handshakes: list[str], announce: bool = False) -> TestClient:
    runtime = _runtime(hub)

    async def resolve(_connect: WsConnect) -> WsConnection:
        handshakes.append("resolve")

        if announce:
            runtime.begin_draining()  # the announcement lands while this upgrade is under way

        return WsConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL))

    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=runtime.get_context,
        resolve=resolve,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        hub=hub,
    )
    app = FastAPI(lifespan=runtime_lifespan(runtime))
    app.include_router(router)

    # Loop-side triggers: the test thread must not touch the app loop's primitives.
    @app.post("/publish")
    async def publish() -> None:  # pyright: ignore[reportUnusedFunction]
        hub.publish(
            RealtimeSignal.of(Audience.principal(str(_PRINCIPAL)), "e", {}), None, event_id="x"
        )

    @app.post("/announce")
    async def announce_drain() -> None:  # pyright: ignore[reportUnusedFunction]
        runtime.begin_draining()

    @app.post("/drain")
    async def drain() -> None:  # pyright: ignore[reportUnusedFunction]
        await runtime.drain(timedelta(seconds=5))

    return TestClient(app)


class TestWebSocketDrain:
    def test_the_drain_closes_a_live_tail_with_1012(self) -> None:
        with (
            _ws(RealtimeSseHub(), handshakes=[]) as client,
            client.websocket_connect("/realtime/ws") as ws,
        ):
            client.post("/publish")
            assert ws.receive_json()["id"] == "x"  # the live tail is up

            client.post("/drain")

            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == WS_RECONNECT_CLOSE

    # ....................... #

    def test_a_new_upgrade_while_draining_is_closed_with_1012_before_any_handshake(
        self,
    ) -> None:
        hub, handshakes = RealtimeSseHub(), []

        with _ws(hub, handshakes=handshakes) as client:
            client.post("/announce")

            with (
                client.websocket_connect("/realtime/ws") as ws,
                pytest.raises(WebSocketDisconnect) as caught,
            ):
                ws.receive_json()  # accepted first: a close before accept is an opaque 403

        assert caught.value.code == WS_RECONNECT_CLOSE
        assert handshakes == []
        assert hub.subscribers == 0

    # ....................... #

    def test_an_upgrade_the_drain_overtook_mid_handshake_is_closed_with_1012(self) -> None:
        hub = RealtimeSseHub()

        with (
            _ws(hub, handshakes=[], announce=True) as client,
            client.websocket_connect("/realtime/ws") as ws,
        ):
            client.post("/publish")  # an open tail would deliver this instead

            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == WS_RECONNECT_CLOSE
        assert hub.subscribers == 0
