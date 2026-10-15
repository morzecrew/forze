"""Draining the hub: SSE streams end and WebSocket tails close with 1012, over the window.

# covers: forze_fastapi.realtime.lifecycle (realtime_sse_drain_lifecycle_step),
#         forze_fastapi.realtime.hub (SseSubscription.end),
#         forze_fastapi.realtime.sse / ws (refusals while draining, an ended live tail)

Each app holds a real runtime scope whose plan carries the hub's drain step, driven by
``runtime.drain()`` — the same path SIGTERM takes under ``serve_uvicorn``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import anyio
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
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

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

    def __init__(self, app: ASGIApp, *, ctx_dep: Callable[[], ExecutionContext]) -> None:
        self.app = app
        self.ctx_dep = ctx_dep

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        ctx = self.ctx_dep()

        with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=_PRINCIPAL)):
            await self.app(scope, receive, send)


def _grant_and_announce(runtime: ExecutionRuntime) -> Any:
    async def authorize(
        _ctx: ExecutionContext, _principal: str, _tenant: UUID | None, requested: frozenset[str]
    ) -> frozenset[str]:
        runtime.begin_draining()  # the announcement lands while this connect is under way
        return requested

    return authorize


def _sse_app(
    hub: RealtimeSseHub | None,
    runtime: ExecutionRuntime,
    *,
    ctx_dep: Callable[[], ExecutionContext] | None = None,
    **attach: Any,
) -> FastAPI:
    ctx_dep = ctx_dep or runtime.get_context
    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=ctx_dep,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        hub=hub,
        **attach,
    )
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.add_middleware(_BindIdentity, ctx_dep=ctx_dep)  # type: ignore[arg-type]

    return app


@asynccontextmanager
async def _sse(
    hub: RealtimeSseHub | None,
    runtime: ExecutionRuntime,
    *,
    ctx_dep: Callable[[], ExecutionContext] | None = None,
    **attach: Any,
) -> AsyncIterator[httpx.AsyncClient]:
    app = _sse_app(hub, runtime, ctx_dep=ctx_dep, **attach)

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


def _assert_told_to_retry(response: httpx.Response) -> None:
    """A 200 stream carrying only a jittered ``retry:``: EventSource reconnects after it,
    where any other status would end it for good."""

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers[ERROR_CODE_HEADER] == "realtime_draining"
    [field] = response.text.split("\n\n")[:-1]
    name, _, value = field.partition(": ")
    assert name == "retry"
    assert 1000 <= int(value) <= 3000


class _RoundTripPresence:
    """Presence whose leave takes a round trip, as a shared store's does — so a leave made
    while its task is being cancelled is cancelled too. An in-memory leave never suspends."""

    cluster_wide = False

    def __init__(self) -> None:
        self.members: set[tuple[str, str]] = set()

    async def joined(self, room: str, sid: str) -> None:
        self.members.add((room, sid))

    async def left(self, room: str, sid: str) -> None:
        await asyncio.sleep(0.01)
        self.members.discard((room, sid))

    async def count(self, room: str) -> int:
        return sum(1 for member_room, _sid in self.members if member_room == room)


def _http_scope() -> dict[str, Any]:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "path": "/realtime/sse",
        "raw_path": b"/realtime/sse",
        "root_path": "",
        "query_string": b"",
        "headers": [],
        "scheme": "http",
        "server": ("test", 80),
        "client": ("test", 1),
    }


# ----------------------- #


def _bare_context() -> ExecutionContext:
    """A context outside the runtime's scope, as the shipped recipes pass to ``ctx_dep``."""

    return context_from_deps(MockDepsModule(state=MockState())())


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

    async def test_a_new_stream_while_draining_is_told_to_retry_before_any_work(self) -> None:
        hub = RealtimeSseHub()
        runtime = _runtime(hub)
        authorized: list[frozenset[str]] = []

        async def authorize(
            _ctx: ExecutionContext, _principal: str, _tenant: UUID | None, topics: frozenset[str]
        ) -> frozenset[str]:
            authorized.append(topics)
            return topics

        async with runtime.scope(), _sse(hub, runtime, authorize_topics=authorize) as client:
            runtime.begin_draining()
            response = await asyncio.wait_for(
                client.get("/realtime/sse", params={"topics": "lobby"}), timeout=5
            )

        _assert_told_to_retry(response)
        assert authorized == []
        assert hub.subscribers == 0

    # ....................... #

    async def test_a_route_given_a_bare_context_learns_the_drain_from_the_hub(self) -> None:
        hub = RealtimeSseHub()
        runtime = _runtime(hub)
        bare = _bare_context()

        async with runtime.scope(), _sse(hub, runtime, ctx_dep=lambda: bare) as client:
            runtime.begin_draining()
            response = await asyncio.wait_for(client.get("/realtime/sse"), timeout=5)

        _assert_told_to_retry(response)

    # ....................... #

    async def test_a_route_without_a_hub_learns_the_drain_from_its_context(self) -> None:
        runtime = _runtime(RealtimeSseHub())

        async with runtime.scope(), _sse(None, runtime) as client:
            runtime.begin_draining()
            response = await asyncio.wait_for(client.get("/realtime/sse"), timeout=5)

        _assert_told_to_retry(response)

    # ....................... #

    async def test_a_stream_the_drain_overtook_mid_connect_is_told_to_retry(self) -> None:
        hub = RealtimeSseHub()
        runtime = _runtime(hub)

        async with (
            runtime.scope(),
            _sse(hub, runtime, authorize_topics=_grant_and_announce(runtime)) as client,
        ):
            response = await asyncio.wait_for(
                client.get("/realtime/sse", params={"topics": "lobby"}), timeout=5
            )

        _assert_told_to_retry(response)
        assert hub.subscribers == 0

    # ....................... #

    async def test_a_stream_blocked_writing_to_a_slow_client_is_cut_off(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze_fastapi.realtime import sse as sse_module

        monkeypatch.setattr(sse_module, "_END_GRACE_SECONDS", 0.1)
        # Held, so its stream is not closed by being collected: only the cut-off closes it.
        responses: list[Any] = []
        init = sse_module._LiveSseResponse.__init__

        def held(response: Any, *args: Any, **kwargs: Any) -> None:
            init(response, *args, **kwargs)
            responses.append(response)

        monkeypatch.setattr(sse_module._LiveSseResponse, "__init__", held)
        hub = RealtimeSseHub()
        runtime = _runtime(hub)
        app = _sse_app(hub, runtime)

        gone = asyncio.Event()
        stalled = asyncio.Event()
        sent: list[dict[str, Any]] = []

        async def receive() -> dict[str, Any]:
            await gone.wait()
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

            if message["type"] == "http.response.body" and message["body"]:
                stalled.set()
                await gone.wait()  # a client that stopped reading: the write never completes

        async with runtime.scope():
            serving = asyncio.ensure_future(app(_http_scope(), receive, send))
            await _until(lambda: hub.subscribers == 1)
            hub.publish(
                RealtimeSignal.of(Audience.principal(str(_PRINCIPAL)), "e", {}), None, event_id="x"
            )
            await asyncio.wait_for(stalled.wait(), timeout=2)

            await runtime.drain(timedelta(seconds=5))

            try:
                done, _ = await asyncio.wait({serving}, timeout=2)
                assert serving in done, "the stream was not cut off while its write was blocked"

            finally:
                gone.set()
                await asyncio.wait_for(serving, timeout=5)

        assert hub.subscribers == 0  # its stream's cleanup ran at the cut-off
        # Cut off, not finished: the client sees the connection drop and reconnects.
        assert not any(one.get("more_body") is False for one in sent)

    # ....................... #

    async def test_a_stream_cut_off_mid_wait_still_leaves_its_presence_rooms(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze_fastapi.realtime import sse as sse_module

        monkeypatch.setattr(sse_module, "_END_GRACE_SECONDS", 0.1)
        hub = RealtimeSseHub()
        hub.ready.clear()  # the stream waits for the hub, and the cut-off lands mid-wait
        runtime = _runtime(hub)
        presence = _RoundTripPresence()
        app = _sse_app(hub, runtime, presence=presence)
        gone = asyncio.Event()

        async def receive() -> dict[str, Any]:
            await gone.wait()
            return {"type": "http.disconnect"}

        async def send(_message: dict[str, Any]) -> None:
            pass

        async with runtime.scope():
            serving = asyncio.ensure_future(app(_http_scope(), receive, send))

            try:
                await _until(lambda: hub.subscribers == 1 and bool(presence.members))
                await runtime.drain(timedelta(seconds=5))
                await asyncio.wait_for(asyncio.shield(serving), timeout=2)

            finally:
                gone.set()
                await asyncio.wait_for(serving, timeout=5)

        assert presence.members == set()

    # ....................... #

    async def test_a_stream_whose_client_leaves_mid_wait_still_leaves_its_presence_rooms(
        self,
    ) -> None:
        hub = RealtimeSseHub()
        runtime = _runtime(hub)
        presence = _RoundTripPresence()
        app = _sse_app(hub, runtime, presence=presence)
        gone = asyncio.Event()

        async def receive() -> dict[str, Any]:
            await gone.wait()
            return {"type": "http.disconnect"}

        async def send(_message: dict[str, Any]) -> None:
            pass

        async with runtime.scope():
            serving = asyncio.ensure_future(app(_http_scope(), receive, send))
            await _until(lambda: hub.subscribers == 1 and bool(presence.members))

            # The live tail waits for a signal; the client goes, and Starlette cancels it.
            gone.set()
            await asyncio.wait_for(serving, timeout=2)

        assert presence.members == set()


# ----------------------- #


def _ws_app(hub: RealtimeSseHub, runtime: ExecutionRuntime, *, presence: Any) -> FastAPI:
    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=runtime.get_context,
        resolve=lambda _connect: WsConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL)),
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        hub=hub,
        presence=presence,
    )
    app = FastAPI()
    app.include_router(router)

    return app


def _ws_scope() -> dict[str, Any]:
    return {
        "type": "websocket",
        "path": "/realtime/ws",
        "raw_path": b"/realtime/ws",
        "query_string": b"",
        "headers": [],
        "scheme": "ws",
        "server": ("test", 80),
        "client": ("test", 1),
        "subprotocols": [],
    }


def _ws(
    hub: RealtimeSseHub,
    *,
    handshakes: list[str],
    announce: bool = False,
    bare: bool = False,
    with_hub: bool = True,
) -> TestClient:
    runtime = _runtime(hub)
    bare_ctx = _bare_context()

    async def resolve(_connect: WsConnect) -> WsConnection:
        handshakes.append("resolve")

        if announce:
            runtime.begin_draining()  # the announcement lands while this upgrade is under way

        return WsConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL))

    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=(lambda: bare_ctx) if bare else runtime.get_context,
        resolve=resolve,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        hub=hub if with_hub else None,
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

    # ....................... #

    def test_a_route_given_a_bare_context_learns_the_drain_from_the_hub(self) -> None:
        hub, handshakes = RealtimeSseHub(), []

        with _ws(hub, handshakes=handshakes, bare=True) as client:
            client.post("/announce")

            with (
                client.websocket_connect("/realtime/ws") as ws,
                pytest.raises(WebSocketDisconnect) as caught,
            ):
                ws.receive_json()

        assert caught.value.code == WS_RECONNECT_CLOSE
        assert handshakes == []

    # ....................... #

    def test_a_route_without_a_hub_learns_the_drain_from_its_context(self) -> None:
        with _ws(RealtimeSseHub(), handshakes=[], with_hub=False) as client:
            client.post("/announce")

            with (
                client.websocket_connect("/realtime/ws") as ws,
                pytest.raises(WebSocketDisconnect) as caught,
            ):
                ws.receive_json()

        assert caught.value.code == WS_RECONNECT_CLOSE

    # ....................... #

    def test_an_upgrade_without_a_hub_the_drain_overtook_mid_handshake_is_closed(self) -> None:
        with (
            _ws(RealtimeSseHub(), handshakes=[], announce=True, with_hub=False) as client,
            client.websocket_connect("/realtime/ws") as ws,
        ):
            ws.send_json({"type": "nothing"})  # an open socket would answer with an error frame

            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == WS_RECONNECT_CLOSE

    # ....................... #

    async def test_a_tail_blocked_writing_to_a_slow_client_still_closes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze_fastapi.realtime import ws as ws_module

        monkeypatch.setattr(ws_module, "_CLOSE_LOCK_TIMEOUT_SECONDS", 0.1)
        hub = RealtimeSseHub()
        runtime = _runtime(hub)
        presence = _RoundTripPresence()
        app = _ws_app(hub, runtime, presence=presence)

        gone = asyncio.Event()
        stalled = asyncio.Event()
        sent: list[dict[str, Any]] = []

        async def receive() -> dict[str, Any]:
            if not sent:
                return {"type": "websocket.connect"}

            await gone.wait()
            return {"type": "websocket.disconnect", "code": 1006}

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

            if message["type"] == "websocket.send":
                stalled.set()
                await gone.wait()  # a client that stopped reading: the write never completes

        async with runtime.scope():
            serving = asyncio.ensure_future(app(_ws_scope(), receive, send))
            await _until(lambda: hub.subscribers == 1)
            hub.publish(
                RealtimeSignal.of(Audience.principal(str(_PRINCIPAL)), "e", {}), None, event_id="x"
            )
            await asyncio.wait_for(stalled.wait(), timeout=2)

            await runtime.drain(timedelta(seconds=5))

            try:
                await _until(
                    lambda: any(one["type"] == "websocket.close" for one in sent), within=2.0
                )
                # Left while the write is still blocked: the end guard's own close.
                await _until(lambda: not presence.members, within=2.0)

            finally:
                gone.set()
                await asyncio.wait_for(serving, timeout=5)

        [close] = [one for one in sent if one["type"] == "websocket.close"]
        assert close["code"] == WS_RECONNECT_CLOSE

    # ....................... #

    async def test_a_tail_cancelled_from_outside_still_leaves_its_presence_rooms(self) -> None:
        hub = RealtimeSseHub()
        runtime = _runtime(hub)
        presence = _RoundTripPresence()
        app = _ws_app(hub, runtime, presence=presence)
        connected: list[bool] = []
        cancels: list[anyio.CancelScope] = []

        async def receive() -> dict[str, Any]:
            if not connected:
                connected.append(True)
                return {"type": "websocket.connect"}

            await asyncio.Event().wait()  # a client that stays
            raise AssertionError("unreachable")

        async def send(_message: dict[str, Any]) -> None:
            pass

        async def serve() -> None:
            # How an ASGI server or a test client cancels an endpoint: through a cancel scope.
            with anyio.CancelScope() as cancel:
                cancels.append(cancel)
                await app(_ws_scope(), receive, send)

        async with runtime.scope():
            serving = asyncio.ensure_future(serve())
            await _until(lambda: hub.subscribers == 1 and bool(presence.members))
            cancels[0].cancel()
            await asyncio.wait_for(serving, timeout=2)

        assert presence.members == set()
