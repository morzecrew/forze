"""``forze_load``'s realtime runs against forze's own SSE and WebSocket routes, over a socket.

The app serves ``attach_realtime_sse_route`` (durable, and live-only) and
``attach_realtime_ws_route`` on one hub and one in-memory mailbox, under uvicorn in a thread.
Its ``POST /notify`` does what the gateway and the tail do for a durable principal signal:
store it in the addressee's mailbox, then publish it to the hub. ``POST /_drain?after=N`` turns
the hub's drain on the way the drain step does, ending open streams, and the app turns it off
again by itself once it has given N draining answers.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from collections.abc import Iterator
from typing import Any
from uuid import UUID

import attrs
import httpx
import pytest
import uvicorn
from fastapi import APIRouter, FastAPI
from starlette.types import ASGIApp, Receive, Scope, Send

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
)
from forze.base.exceptions import exc
from forze.base.primitives import HlcTimestamp, uuid7
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.realtime import (
    RealtimeSseHub,
    WsConnect,
    WsConnection,
    attach_realtime_sse_route,
    attach_realtime_ws_route,
)
from forze_load import Call, Frame, LoadPlan, Principal, Realtime, Scenario, run
from forze_mock import MockDepsModule

pytestmark = pytest.mark.integration

_PRINCIPALS = {
    "ada": UUID("11111111-1111-1111-1111-111111111111"),
    "bob": UUID("22222222-2222-2222-2222-222222222222"),
}

# ----------------------- #


class _UndrainAfter:
    """Counts draining answers, SSE's header or WebSocket's 1012, and ends the drain after N."""

    def __init__(self, app: ASGIApp, *, draining: dict[str, Any]) -> None:
        self.app = app
        self.draining = draining

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def counting(message: Any) -> None:
            headers = dict(message.get("headers", []))
            told = headers.get(b"x-error-code") == b"realtime_draining" or (
                message["type"] == "websocket.close" and message.get("code") == 1012
            )

            if self.draining["on"] and told:
                self.draining["seen"] += 1
                self.draining["on"] = self.draining["seen"] < self.draining["after"]

            await send(message)

        await self.app(scope, receive, counting)


class _BindPrincipal:
    """Stands in for the security middleware: ``X-User`` names the principal."""

    def __init__(self, app: ASGIApp, *, ctx: ExecutionContext) -> None:
        self.app = app
        self.ctx = ctx

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        user = dict(scope.get("headers", [])).get(b"x-user", b"").decode()

        if scope["type"] != "http" or user not in _PRINCIPALS:
            await self.app(scope, receive, send)
            return

        with self.ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=_PRINCIPALS[user])):
            await self.app(scope, receive, send)


async def _resolve(connect: WsConnect) -> WsConnection:
    user = connect.websocket.headers.get("x-user", "")

    if user not in _PRINCIPALS:
        raise exc.authentication("unknown user")

    device = connect.websocket.query_params.get("device_id")

    return WsConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPALS[user]),
        client=ClientIdentity(device_id=device),
    )


def _app() -> FastAPI:
    ctx = ExecutionContext(deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve())
    hub = RealtimeSseHub()
    draining: dict[str, Any] = {"on": False, "seen": 0, "after": 0}
    hub.follow_drain(lambda: draining["on"])
    mailbox = InMemoryRealtimeMailbox()
    cursors = InMemoryMailboxCursors()
    marks = iter(range(1, 10**9))

    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=lambda: ctx,
        mailbox_factory=lambda _ctx: mailbox,
        cursors_factory=lambda _ctx: cursors,
        hub=hub,
    )
    attach_realtime_sse_route(router, ctx_dep=lambda: ctx, hub=hub, path="/realtime/live")
    attach_realtime_ws_route(
        router,
        ctx_dep=lambda: ctx,
        resolve=_resolve,
        mailbox_factory=lambda _ctx: mailbox,
        cursors_factory=lambda _ctx: cursors,
        hub=hub,
    )

    @router.post("/notify")
    async def notify(body: dict[str, Any]) -> dict[str, Any]:
        key, event_id = str(uuid7()), str(uuid7())
        principal = str(_PRINCIPALS[body["to"]])
        signal = RealtimeSignal.of(Audience.principal(principal), "notified", {"key": key})

        if body.get("durable", True):
            hlc = HlcTimestamp(physical_ms=next(marks), logical=0)
            await mailbox.store(principal=principal, event_id=event_id, hlc=hlc, signal=signal)

        hub.publish(signal, None, event_id=event_id if body.get("durable", True) else None)

        return {"key": key, "to": body["to"]}

    @router.post("/_drain")
    async def drain(after: int) -> None:
        draining.update(on=True, seen=0, after=after)

        for subscription in hub.subscriptions:
            subscription.end()

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.add_middleware(_BindPrincipal, ctx=ctx)  # type: ignore[arg-type]
    app.add_middleware(_UndrainAfter, draining=draining)  # type: ignore[arg-type]

    return app


@attrs.define
class _Served:
    base_url: str


@pytest.fixture
def served() -> Iterator[_Served]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    server = uvicorn.Server(uvicorn.Config(_app(), log_level="warning", lifespan="off"))
    thread = threading.Thread(target=lambda: asyncio.run(server.serve(sockets=[sock])), daemon=True)
    thread.start()

    try:
        deadline = time.perf_counter() + 15

        while not server.started:
            assert time.perf_counter() < deadline, "uvicorn did not start"
            time.sleep(0.02)

        yield _Served(base_url=f"http://127.0.0.1:{sock.getsockname()[1]}")

    finally:
        server.should_exit = True
        thread.join(timeout=15)
        sock.close()


def _user(name: str) -> Any:
    async def headers(_client: httpx.AsyncClient) -> dict[str, str]:
        return {"X-User": name}

    return headers


def _plan(base_url: str, *, durable: bool = True) -> LoadPlan:
    async def notify(call: Call) -> httpx.Response:
        to = "ada" if call.rng.random() < 0.5 else "bob"

        return await call.request("POST", "/notify", json={"to": to, "durable": durable})

    return LoadPlan(
        base_url=base_url,
        principals=[
            Principal(name=name, mix={"notify": 1}, auth=_user(name)) for name in _PRINCIPALS
        ],
        scenarios=[Scenario("notify", notify, writes=True)],
        allow_writes=True,
    )


def _key(frame: Frame) -> str | None:
    return frame.data.get("key") if frame.event == "notified" else None


def _realtime(transport: str, path: str, **kw: Any) -> Realtime:
    options: dict[str, Any] = {"subscribers": 6, "duration_s": 0.8, "timeout_s": 1.0, **kw}

    return Realtime(
        transport=transport,  # type: ignore[arg-type]
        path=path,
        rate=40,
        match=_key,
        key_of=lambda response: response.json()["key"],
        audience=lambda response: [response.json()["to"]],
        **options,
    )


# ....................... #


@pytest.mark.parametrize(("transport", "path"), [("sse", "/realtime/sse"), ("ws", "/realtime/ws")])
async def test_every_write_reaches_its_principal_s_streams_and_the_storm_resumes(
    served: _Served, transport: str, path: str
) -> None:
    result = await run(_plan(served.base_url), _realtime(transport, path))

    steady, storm = result.rows
    assert result.meta["model"] == "realtime"
    assert result.summary["stopped"] is None

    for row in (steady, storm):
        delivery = row["delivery"]
        assert row["outcomes"]["ok"] > 0
        assert delivery["expected"] > 0
        # Principal-addressed: each write is expected at its principal's three streams only.
        assert delivery["expected"] == 3 * row["outcomes"]["ok"]
        assert delivery["delivered"] == delivery["expected"]
        assert delivery["missing"] == delivery["gaps"] == 0
        assert delivery["send_ms"]["p50"] is not None
        assert row["streams"]["refused"] == row["streams"]["errors"] == 0

    assert steady["streams"]["opened"] == 6
    assert storm["streams"]["back"] == 6
    assert storm["streams"]["back_in_s"] is not None
    assert storm["streams"]["resume"] == "last_event_id"
    assert storm["streams"]["resumed"] == 6


async def test_a_live_only_route_records_no_resume(served: _Served) -> None:
    result = await run(
        _plan(served.base_url, durable=False),
        _realtime("sse", "/realtime/live", resume=False),
    )

    storm = result.rows[1]
    assert storm["streams"]["resume"] == "none"
    assert storm["streams"]["resumed"] == 0
    assert storm["streams"]["back"] == 6
    assert storm["delivery"]["delivered"] > 0


@pytest.mark.parametrize(("transport", "path"), [("sse", "/realtime/sse"), ("ws", "/realtime/ws")])
async def test_a_drain_is_a_reconnect_never_an_error(
    served: _Served, transport: str, path: str
) -> None:
    async with httpx.AsyncClient(base_url=served.base_url) as control:
        await control.post("/_drain", params={"after": 2})

    result = await run(
        _plan(served.base_url),
        _realtime(transport, path, storm=False, timeout_s=5.0, subscribers=2),
    )

    streams = result.rows[0]["streams"]
    assert streams["draining"] == 2  # each stream was told to retry once, then opened
    assert streams["opened"] == 2
    assert streams["refused"] == streams["errors"] == 0
    assert result.rows[0]["delivery"]["delivered"] == result.rows[0]["delivery"]["expected"]
