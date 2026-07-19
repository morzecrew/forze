"""SSE presence — open streams occupy the same rooms Socket.IO connections do.

# covers: forze_fastapi.realtime (presence_rooms, SseSubscription.rooms/key,
#         attach_realtime_sse_route presence join/leave, refresh_sse_presence,
#         realtime_sse_presence_heartbeat_lifecycle_step)

Presence is only honest if every transport reports into the same store under the
same names — so the rooms here must be byte-identical to what ``room_for`` gives
the Socket.IO side, and a failed leave must never mask the stream's own exit.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.realtime import Audience
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
)
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    InMemoryRealtimePresence,
    room_for,
)
from forze.base.exceptions import CoreException
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.realtime import (
    RealtimeSseHub,
    attach_realtime_sse_route,
    presence_rooms,
    realtime_sse_presence_heartbeat_lifecycle_step,
    refresh_sse_presence,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #

_PRINCIPAL = uuid4()
_TENANT = uuid4()


class _RecordingPresence:
    """Records join/leave order; optionally fails every ``left`` call."""

    def __init__(self, *, fail_left: bool = False) -> None:
        self.joins: list[tuple[str, str]] = []
        self.leaves: list[tuple[str, str]] = []
        self.fail_left = fail_left

    async def joined(self, room: str, sid: str) -> None:
        self.joins.append((room, sid))

    async def left(self, room: str, sid: str) -> None:
        if self.fail_left:
            raise RuntimeError("presence store down")

        self.leaves.append((room, sid))

    async def count(self, room: str) -> int:
        return sum(1 for r, _ in self.joins if r == room)


class _Bind:
    def __init__(self, app, *, ctx: ExecutionContext) -> None:  # type: ignore[no-untyped-def]
        self.app = app
        self.ctx = ctx

    async def __call__(self, scope, receive, send) -> None:  # type: ignore[no-untyped-def]
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        with self.ctx.inv_ctx.bind_identity(
            authn=AuthnIdentity(principal_id=_PRINCIPAL),
            tenant=TenantIdentity(tenant_id=_TENANT),
        ):
            await self.app(scope, receive, send)


def _client(presence: _RecordingPresence) -> TestClient:
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=lambda: ctx,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        presence=presence,  # type: ignore[arg-type]
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.add_middleware(_Bind, ctx=ctx)  # type: ignore[arg-type]

    return TestClient(app)


# ----------------------- #


class TestRooms:
    def test_rooms_match_the_socketio_naming_exactly(self) -> None:
        rooms = presence_rooms(
            principal="p1", tenant=_TENANT, topics=frozenset({"b", "a"})
        )

        assert rooms == (
            room_for(Audience.principal("p1"), _TENANT),
            room_for(Audience.topic("a"), _TENANT),
            room_for(Audience.topic("b"), _TENANT),
        )

    def test_subscription_rooms_and_key(self) -> None:
        hub = RealtimeSseHub()
        sub = hub.subscribe(principal="p1", tenant=None, topics=frozenset({"t"}))

        assert sub.rooms() == ("principal:p1", "topic:t")
        assert sub.key.startswith("sse:")
        assert sub.key != hub.subscribe(principal="p1", tenant=None).key  # per-response


class TestRoutePresence:
    def test_stream_joins_then_leaves_its_rooms(self) -> None:
        presence = _RecordingPresence()
        client = _client(presence)

        response = client.get("/realtime/sse", params={"topics": "t1"})
        assert response.status_code == 200

        principal_room = room_for(Audience.principal(str(_PRINCIPAL)), _TENANT)
        topic_room = room_for(Audience.topic("t1"), _TENANT)

        joined_rooms = [room for room, _ in presence.joins]
        assert joined_rooms == [principal_room, topic_room]
        assert [room for room, _ in presence.leaves] == joined_rooms  # paired exit
        # one member key for the whole connection, on every room
        keys = {key for _, key in presence.joins} | {key for _, key in presence.leaves}
        assert len(keys) == 1 and next(iter(keys)).startswith("sse:")

    def test_failed_leave_does_not_fail_the_stream(self) -> None:
        presence = _RecordingPresence(fail_left=True)
        client = _client(presence)

        response = client.get("/realtime/sse")

        assert response.status_code == 200  # the stream completed despite the store
        assert presence.joins and not presence.leaves

    def test_no_presence_wired_means_no_calls(self) -> None:
        presence = _RecordingPresence()
        ctx = context_from_deps(MockDepsModule(state=MockState())())
        router = APIRouter()
        attach_realtime_sse_route(
            router,
            ctx_dep=lambda: ctx,
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
            cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        )
        app = FastAPI()
        app.include_router(router)
        register_exception_handlers(app)
        app.add_middleware(_Bind, ctx=ctx)  # type: ignore[arg-type]

        assert TestClient(app).get("/realtime/sse").status_code == 200
        assert not presence.joins


# ----------------------- #


class TestHeartbeat:
    async def test_refresh_reasserts_every_live_subscription(self) -> None:
        hub = RealtimeSseHub()
        store = InMemoryRealtimePresence()
        a = hub.subscribe(principal="p1", tenant=None, topics=frozenset({"t"}))
        b = hub.subscribe(principal="p2", tenant=None)

        refreshed = await refresh_sse_presence(hub, store)

        assert refreshed == 2
        assert await store.count("principal:p1") == 1
        assert await store.count("topic:t") == 1
        assert await store.count("principal:p2") == 1

        hub.unsubscribe(a)
        hub.unsubscribe(b)
        assert await refresh_sse_presence(hub, store) == 0

    async def test_heartbeat_step_ticks_until_stopped(self) -> None:
        hub = RealtimeSseHub()
        store = InMemoryRealtimePresence()
        hub.subscribe(principal="p1", tenant=None)

        step = realtime_sse_presence_heartbeat_lifecycle_step(
            hub, store, interval=timedelta(milliseconds=10)
        )

        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            import asyncio

            waited = 0.0
            while await store.count("principal:p1") == 0 and waited < 5.0:
                await asyncio.sleep(0.01)
                waited += 0.01

            assert await store.count("principal:p1") == 1
            assert step.startup.loop_name == "realtime_sse_presence_heartbeat"  # type: ignore[attr-defined]

            await step.shutdown(ctx)
            task = step.startup.task  # type: ignore[attr-defined]
            assert task is not None and task.done() and not task.cancelled()

    def test_non_positive_interval_is_refused(self) -> None:
        with pytest.raises(CoreException):
            realtime_sse_presence_heartbeat_lifecycle_step(
                RealtimeSseHub(), InMemoryRealtimePresence(), interval=timedelta(0)
            )
