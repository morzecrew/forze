"""The SSE egress route — replay-from-cursor, cumulative ack, live tail, hub fan-out.

# covers: forze_fastapi.realtime.sse (attach_realtime_sse_route, replay/ack endpoints,
#         Last-Event-ID precedence, protocol refusal, frame shape),
#         forze_fastapi.realtime.hub (matching, drop-on-full, subscribe/unsubscribe)

Replay and ack run end-to-end through the app (identity bound by a middleware, like
production); the live leg is an infinite stream a buffered test transport cannot
drive, so its generator (`_live_frames`) is exercised directly against a hub
subscription — the same composition the endpoint uses.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient
from starlette.types import ASGIApp, Receive, Scope, Send

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import HlcTimestamp
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.realtime import RealtimeSseHub, attach_realtime_sse_route
from forze_fastapi.realtime.sse import _live_frames  # pyright: ignore[reportPrivateUsage]
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #

_PRINCIPAL = uuid4()


def _hlc(ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=ms, logical=0)


def _signal(n: int, *, audience: Audience | None = None) -> RealtimeSignal:
    return RealtimeSignal.of(audience or Audience.principal(str(_PRINCIPAL)), "e", {"n": n})


async def _seed(mailbox: InMemoryRealtimeMailbox, count: int = 3) -> list[str]:
    ids = [str(UUID(int=i + 1)) for i in range(count)]

    for i, event_id in enumerate(ids):
        await mailbox.store(
            principal=str(_PRINCIPAL), event_id=event_id, hlc=_hlc(i + 1), signal=_signal(i)
        )

    return ids


class _BindIdentity:
    """Test stand-in for the security middleware: binds a fixed identity per request."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        ctx: ExecutionContext,
        authn: AuthnIdentity | None,
        tenant: TenantIdentity | None = None,
    ) -> None:
        self.app = app
        self.ctx = ctx
        self.authn = authn
        self.tenant = tenant

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or self.authn is None:
            await self.app(scope, receive, send)
            return

        with self.ctx.inv_ctx.bind_identity(authn=self.authn, tenant=self.tenant):
            await self.app(scope, receive, send)


def _build_client(
    *,
    mailbox: InMemoryRealtimeMailbox,
    cursors: InMemoryMailboxCursors,
    hub: RealtimeSseHub | None = None,
    authenticated: bool = True,
) -> TestClient:
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=lambda: ctx,
        mailbox_factory=lambda _ctx: mailbox,
        cursors_factory=lambda _ctx: cursors,
        hub=hub,
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.add_middleware(
        _BindIdentity,  # type: ignore[arg-type]
        ctx=ctx,
        authn=AuthnIdentity(principal_id=_PRINCIPAL) if authenticated else None,
    )

    return TestClient(app)


def _frames(body: str) -> list[dict[str, Any]]:
    """Parse SSE frames into ``{id?, event, data}`` dicts (comments skipped)."""

    parsed: list[dict[str, Any]] = []

    for block in body.split("\n\n"):
        if not block.strip() or block.startswith(":"):
            continue

        frame: dict[str, Any] = {}

        for line in block.splitlines():
            field, _, value = line.partition(": ")
            frame[field] = json.loads(value) if field == "data" else value

        parsed.append(frame)

    return parsed


# ----------------------- #


class TestReplay:
    async def test_full_backlog_replayed_as_catch_up(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        ids = await _seed(mailbox)
        client = _build_client(mailbox=mailbox, cursors=InMemoryMailboxCursors())

        response = client.get("/realtime/sse")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert response.headers["cache-control"] == "no-store"

        frames = _frames(response.text)
        assert [f["id"] for f in frames] == ids
        assert [f["event"] for f in frames] == ["e", "e", "e"]
        # the shared {id, data} envelope — identical to the Socket.IO transport
        assert frames[0]["data"] == {"id": ids[0], "data": {"n": 0}}

    async def test_last_event_id_beats_the_stored_cursor(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        ids = await _seed(mailbox)
        client = _build_client(mailbox=mailbox, cursors=InMemoryMailboxCursors())

        response = client.get("/realtime/sse", headers={"Last-Event-ID": ids[1]})

        assert [f["id"] for f in _frames(response.text)] == [ids[2]]

    async def test_unretained_last_event_id_falls_back_to_the_cursor(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        ids = await _seed(mailbox)
        client = _build_client(mailbox=mailbox, cursors=InMemoryMailboxCursors())

        response = client.get("/realtime/sse", headers={"Last-Event-ID": "not-retained"})

        # no cursor either — the whole backlog replays; the client dedups by id
        assert [f["id"] for f in _frames(response.text)] == ids


class TestAck:
    async def test_ack_advances_the_cursor_for_the_next_replay(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        cursors = InMemoryMailboxCursors()
        ids = await _seed(mailbox)
        client = _build_client(mailbox=mailbox, cursors=cursors)

        acked = client.post(
            "/realtime/sse/ack", json={"up_to": ids[1]}, params={"device_id": "d1"}
        )
        assert acked.status_code == 200
        assert acked.json() == {"acked": True}

        response = client.get("/realtime/sse", params={"device_id": "d1"})
        assert [f["id"] for f in _frames(response.text)] == [ids[2]]

    async def test_ack_of_an_unretained_id_reports_false(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        await _seed(mailbox)
        client = _build_client(mailbox=mailbox, cursors=InMemoryMailboxCursors())

        acked = client.post(
            "/realtime/sse/ack", json={"up_to": "gone"}, params={"device_id": "d1"}
        )
        assert acked.json() == {"acked": False}

    async def test_truncated_replay_ends_the_stream_instead_of_entering_live(self) -> None:
        # A replay that stopped at the mailbox cap with entries still retained past
        # it: entering the live tail would skip that undelivered middle, and a later
        # unclamped ack would let the trim delete it. The stream ends instead — the
        # browser reconnects with Last-Event-ID and continues from exactly here.
        # (Without the guard this request would hang in the live tail. An
        # exactly-drained cap-filled replay proceeds to the live tail, which the sync
        # TestClient cannot drive — that classification is pinned by the shared
        # probe's tests and the Socket.IO clamp tests.)
        inner = InMemoryRealtimeMailbox()
        ids = await _seed(inner)  # 3 entries retained

        class _CapTruncatedMailbox:
            cap = 2  # the replay window stops here, one entry short of the tail

            async def replay_since(self, *, principal: str, since: Any) -> Any:
                from contextlib import aclosing

                delivered = 0

                # aclosing: the early cap-return must close the inner stream
                # deterministically, mirroring iter_replay one level up.
                async with aclosing(
                    inner.replay_since(principal=principal, since=since)
                ) as entries:
                    async for entry in entries:
                        if delivered >= self.cap:
                            return

                        delivered += 1
                        yield entry

            def __getattr__(self, name: str) -> Any:
                return getattr(inner, name)

        hub = RealtimeSseHub()
        hub.ready.set()
        client = _build_client(
            mailbox=_CapTruncatedMailbox(),  # type: ignore[arg-type]
            cursors=InMemoryMailboxCursors(),
            hub=hub,
        )

        response = client.get("/realtime/sse")  # completes despite the wired live hub

        assert [f["id"] for f in _frames(response.text)] == ids[:2]

    async def test_device_less_ack_is_refused(self) -> None:
        # Device-less streams share one per-principal fallback cursor: a cumulative ack
        # from one tab would advance the shared trim floor over another tab's
        # undelivered backlog, which the all-device trim hard-deletes. Fail closed.
        mailbox = InMemoryRealtimeMailbox()
        ids = await _seed(mailbox)
        client = _build_client(mailbox=mailbox, cursors=InMemoryMailboxCursors())

        response = client.post("/realtime/sse/ack", json={"up_to": ids[0]})

        assert response.status_code == 422
        assert "requires ?device_id" in response.text

    async def test_device_scoped_cursor_via_query_param(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        cursors = InMemoryMailboxCursors()
        ids = await _seed(mailbox)
        client = _build_client(mailbox=mailbox, cursors=cursors)

        # register the lagging device first so the trim floor respects it
        client.post("/realtime/sse/ack", json={"up_to": ids[0]}, params={"device_id": "d2"})
        client.post("/realtime/sse/ack", json={"up_to": ids[1]}, params={"device_id": "d1"})

        d1 = client.get("/realtime/sse", params={"device_id": "d1"})
        d2 = client.get("/realtime/sse", params={"device_id": "d2"})

        assert [f["id"] for f in _frames(d1.text)] == [ids[2]]
        assert [f["id"] for f in _frames(d2.text)] == ids[1:]


class TestHandshake:
    def test_unauthenticated_is_refused(self) -> None:
        client = _build_client(
            mailbox=InMemoryRealtimeMailbox(),
            cursors=InMemoryMailboxCursors(),
            authenticated=False,
        )

        assert client.get("/realtime/sse").status_code == 401
        assert client.post("/realtime/sse/ack", json={"up_to": "x"}).status_code == 401

    def test_unsupported_protocol_is_refused(self) -> None:
        client = _build_client(
            mailbox=InMemoryRealtimeMailbox(), cursors=InMemoryMailboxCursors()
        )

        response = client.get("/realtime/sse", params={"protocol": "2"})

        assert response.status_code >= 400
        assert response.headers[ERROR_CODE_HEADER] == "realtime_protocol_unsupported"

    def test_missing_or_current_protocol_accepted(self) -> None:
        client = _build_client(
            mailbox=InMemoryRealtimeMailbox(), cursors=InMemoryMailboxCursors()
        )

        assert client.get("/realtime/sse").status_code == 200
        assert client.get("/realtime/sse", params={"protocol": "1"}).status_code == 200

    def test_invalid_keepalive_is_refused_at_attach(self) -> None:
        with pytest.raises(CoreException):
            attach_realtime_sse_route(
                APIRouter(),
                ctx_dep=lambda: None,  # type: ignore[arg-type, return-value]
                mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
                cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
                keepalive_interval=timedelta(0),
            )


# ----------------------- #


class TestHub:
    def test_principal_match_is_tenant_strict(self) -> None:
        hub = RealtimeSseHub()
        tenant = uuid4()
        sub = hub.subscribe(principal="p1", tenant=tenant)

        hub.publish(_signal(1, audience=Audience.principal("p1")), tenant, event_id="e1")
        hub.publish(_signal(2, audience=Audience.principal("p1")), None, event_id="e2")
        hub.publish(_signal(3, audience=Audience.principal("p2")), tenant, event_id="e3")

        assert sub.queue.qsize() == 1
        signal, event_id = sub.queue.get_nowait()
        assert (signal.payload, event_id) == ({"n": 1}, "e1")

    def test_topic_match_requires_subscription(self) -> None:
        hub = RealtimeSseHub()
        sub = hub.subscribe(principal="p1", tenant=None, topics=frozenset({"t1"}))

        hub.publish(_signal(1, audience=Audience.topic("t1")), None, event_id=None)
        hub.publish(_signal(2, audience=Audience.topic("t2")), None, event_id=None)

        assert sub.queue.qsize() == 1

    def test_full_queue_drops_and_counts(self) -> None:
        hub = RealtimeSseHub(queue_size=1)
        sub = hub.subscribe(principal="p1", tenant=None)
        audience = Audience.principal("p1")

        hub.publish(_signal(1, audience=audience), None, event_id=None)
        hub.publish(_signal(2, audience=audience), None, event_id=None)

        assert sub.queue.qsize() == 1
        assert hub.dropped == 1

    def test_unsubscribe_is_idempotent(self) -> None:
        hub = RealtimeSseHub()
        sub = hub.subscribe(principal="p1", tenant=None)

        assert hub.subscribers == 1
        hub.unsubscribe(sub)
        hub.unsubscribe(sub)
        assert hub.subscribers == 0

    def test_non_positive_queue_size_refused(self) -> None:
        with pytest.raises(CoreException):
            RealtimeSseHub(queue_size=0)


# ----------------------- #


class TestHubReadyGate:
    async def test_ready_hub_admits_immediately(self) -> None:
        from forze_fastapi.realtime.sse import (
            _await_hub_ready,  # pyright: ignore[reportPrivateUsage]
        )

        hub = RealtimeSseHub()  # ready from construction (manual/test mode)
        await asyncio.wait_for(_await_hub_ready(hub), timeout=1)

    async def test_replay_waits_for_the_fast_forward_to_finish(self) -> None:
        from forze_fastapi.realtime.sse import (
            _await_hub_ready,  # pyright: ignore[reportPrivateUsage]
        )

        hub = RealtimeSseHub()
        hub.ready.clear()  # the tail is still fast-forwarding

        gate = asyncio.ensure_future(_await_hub_ready(hub))
        await asyncio.sleep(0.05)
        assert not gate.done()  # the replay cursor must not resolve yet

        hub.ready.set()
        await asyncio.wait_for(gate, timeout=5)

    async def test_never_ready_hub_fails_open_after_the_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import forze_fastapi.realtime.sse as sse_module

        monkeypatch.setattr(sse_module, "_HUB_READY_TIMEOUT", 0.05)

        hub = RealtimeSseHub()
        hub.ready.clear()  # miswired: hub configured but no tail step feeds it

        # proceeds (catch-up quality, logged) instead of hanging the connect
        await asyncio.wait_for(sse_module._await_hub_ready(hub), timeout=5)  # pyright: ignore[reportPrivateUsage]


class TestLiveFrames:
    async def test_matched_signal_becomes_a_frame(self) -> None:
        hub = RealtimeSseHub()
        sub = hub.subscribe(principal="p1", tenant=None)
        hub.publish(_signal(7, audience=Audience.principal("p1")), None, event_id="evt-7")

        frames = _live_frames(sub, keepalive_interval=timedelta(seconds=5))
        frame = await asyncio.wait_for(anext(frames), timeout=5)
        await frames.aclose()

        parsed = _frames(frame)[0]
        assert parsed["id"] == "evt-7"
        assert parsed["data"] == {"id": "evt-7", "data": {"n": 7}}

    async def test_idle_stream_emits_keepalive_comments(self) -> None:
        hub = RealtimeSseHub()
        sub = hub.subscribe(principal="p1", tenant=None)

        frames = _live_frames(sub, keepalive_interval=timedelta(milliseconds=20))
        first = await asyncio.wait_for(anext(frames), timeout=5)

        assert first == ": keepalive\n\n"

        # a signal published after keepalives still arrives on the same pending get
        hub.publish(_signal(1, audience=Audience.principal("p1")), None, event_id=None)
        following = await asyncio.wait_for(anext(frames), timeout=5)
        while following.startswith(":"):
            following = await asyncio.wait_for(anext(frames), timeout=5)
        await frames.aclose()

        assert _frames(following)[0]["data"] == {"id": None, "data": {"n": 1}}

    async def test_ephemeral_frame_has_no_id_field(self) -> None:
        hub = RealtimeSseHub()
        sub = hub.subscribe(principal="p1", tenant=None)
        hub.publish(_signal(1, audience=Audience.principal("p1")), None, event_id=None)

        frames = _live_frames(sub, keepalive_interval=timedelta(seconds=5))
        frame = await asyncio.wait_for(anext(frames), timeout=5)
        await frames.aclose()

        assert not frame.startswith("id:")  # nothing for Last-Event-ID to anchor on
        assert _frames(frame)[0]["data"]["id"] is None
