"""The raw-WebSocket realtime transport — replay, acks, live leg, duplex commands.

# covers: forze_fastapi.realtime.ws (attach_realtime_ws_route: handshake refusals,
#         replay + Last-Event-ID, ack/reauth frames, cmd dispatch + error acks,
#         in-flight and frame-size limits, presence, attach validation)

Driven end-to-end through Starlette's duplex websocket TestClient against the real
route — the same envelope as SSE/Socket.IO with the event name in-band, refusals as
policy closes carrying the client-safe summary, and command dispatch through the
same frozen-registry discipline as HTTP.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import OperationRegistry
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    RealtimeCommandRoute,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp
from forze_fastapi.realtime import (
    RealtimeSseHub,
    WsConnect,
    WsConnection,
    attach_realtime_ws_route,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

# ----------------------- #

_PRINCIPAL = uuid4()


class _CreateNote(BaseModel):
    text: str


class _NoteAck(BaseModel):
    note_id: str


async def _create_note(args: _CreateNote) -> _NoteAck:
    if args.text == "boom":
        raise exc.precondition("Note text is cursed", code="note_cursed")

    return _NoteAck(note_id=f"note:{args.text}")


async def _note_when(_args: _CreateNote) -> Any:
    from datetime import datetime, timezone

    # an untyped ack (ack_type=None) passes through parse_ack verbatim — a datetime
    # here is exactly the non-JSON value the serialization guard must contain
    return {"at": datetime(2026, 1, 1, tzinfo=timezone.utc)}


def _registry() -> Any:
    return (
        OperationRegistry()
        .set_handler("note.create", lambda _ctx: _create_note)
        .set_handler("note.when", lambda _ctx: _note_when)
        .freeze()
    )


_COMMANDS = (
    RealtimeCommandRoute[Any, Any](
        event="note.create",
        operation="note.create",
        payload_type=_CreateNote,
        ack_type=_NoteAck,
    ),
    RealtimeCommandRoute[Any, Any](
        event="note.when",
        operation="note.when",
        payload_type=_CreateNote,  # untyped ack: no ack_type
    ),
)


async def _resolver(connect: WsConnect) -> WsConnection | None:
    # connect: token from the upgrade query · reauth: token from the frame's auth payload
    auth = connect.auth or {}
    token = auth.get("token") or connect.websocket.query_params.get("token")

    if token == "anon":
        return None

    if token == "bad":
        raise exc.authentication("Bad realtime token")

    if token == "other":
        return WsConnection(authn=AuthnIdentity(principal_id=uuid4()))

    if token == "expired":
        from datetime import timedelta

        from forze.base.primitives import utcnow

        return WsConnection(
            authn=AuthnIdentity(principal_id=_PRINCIPAL),
            expires_at=utcnow() - timedelta(seconds=1),
        )

    device = auth.get("device_id") or connect.websocket.query_params.get("device_id")

    return WsConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL),
        client=ClientIdentity(device_id=device) if device else None,
    )


async def _allow_all(
    _ctx: ExecutionContext, _principal: str, _tenant: UUID | None, requested: frozenset[str]
) -> frozenset[str]:
    return requested


def _build(
    *,
    mailbox: InMemoryRealtimeMailbox | None = None,
    hub: RealtimeSseHub | None = None,
    presence: Any = None,
    with_commands: bool = False,
    **attach_kwargs: Any,
) -> tuple[TestClient, InMemoryRealtimeMailbox]:
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    mailbox = mailbox if mailbox is not None else InMemoryRealtimeMailbox()
    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=lambda: ctx,
        resolve=_resolver,
        mailbox_factory=lambda _ctx: mailbox,
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        hub=hub,
        presence=presence,
        authorize_topics=_allow_all,
        registry=_registry() if with_commands else None,
        commands=_COMMANDS if with_commands else None,
        **attach_kwargs,
    )

    app = FastAPI()
    app.include_router(router)

    if hub is not None:
        # a loop-side publish trigger: the test thread must not touch the app
        # loop's asyncio primitives directly
        @app.post("/publish")
        async def publish(body: dict[str, Any]) -> dict[str, bool]:  # pyright: ignore[reportUnusedFunction]
            hub.publish(
                RealtimeSignal.of(Audience.principal(str(_PRINCIPAL)), "e", body["payload"]),
                None,
                event_id=body.get("event_id"),
            )
            return {"ok": True}

    return TestClient(app), mailbox


def _hlc(ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=ms, logical=0)


async def _seed(mailbox: InMemoryRealtimeMailbox, count: int = 3) -> list[str]:
    ids = [str(UUID(int=i + 1)) for i in range(count)]

    for i, event_id in enumerate(ids):
        await mailbox.store(
            principal=str(_PRINCIPAL),
            event_id=event_id,
            hlc=_hlc(i + 1),
            signal=RealtimeSignal.of(Audience.principal(str(_PRINCIPAL)), "e", {"n": i}),
        )

    return ids


# ----------------------- #


class TestHandshake:
    def test_anonymous_is_refused_with_policy_close(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws?token=anon") as ws:
            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == 1008
        assert "authenticated principal" in str(caught.value.reason)

    def test_resolver_refusal_carries_the_client_safe_summary(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws?token=bad") as ws:
            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == 1008
        assert "Bad realtime token" in str(caught.value.reason)

    def test_unsupported_protocol_is_refused(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws?protocol=2") as ws:
            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == 1008
        assert "Unsupported realtime protocol" in str(caught.value.reason)


class TestReplayAndAck:
    def test_replay_then_ack_then_resume(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        client, _ = _build(mailbox=mailbox)
        import asyncio

        ids = asyncio.run(_seed(mailbox))

        with client.websocket_connect("/realtime/ws?device_id=d1") as ws:
            frames = [ws.receive_json() for _ in range(3)]
            assert [f["id"] for f in frames] == ids
            assert frames[0] == {"event": "e", "id": ids[0], "data": {"n": 0}}

            ws.send_json({"type": "realtime.ack", "up_to": ids[1]})

        # the ack advanced d1's cursor: a reconnect replays only the tail
        with client.websocket_connect("/realtime/ws?device_id=d1") as ws:
            assert ws.receive_json()["id"] == ids[2]

    def test_malformed_acks_error_instead_of_silently_dropping(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws") as ws:
            for bad_up_to in (None, "", 7, {"id": "x"}, ["x"]):
                ws.send_json({"type": "realtime.ack", "up_to": bad_up_to})
                frame = ws.receive_json()
                assert frame["type"] == "error"
                assert frame["error"]["code"] == "realtime_invalid_frame"

    def test_ack_follows_a_reauth_rotated_client_identity(self) -> None:
        import asyncio

        mailbox = InMemoryRealtimeMailbox()
        cursors = InMemoryMailboxCursors()
        ids = asyncio.run(_seed(mailbox, count=2))

        ctx = context_from_deps(MockDepsModule(state=MockState())())
        router = APIRouter()
        attach_realtime_ws_route(
            router,
            ctx_dep=lambda: ctx,
            resolve=_resolver,
            mailbox_factory=lambda _ctx: mailbox,
            cursors_factory=lambda _ctx: cursors,
        )
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        with client.websocket_connect("/realtime/ws?device_id=d1") as ws:
            assert [ws.receive_json()["id"] for _ in range(2)] == ids

            # rotate the client identity in place, then ack — the cursor must
            # belong to the refreshed device, not the connect-time closure
            ws.send_json(
                {"type": "realtime.reauth", "cid": "r", "auth": {"device_id": "d2"}}
            )
            assert ws.receive_json()["data"] == {"ok": True}
            ws.send_json({"type": "realtime.ack", "up_to": ids[0]})

        async def _positions() -> tuple[Any, Any]:
            return (
                await cursors.get(principal=str(_PRINCIPAL), client_key="d2"),
                await cursors.get(principal=str(_PRINCIPAL), client_key="d1"),
            )

        d2_cursor, d1_cursor = asyncio.run(_positions())
        assert d2_cursor is not None  # the ack landed on the refreshed identity...
        assert d1_cursor is None  # ...never on the connect-time device

    def test_failing_ack_store_costs_one_error_frame_not_the_connection(self) -> None:
        class _FlakyMailbox(InMemoryRealtimeMailbox):
            async def position_of(self, *, principal: str, event_id: str) -> Any:
                raise RuntimeError("cursor store down")

        client, _ = _build(mailbox=_FlakyMailbox())

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json({"type": "realtime.ack", "up_to": "evt-1"})
            frame = ws.receive_json()
            assert frame["type"] == "error"
            assert frame["error"]["kind"] == "internal"  # masked server-side failure

            # the connection survived — the next frame is still served
            ws.send_json({"type": "mystery"})
            assert ws.receive_json()["error"]["code"] == "realtime_invalid_frame"

    def test_last_event_id_query_param_resumes(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        client, _ = _build(mailbox=mailbox)
        import asyncio

        ids = asyncio.run(_seed(mailbox))

        with client.websocket_connect(f"/realtime/ws?last_event_id={ids[0]}") as ws:
            assert [ws.receive_json()["id"] for _ in range(2)] == ids[1:]


class TestLiveLeg:
    def test_live_signals_flow_after_replay(self) -> None:
        hub = RealtimeSseHub()
        mailbox = InMemoryRealtimeMailbox()
        client, _ = _build(mailbox=mailbox, hub=hub)
        import asyncio

        ids = asyncio.run(_seed(mailbox, count=1))

        with client.websocket_connect("/realtime/ws") as ws:
            assert ws.receive_json()["id"] == ids[0]  # replay first

            client.post("/publish", json={"payload": {"n": 7}, "event_id": "evt-7"})
            live = ws.receive_json()
            assert live == {"event": "e", "id": "evt-7", "data": {"n": 7}}


class TestCommands:
    def test_dispatch_acks_with_the_typed_result(self) -> None:
        client, _ = _build(with_commands=True)

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json(
                {"type": "cmd", "event": "note.create", "cid": "c1", "payload": {"text": "hi"}}
            )
            ack = ws.receive_json()

        assert ack == {"type": "ack", "cid": "c1", "data": {"note_id": "note:hi"}}

    def test_handler_core_exception_becomes_an_error_ack(self) -> None:
        client, _ = _build(with_commands=True)

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json(
                {"type": "cmd", "event": "note.create", "cid": "c2", "payload": {"text": "boom"}}
            )
            ack = ws.receive_json()

        assert ack["type"] == "ack" and ack["cid"] == "c2"
        assert ack["error"]["code"] == "note_cursed"
        assert ack["error"]["kind"] == "precondition"

    def test_invalid_payload_is_a_sanitized_validation_ack(self) -> None:
        client, _ = _build(with_commands=True)

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json({"type": "cmd", "event": "note.create", "cid": "c3", "payload": {}})
            ack = ws.receive_json()

        assert ack["error"]["code"] == "realtime_invalid_payload"

    def test_unknown_command_is_refused(self) -> None:
        client, _ = _build(with_commands=True)

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json({"type": "cmd", "event": "nope", "cid": "c4", "payload": {}})
            ack = ws.receive_json()

        assert ack["error"]["code"] == "realtime_command_unknown"

    def test_unknown_frame_type_and_non_object_frames_error(self) -> None:
        client, _ = _build(with_commands=True)

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json({"type": "mystery"})
            assert ws.receive_json()["error"]["code"] == "realtime_invalid_frame"

            ws.send_text("[1, 2, 3]")
            assert ws.receive_json()["error"]["code"] == "realtime_invalid_frame"

    def test_malformed_governance_fields_are_refused_not_weakened(self) -> None:
        client, _ = _build(with_commands=True)

        with client.websocket_connect("/realtime/ws") as ws:
            for field, value in (
                ("idempotency_key", {"k": 1}),  # would dedup on a repr string
                ("idempotency_key", 42),
                ("deadline_budget", "5.0"),  # would silently bind no deadline
                ("deadline_budget", True),
                ("deadline_budget", -1),
            ):
                ws.send_json(
                    {
                        "type": "cmd",
                        "event": "note.create",
                        "cid": "g",
                        "payload": {"text": "hi"},
                        field: value,
                    }
                )
                ack = ws.receive_json()
                assert ack["cid"] == "g"
                assert ack["error"]["code"] == "realtime_invalid_frame"

            # valid governance fields still dispatch
            ws.send_json(
                {
                    "type": "cmd",
                    "event": "note.create",
                    "cid": "ok",
                    "payload": {"text": "hi"},
                    "idempotency_key": "k-1",
                    "deadline_budget": 5,
                }
            )
            assert ws.receive_json()["data"] == {"note_id": "note:hi"}

    def test_inflight_limit_error_acks_without_running(self) -> None:
        import asyncio as aio

        async def _slow(args: _CreateNote) -> _NoteAck:
            await aio.sleep(30)
            return _NoteAck(note_id="never")  # pragma: no cover - cancelled at close

        registry = OperationRegistry().set_handler("slow", lambda _ctx: _slow).freeze()
        commands = (
            RealtimeCommandRoute[Any, Any](
                event="slow", operation="slow", payload_type=_CreateNote
            ),
        )
        ctx = context_from_deps(MockDepsModule(state=MockState())())
        router = APIRouter()
        attach_realtime_ws_route(
            router,
            ctx_dep=lambda: ctx,
            resolve=_resolver,
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
            cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
            registry=registry,
            commands=commands,
            max_inflight_commands=1,
        )
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        with client.websocket_connect("/realtime/ws") as ws:
            # s1 occupies the single slot (its handler sleeps); s2 must be refused
            # without running, not queued behind it
            ws.send_json({"type": "cmd", "event": "slow", "cid": "s1", "payload": {"text": "x"}})
            ws.send_json({"type": "cmd", "event": "slow", "cid": "s2", "payload": {"text": "x"}})

            ack = ws.receive_json()
            assert ack["cid"] == "s2"
            assert ack["error"]["code"] == "realtime_commands_limit"

    def test_oversized_frame_closes_the_socket(self) -> None:
        client, _ = _build(with_commands=True, max_frame_bytes=64)

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_text(json.dumps({"type": "cmd", "payload": "x" * 200}))

            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == 1009


class TestReauth:
    def test_same_principal_reauth_acks_ok(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json({"type": "realtime.reauth", "cid": "r1", "auth": {"token": "fresh"}})
            ack = ws.receive_json()

        assert ack == {"type": "ack", "cid": "r1", "data": {"ok": True}}

    def test_reauth_to_a_different_principal_is_refused(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json({"type": "realtime.reauth", "cid": "r2", "auth": {"token": "other"}})
            ack = ws.receive_json()

        assert ack["cid"] == "r2"
        assert "same principal" in ack["error"]["detail"]
        assert ack["error"]["kind"] == "authentication"

    def test_reauth_resolving_anonymous_is_refused(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_json({"type": "realtime.reauth", "cid": "r3", "auth": {"token": "anon"}})
            ack = ws.receive_json()

        assert ack["cid"] == "r3" and ack["error"]["kind"] == "authentication"


class TestWiring:
    def test_commands_without_registry_are_refused(self) -> None:
        with pytest.raises(CoreException):
            attach_realtime_ws_route(
                APIRouter(),
                ctx_dep=lambda: None,  # type: ignore[arg-type, return-value]
                resolve=_resolver,
                mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
                cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
                commands=_COMMANDS,
            )

    def test_duplicate_command_events_are_refused(self) -> None:
        with pytest.raises(CoreException):
            attach_realtime_ws_route(
                APIRouter(),
                ctx_dep=lambda: None,  # type: ignore[arg-type, return-value]
                resolve=_resolver,
                mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
                cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
                registry=_registry(),
                commands=(*_COMMANDS, *_COMMANDS),
            )

    @pytest.mark.parametrize(
        "kwargs",
        [{"max_topics": 0}, {"max_frame_bytes": 0}, {"max_inflight_commands": 0}],
    )
    def test_non_positive_limits_are_refused(self, kwargs: dict[str, int]) -> None:
        with pytest.raises(CoreException):
            attach_realtime_ws_route(
                APIRouter(),
                ctx_dep=lambda: None,  # type: ignore[arg-type, return-value]
                resolve=_resolver,
                mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
                cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
                **kwargs,
            )


class _RecordingPresence:
    def __init__(self) -> None:
        self.joins: list[tuple[str, str]] = []
        self.leaves: list[tuple[str, str]] = []

    async def joined(self, room: str, sid: str) -> None:
        self.joins.append((room, sid))

    async def left(self, room: str, sid: str) -> None:
        self.leaves.append((room, sid))

    async def count(self, room: str) -> int:
        return 0


class TestPresence:
    def test_connection_joins_and_leaves_its_rooms(self) -> None:
        presence = _RecordingPresence()
        client, _ = _build(presence=presence)

        with client.websocket_connect("/realtime/ws?topics=t1") as ws:
            ws.send_json({"type": "realtime.ack", "up_to": "noop"})  # keep the socket busy

        assert [room for room, _ in presence.joins] == [
            f"principal:{_PRINCIPAL}",
            "topic:t1",
        ]
        assert [room for room, _ in presence.leaves] == [room for room, _ in presence.joins]


# ----------------------- #
# perimeter: origin allowlist + credential expiry


class TestOriginAllowlist:
    def test_disallowed_origin_is_refused_with_policy_close(self) -> None:
        client, _ = _build(allowed_origins=["https://app.example.com"])

        with client.websocket_connect(
            "/realtime/ws", headers={"Origin": "https://evil.example"}
        ) as ws:
            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == 1008
        assert "Origin" in str(caught.value.reason)

    def test_allowed_origin_connects(self) -> None:
        client, _ = _build(allowed_origins=["https://app.example.com"])

        with client.websocket_connect(
            "/realtime/ws", headers={"Origin": "https://app.example.com"}
        ) as ws:
            ws.send_text(json.dumps({"type": "nope"}))
            frame = ws.receive_json()  # a live socket answers with an error frame

        assert frame["type"] == "error"

    def test_missing_origin_is_a_non_browser_client_and_passes(self) -> None:
        client, _ = _build(allowed_origins=["https://app.example.com"])

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_text(json.dumps({"type": "nope"}))
            frame = ws.receive_json()

        assert frame["type"] == "error"

    def test_empty_allowlist_is_refused_at_attach(self) -> None:
        with pytest.raises(CoreException, match="allowed_origins"):
            _build(allowed_origins=[])


class TestCredentialExpiry:
    def test_expired_credential_closes_the_socket(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws?token=expired") as ws:
            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == 1008
        assert "credential expired" in str(caught.value.reason)

    def test_unexpiring_credential_stays_connected(self) -> None:
        client, _ = _build()

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_text(json.dumps({"type": "nope"}))
            frame = ws.receive_json()

        assert frame["type"] == "error"


# ----------------------- #
# hostile frames must cost the frame (or a deliberate close), never a teardown


class TestHostileFrames:
    def test_binary_frame_closes_deliberately_with_1003(self) -> None:
        # receive_text would surface a binary frame as a KeyError escaping every
        # except* clause — the route must instead refuse it with a clean 1003 close
        client, _ = _build()

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_bytes(b"\x00\x01\x02")

            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()

        assert caught.value.code == 1003

    def test_unserializable_ack_costs_an_error_ack_not_the_connection(self) -> None:
        # note.when returns a datetime through an untyped ack: json.dumps raises AFTER
        # guard_frame — unguarded, that TypeError would cancel every in-flight command
        client, _ = _build(with_commands=True)

        with client.websocket_connect("/realtime/ws") as ws:
            ws.send_text(
                json.dumps(
                    {"type": "cmd", "event": "note.when", "cid": 1, "payload": {"text": "x"}}
                )
            )
            frame = ws.receive_json()

            # the internal-kind detail is scrubbed on the wire (no server internals
            # leak); the code still names the failure for the client
            assert frame == {
                "type": "ack",
                "cid": 1,
                "error": {
                    "detail": "Internal server error",
                    "code": "realtime_ack_unserializable",
                    "kind": "internal",
                },
            }

            # the socket survives: the next command still dispatches normally
            ws.send_text(
                json.dumps(
                    {"type": "cmd", "event": "note.create", "cid": 2, "payload": {"text": "ok"}}
                )
            )
            after = ws.receive_json()

        assert after == {"type": "ack", "cid": 2, "data": {"note_id": "note:ok"}}
