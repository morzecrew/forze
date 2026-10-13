"""The fan-out managers send what the library's manager sends, without a task per socket."""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from typing import Any
from unittest.mock import patch

import pytest
from socketio.async_manager import AsyncManager
from socketio.async_server import AsyncServer

from forze_socketio import FanoutManager, FanoutRedisManager, build_socketio_server

# ----------------------- #

NS = "/lobby"


async def _server(manager: AsyncManager) -> tuple[AsyncServer, dict[str, list[Any]]]:
    """A server whose Engine.IO sends are recorded per socket, with six sockets in rooms."""

    server = AsyncServer(client_manager=manager)
    sent: dict[str, list[Any]] = defaultdict(list)

    async def _send(eio_sid: str, pkt: Any) -> None:
        sent[eio_sid].append(pkt.encode())

    server._send_eio_packet = _send  # type: ignore[method-assign]

    for index in range(6):
        sid = await manager.connect(f"eio-{index}", NS)

        for room in ("even" if index % 2 == 0 else "odd", "all"):
            # The pub/sub manager's is a coroutine, the in-process one's is not.
            entered = manager.enter_room(sid, NS, room)

            if inspect.isawaitable(entered):
                await entered

    assert len(manager.rooms[NS]["all"]) == 6
    return server, sent


def _sid(manager: AsyncManager, eio_sid: str) -> str:
    sid = manager.sid_from_eio_sid(eio_sid, NS)
    assert sid is not None
    return sid


EMITS: list[dict[str, Any]] = [
    {"event": "chat", "data": {"text": "hi"}, "room": "all"},
    {"event": "chat", "data": ("a", 2, None), "room": "even"},
    {"event": "ping", "data": None, "room": "odd"},
    {"event": "blob", "data": b"\x00\x01binary", "room": "all"},
    {"event": "mixed", "data": {"pic": b"\xff", "n": 1}, "room": ["odd", "even"]},
    {"event": "chat", "data": "everyone", "room": None},
    {"event": "chat", "data": "addressed", "to": "odd"},
    {"event": "chat", "data": "nobody", "room": "no-such-room"},
]


@pytest.mark.parametrize("emit", EMITS)
async def test_the_fan_out_sends_what_the_library_sends(emit: dict[str, Any]) -> None:
    library, library_sent = await _server(AsyncManager())
    fanout, fanout_sent = await _server(FanoutManager())

    await library.manager.emit(namespace=NS, **emit)
    await fanout.manager.emit(namespace=NS, **emit)

    assert fanout_sent == library_sent


async def test_the_fan_out_skips_the_sids_it_is_told_to() -> None:
    library, library_sent = await _server(AsyncManager())
    fanout, fanout_sent = await _server(FanoutManager())

    for server in (library, fanout):
        manager = server.manager
        skip = [_sid(manager, "eio-0"), _sid(manager, "eio-2")]
        await manager.emit("chat", "x", NS, room="all", skip_sid=skip)
        await manager.emit("chat", "y", NS, room="all", skip_sid=_sid(manager, "eio-1"))

    assert fanout_sent == library_sent
    assert fanout_sent["eio-0"] == ['42/lobby,["chat","y"]']  # skipped the first, not the second


async def test_an_emit_to_an_unknown_namespace_sends_nothing() -> None:
    fanout, sent = await _server(FanoutManager())

    await fanout.manager.emit("chat", "x", "/elsewhere", room="all")

    assert sent == {}


async def test_the_fan_out_starts_no_task_per_socket() -> None:
    fanout, sent = await _server(FanoutManager())

    with patch.object(asyncio, "create_task", side_effect=AssertionError("a task")):
        await fanout.manager.emit("chat", "x", NS, room="all")

    assert len(sent) == 6


async def test_a_socket_that_fails_does_not_stop_the_rest() -> None:
    fanout, sent = await _server(FanoutManager())
    record = fanout._send_eio_packet

    async def _closed_one(eio_sid: str, pkt: Any) -> None:
        if eio_sid == "eio-2":
            raise ConnectionError("socket closed")

        await record(eio_sid, pkt)

    fanout._send_eio_packet = _closed_one  # type: ignore[method-assign]

    await fanout.manager.emit("chat", "x", NS, room="all")

    assert sorted(sent) == ["eio-0", "eio-1", "eio-3", "eio-4", "eio-5"]


async def test_an_emit_with_a_callback_keeps_the_library_path() -> None:
    fanout, _ = await _server(FanoutManager())

    with patch.object(AsyncManager, "emit", autospec=True) as library_emit:
        await fanout.manager.emit("ask", "x", NS, room="all", callback=lambda *_: None)

    library_emit.assert_awaited_once()


async def test_the_redis_backplane_delivers_its_share_through_the_fan_out() -> None:
    manager = FanoutRedisManager("redis://localhost:6379/0", write_only=False)
    server, sent = await _server(manager)

    # What a message arriving from the backplane does: local delivery via `super().emit`.
    with patch.object(asyncio, "create_task", side_effect=AssertionError("a task")):
        await manager._handle_emit({"event": "chat", "data": "x", "namespace": NS, "room": "all"})

    assert len(sent) == 6
    assert server.manager is manager


async def test_an_emit_on_the_redis_backplane_is_still_published() -> None:
    manager = FanoutRedisManager("redis://localhost:6379/0", write_only=False)
    _, sent = await _server(manager)
    published: list[dict[str, Any]] = []

    async def _publish(message: dict[str, Any]) -> None:
        published.append(message)

    with patch.object(manager, "_publish", _publish):
        await manager.emit("chat", "x", NS, room="all")

    assert [message["event"] for message in published] == ["chat"]
    assert len(sent) == 6  # and this host's share delivered locally


def test_the_builder_fans_out_by_default() -> None:
    assert isinstance(build_socketio_server().manager, FanoutManager)
    assert isinstance(
        build_socketio_server(redis_url="redis://localhost:6379/0").manager, FanoutRedisManager
    )


def test_a_manager_passed_in_is_kept() -> None:
    library = AsyncManager()

    assert build_socketio_server(client_manager=library).manager is library
