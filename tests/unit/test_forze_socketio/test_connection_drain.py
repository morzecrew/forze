"""Draining a replica's connections: the spread close, the drain step, refusals, the counts.

# covers: forze_socketio.drain (close_spread, connection_drain_lifecycle_step),
#         forze_socketio.connection (draining refusals, the late refusal's undo, counts)

Run against python-socketio's own ``AsyncServer`` and manager — the listing and the
disconnect path are the library's — with Engine.IO sockets stood in by a fake whose close
fires the library's own Engine.IO disconnect, as a real socket does.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from engineio.async_socket import AsyncSocket
from socketio.async_server import AsyncServer
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import build_runtime
from forze.application.execution.runtime import ExecutionRuntime
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    InMemoryRealtimePresence,
)
from forze.base.exceptions import GENERIC_INTERNAL_DETAIL, CoreException, ExceptionKind, exc
from forze_socketio import (
    CONNECTION_SESSION_KEY,
    IDENTITY_SESSION_KEY,
    RealtimeConnection,
    RealtimeConnectionStats,
    SocketIOConnect,
    attach_realtime_connection,
    close_spread,
    connection_drain_lifecycle_step,
)
from forze_socketio import connection as connection_module
from forze_socketio import drain as drain_module
from forze_socketio.drain import close_transport

# ----------------------- #

_PRINCIPAL = UUID("11111111-1111-1111-1111-111111111111")
_ROOM = RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL)).principal_room


class _Socket:
    """An Engine.IO socket stand-in: its close fires the server's Engine.IO disconnect."""

    def __init__(self, server: AsyncServer, eio_sid: str, *, fails: bool = False) -> None:
        self.server = server
        self.sid = eio_sid
        self.session: dict[str, Any] = {}
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.closed = False
        self.closing = False
        self.closes = 0
        self.fails = fails

    async def close(self, wait: bool = True, abort: bool = False, reason: Any = None) -> None:
        self.closes += 1
        self.closing = True

        if self.fails:
            raise ConnectionResetError("transport already gone")

        await self.server.eio._trigger_event("disconnect", self.sid, reason, run_async=False)
        self.closed = True


class _SlowPresence(InMemoryRealtimePresence):
    """A presence store slow to record a leave, as one may be in a drain's reconnect storm."""

    def __init__(self, *, leaves_in: float) -> None:
        super().__init__()
        self.leaves_in = leaves_in
        self.leaving = 0

    async def left(self, room: str, sid: str) -> None:
        self.leaving += 1
        await asyncio.sleep(self.leaves_in)
        await super().left(room, sid)


class _GatedPresence(InMemoryRealtimePresence):
    """Holds the first leave until released, so a close can be caught mid-handler."""

    def __init__(self) -> None:
        super().__init__()
        self.leaving = asyncio.Event()
        self.release = asyncio.Event()

    async def left(self, room: str, sid: str) -> None:
        if not self.leaving.is_set():
            self.leaving.set()
            await self.release.wait()

        await super().left(room, sid)


def _engine_socket(server: AsyncServer, eio_sid: str) -> AsyncSocket:
    """python-engineio's own socket, registered as its server would."""

    socket = AsyncSocket(server.eio, eio_sid)
    server.eio.sockets[eio_sid] = socket
    server.environ[eio_sid] = {}

    return socket


async def _until(check: Any, *, within: float) -> None:
    for _ in range(int(within / 0.01)):  # bounded
        if check():
            return

        await asyncio.sleep(0.01)

    raise AssertionError(f"not reached within {within}s")


async def _resolve(connect: SocketIOConnect) -> RealtimeConnection | None:
    token = (connect.auth or {}).get("token")

    if token == "anon":
        return None

    if token == "bad":
        raise exc.authentication("Invalid access token")

    if token == "boom":
        raise RuntimeError("resolver crashed")

    return RealtimeConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL), expires_at=datetime(2099, 1, 1, tzinfo=UTC)
    )


def _server() -> AsyncServer:
    return AsyncServer()


def _socket(server: AsyncServer, eio_sid: str, *, fails: bool = False) -> _Socket:
    if eio_sid not in server.eio.sockets:
        server.eio.sockets[eio_sid] = _Socket(server, eio_sid, fails=fails)
        server.environ[eio_sid] = {}

    return server.eio.sockets[eio_sid]


async def _connect(
    server: AsyncServer, eio_sid: str, namespace: str = "/", auth: Any = None
) -> str:
    """Connect *namespace* over Engine.IO connection *eio_sid*, as python-socketio would."""

    _socket(server, eio_sid)
    sid = await server.manager.connect(eio_sid, namespace)
    handler = server.handlers.get(namespace, {}).get("connect")

    if handler is not None:
        await handler(sid, {}, auth if auth is not None else {"token": "ok"})

    return sid


def _open(server: AsyncServer) -> set[str]:
    return {eio_sid for eio_sid, socket in server.eio.sockets.items() if not socket.closed}


def _runtime(*steps: Any) -> ExecutionRuntime:
    return build_runtime(lifecycle_steps=steps, drain_timeout=timedelta(0))


# ----------------------- #


class TestCloseSpread:
    async def test_targets_sharing_an_engine_io_connection_close_once(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        await _connect(server, "eio-1", "/")
        await _connect(server, "eio-1", "/chat")
        await _connect(server, "eio-2", "/")
        sockets = [_socket(server, "eio-1"), _socket(server, "eio-2")]

        closed = await close_spread(
            server,
            [("/", "eio-1"), ("/chat", "eio-1"), ("/", "eio-2")],
            timedelta(seconds=0.05),
            tail=timedelta(seconds=1),
            stats=stats,
        )

        assert closed == 2
        assert [socket.closes for socket in sockets] == [1, 1]
        assert stats.drain_closed == {"closed": 2, "failed": 0, "timed_out": 0}
        assert server.eio.sockets == {}
        # The writer's end-of-stream sentinel follows the close, so a WebSocket closes too.
        assert [socket.queue.get_nowait() for socket in sockets] == [None, None]

    # ....................... #

    async def test_a_failing_close_is_counted_and_not_raised(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        await _connect(server, "eio-1")
        _socket(server, "eio-2", fails=True)
        await _connect(server, "eio-2")

        closed = await close_spread(
            server,
            [("/", "eio-1"), ("/", "eio-2")],
            timedelta(seconds=0.05),
            tail=timedelta(seconds=1),
            stats=stats,
        )

        assert closed == 1
        assert stats.drain_closed == {"closed": 1, "failed": 1, "timed_out": 0}

    # ....................... #

    async def test_a_close_past_the_tail_finishes_its_handlers_rather_than_being_cut(
        self,
    ) -> None:
        server = _server()
        presence = _SlowPresence(leaves_in=0.4)
        stats = RealtimeConnectionStats()
        attach_realtime_connection(server, resolve=_resolve, presence=presence, stats=stats)
        await _connect(server, "eio-1")
        socket = _socket(server, "eio-1")

        closed = await close_spread(
            server,
            [("/", "eio-1")],
            timedelta(seconds=0.01),
            tail=timedelta(seconds=0.1),
            stats=stats,
        )

        assert closed == 0
        assert stats.drain_closed == {"closed": 0, "failed": 0, "timed_out": 1}

        # Not cancelled inside the app's disconnect handler, where python-socketio would
        # swallow the cancellation and leave the presence entry behind: it finishes.
        await _until(lambda: socket.closed, within=2.0)
        assert await presence.count(_ROOM) == 0
        assert socket.queue.get_nowait() is None  # and the writer is still ended

    # ....................... #

    async def test_cancelled_midway_it_lets_started_closes_finish_and_still_counts(
        self,
    ) -> None:
        server = _server()
        presence = _SlowPresence(leaves_in=0.3)
        stats = RealtimeConnectionStats()
        attach_realtime_connection(server, resolve=_resolve, presence=presence, stats=stats)

        for index in range(3):
            await _connect(server, f"eio-{index}")

        sockets = [_socket(server, f"eio-{index}") for index in range(3)]
        spreading = asyncio.ensure_future(
            close_spread(
                server,
                [("/", f"eio-{index}") for index in range(3)],
                timedelta(seconds=30),  # the second close would start 10 s in
                tail=timedelta(seconds=1),
                stats=stats,
            )
        )
        await _until(lambda: presence.leaving > 0, within=2.0)
        spreading.cancel()

        with pytest.raises(asyncio.CancelledError):
            await spreading

        assert stats.drain_closed == {"closed": 0, "failed": 0, "timed_out": 1}
        await _until(lambda: sockets[0].closed, within=2.0)
        assert [socket.closed for socket in sockets] == [True, False, False]

    # ....................... #

    async def test_a_target_already_gone_counts_as_closed(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()

        closed = await close_spread(
            server, [("/", "eio-gone")], timedelta(seconds=0.01), tail=timedelta(1), stats=stats
        )

        assert closed == 1
        assert stats.drain_closed == {"closed": 1, "failed": 0, "timed_out": 0}

    # ....................... #

    async def test_a_close_that_fails_or_is_cancelled_past_the_tail_is_still_logged(
        self, error_log_buf: Any
    ) -> None:
        server = _server()
        release = asyncio.Event()

        class _LateSocket(_Socket):
            async def close(
                self, wait: bool = True, abort: bool = False, reason: Any = None
            ) -> None:
                await release.wait()

                if self.sid == "eio-1":
                    raise ConnectionResetError("transport gone at last")

                await asyncio.sleep(60)  # still closing at process exit

        for eio_sid in ("eio-1", "eio-2"):
            server.eio.sockets[eio_sid] = _LateSocket(server, eio_sid)

        await close_spread(
            server,
            [("/", "eio-1"), ("/", "eio-2")],
            timedelta(seconds=0.01),
            tail=timedelta(seconds=0.05),
        )
        release.set()
        late = list(drain_module._FINISHING.get(server, ()))  # pyright: ignore[reportPrivateUsage]
        await _until(lambda: any(task.done() for task in late), within=1.0)

        for task in late:
            task.cancel()  # what process exit does to a close still running

        await asyncio.gather(*late, return_exceptions=True)
        await asyncio.sleep(0)

        logged = error_log_buf.getvalue()
        assert "transport gone at last" in logged
        assert "eio-2 was cancelled before it finished" in logged

    # ....................... #

    async def test_a_close_overlapping_another_leaves_every_namespace_to_that_one(self) -> None:
        server = _server()
        presence = _GatedPresence()
        stats = RealtimeConnectionStats()

        for namespace in ("/", "/game"):
            attach_realtime_connection(
                server, namespace=namespace, resolve=_resolve, presence=presence, stats=stats
            )

        socket = _engine_socket(server, "eio-1")

        for namespace in ("/", "/game"):
            await _connect(server, "eio-1", namespace)

        # The client's own close (or a ping timeout) is mid-way through its handlers...
        own = asyncio.ensure_future(socket.close(wait=False, abort=True))
        await asyncio.wait_for(presence.leaving.wait(), timeout=1.0)
        # ...when the drain's close of the same connection lands.
        await asyncio.wait_for(close_transport(server, "eio-1"), timeout=1.0)
        presence.release.set()
        await asyncio.wait_for(own, timeout=1.0)

        # The first close ran every namespace's cleanup: nothing left behind.
        assert await presence.count(_ROOM) == 0
        assert list(server.manager.get_participants("/game", None)) == []
        assert stats.connections == {"/": 0, "/game": 0}

    # ....................... #

    async def test_two_closes_of_one_connection_share_one_close(self) -> None:
        server = _server()
        presence = _GatedPresence()
        attach_realtime_connection(server, resolve=_resolve, presence=presence)
        socket = _engine_socket(server, "eio-1")
        await _connect(server, "eio-1")

        first = asyncio.ensure_future(close_transport(server, "eio-1"))
        await asyncio.wait_for(presence.leaving.wait(), timeout=1.0)
        second = asyncio.ensure_future(close_transport(server, "eio-1"))
        await asyncio.sleep(0.05)

        assert not second.done()  # it waits for the close under way, rather than racing it

        presence.release.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=1.0)

        assert socket.closed
        assert "eio-1" not in server.eio.sockets
        assert await presence.count(_ROOM) == 0


# ----------------------- #


class TestDrainStep:
    async def _drained(
        self,
        server: AsyncServer,
        *,
        budget: float = 5.0,
        **options: Any,
    ) -> None:
        step = connection_drain_lifecycle_step(
            server, window=timedelta(seconds=0.1), tail=timedelta(seconds=1), **options
        )
        runtime = _runtime(step)

        async with runtime.scope():
            await runtime.drain(timedelta(seconds=budget))

    # ....................... #

    async def test_selected_connections_close_and_the_others_stay(self) -> None:
        server = _server()
        presence = InMemoryRealtimePresence()
        attach_realtime_connection(server, resolve=_resolve, presence=presence)
        await _connect(server, "eio-1")
        await _connect(server, "eio-2")
        await _connect(server, "eio-3", "/admin")  # a namespace the drain does not cover

        assert await presence.count(_ROOM) == 2

        await self._drained(server)

        assert _open(server) == {"eio-3"}
        # The library's own disconnect path ran each connection's disconnect handler.
        assert await presence.count(_ROOM) == 0

    # ....................... #

    async def test_a_transport_carrying_a_namespace_the_drain_skips_stays_open(self) -> None:
        server = _server()
        await _connect(server, "eio-1", "/")
        await _connect(server, "eio-2", "/")
        await _connect(server, "eio-2", "/admin")

        await self._drained(server)

        assert _open(server) == {"eio-2"}

    # ....................... #

    async def test_every_namespace_named_and_selected_closes_a_shared_transport(self) -> None:
        server = _server()
        await _connect(server, "eio-1", "/")
        await _connect(server, "eio-1", "/admin")

        await self._drained(server, namespaces=("/", "/admin"))

        assert _open(server) == set()

    # ....................... #

    async def test_a_connection_select_refuses_or_fails_on_keeps_its_transport(self) -> None:
        server = _server()
        keep = await _connect(server, "eio-1")
        broken = await _connect(server, "eio-2")
        await _connect(server, "eio-3")

        def select(_namespace: str, sid: str) -> bool:
            if sid == broken:
                raise RuntimeError("selector bug")

            return sid != keep

        await self._drained(server, select=select)

        assert _open(server) == {"eio-1", "eio-2"}

    # ....................... #

    async def test_a_connection_that_opened_a_kept_namespace_before_its_close_stays_open(
        self,
    ) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        runtime = _runtime(
            connection_drain_lifecycle_step(
                server, window=timedelta(seconds=0.6), tail=timedelta(seconds=0.5), stats=stats
            )
        )

        for namespace in ("/", "/game"):
            attach_realtime_connection(
                server, namespace=namespace, resolve=_resolve, draining=lambda: runtime.draining
            )

        first = _engine_socket(server, "eio-0")
        joining = _engine_socket(server, "eio-1")

        async with runtime.scope():
            await _connect(server, "eio-0")
            await _connect(server, "eio-1")
            draining = asyncio.ensure_future(runtime.drain(timedelta(seconds=5)))
            await _until(lambda: first.closed, within=1.0)  # the drain is under way

            # Before its own slot, the client opens /game, which the drain keeps; admitted,
            # since its connection carries an accepted namespace.
            game = await _connect(server, "eio-1", "/game")
            await asyncio.wait_for(draining, timeout=5)

        assert not joining.closed  # every namespace re-checked right before the close
        assert server.manager.is_connected(game, "/game")
        assert stats.drain_closed == {"closed": 1, "failed": 0, "timed_out": 0}

    # ....................... #

    async def test_a_connection_the_selector_keeps_after_listing_stays_open(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        kept: set[str] = set()
        runtime = _runtime(
            connection_drain_lifecycle_step(
                server,
                window=timedelta(seconds=0.6),
                tail=timedelta(seconds=0.5),
                select=lambda _namespace, sid: sid not in kept,
                stats=stats,
            )
        )
        attach_realtime_connection(
            server, namespace="/", resolve=_resolve, draining=lambda: runtime.draining
        )
        first = _engine_socket(server, "eio-0")
        second = _engine_socket(server, "eio-1")

        async with runtime.scope():
            await _connect(server, "eio-0")
            sid = await _connect(server, "eio-1")
            draining = asyncio.ensure_future(runtime.drain(timedelta(seconds=5)))
            await _until(lambda: first.closed, within=1.0)

            kept.add(sid)  # the selector changes its mind after the listing
            await asyncio.wait_for(draining, timeout=5)

        assert not second.closed
        assert stats.drain_closed == {"closed": 1, "failed": 0, "timed_out": 0}

    # ....................... #

    async def test_closes_are_spread_over_the_window(self) -> None:
        server = _server()
        clock = asyncio.get_running_loop()
        closed_at: list[float] = []

        for index in range(5):
            await _connect(server, f"eio-{index}")

        for index in range(5):
            socket = _socket(server, f"eio-{index}")
            original = socket.close

            async def recording(*args: Any, _original: Any = original, **kwargs: Any) -> None:
                closed_at.append(clock.time())
                await _original(*args, **kwargs)

            socket.close = recording  # type: ignore[method-assign]
        step = connection_drain_lifecycle_step(
            server, window=timedelta(seconds=0.5), tail=timedelta(seconds=0.5)
        )
        runtime = _runtime(step)

        async with runtime.scope():
            began = clock.time()
            await runtime.drain(timedelta(seconds=30))

        # Close i of five starts at i tenths of a second: not at once, not at the end. Each
        # is held to its own slot, so one late start does not fail the rest.
        assert len(closed_at) == 5

        for index, at in enumerate(sorted(closed_at)):
            assert at - began == pytest.approx(index * 0.1, abs=0.08)

    # ....................... #

    @pytest.mark.parametrize("missing", ["sockets", "create_queue"])
    def test_an_engine_io_without_what_the_close_uses_is_refused_when_built(
        self, missing: str
    ) -> None:
        server = _server()
        eio = server.eio

        class _Narrower:
            def __getattr__(self, name: str) -> Any:
                if name == missing:
                    raise AttributeError(name)

                return getattr(eio, name)

        server.eio = _Narrower()  # type: ignore[assignment]

        with pytest.raises(CoreException) as caught:
            connection_drain_lifecycle_step(
                server, window=timedelta(seconds=1), tail=timedelta(seconds=1)
            )

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    # ....................... #

    @pytest.mark.parametrize("missing", ["close_wait", "poll"])
    def test_a_socket_class_without_what_the_close_uses_is_refused(
        self, missing: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        if missing == "close_wait":

            async def close(self: Any, abort: bool = False, reason: Any = None) -> None:
                return None

            monkeypatch.setattr(AsyncSocket, "close", close)

        else:
            monkeypatch.delattr(AsyncSocket, "poll")

        with pytest.raises(CoreException) as caught:
            connection_drain_lifecycle_step(
                _server(), window=timedelta(seconds=1), tail=timedelta(seconds=1)
            )

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    # ....................... #

    def test_a_turn_away_is_checked_too_when_draining_is_wired(self) -> None:
        server = _server()
        server.eio.sockets = None  # type: ignore[assignment]

        with pytest.raises(CoreException) as caught:
            attach_realtime_connection(server, resolve=_resolve, draining=lambda: False)

        assert caught.value.kind == ExceptionKind.CONFIGURATION
        attach_realtime_connection(_server(), resolve=_resolve)  # without draining: no close

    # ....................... #

    async def test_teardown_waits_for_closes_still_running_past_the_tail(self) -> None:
        server = _server()
        store = {"open": True}
        order: list[object] = []

        class _StorePresence(InMemoryRealtimePresence):
            async def left(self, room: str, sid: str) -> None:
                await asyncio.sleep(0.4)  # slower than the tail
                order.append(("left", store["open"]))
                await super().left(room, sid)

        async def close_store(_ctx: Any) -> None:
            store["open"] = False
            order.append("teardown")

        presence = _StorePresence()
        runtime = build_runtime(
            lifecycle_steps=(
                LifecycleStep(id="presence_store", shutdown=close_store),
                connection_drain_lifecycle_step(
                    server, window=timedelta(seconds=0.05), tail=timedelta(seconds=0.1)
                ),
            ),
            drain_timeout=timedelta(0),
            drain_budget=timedelta(seconds=2),
        )
        attach_realtime_connection(server, resolve=_resolve, presence=presence)

        async with runtime.scope():
            await _connect(server, "eio-1")

        # The close's disconnect handler finished against an open store, then teardown ran.
        assert order == [("left", True), "teardown"]
        assert await presence.count(_ROOM) == 0

    # ....................... #

    async def test_the_drain_counts_its_closes(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        await _connect(server, "eio-1")

        await self._drained(server, stats=stats)

        assert stats.drain_closed == {"closed": 1, "failed": 0, "timed_out": 0}

    # ....................... #

    async def test_a_connection_something_else_is_closing_is_skipped_and_not_counted(
        self,
    ) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        await _connect(server, "eio-1")
        await _connect(server, "eio-2")
        _socket(server, "eio-2").closing = True  # its own close is under way: a ping timeout

        await self._drained(server, stats=stats)

        assert stats.drain_closed == {"closed": 1, "failed": 0, "timed_out": 0}
        assert _socket(server, "eio-2").closes == 0  # left to the close under way


# ----------------------- #


def _replayed(server: AsyncServer, runtime: ExecutionRuntime, **options: Any) -> None:
    attach_realtime_connection(
        server,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        runtime=runtime,
        **options,
    )


class TestDrainingRefusal:
    @pytest.mark.parametrize("wiring", ["replay", "explicit"])
    async def test_a_draining_replica_closes_the_transport_before_authenticating(
        self, wiring: str
    ) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        runtime = _runtime()
        resolved: list[SocketIOConnect] = []

        async def resolve(connect: SocketIOConnect) -> RealtimeConnection | None:
            resolved.append(connect)
            return await _resolve(connect)

        if wiring == "replay":
            # Replay runs on the serving runtime, the same one the draining source reads.
            _replayed(
                server,
                runtime,
                resolve=resolve,
                draining=lambda: runtime.draining,
                stats=stats,
            )

        else:
            attach_realtime_connection(
                server, resolve=resolve, draining=lambda: runtime.draining, stats=stats
            )

        async with runtime.scope():
            runtime.begin_draining()

            with pytest.raises(SocketIOConnectionRefusedError) as caught:
                await _connect(server, "eio-1")

        # The Engine.IO connection is closed, so socket.io's JavaScript client reconnects
        # with its backoff; a CONNECT_ERROR alone would make it give up for good. The
        # refusal, raised too, reaches a client only if that close failed.
        assert _open(server) == set()
        assert caught.value.error_args == {
            "message": "Server is draining",
            "data": {"code": "realtime_draining"},
        }
        assert resolved == []
        assert stats.connect_refused == {("/", "draining"): 1}
        assert stats.connections == {}

    # ....................... #

    async def test_replay_without_a_draining_source_warns_at_attach(
        self, error_log_buf: Any
    ) -> None:
        _replayed(_server(), _runtime(), resolve=_resolve)

        assert "draining" in error_log_buf.getvalue()

    # ....................... #

    async def test_existing_refusals_stay_message_only(self) -> None:
        server = _server()
        attach_realtime_connection(server, resolve=_resolve)

        with pytest.raises(SocketIOConnectionRefusedError) as caught:
            await _connect(server, "eio-1", auth={"token": "bad"})

        assert caught.value.error_args == {"message": "Invalid access token"}

    # ....................... #

    async def test_without_a_draining_source_nothing_is_refused(self) -> None:
        server = _server()
        attach_realtime_connection(server, resolve=_resolve)
        runtime = _runtime()

        async with runtime.scope():
            runtime.begin_draining()
            await _connect(server, "eio-1")

        assert _open(server) == {"eio-1"}


class TestTurnAwayHold:
    """A connect turned away is held before its connection closes: the client's own
    backoff starts over on each new connection, so the hold is the backoff."""

    async def test_the_close_comes_after_a_jittered_hold(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(connection_module, "_TURN_AWAY_HOLD_SECONDS", (0.3, 0.4))
        server = _server()
        runtime = _runtime()
        attach_realtime_connection(server, resolve=_resolve, draining=lambda: runtime.draining)
        socket = _engine_socket(server, "eio-1")
        clock = asyncio.get_running_loop()

        async with runtime.scope():
            runtime.begin_draining()
            began = clock.time()

            with pytest.raises(SocketIOConnectionRefusedError):
                await _connect(server, "eio-1")

        assert socket.closed
        assert 0.3 <= clock.time() - began < 0.6

    # ....................... #

    async def test_one_whose_connection_the_drain_closes_while_held_stops_quietly(
        self, monkeypatch: pytest.MonkeyPatch, error_log_buf: Any
    ) -> None:
        monkeypatch.setattr(connection_module, "_TURN_AWAY_HOLD_SECONDS", (1.0, 1.0))
        server = _server()
        stats = RealtimeConnectionStats()
        runtime = _runtime(
            connection_drain_lifecycle_step(
                server, window=timedelta(seconds=0.05), tail=timedelta(seconds=0.5)
            )
        )
        attach_realtime_connection(
            server, resolve=_resolve, draining=lambda: runtime.draining, stats=stats
        )
        socket = _engine_socket(server, "eio-1")
        clock = asyncio.get_running_loop()

        async with runtime.scope():
            runtime.begin_draining()
            held = asyncio.ensure_future(_connect(server, "eio-1"))
            await asyncio.sleep(0.05)

            began = clock.time()
            await runtime.drain(timedelta(seconds=5))

            assert clock.time() - began < 0.9  # the drain did not wait out the hold
            assert socket.closed  # it closed the held connect's connection at its slot

            await asyncio.wait_for(held, timeout=2.0)  # stops quietly at the end of the hold

        assert stats.connect_refused == {("/", "draining"): 1}
        assert error_log_buf.getvalue() == ""

    # ....................... #

    async def test_a_sibling_accepted_during_the_hold_keeps_the_connection_and_admits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(connection_module, "_TURN_AWAY_HOLD_SECONDS", (0.3, 0.3))
        server = _server()
        stats = RealtimeConnectionStats()
        runtime = _runtime()
        attach_realtime_connection(
            server, resolve=_resolve, draining=lambda: runtime.draining, stats=stats
        )
        # A namespace the app never turns away: it has no draining source.
        attach_realtime_connection(server, namespace="/game", resolve=_resolve, stats=stats)
        socket = _engine_socket(server, "eio-1")

        async with runtime.scope():
            runtime.begin_draining()
            lobby = asyncio.ensure_future(_connect(server, "eio-1", "/"))
            await asyncio.sleep(0.05)  # held
            game = await _connect(server, "eio-1", "/game")  # accepted meanwhile

            lobby_sid = await asyncio.wait_for(lobby, timeout=2.0)

        # Closing the connection now would end /game; the held connect is admitted beside it.
        assert not socket.closed
        assert server.manager.is_connected(game, "/game")
        assert server.manager.is_connected(lobby_sid, "/")
        assert stats.connections == {"/": 1, "/game": 1}
        assert stats.connect_refused == {}

    # ....................... #

    async def test_after_the_hold_a_connection_being_closed_gets_no_refusal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(connection_module, "_TURN_AWAY_HOLD_SECONDS", (0.2, 0.2))
        server = _server()
        runtime = _runtime()
        attach_realtime_connection(server, resolve=_resolve, draining=lambda: runtime.draining)
        socket = _engine_socket(server, "eio-1")

        async with runtime.scope():
            runtime.begin_draining()
            held = asyncio.ensure_future(_connect(server, "eio-1"))
            await asyncio.sleep(0.05)
            # A close of the connection is running its disconnect handlers — the drain's,
            # whose CLOSE has not gone out yet. A refusal sent now could reach the client
            # first, and its JavaScript client would give up for good.
            [(sid, _eio_sid)] = list(server.manager.get_participants("/", None))
            server.manager.pre_disconnect(sid, "/")

            await asyncio.wait_for(held, timeout=1.0)  # quietly: no refusal raised

        assert not socket.closed  # left to the close under way

    # ....................... #

    async def test_after_the_hold_a_connection_already_closed_is_left_alone(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(connection_module, "_TURN_AWAY_HOLD_SECONDS", (0.2, 0.2))
        server = _server()
        runtime = _runtime()
        attach_realtime_connection(server, resolve=_resolve, draining=lambda: runtime.draining)
        socket = _engine_socket(server, "eio-1")

        async with runtime.scope():
            runtime.begin_draining()
            held = asyncio.ensure_future(_connect(server, "eio-1"))
            await asyncio.sleep(0.05)
            await socket.close(wait=False, abort=True)  # the client gave up first

            await asyncio.wait_for(held, timeout=1.0)  # stops quietly: nothing raised


# ----------------------- #


class TestLateConnect:
    async def test_a_connect_finishing_after_the_drain_listed_is_refused_and_undone(
        self,
    ) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        joining = asyncio.Event()
        release = asyncio.Event()
        held: list[str] = []

        class _HeldJoin(InMemoryRealtimePresence):
            """Holds the late connect after it stored its identity, in its room join."""

            async def joined(self, room: str, sid: str) -> None:
                if sid in held:
                    joining.set()
                    await release.wait()

                await super().joined(room, sid)

        presence = _HeldJoin()
        runtime = _runtime(
            connection_drain_lifecycle_step(
                server,
                window=timedelta(seconds=0.05),
                tail=timedelta(seconds=1),
                # An app's selector keeping a connection it is not done with: the drain lists
                # the late connect's transport and leaves it open.
                select=lambda _namespace, sid: sid not in held,
            )
        )

        async def resolve(connect: SocketIOConnect) -> RealtimeConnection | None:
            if connect.auth.get("hold"):
                held.append(connect.sid)

            return await _resolve(connect)

        attach_realtime_connection(
            server,
            resolve=resolve,
            presence=presence,
            draining=lambda: runtime.draining,
            stats=stats,
        )

        async with runtime.scope():
            await _connect(server, "eio-1")
            late_socket = _socket(server, "eio-2")
            late = asyncio.ensure_future(_connect(server, "eio-2", "/", {"token": "ok", "hold": 1}))
            await asyncio.wait_for(joining.wait(), timeout=1.0)

            # Past every earlier check, with its identity stored: only the last one is left.
            assert IDENTITY_SESSION_KEY in late_socket.session["/"]

            await runtime.drain(timedelta(seconds=5))
            assert _open(server) == {"eio-2"}  # the drain listed, closed eio-1, and is done

            release.set()

            with pytest.raises(SocketIOConnectionRefusedError) as caught:
                await late

        assert caught.value.error_args["data"] == {"code": "realtime_draining"}
        # Its Engine.IO connection is closed, so the client reconnects elsewhere, and its
        # session went with it; its own disconnect handlers undid the rest.
        assert late_socket.closed
        assert "eio-2" not in server.eio.sockets
        assert _open(server) == set()
        assert await presence.count(_ROOM) == 0
        assert list(server.manager.get_participants("/", _ROOM)) == []
        # eio-1 counted then uncounted by the drain; the late connect never counted.
        assert stats.connections == {"/": 0}
        assert stats.connect_refused == {("/", "draining"): 1}

    # ....................... #

    async def test_a_connect_beside_a_namespace_the_drain_keeps_is_admitted(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        runtime = _runtime(
            connection_drain_lifecycle_step(
                server, window=timedelta(seconds=0.05), tail=timedelta(seconds=1)
            )
        )

        for namespace in ("/", "/game"):
            attach_realtime_connection(
                server,
                namespace=namespace,
                resolve=_resolve,
                draining=lambda: runtime.draining,
                stats=stats,
            )

        socket = _engine_socket(server, "eio-1")

        async with runtime.scope():
            game = await _connect(server, "eio-1", "/game")
            await runtime.drain(timedelta(seconds=2))  # keeps eio-1: /game is not drained
            lobby = await _connect(server, "eio-1", "/")  # the same client opens the lobby

        # Closing the connection would have ended /game, which the drain keeps; the connect
        # adds no connection of its own, and goes when this one is closed.
        assert server.manager.is_connected(game, "/game")
        assert server.manager.is_connected(lobby, "/")
        assert not socket.closed
        assert stats.connections == {"/": 1, "/game": 1}
        assert stats.connect_refused == {}

    # ....................... #

    async def test_a_sibling_still_connecting_does_not_count_as_accepted(self) -> None:
        server = _server()
        runtime = _runtime()
        resolving, release = asyncio.Event(), asyncio.Event()

        async def held(connect: SocketIOConnect) -> RealtimeConnection | None:
            resolving.set()
            await release.wait()
            return await _resolve(connect)

        attach_realtime_connection(server, namespace="/game", resolve=held)
        attach_realtime_connection(server, resolve=_resolve, draining=lambda: runtime.draining)
        socket = _engine_socket(server, "eio-1")

        async with runtime.scope():
            sibling = asyncio.ensure_future(_connect(server, "eio-1", "/game"))
            await asyncio.wait_for(resolving.wait(), timeout=1.0)
            runtime.begin_draining()

            with pytest.raises(SocketIOConnectionRefusedError):
                await _connect(server, "eio-1", "/")

            release.set()
            await asyncio.gather(sibling, return_exceptions=True)

        assert socket.closed  # /game had not been accepted yet: nothing kept the connection

    # ....................... #

    async def test_an_undo_step_that_fails_does_not_replace_the_refusal(self) -> None:
        server = _server()
        runtime = _runtime()

        class _BrokenPresence(InMemoryRealtimePresence):
            async def left(self, room: str, sid: str) -> None:
                raise ConnectionResetError("presence store unreachable")

        async def resolve(connect: SocketIOConnect) -> RealtimeConnection | None:
            runtime.begin_draining()  # the announcement lands while this connect is under way
            return await _resolve(connect)

        attach_realtime_connection(
            server,
            resolve=resolve,
            presence=_BrokenPresence(),
            draining=lambda: runtime.draining,
        )

        async with runtime.scope():
            with pytest.raises(SocketIOConnectionRefusedError) as caught:
                await _connect(server, "eio-1")

        assert caught.value.error_args["data"] == {"code": "realtime_draining"}
        # The steps after the failed one still ran.
        assert list(server.manager.get_participants("/", _ROOM)) == []
        assert IDENTITY_SESSION_KEY not in _socket(server, "eio-1").session.get("/", {})

    # ....................... #

    async def test_an_anonymous_connect_finishing_while_draining_is_refused(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        runtime = _runtime()

        async def resolve(_connect: SocketIOConnect) -> RealtimeConnection | None:
            runtime.begin_draining()
            return None

        attach_realtime_connection(
            server, resolve=resolve, draining=lambda: runtime.draining, stats=stats
        )

        async with runtime.scope():
            with pytest.raises(SocketIOConnectionRefusedError):
                await _connect(server, "eio-1")

        assert stats.connect_refused == {("/", "draining"): 1}
        assert stats.connections == {}


# ----------------------- #


class _RecordingPresence(InMemoryRealtimePresence):
    joins = 0

    async def joined(self, room: str, sid: str) -> None:
        self.joins += 1
        await super().joined(room, sid)


class TestConnectUnderWay:
    """A connect is checked again once admission and authentication have run."""

    async def _held(
        self, server: AsyncServer, *, where: str, **options: Any
    ) -> tuple[asyncio.Future[str], asyncio.Event, list[str]]:
        held, release = asyncio.Event(), asyncio.Event()
        resolved: list[str] = []

        async def admission(_connect: SocketIOConnect) -> None:
            if where == "admission":
                held.set()
                await release.wait()

        async def resolve(connect: SocketIOConnect) -> RealtimeConnection | None:
            resolved.append(connect.sid)

            if where == "auth":
                held.set()
                await release.wait()

            return await _resolve(connect)

        attach_realtime_connection(server, resolve=resolve, admission=admission, **options)
        connecting = asyncio.ensure_future(_connect(server, "eio-1"))
        await asyncio.wait_for(held.wait(), timeout=1.0)

        return connecting, release, resolved

    # ....................... #

    async def test_one_whose_client_left_while_it_joined_leaves_presence_and_logs_nothing(
        self, error_log_buf: Any
    ) -> None:
        server = _server()
        stats = RealtimeConnectionStats()

        class _HeldJoin(InMemoryRealtimePresence):
            def __init__(self) -> None:
                super().__init__()
                self.joining = asyncio.Event()
                self.release = asyncio.Event()

            async def joined(self, room: str, sid: str) -> None:
                self.joining.set()
                await self.release.wait()
                await super().joined(room, sid)

        presence = _HeldJoin()
        attach_realtime_connection(server, resolve=_resolve, presence=presence, stats=stats)
        socket = _engine_socket(server, "eio-1")
        connecting = asyncio.ensure_future(_connect(server, "eio-1"))
        await asyncio.wait_for(presence.joining.wait(), timeout=1.0)

        await socket.close(wait=False, abort=True)  # the client goes, no drain anywhere
        presence.release.set()
        await asyncio.wait_for(connecting, timeout=1.0)

        assert await presence.count(_ROOM) == 0  # joined after its disconnect ran: undone
        assert stats.connections == {}
        assert error_log_buf.getvalue() == ""  # its room and session went with it: no noise

    # ....................... #

    async def test_one_whose_engine_io_session_went_first_is_still_accepted(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()

        async def resolve(_connect: SocketIOConnect) -> RealtimeConnection | None:
            # The Engine.IO side forgot the connection before Socket.IO's side did.
            server.eio.sockets.pop("eio-1", None)
            return None

        attach_realtime_connection(server, resolve=resolve, stats=stats)
        await _connect(server, "eio-1")

        assert stats.connections == {"/": 1}

    # ....................... #

    @pytest.mark.parametrize("where", ["admission", "auth"])
    async def test_one_whose_transport_closed_meanwhile_stops_quietly(self, where: str) -> None:
        server = _server()
        presence = InMemoryRealtimePresence()
        stats = RealtimeConnectionStats()
        connecting, release, resolved = await self._held(
            server, where=where, presence=presence, stats=stats
        )

        await server.eio.disconnect("eio-1")  # the drain, or the client, closed it
        release.set()
        await asyncio.wait_for(connecting, timeout=1.0)  # no KeyError from a gone session

        assert resolved == ([] if where == "admission" else [connecting.result()])
        assert await presence.count(_ROOM) == 0
        assert stats.connections == {}
        assert stats.connect_refused == {}

    # ....................... #

    @pytest.mark.parametrize("where", ["admission", "auth"])
    async def test_one_the_drain_overtook_is_turned_away_before_it_is_stored(
        self, where: str
    ) -> None:
        server = _server()
        presence = _RecordingPresence()
        runtime = _runtime()
        socket = _socket(server, "eio-1")

        async with runtime.scope():
            connecting, release, resolved = await self._held(
                server, where=where, presence=presence, draining=lambda: runtime.draining
            )
            runtime.begin_draining()
            release.set()

            with pytest.raises(SocketIOConnectionRefusedError):
                await asyncio.wait_for(connecting, timeout=1.0)

        assert len(resolved) == (0 if where == "admission" else 1)
        assert socket.closed
        assert IDENTITY_SESSION_KEY not in socket.session.get("/", {})
        assert presence.joins == 0  # turned away before it joined, not undone after

    # ....................... #

    @pytest.mark.parametrize("late", [False, True])
    async def test_a_draining_source_that_raises_refuses_as_internal_and_undoes(
        self, late: bool
    ) -> None:
        server = _server()
        presence = _RecordingPresence()
        stats = RealtimeConnectionStats()

        def draining() -> bool:
            # Late: fails only at the last check, once the connect joined its room.
            if presence.joins or not late:
                raise RuntimeError("readiness source unavailable")

            return False

        attach_realtime_connection(
            server, resolve=_resolve, presence=presence, draining=draining, stats=stats
        )

        with pytest.raises(SocketIOConnectionRefusedError) as caught:
            await _connect(server, "eio-1")

        assert caught.value.error_args == {"message": GENERIC_INTERNAL_DETAIL}
        assert stats.connect_refused == {("/", "internal"): 1}
        assert presence.joins == (1 if late else 0)
        assert await presence.count(_ROOM) == 0
        assert list(server.manager.get_participants("/", _ROOM)) == []
        assert IDENTITY_SESSION_KEY not in _socket(server, "eio-1").session.get("/", {})

    # ....................... #

    async def test_a_late_turn_away_drops_the_replay_progress_even_if_the_close_fails(
        self,
    ) -> None:
        server = _server()
        runtime = _runtime()
        socket = _socket(server, "eio-1", fails=True)  # the transport close raises

        class _Announcing(InMemoryRealtimePresence):
            async def joined(self, room: str, sid: str) -> None:
                runtime.begin_draining()  # lands after the replay progress was registered
                await super().joined(room, sid)

        presence = _Announcing()
        _replayed(
            server,
            runtime,
            resolve=_resolve,
            presence=presence,
            draining=lambda: runtime.draining,
        )
        lifecycle = server.handlers["/"]["connect"].__self__

        async with runtime.scope():
            with pytest.raises(SocketIOConnectionRefusedError) as caught:
                await _connect(server, "eio-1")

        # The close failed, so the refusal is what reaches the client, and the connect was
        # undone here: as before it arrived.
        assert caught.value.error_args["data"] == {"code": "realtime_draining"}
        assert lifecycle._replay_progress == {}  # pyright: ignore[reportPrivateUsage]
        assert await presence.count(_ROOM) == 0
        assert list(server.manager.get_participants("/", _ROOM)) == []
        assert IDENTITY_SESSION_KEY not in socket.session["/"]
        assert CONNECTION_SESSION_KEY not in socket.session["/"]


# ----------------------- #


class TestConnectionCounts:
    async def test_the_count_follows_connects_and_disconnects(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        attach_realtime_connection(server, resolve=_resolve, stats=stats)
        attach_realtime_connection(server, namespace="/chat", resolve=_resolve, stats=stats)

        first = await _connect(server, "eio-1")
        await _connect(server, "eio-2", auth={"token": "anon"})
        await _connect(server, "eio-1", "/chat")

        assert stats.connections == {"/": 2, "/chat": 1}

        await server.handlers["/"]["disconnect"](first)
        await server.handlers["/"]["disconnect"](first)  # once only

        assert stats.connections == {"/": 1, "/chat": 1}

    # ....................... #

    async def test_refusals_are_counted_by_reason_and_never_counted_as_open(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        attach_realtime_connection(server, resolve=_resolve, stats=stats)

        for auth in ({"token": "bad"}, {"token": "boom"}, {"token": "ok", "protocol": 99}):
            with pytest.raises(SocketIOConnectionRefusedError):
                await _connect(server, f"eio-{auth['token']}", auth=auth)

        assert stats.connect_refused == {
            ("/", "auth"): 1,
            ("/", "internal"): 1,
            ("/", "protocol"): 1,
        }
        assert stats.connections == {}

        # A refused connect never reaches on_disconnect; one that did must not uncount.
        await server.handlers["/"]["disconnect"]("never-counted")

        assert stats.connections == {}

    # ....................... #

    async def test_a_transport_that_closed_during_the_connect_is_not_counted(self) -> None:
        server = _server()
        stats = RealtimeConnectionStats()
        resolving = asyncio.Event()
        release = asyncio.Event()

        async def resolve(connect: SocketIOConnect) -> RealtimeConnection | None:
            resolving.set()
            await release.wait()
            return None  # anonymous: no session write that would fail on the dead socket

        attach_realtime_connection(server, resolve=resolve, stats=stats)

        connecting = asyncio.ensure_future(_connect(server, "eio-1"))
        await asyncio.wait_for(resolving.wait(), timeout=1.0)
        await server.eio.disconnect("eio-1")  # the client went away mid-connect
        release.set()
        await connecting

        assert stats.connections == {}
