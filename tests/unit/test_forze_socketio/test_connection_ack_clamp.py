"""Mid-replay cumulative acks are clamped to the delivered prefix.

# covers: forze_socketio.connection (_ReplayProgress; on_ack delivered_floor clamp)
#         forze.application.integrations.realtime.replay (acknowledge_up_to clamp)

Socket.IO live emits (room fan-out) race the connect-time replay: a client can
receive — and ack — a live frame while older mailbox entries are still draining.
Unclamped, that ack's cumulative claim would advance the cursor over the
undelivered middle, and the all-device trim would delete those entries (silent
loss). The connection layer tracks how far the replay has contiguously delivered
and clamps acks to that floor until the drain completes.
"""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience, MailboxEntry, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_mock import MockDepsModule
from forze_socketio import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    RealtimeConnection,
    attach_realtime_connection,
)
from forze_socketio.routing import SocketIOConnect

# ----------------------- #

_PRINCIPAL = UUID("22222222-2222-2222-2222-222222222222")
_PRINCIPAL_STR = str(_PRINCIPAL)


class _ConcurrentRuntime:
    """A runtime facade whose scopes may overlap — one fresh ``ExecutionRuntime`` per
    unit of work, so an ack can run while the connect-time replay still holds its
    scope (an ``ExecutionRuntime`` allows a single open scope at a time). The mailbox
    and cursors are injected in-memory objects, so state is shared regardless."""

    def __init__(self) -> None:
        self._current: contextvars.ContextVar[ExecutionRuntime | None] = contextvars.ContextVar(
            "ack_clamp_runtime", default=None
        )

    @asynccontextmanager
    async def scope(self) -> AsyncIterator[None]:
        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
        token = self._current.set(runtime)

        try:
            async with runtime.scope():
                yield

        finally:
            self._current.reset(token)

    def get_context(self) -> ExecutionContext:
        runtime = self._current.get()
        assert runtime is not None

        return runtime.get_context()


def _runtime() -> Any:
    return _ConcurrentRuntime()


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal(_PRINCIPAL_STR), "order.shipped", {"text": text})


class _StubSio:
    def __init__(self) -> None:
        self.handlers: dict[str, Any] = {}
        self.sessions: dict[str, dict[str, Any]] = {}
        self.emits: list[dict[str, Any]] = []

    def on(self, event: str, handler: Any, namespace: str | None = None) -> None:
        self.handlers[event] = handler

    async def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None: ...

    async def get_session(self, sid: str, namespace: str | None = None) -> dict[str, Any]:
        return self.sessions.setdefault(sid, {})

    async def save_session(
        self, sid: str, session: dict[str, Any], namespace: str | None = None
    ) -> None:
        self.sessions[sid] = session

    async def emit(
        self,
        event: str,
        data: Any = None,
        *,
        to: str | None = None,
        room: str | None = None,
        namespace: str | None = None,
        **_: Any,
    ) -> None:
        self.emits.append({"event": event, "data": data, "to": to})


class _GatedMailbox:
    """Wraps the in-memory mailbox; the replay stream pauses at a chosen point —
    before anything is delivered, or right after the first entry — holding the
    connection mid-drain so the test can ack a "live" frame meanwhile."""

    def __init__(self, inner: InMemoryRealtimeMailbox, *, hold_before_first: bool = False) -> None:
        self.inner = inner
        self.hold_before_first = hold_before_first
        self.gate = asyncio.Event()
        self.held = asyncio.Event()

    async def store(self, **kwargs: Any) -> None:
        await self.inner.store(**kwargs)

    async def read_since(self, **kwargs: Any) -> list[MailboxEntry]:  # pragma: no cover
        return await self.inner.read_since(**kwargs)

    async def position_of(self, **kwargs: Any) -> HlcTimestamp | None:
        return await self.inner.position_of(**kwargs)

    async def trim(self, **kwargs: Any) -> None:
        await self.inner.trim(**kwargs)

    async def replay_since(self, *, principal: str, since: HlcTimestamp | None) -> Any:
        if self.hold_before_first:
            self.held.set()
            await self.gate.wait()

        first = True
        async for entry in self.inner.replay_since(principal=principal, since=since):
            yield entry

            if first and not self.hold_before_first:
                first = False
                self.held.set()
                await self.gate.wait()


def _resolver(connection: RealtimeConnection):  # type: ignore[no-untyped-def]
    async def resolve(_c: SocketIOConnect) -> RealtimeConnection:
        return connection

    return resolve


def _connection() -> RealtimeConnection:
    return RealtimeConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL),
        client=ClientIdentity(device_id="d1"),
    )


# ----------------------- #


async def test_mid_replay_ack_of_a_live_frame_is_clamped_to_the_delivered_floor() -> None:
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    inner = InMemoryRealtimeMailbox()
    mailbox = _GatedMailbox(inner)

    for n in (1, 2, 3):
        await inner.store(
            principal=_PRINCIPAL_STR, event_id=f"e{n}", hlc=_hlc(n), signal=_signal(str(n))
        )

    attach_realtime_connection(
        sio,  # pyright: ignore[reportArgumentType]
        resolve=_resolver(_connection()),
        mailbox_factory=lambda _ctx: mailbox,  # pyright: ignore[reportArgumentType]
        cursors_factory=lambda _ctx: cursors,
        runtime=_runtime(),
    )

    connect = asyncio.create_task(sio.handlers["connect"]("sid-1", {}, None))
    await mailbox.held.wait()  # replay delivered e1 and is now held mid-drain

    # the client acks e3 — a frame it received LIVE while e2 is still undrained; the
    # cumulative claim must be clamped to the delivered floor (e1), or e2 would be
    # skipped forever and the all-device trim would delete it
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e3"})

    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(1)
    retained = [e.event_id for e in await inner.read_since(principal=_PRINCIPAL_STR, since=None)]
    assert retained == ["e2", "e3"]  # the undelivered middle is NOT trimmed

    mailbox.gate.set()
    await connect  # the drain completes → the clamp lifts

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e3"})
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(3)


async def test_ack_before_anything_is_delivered_is_ignored() -> None:
    # a fresh device (no cursor row) acking a live frame before the replay has
    # delivered anything has no delivered prefix to stand on — the cursor must not
    # move at all (moving it would freeze a bogus trim floor AND skip the backlog)
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    inner = InMemoryRealtimeMailbox()
    mailbox = _GatedMailbox(inner, hold_before_first=True)

    for n in (1, 2):
        await inner.store(
            principal=_PRINCIPAL_STR, event_id=f"e{n}", hlc=_hlc(n), signal=_signal(str(n))
        )

    attach_realtime_connection(
        sio,  # pyright: ignore[reportArgumentType]
        resolve=_resolver(_connection()),
        mailbox_factory=lambda _ctx: mailbox,  # pyright: ignore[reportArgumentType]
        cursors_factory=lambda _ctx: cursors,
        runtime=_runtime(),
    )

    connect = asyncio.create_task(sio.handlers["connect"]("sid-1", {}, None))
    await mailbox.held.wait()  # the replay is registered but has delivered nothing

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e2"})  # a live-frame ack

    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") is None

    mailbox.gate.set()
    await connect  # drain completes; a later ack advances normally

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e2"})
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(2)


async def test_failed_room_join_does_not_leak_replay_progress() -> None:
    # a raise between progress registration and connect completion refuses the
    # connect, and a refused connect never reaches on_disconnect — the entry must
    # be dropped on the failure path or every failed join leaks one forever
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    mailbox = InMemoryRealtimeMailbox()

    async def _exploding_enter_room(sid: str, room: str, namespace: str | None = None) -> None:
        raise RuntimeError("room join failed")

    sio.enter_room = _exploding_enter_room  # type: ignore[method-assign]

    attach_realtime_connection(
        sio,  # pyright: ignore[reportArgumentType]
        resolve=_resolver(_connection()),
        mailbox_factory=lambda _ctx: mailbox,
        cursors_factory=lambda _ctx: cursors,
        runtime=_runtime(),
    )

    import pytest

    with pytest.raises(RuntimeError, match="room join failed"):
        await sio.handlers["connect"]("sid-1", {}, None)

    lifecycle = sio.handlers["connect"].__self__
    assert lifecycle._replay_progress == {}
