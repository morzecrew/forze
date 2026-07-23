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
from contextlib import aclosing, asynccontextmanager
from typing import Any
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience, MailboxEntry, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.integrations.realtime.replay import _MAX_BACKLOG_ROUNDS
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
    before anything is delivered, or right after the ``hold_after``-th entry —
    holding the connection mid-drain so the test can ack a "live" frame meanwhile."""

    def __init__(
        self,
        inner: InMemoryRealtimeMailbox,
        *,
        hold_before_first: bool = False,
        hold_after: int = 1,
    ) -> None:
        self.inner = inner
        self.hold_before_first = hold_before_first
        self.hold_after = hold_after
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

        delivered = 0
        async for entry in self.inner.replay_since(principal=principal, since=since):
            yield entry
            delivered += 1

            if delivered == self.hold_after and not self.hold_before_first:
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
    # Held after e2: e1's position is then RUN-PROVEN delivered (an entry with a
    # greater HLC drained past it), which is what the mid-drain floor exposes —
    # e2's own run could still have undelivered equal-HLC siblings in flight, so
    # an ack must not claim it yet.
    mailbox = _GatedMailbox(inner, hold_after=2)

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
    await mailbox.held.wait()  # replay delivered e1+e2 and is now held mid-drain

    # the client acks e3 — a frame it received LIVE while e3 is still undrained; the
    # cumulative claim must be clamped to the proven floor (e1), or the undrained
    # middle would be skipped forever and the all-device trim would delete it
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e3"})

    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(1)
    retained = [e.event_id for e in await inner.read_since(principal=_PRINCIPAL_STR, since=None)]
    assert retained == ["e2", "e3"]  # the undelivered middle is NOT trimmed

    mailbox.gate.set()
    await connect  # the drain completes → the clamp lifts

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e3"})
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(3)


async def test_mid_drain_ack_cannot_claim_a_partially_delivered_run() -> None:
    # The replay is held right after s1 — the first entry of an equal-HLC run whose
    # sibling s2 is still in flight. Acking s1 claims its WHOLE run cumulatively
    # (the trim deletes ``<= floor``), so the mid-drain floor must hold at the last
    # proven-complete position (e1) or s2 would be hard-deleted before delivery.
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    inner = InMemoryRealtimeMailbox()
    mailbox = _GatedMailbox(inner, hold_after=2)  # held between s1 and s2

    await inner.store(principal=_PRINCIPAL_STR, event_id="e1", hlc=_hlc(1), signal=_signal("1"))

    for sibling in ("s1", "s2"):  # one shared position, distinct entries
        await inner.store(
            principal=_PRINCIPAL_STR, event_id=sibling, hlc=_hlc(2), signal=_signal(sibling)
        )

    attach_realtime_connection(
        sio,  # pyright: ignore[reportArgumentType]
        resolve=_resolver(_connection()),
        mailbox_factory=lambda _ctx: mailbox,  # pyright: ignore[reportArgumentType]
        cursors_factory=lambda _ctx: cursors,
        runtime=_runtime(),
    )

    connect = asyncio.create_task(sio.handlers["connect"]("sid-1", {}, None))
    await mailbox.held.wait()  # e1 and s1 delivered; s2 (same HLC as s1) in flight

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "s1"})

    # clamped BELOW the run: claiming s1's position would trim the undelivered s2
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(1)
    retained = [e.event_id for e in await inner.read_since(principal=_PRINCIPAL_STR, since=None)]
    assert retained == ["s1", "s2"]  # the run survives whole

    mailbox.gate.set()
    await connect  # the drain completes → the run is proven → the clamp lifts

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "s2"})
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(2)


class _CapTruncatedMailbox:
    """A mailbox whose replay window stops at ``cap`` while more entries exist —
    the durable-store shape (``DocumentRealtimeMailbox``) the in-memory mailbox
    cannot reproduce, since its cap *evicts* instead of bounding the window."""

    def __init__(self, inner: InMemoryRealtimeMailbox, cap: int) -> None:
        self.inner = inner
        self.cap = cap

    async def replay_since(self, *, principal: str, since: HlcTimestamp | None) -> Any:
        delivered = 0

        # aclosing: the early cap-return must close the inner stream deterministically
        # (the same closure propagation iter_replay applies one level up).
        async with aclosing(self.inner.replay_since(principal=principal, since=since)) as entries:
            async for entry in entries:
                if delivered >= self.cap:
                    return

                delivered += 1
                yield entry

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


async def test_truncated_replay_keeps_the_ack_clamp() -> None:
    # A backlog so deep the round budget cannot drain it: entries stay retained past
    # the delivered prefix, so lifting the clamp would let a live-frame ack advance
    # the cursor over the undelivered middle, which the all-device trim then
    # hard-deletes. The clamp must hold at the claimable floor until a reconnect
    # drains further.
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    inner = InMemoryRealtimeMailbox()
    mailbox = _CapTruncatedMailbox(inner, cap=2)

    # round 1 delivers two entries, every later round re-fetches one and delivers one
    # more — so the budget drains ``rounds + 1`` entries; seed comfortably past that
    total = _MAX_BACKLOG_ROUNDS + 4

    for n in range(1, total + 1):
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

    await sio.handlers["connect"]("sid-1", {}, None)  # rounds exhaust before the tail

    # a live-frame ack past the undelivered tail
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": f"e{total}"})

    # Clamped to the last proven-complete position (the newest delivered entry's run
    # is unproven, so the floor sits one position behind the delivered prefix).
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(_MAX_BACKLOG_ROUNDS)
    retained = [e.event_id for e in await inner.read_since(principal=_PRINCIPAL_STR, since=None)]
    # the undelivered tail survives the trim (e{rounds+1} was delivered but unclaimed)
    assert retained == [f"e{n}" for n in range(_MAX_BACKLOG_ROUNDS + 1, total + 1)]


async def test_split_equal_hlc_run_keeps_the_clamp_and_its_siblings() -> None:
    # The replay window cuts INSIDE an equal-HLC run whose length reaches the cap:
    # a strict-greater re-fetch can never see the remaining sibling, so the drain
    # stays unconfirmed and the floor retreats BELOW the run — were the run claimed,
    # an ack would trim it whole and hard-delete the never-delivered sibling.
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    inner = InMemoryRealtimeMailbox()
    mailbox = _CapTruncatedMailbox(inner, cap=2)

    await inner.store(principal=_PRINCIPAL_STR, event_id="e1", hlc=_hlc(1), signal=_signal("1"))

    for sibling in ("s1", "s2", "s3"):  # one shared position, distinct entries
        await inner.store(
            principal=_PRINCIPAL_STR, event_id=sibling, hlc=_hlc(2), signal=_signal(sibling)
        )

    attach_realtime_connection(
        sio,  # pyright: ignore[reportArgumentType]
        resolve=_resolver(_connection()),
        mailbox_factory=lambda _ctx: mailbox,  # pyright: ignore[reportArgumentType]
        cursors_factory=lambda _ctx: cursors,
        runtime=_runtime(),
    )

    await sio.handlers["connect"]("sid-1", {}, None)

    assert [e["data"]["id"] for e in sio.emits] == ["e1", "s1", "s2"]  # s3 unreachable

    # acking a DELIVERED sibling must not claim the run: the trim would delete s3
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "s2"})

    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(1)
    retained = [e.event_id for e in await inner.read_since(principal=_PRINCIPAL_STR, since=None)]
    assert retained == ["s1", "s2", "s3"]  # the whole run survives, s3 included


class _LiveArrivalMailbox(_CapTruncatedMailbox):
    """Stores a live entry the moment the first replay round exhausts — landing it in
    the race window between that round's drain and the cap probe."""

    def __init__(self, inner: InMemoryRealtimeMailbox, cap: int) -> None:
        super().__init__(inner, cap)
        self.rounds = 0

    async def replay_since(self, *, principal: str, since: HlcTimestamp | None) -> Any:
        self.rounds += 1
        first = self.rounds == 1

        async with aclosing(super().replay_since(principal=principal, since=since)) as entries:
            async for entry in entries:
                yield entry

        if first:
            await self.inner.store(
                principal=principal, event_id="e-live", hlc=_hlc(99), signal=_signal("live")
            )


async def test_live_arrival_after_a_drained_cap_replay_does_not_keep_the_clamp() -> None:
    # An entry stored between a cap-filled-but-drained replay and the probe is a
    # LIVE arrival, not evidence of truncation. The follow-up round drains it, so
    # the clamp lifts — read as truncation instead, acks would stay pinned to the
    # replayed floor until a reconnect that may never come.
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    inner = InMemoryRealtimeMailbox()
    mailbox = _LiveArrivalMailbox(inner, cap=2)

    for n in (1, 2):  # exactly cap entries: the first round fills the cap AND drains
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

    await sio.handlers["connect"]("sid-1", {}, None)

    # the follow-up round delivered the live arrival instead of declaring truncation
    assert [e["data"]["id"] for e in sio.emits] == ["e1", "e2", "e-live"]

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e-live"})
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(99)  # unclamped


async def test_exactly_drained_cap_replay_lifts_the_clamp() -> None:
    # A backlog of exactly ``cap`` entries fills the count but IS fully drained: the
    # one-entry probe finds nothing past the last delivered position, so the clamp
    # lifts like any complete replay — a later live-frame ack advances normally.
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    mailbox = InMemoryRealtimeMailbox(cap=2)

    for n in (1, 2):
        await mailbox.store(
            principal=_PRINCIPAL_STR, event_id=f"e{n}", hlc=_hlc(n), signal=_signal(str(n))
        )

    attach_realtime_connection(
        sio,  # pyright: ignore[reportArgumentType]
        resolve=_resolver(_connection()),
        mailbox_factory=lambda _ctx: mailbox,
        cursors_factory=lambda _ctx: cursors,
        runtime=_runtime(),
    )

    await sio.handlers["connect"]("sid-1", {}, None)  # exactly cap entries, all drained

    # A later live frame, delivered and acked after the complete replay.
    await mailbox.store(principal=_PRINCIPAL_STR, event_id="e3", hlc=_hlc(3), signal=_signal("3"))
    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e3"})

    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") == _hlc(3)  # unclamped


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


async def test_failed_replay_keeps_the_connection_and_the_ack_clamp() -> None:
    # replay is best-effort: a drain error must not refuse the live connection — but
    # the progress stays incomplete, so acks remain refused (nothing was delivered)
    # until a reconnect replays successfully
    sio, cursors = _StubSio(), InMemoryMailboxCursors()
    inner = InMemoryRealtimeMailbox()

    class _BrokenReplayMailbox(_GatedMailbox):
        async def replay_since(self, *, principal: str, since: HlcTimestamp | None) -> Any:
            raise RuntimeError("store down")
            yield  # pragma: no cover — makes this an async generator like the real one

    mailbox = _BrokenReplayMailbox(inner)
    await inner.store(principal=_PRINCIPAL_STR, event_id="e1", hlc=_hlc(1), signal=_signal("a"))

    attach_realtime_connection(
        sio,  # pyright: ignore[reportArgumentType]
        resolve=_resolver(_connection()),
        mailbox_factory=lambda _ctx: mailbox,  # pyright: ignore[reportArgumentType]
        cursors_factory=lambda _ctx: cursors,
        runtime=_runtime(),
    )

    await sio.handlers["connect"]("sid-1", {}, None)  # no raise — replay failure is logged

    await sio.handlers["realtime.ack"]("sid-1", {"up_to": "e1"})
    assert await cursors.get(principal=_PRINCIPAL_STR, client_key="d1") is None  # still clamped

    # disconnect drops the per-connection progress (with or without presence wired)
    await sio.handlers["disconnect"]("sid-1")
    lifecycle = sio.handlers["connect"].__self__
    assert lifecycle._replay_progress == {}
