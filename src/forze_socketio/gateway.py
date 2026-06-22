"""The realtime egress gateway — consume realtime signals, bridge to connections.

The egress twin of the inbound :class:`ForzeSocketIOAdapter`. It is **not** a
contract — it is an edge adapter that consumes :class:`RealtimeSignal` s from a
messaging substrate and emits them to the live Socket.IO connections it owns.

Three separate seams (RFC 0002 §7):

- **source** — where signals come from (a stream consumer group here); swappable.
- **bridge** — :meth:`RealtimeGateway._emit`: ``signal → room → sio.emit``. The
  Socket.IO Redis manager fans the emit to whichever node holds the room.
- **supervision** — a *minimal* :class:`~forze.application.contracts.execution.LifecycleStep`
  (see :mod:`forze_socketio.gateway_lifecycle`) that owns the ``run`` task; it
  does **not** carry restart/backoff (a future unified runner does that).

Room membership (auto-join, topic subscription) is a transport-edge concern too;
the helpers here build the same tenant-scoped room names the gateway emits to, so
publish and membership always agree.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
from datetime import timedelta
from typing import Any, Awaitable, Callable, Protocol, cast, final, runtime_checkable
from uuid import UUID

import attrs
from socketio.async_server import AsyncServer

from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_TENANT_ID
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.realtime import (
    DEFAULT_REALTIME_GROUP,
    Audience,
    RealtimeSignal,
)
from forze.application.contracts.stream import StreamGroupQueryDepKey, StreamSpec
from forze.application.execution import ExecutionContext
from forze.base.logging import Logger
from forze.base.primitives import JsonDict, StrKey

from ._logging import ForzeSocketIOLogger

# ----------------------- #

_logger = Logger(ForzeSocketIOLogger.ERRORS)

_IDLE_FLOOR = 0.05
"""Seconds: a small idle pause floor so a non-blocking backend can't hot-loop."""

SignalHandler = Callable[[RealtimeSignal, UUID | None, str | None], Awaitable[None]]
"""A per-signal bridge: receives a decoded signal, its tenant, and a dedup id.

The dedup id is the durable ``forze_event_id`` (``None`` for ephemeral signals).
"""

# ....................... #


def room_for(audience: Audience, tenant: UUID | None) -> str:
    """Resolve a logical *audience* to a tenant-scoped Socket.IO room name.

    The only place audience→room naming exists; the gateway emits to it and the
    membership helpers join it, so they always agree. When a tenant is bound the
    room is prefixed ``t:<tenant>:`` so tenants cannot share a room.
    """

    base = f"{audience.kind.value}:{audience.name}"

    return f"t:{tenant}:{base}" if tenant is not None else base


# ....................... #


def _tenant_from_headers(headers: object) -> UUID | None:
    """Extract the tenant id from the headers."""

    if not hasattr(headers, "get"):
        return None

    # dirty cast to supress pyright
    headers = cast(JsonDict, headers)
    raw = headers.get(HEADER_TENANT_ID)

    return UUID(raw) if raw else None


# ----------------------- #


@runtime_checkable
class RealtimeSignalSource(Protocol):
    """A source of realtime signals — decode from *some* substrate, deliver once.

    ``run`` reads signals and invokes *handler* for each, acknowledging only after
    the handler returns, so each signal is delivered to exactly one gateway.
    """

    def run(self, ctx: ExecutionContext, handler: SignalHandler) -> Awaitable[None]:
        """Consume signals forever, invoking *handler* per signal."""

        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamGroupSignalSource(RealtimeSignalSource):
    """A signal source backed by a stream **consumer group** (exactly-once delivery).

    Each signal goes to exactly one consumer in *group*, so multiple gateway
    instances share the load without double-emitting. A handler that raises is
    logged and the message is still acknowledged (ephemeral is at-most-once; the
    durable path dedupes downstream), so one bad signal cannot wedge the stream.
    """

    stream_spec: StreamSpec[RealtimeSignal]
    """The realtime stream to consume (same spec the publisher appends to)."""

    group: str = DEFAULT_REALTIME_GROUP
    """Consumer group name shared by all gateway instances."""

    consumer: str = "gateway"
    """This instance's consumer name within the group."""

    batch: int = 64
    """Maximum signals to read per poll."""

    poll_interval: timedelta = timedelta(seconds=1)
    """Block timeout for one group read."""

    reclaim_idle: timedelta | None = timedelta(seconds=60)
    """Reclaim entries stranded (delivered, unacked) at least this long.

    Recovers durable signals whose consumer died after read but before ack (the
    ``">"`` cursor never redelivers them): each tick claims stale pending entries
    and reprocesses them — deduped, so a recovered durable signal still emits at
    most once. ``None`` disables recovery (e.g. a single ephemeral-only node).
    """

    # ....................... #

    async def run(self, ctx: ExecutionContext, handler: SignalHandler) -> None:
        group = ctx.deps.resolve_configurable(
            ctx,
            StreamGroupQueryDepKey,
            self.stream_spec,
            route=self.stream_spec.name,
        )
        stream = str(self.stream_spec.name)
        mapping = {stream: ">"}

        while True:
            try:
                fresh = await group.read(
                    self.group,
                    self.consumer,
                    mapping,
                    limit=self.batch,
                    timeout=self.poll_interval,
                )
                await self._process(group, stream, fresh, handler)

                reclaimed: list[Any] = []
                if self.reclaim_idle is not None:
                    reclaimed = await group.claim(
                        self.group,
                        self.consumer,
                        stream,
                        idle=self.reclaim_idle,
                        limit=self.batch,
                    )
                    await self._process(group, stream, reclaimed, handler)

                if not fresh and not reclaimed:
                    # the read timeout already paces blocking backends; this is a
                    # small floor so a non-blocking backend cannot hot-loop.
                    await asyncio.sleep(
                        min(_IDLE_FLOOR, self.poll_interval.total_seconds())
                    )

            except asyncio.CancelledError:
                raise

            except (
                Exception
            ):  # noqa: BLE001 - a transient broker error must not kill the gateway
                _logger.critical_exception("Realtime gateway loop error", stream=stream)
                await asyncio.sleep(self.poll_interval.total_seconds())

    # ....................... #

    async def _process(
        self, group: Any, stream: str, messages: list[Any], handler: SignalHandler
    ) -> None:
        for message in messages:
            # A durable signal (relayed from the outbox) carries an event id; it is
            # acked only on success, so a transient failure stays pending and is
            # recovered (at-least-once). An ephemeral signal is acked regardless, so
            # one bad signal can never wedge the live stream (at-most-once).
            durable = HEADER_EVENT_ID in message.headers
            dedup_id = message.headers.get(HEADER_EVENT_ID)
            ack = True

            try:
                await handler(
                    message.payload, _tenant_from_headers(message.headers), dedup_id
                )

            except Exception:  # noqa: BLE001
                _logger.critical_exception(
                    "Realtime bridge failed", stream=stream, message_id=message.id
                )
                ack = not durable

            if ack:
                await group.ack(group=self.group, stream=stream, ids=[message.id])


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GatewayDedup:
    """Inbox-based exactly-once for durable signals at the gateway."""

    inbox_spec: InboxSpec
    """The inbox route that records already-emitted durable signals."""

    tx_route: StrKey
    """Transaction route the dedup mark commits on."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RealtimeGateway:
    """Bridge realtime signals from a *source* to live Socket.IO connections."""

    sio: AsyncServer
    """Socket.IO async server used for delivery and room membership."""

    source: RealtimeSignalSource
    """Where signals come from (substrate-agnostic)."""

    namespace: str = "/"
    """Namespace this gateway emits on and manages rooms within."""

    dedup: GatewayDedup | None = None
    """When set, durable signals (those with a dedup id) emit at most once."""

    emit_timeout: timedelta | None = None
    """Bound on a single ``sio.emit``; ``None`` waits indefinitely.

    Transport-level flow control (a slow consumer) is engine.io's; this only stops
    one stuck delivery from wedging the whole consume loop. On timeout the emit
    raises, so the source's per-signal error policy applies — an ephemeral signal is
    acked (at-most-once) and a durable one is left pending to be redelivered.
    """

    # ....................... #

    async def run(self, ctx: ExecutionContext) -> None:
        """Consume signals forever and emit each to its room. Cancel to stop."""

        async def handle(
            signal: RealtimeSignal, tenant: UUID | None, dedup_id: str | None
        ) -> None:
            await self._handle(ctx, signal, tenant, dedup_id)

        await self.source.run(ctx, handle)

    # ....................... #

    async def _handle(
        self,
        ctx: ExecutionContext,
        signal: RealtimeSignal,
        tenant: UUID | None,
        dedup_id: str | None,
    ) -> None:
        if self.dedup is None or dedup_id is None:
            # ephemeral, or durable with no dedup configured — emit directly
            await self._emit(signal, tenant)
            return

        # durable: mark + emit inside one transaction, so a redelivered signal
        # (relay retry / consumer claim) is recognised and emitted at most once
        async with ctx.tx_ctx.scope(self.dedup.tx_route):
            inbox = ctx.inbox(self.dedup.inbox_spec)

            if await inbox.mark_if_unseen(str(self.dedup.inbox_spec.name), dedup_id):
                await self._emit(signal, tenant)

    # ....................... #

    async def _emit(self, signal: RealtimeSignal, tenant: UUID | None) -> None:
        emit = self.sio.emit(
            signal.event,
            data=signal.payload,
            room=room_for(signal.audience, tenant),
            namespace=self.namespace,
        )

        if self.emit_timeout is None:
            await emit
            return

        await asyncio.wait_for(emit, timeout=self.emit_timeout.total_seconds())

    # ....................... #

    async def join_principal(
        self, sid: str, principal_id: UUID | str, tenant: UUID | None
    ) -> None:
        """Join *sid* to its tenant-scoped principal room (auto-join on connect)."""

        await self.sio.enter_room(
            sid,
            room_for(Audience.principal(str(principal_id)), tenant),
            namespace=self.namespace,
        )

    # ....................... #

    async def join_topic(self, sid: str, topic: str, tenant: UUID | None) -> None:
        """Subscribe *sid* to a tenant-scoped topic room (app-driven)."""

        await self.sio.enter_room(
            sid, room_for(Audience.topic(topic), tenant), namespace=self.namespace
        )

    # ....................... #

    async def leave_topic(self, sid: str, topic: str, tenant: UUID | None) -> None:
        """Unsubscribe *sid* from a tenant-scoped topic room."""

        await self.sio.leave_room(
            sid, room_for(Audience.topic(topic), tenant), namespace=self.namespace
        )
