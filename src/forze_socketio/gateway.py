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
from typing import Awaitable, Callable, Protocol, final, runtime_checkable
from uuid import UUID

import attrs
from socketio.async_server import AsyncServer

from forze.application.contracts.envelope import HEADER_TENANT_ID
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.stream import StreamGroupQueryDepKey, StreamSpec
from forze.application.execution import ExecutionContext
from forze.base.logging import Logger

from ._logging import ForzeSocketIOLogger

# ----------------------- #

_logger = Logger(ForzeSocketIOLogger.ERRORS)

SignalHandler = Callable[[RealtimeSignal, UUID | None], Awaitable[None]]
"""A per-signal bridge: receives a decoded signal and its tenant (from headers)."""

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
    raw = headers.get(HEADER_TENANT_ID) if hasattr(headers, "get") else None

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

    group: str = "realtime-gateway"
    """Consumer group name shared by all gateway instances."""

    consumer: str = "gateway"
    """This instance's consumer name within the group."""

    batch: int = 64
    """Maximum signals to read per poll."""

    poll_interval: timedelta = timedelta(seconds=1)
    """Read block timeout, and the idle pause when a poll returns nothing."""

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
            messages = await group.read(
                self.group,
                self.consumer,
                mapping,
                limit=self.batch,
                timeout=self.poll_interval,
            )

            if not messages:
                await asyncio.sleep(self.poll_interval.total_seconds())
                continue

            for message in messages:
                try:
                    await handler(message.payload, _tenant_from_headers(message.headers))

                except Exception:  # noqa: BLE001 - one bad signal must not wedge the stream
                    _logger.critical_exception(
                        "Realtime bridge failed", stream=stream, message_id=message.id
                    )

                await group.ack(group=self.group, stream=stream, ids=[message.id])


# ----------------------- #


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

    # ....................... #

    async def run(self, ctx: ExecutionContext) -> None:
        """Consume signals forever and emit each to its room. Cancel to stop."""

        await self.source.run(ctx, self._emit)

    # ....................... #

    async def _emit(self, signal: RealtimeSignal, tenant: UUID | None) -> None:
        await self.sio.emit(
            signal.event,
            data=signal.payload,
            room=room_for(signal.audience, tenant),
            namespace=self.namespace,
        )

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
