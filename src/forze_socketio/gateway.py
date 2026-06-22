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
from contextlib import AbstractContextManager, nullcontext
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Protocol,
    cast,
    final,
    runtime_checkable,
)
from uuid import UUID

import attrs
from socketio.async_server import AsyncServer

from forze.application.contracts.envelope import (
    HEADER_EVENT_ID,
    HEADER_HLC,
    HEADER_TENANT_ID,
)
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.realtime import (
    DEFAULT_REALTIME_GROUP,
    Audience,
    AudienceKind,
    RealtimeEventCatalog,
    RealtimeSignal,
)
from forze.application.contracts.stream import StreamGroupQueryDepKey, StreamSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.logging import Logger
from forze.base.primitives import HlcTimestamp, JsonDict, StrKey, utcnow

from ._logging import ForzeSocketIOLogger
from .mailbox import RealtimeMailbox

if TYPE_CHECKING:
    from .connection import RealtimePresence

# ----------------------- #

_logger = Logger(ForzeSocketIOLogger.ERRORS)

_IDLE_FLOOR = 0.05
"""Seconds: a small idle pause floor so a non-blocking backend can't hot-loop."""

SignalHandler = Callable[
    [RealtimeSignal, UUID | None, str | None, HlcTimestamp], Awaitable[None]
]
"""A per-signal bridge: a decoded signal, its tenant, a dedup id, and its HLC position.

The dedup id is the durable ``forze_event_id`` (``None`` for ephemeral signals); the
HLC is the carried ``forze_hlc`` (or a wall-clock fallback) used for mailbox ordering.
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


# ....................... #


def _hlc_from_headers(headers: object) -> HlcTimestamp:
    """The carried HLC (``forze_hlc``), or a wall-clock fallback when absent.

    The durable relay forwards the outbox HLC on HLC-ordering backends; when no
    HLC is carried, a ``(now_ms, 0)`` stamp keeps mailbox ordering wall-clock-close.
    """

    raw = (  # pyright: ignore[reportUnknownVariableType]
        headers.get(  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
            HEADER_HLC
        )
        if hasattr(headers, "get")
        else None
    )

    if raw:
        return HlcTimestamp.parse(cast(str, raw))

    return HlcTimestamp(physical_ms=int(utcnow().timestamp() * 1000), logical=0)


# ....................... #


def _bind_tenant(
    ctx: ExecutionContext,
    tenant: UUID | None,
) -> AbstractContextManager[None]:
    """Bind the per-signal *tenant* (from the header) so the mailbox scopes ambiently.

    The gateway consumes a tenant-global stream, so it binds each signal's tenant the
    way the inbox consumer does — the mailbox then reads the ambient tenant, never a
    parameter. ``None`` (untenanted signal) binds nothing.
    """

    if tenant is None:
        return nullcontext()

    return ctx.inv_ctx.bind_identity(
        authn=ctx.inv_ctx.get_authn(), tenant=TenantIdentity(tenant_id=tenant)
    )


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
        self,
        group: Any,
        stream: str,
        messages: list[Any],
        handler: SignalHandler,
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
                    message.payload,
                    _tenant_from_headers(message.headers),
                    dedup_id,
                    _hlc_from_headers(message.headers),
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

    mailbox: RealtimeMailbox | None = None
    """When set (with ``dedup``), a durable **principal** signal is stored for offline
    replay before it is emitted, so a recipient offline at emit time receives it on
    reconnect (RFC 0006). Topic and ephemeral signals are never mailboxed."""

    event_catalog: RealtimeEventCatalog | None = None
    """Optional catalog consulted for the per-event ``offline_delivery`` opt-out; when
    absent, every durable principal signal is mailboxed (always-store default)."""

    presence: "RealtimePresence | None" = None
    """When set with a mailbox, the live emit is skipped for a mailboxed signal whose
    principal room is empty (saves a cross-node fan-out; the reconnect drain delivers
    it). Never skips a signal that is not recoverable from the mailbox."""

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
            signal: RealtimeSignal,
            tenant: UUID | None,
            dedup_id: str | None,
            hlc: HlcTimestamp,
        ) -> None:
            await self._handle(ctx, signal, tenant, dedup_id, hlc)

        await self.source.run(ctx, handle)

    # ....................... #

    async def _handle(
        self,
        ctx: ExecutionContext,
        signal: RealtimeSignal,
        tenant: UUID | None,
        dedup_id: str | None,
        hlc: HlcTimestamp,
    ) -> None:
        if self.dedup is None or dedup_id is None:
            # ephemeral, or durable with no dedup configured — emit directly
            await self._emit(signal, tenant, event_id=dedup_id)
            return

        # durable: mark (+ store) + emit inside one transaction, so a redelivered
        # signal (relay retry / consumer claim) is recognised and handled once. The
        # tenant is bound (from the header) so the mailbox scopes by the ambient tenant.
        with _bind_tenant(ctx, tenant):
            async with ctx.tx_ctx.scope(self.dedup.tx_route):
                inbox = ctx.inbox(self.dedup.inbox_spec)

                if not await inbox.mark_if_unseen(
                    str(self.dedup.inbox_spec.name), dedup_id
                ):
                    return

                mailbox = self.mailbox if self._should_mailbox(signal) else None

                if mailbox is not None:
                    await mailbox.store(
                        ctx,
                        principal=signal.audience.name,
                        event_id=dedup_id,
                        hlc=hlc,
                        signal=signal,
                    )

                await self._emit_live(
                    signal, tenant, event_id=dedup_id, recoverable=mailbox is not None
                )

    # ....................... #

    def _should_mailbox(self, signal: RealtimeSignal) -> bool:
        """Whether this durable signal is stored for offline replay."""

        if self.mailbox is None or signal.audience.kind is not AudienceKind.PRINCIPAL:
            return False

        if self.event_catalog is None:
            return True  # always-store default

        event = self.event_catalog.get(signal.event)

        return event is None or event.offline_delivery

    # ....................... #

    async def _emit_live(
        self,
        signal: RealtimeSignal,
        tenant: UUID | None,
        *,
        event_id: str | None,
        recoverable: bool,
    ) -> None:
        if (
            recoverable
            and self.presence is not None
            and await self.presence.count(room_for(signal.audience, tenant)) == 0
        ):
            return

        await self._emit(signal, tenant, event_id=event_id)

    # ....................... #

    async def _emit(
        self,
        signal: RealtimeSignal,
        tenant: UUID | None,
        *,
        event_id: str | None = None,
    ) -> None:
        # Uniform delivery envelope (RFC 0006): every frame is ``{id, data}`` — the id
        # is the durable event id (``None`` for ephemeral) so the client dedups
        # live-vs-replayed and acks by it.
        emit = self.sio.emit(
            signal.event,
            data={"id": event_id, "data": signal.payload},
            room=room_for(signal.audience, tenant),
            namespace=self.namespace,
        )

        if self.emit_timeout is None:
            await emit
            return

        await asyncio.wait_for(emit, timeout=self.emit_timeout.total_seconds())

    # ....................... #

    async def join_principal(
        self,
        sid: str,
        principal_id: UUID | str,
        tenant: UUID | None,
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
            sid,
            room_for(Audience.topic(topic), tenant),
            namespace=self.namespace,
        )

    # ....................... #

    async def leave_topic(self, sid: str, topic: str, tenant: UUID | None) -> None:
        """Unsubscribe *sid* from a tenant-scoped topic room."""

        await self.sio.leave_room(
            sid,
            room_for(Audience.topic(topic), tenant),
            namespace=self.namespace,
        )
