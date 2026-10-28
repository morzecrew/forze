"""The realtime egress gateway — consume realtime signals, bridge to connections.

The egress twin of the inbound :class:`ForzeSocketIOAdapter`. It is **not** a
contract — it is an edge adapter that consumes :class:`RealtimeSignal` s from a
messaging substrate and emits them to the live Socket.IO connections it owns.

Three separate seams:

- **source** — where signals come from (a stream consumer group here); swappable.
- **bridge** — admission, the tenancy gate and the durable mark + mailbox store are the
  shared :class:`~forze.application.execution.realtime.RealtimeDelivery`; the live leg is
  :meth:`RealtimeGateway._emit`: ``signal → room → sio.emit``. The Socket.IO Redis manager
  fans the emit to whichever node holds the room.
- **supervision** — the lifecycle step (see :mod:`forze_socketio.gateway_lifecycle`) runs
  the gateway under the shared core runner
  (:func:`~forze.application.execution.background.run_supervised`): restart on crash with
  jittered backoff, graceful stop at a batch boundary, registered in ``ctx.drainables``.

Room membership (auto-join, topic subscription) is a transport-edge concern too;
the helpers here build the same tenant-scoped room names the gateway emits to, so
publish and membership always agree.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, final
from uuid import UUID

import attrs
from socketio.async_server import AsyncServer

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.realtime import (
    Audience,
    RealtimeEventCatalog,
    RealtimeSignal,
)
from forze.application.execution import ExecutionContext
from forze.application.execution.realtime import (
    GatewayDedup as GatewayDedup,  # re-export: established home
)
from forze.application.execution.realtime import (
    PubSubSignalSource as PubSubSignalSource,  # re-export: established home
)
from forze.application.execution.realtime import RealtimeDelivery, RealtimeGatewayStats
from forze.application.execution.realtime import (
    RealtimeSignalSource as RealtimeSignalSource,  # re-export: established home
)
from forze.application.execution.realtime import (
    SignalHandler as SignalHandler,  # re-export: established home
)
from forze.application.execution.realtime import (
    StreamGroupSignalSource as StreamGroupSignalSource,  # re-export: established home
)
from forze.application.execution.realtime import (
    TenantShardedSignalSource as TenantShardedSignalSource,  # re-export: established home
)
from forze.application.integrations.realtime import (
    refuse_delegated_identity,
)
from forze.application.integrations.realtime import (
    room_for as room_for,  # re-export: established home
)
from forze.base.exceptions import CoreException, exc
from forze.base.logging import Logger
from forze.base.primitives import HlcTimestamp

from .mailbox import RealtimeMailbox

if TYPE_CHECKING:
    from .connection import RealtimePresence

# ----------------------- #

_logger = Logger("socketio.errors")

# ....................... #


# room_for lives in the transport-neutral kernel (imported above, re-exported here):
# the gateway emits to it, the membership helpers join it, and SSE presence reports
# under it — one naming scheme, so publish, membership, and presence always agree.
# The signal sources, the dedup settings and the durable store half live there too
# (forze.application.execution.realtime), shared with the transport-neutral mailbox
# writer, and are re-exported from this module.

# ....................... #


def _with_gateway_logger(source: RealtimeSignalSource) -> RealtimeSignalSource:
    """*source*, logging under ``socketio.errors`` when it names no logger of its own.

    The kernel's sources default to the application logger; under a gateway their delivery
    failures keep landing where they always have, so log routing that filters on
    ``socketio.errors`` still sees them.
    """

    if (
        isinstance(source, StreamGroupSignalSource | PubSubSignalSource | TenantShardedSignalSource)
        and source.logger is None
    ):
        return attrs.evolve(source, logger=_logger)

    return source


# ....................... #


def _mailbox_tenant_unbound(event_id: str | None) -> CoreException:
    """Actionable **per-signal** error when a tenant-aware mailbox has no tenant to
    scope by.

    The gateway is a **cross-tenant** consumer with no ambient tenant of its own
    (the realtime stream is tenant-global). So a tenant-aware mailbox's
    only possible tenant is the stream's ``forze_tenant_id`` header — bound only
    when :attr:`RealtimeGateway.bind_tenant_from_headers` is enabled *and* the header is
    present. Otherwise the adapter raises a bare ``tenant_required``; this names the wiring
    contract instead.

    Deliberately **not** a configuration error: whether a tenant is bound is a
    per-message data condition (one producer omitting the header), and a
    configuration verdict is process-terminal in the consume loop — one untenanted
    durable signal would stop supervision for *every* tenant, redeliver on restart,
    and kill the loop again. Raising a per-signal kind instead leaves this message
    pending (parked): reclaim retries it, and the poison ceiling
    (``max_deliveries``) acks and drops it — a bounded, observable loss scoped to
    the one signal. (Per-tenant *trusted* mailbox scoping without header trust is
    :class:`TenantShardedSignalSource`.)
    """

    return exc.precondition(
        "Realtime gateway cannot store into a tenant-aware mailbox: no tenant is "
        "bound for this signal. The gateway has no ambient tenant of its own, so "
        "the only tenant source is the stream's forze_tenant_id header. Either set "
        "RealtimeGateway.bind_tenant_from_headers=True to bind it (the header must "
        "be present on every signal, and is untrusted/forgeable — enable only where "
        "every stream producer is trusted to assert tenancy), or wire a "
        "tenant-global mailbox route (tenant_aware=False). The signal stays pending "
        "until the delivery ceiling drops it.",
        code="realtime_mailbox_tenant_unbound",
        details={"event_id": event_id},
    )


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
    """When set, durable signals (those with a dedup id) are deduplicated at the gateway —
    exactly-once for mailboxed signals, at-least-once otherwise (see :class:`GatewayDedup`)."""

    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox] | None = None
    """Builds the mailbox once at ``run(ctx)`` start, with its ports resolved. When set
    (with ``dedup``), a durable **principal** signal is stored for offline replay before it
    is emitted, so a recipient offline at emit time receives it on reconnect. Topic and
    ephemeral signals are never mailboxed. A factory (not a built object) so deps
    materialize against the run ctx.

    This is where a gateway declares its mailbox's retention, since it is the gateway that
    decides a mailbox exists at all::

        mailbox_factory=lambda ctx: build_realtime_mailbox(
            ctx, retention=MailboxRetention(max_age=timedelta(days=7)),
        )

    A declared window is only half of it: the build refuses unless a
    :func:`realtime_mailbox_retention_lifecycle_step` is already running for **the same
    mailbox spec and the same window**, so the example above must be wired alongside::

        realtime_mailbox_retention_lifecycle_step(max_age=timedelta(days=7))

    Both omit ``mailbox_spec``, so both resolve the default spec and the step's marker
    covers the factory's build. Declaring a window with nothing sweeping it fails
    ``run(ctx)`` with ``realtime_mailbox_retention_unwired``, and a step whose window
    differs fails with ``realtime_mailbox_retention_mismatch``.

    ``build_realtime_mailbox`` has no retention default, so it cannot be passed bare — the
    mailbox has no delete path of its own, and a gateway that stores signals forever is a
    decision somebody should have made on purpose. Say so explicitly with
    :meth:`MailboxRetention.unbounded`, which needs no sweeper."""

    event_catalog: RealtimeEventCatalog | None = None
    """Optional catalog consulted for the per-event ``offline_delivery`` opt-out; when
    absent, every durable principal signal is mailboxed (always-store default)."""

    presence: "RealtimePresence | None" = None
    """When set with a mailbox, the live emit is skipped for a mailboxed signal whose
    principal room is empty (saves a cross-node fan-out; the reconnect drain delivers
    it). Never skips a signal that is not recoverable from the mailbox."""

    multi_node_backplane: bool | None = None
    """Whether the Socket.IO manager fans emits across processes — the deployment fact
    the presence-skip guard checks. ``None`` (default) infers it from the manager type
    (``AsyncPubSubManager`` and subclasses are multi-node). A custom multi-node manager
    that does not inherit ``AsyncPubSubManager`` is invisible to that inference and
    **must** declare ``True`` here, or a node-local presence tracker would silently
    suppress every mailboxed live emit for users connected on other nodes. ``False``
    overrides the inference for a manager that never crosses processes."""

    bind_tenant_from_headers: bool = False
    """Opt-in: bind each signal's ``forze_tenant_id`` header so a tenant-aware mailbox
    scopes by it. Off by default because the header is untrusted/forgeable — enable only
    on brokers where every producer is trusted to assert tenancy (see :func:`_bind_tenant`)."""

    require_tenant: bool | None = None
    """Refuse (drop, loudly) any signal that resolves **no tenant**, instead of emitting it
    to the untenanted global room.

    The room name is the isolation boundary, and on the tenant-global stream the tenant
    comes from an untrusted header: absent or malformed, the signal would otherwise degrade
    to the unprefixed room — the wrong direction for a tenanted deployment, where "no
    tenant" means "misrouted", never "everyone". ``None`` (the default) **follows the
    deployment's own tenancy declaration**: it resolves to
    :attr:`bind_tenant_from_headers` — a gateway that binds per-signal tenancy is a
    tenanted deployment, and a signal that resolves none there is misrouted, so the
    requirement is on unless explicitly declared off. Set ``False`` only where
    untenanted delivery is the intended, declared posture; ``True`` forces the
    requirement on a gateway that resolves tenants some other way. Dropped signals are
    counted (``stats.untenanted_dropped``) and logged critically; a missing tenant is a
    deterministic property of the signal, so the drop is final (retrying cannot heal
    it)."""

    emit_timeout: timedelta | None = timedelta(seconds=5)
    """Bound on a single ``sio.emit`` — **bounded by default**; ``None`` opts into
    waiting indefinitely.

    Transport-level flow control (a slow consumer) is engine.io's; this only stops
    one stuck delivery (a dead backplane, a wedged manager) from freezing the whole
    consume loop, which an unbounded default silently allowed. On timeout the emit
    raises, so the source's per-signal error policy applies — an ephemeral signal is
    acked (at-most-once) and a durable one is left pending to be redelivered.
    """

    stats: RealtimeGatewayStats | None = None
    """Delivery counters for the live path (emit outcomes, dedup/presence skips,
    admission rejects, mailbox stores). Share the **same** instance with the source's
    ``stats`` and hand it to ``instrument_realtime_gateway`` at assembly."""

    _delivery: RealtimeDelivery = attrs.field(
        init=False,
        repr=False,
        eq=False,
        default=attrs.Factory(
            lambda self: RealtimeDelivery(
                tenant_unbound=_mailbox_tenant_unbound,
                dedup=self.dedup,
                event_catalog=self.event_catalog,
                bind_tenant_from_headers=self.bind_tenant_from_headers,
                require_tenant=self.require_tenant,
                stats=self.stats,
                logger=_logger,
            ),
            takes_self=True,
        ),
    )
    """Admission, the tenancy gate and the durable store — the copy the mailbox writer
    runs too. The resolved tenancy requirement (an explicit :attr:`require_tenant` wins;
    unset follows :attr:`bind_tenant_from_headers`) is decided there."""

    # ....................... #

    async def run(self, ctx: ExecutionContext, *, stop: asyncio.Event | None = None) -> None:
        """Consume signals and emit each to its room, until *stop* (forever when ``None``).

        A stop request is honored at the source's unit boundary (an in-flight batch
        finishes and acks first), so a graceful shutdown never abandons a signal mid-bridge.
        """

        self._refuse_node_local_presence_on_backplane()

        # resolve the mailbox's ports once, against the run ctx (worker-resolved-once)
        mailbox = self.mailbox_factory(ctx) if self.mailbox_factory is not None else None

        async def handle(
            signal: RealtimeSignal,
            tenant: UUID | None,
            dedup_id: str | None,
            hlc: HlcTimestamp,
        ) -> None:
            await self._handle(ctx, mailbox, signal, tenant, dedup_id, hlc)

        await _with_gateway_logger(self.source).run(ctx, handle, stop=stop)

    # ....................... #

    def _refuse_node_local_presence_on_backplane(self) -> None:
        """Fail closed on a node-local presence tracker paired with a multi-node backplane.

        The presence-based emit skip asks "is the principal room empty?" — and with a
        pub/sub manager the room's members live on *other* nodes, invisible to a
        node-local tracker. Every mailboxed signal's live emit would then be skipped
        (clients receive only reconnect replays) while every metric looks healthy — a
        silent multi-node delivery outage. A cluster-wide store (e.g.
        ``RedisRealtimePresence``) marks itself ``cluster_wide = True``; anything else
        combined with a multi-node backplane is refused at run start, the same seam
        that refuses an encrypted realtime route. Whether the backplane is multi-node
        is :attr:`multi_node_backplane` when declared; the ``AsyncPubSubManager`` type
        check is only the fallback inference — a custom multi-node manager need not
        inherit it, and must declare instead.
        """

        if self.presence is None or self.mailbox_factory is None:
            return  # the presence skip is only ever taken for mailboxed signals

        if self.multi_node_backplane is not None:
            multi_node = self.multi_node_backplane
        else:
            from socketio.async_pubsub_manager import AsyncPubSubManager

            multi_node = isinstance(self.sio.manager, AsyncPubSubManager)

        if not multi_node:
            return  # single-process backplane: node-local presence sees every room

        if getattr(self.presence, "cluster_wide", False):
            return

        raise exc.configuration(
            "RealtimeGateway.presence is a node-local tracker, but the Socket.IO "
            "backplane is multi-node (declared via multi_node_backplane, or inferred "
            "from a pub/sub manager): room members on other nodes are invisible here, "
            "so every live emit for a mailboxed signal would be skipped as 'nobody "
            "present' and clients would only receive signals on reconnect-replay. Use "
            "a cluster-wide presence store (e.g. RedisRealtimePresence, which sets "
            "cluster_wide=True) or drop presence from the gateway.",
            code="realtime_presence_node_local",
        )

    # ....................... #

    async def _handle(
        self,
        ctx: ExecutionContext,
        mailbox: RealtimeMailbox | None,
        signal: RealtimeSignal,
        tenant: UUID | None,
        dedup_id: str | None,
        hlc: HlcTimestamp,
    ) -> None:
        await self._delivery.deliver(
            ctx, mailbox, signal, tenant, dedup_id, hlc, emit=self._emit_live
        )

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
            if self.stats is not None:
                self.stats.presence_skipped += 1
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
        # Uniform delivery envelope: every frame is ``{id, data}`` — the id
        # is the durable event id (``None`` for ephemeral) so the client dedups
        # live-vs-replayed and acks by it.
        emit = self.sio.emit(
            signal.event,
            data={"id": event_id, "data": signal.payload},
            room=room_for(signal.audience, tenant),
            namespace=self.namespace,
        )

        try:
            if self.emit_timeout is None:
                await emit
            else:
                await asyncio.wait_for(emit, timeout=self.emit_timeout.total_seconds())

        except Exception:
            # Exception, not BaseException: a shutdown's CancelledError is not an emit
            # failure and must not inflate the counter on its way out.
            if self.stats is not None:
                self.stats.emit_failed += 1
            raise

        if self.stats is not None:
            self.stats.emitted += 1

    # ....................... #

    async def join_principal(
        self,
        sid: str,
        principal_id: UUID | str,
        tenant: UUID | None,
        *,
        identity: AuthnIdentity | None = None,
    ) -> None:
        """Join *sid* to its tenant-scoped principal room (auto-join on connect).

        Never call this for a delegated identity (one with an ``actor``): the principal room
        carries everything addressed to that principal, and the connection path refuses a
        delegated identity for that reason. Pass the connection's *identity* to have it
        refused here too (``delegate_denied``).
        """

        refuse_delegated_identity(identity)

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
