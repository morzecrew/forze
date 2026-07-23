"""Connection lifecycle for the realtime gateway — auto-join + presence.

On **connect**, a connection is auto-joined to its tenant-scoped *principal* room
(derived from the authenticated identity), so a later emit to that principal
reaches it. On **disconnect**, Socket.IO drops the room membership automatically;
the handler here only updates presence.

Presence is optional and pluggable (:class:`RealtimePresence`): the in-memory
tracker is fine for a single node; a cross-node deployment wants a TTL-backed
store (e.g. Redis) so a crashed node's entries expire rather than leak.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractContextManager
from datetime import datetime
from inspect import isawaitable
from typing import (
    Any,
    final,
)
from uuid import UUID

import attrs
from socketio.async_server import AsyncServer
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.application.integrations.realtime import (
    InMemoryRealtimePresence as InMemoryRealtimePresence,  # re-export: established home
)
from forze.application.integrations.realtime import (
    RealtimePresence as RealtimePresence,  # re-export: established home
)
from forze.application.integrations.realtime import (
    acknowledge_up_to,
    iter_replay,
    negotiate_realtime_protocol,
    resolve_client_key,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp, utcnow

from .exceptions import (
    GENERIC_INTERNAL_DETAIL,
    build_core_exception_ack,
    build_unhandled_exception_ack,
    is_server_error_kind,
    log_server_error,
)
from .gateway import room_for
from .mailbox import MailboxCursors, RealtimeMailbox
from .routing import IDENTITY_SESSION_KEY, SocketIOConnect

# ----------------------- #

CONNECTION_SESSION_KEY = "forze.realtime_connection"
"""Session key holding the :class:`RealtimeConnection` for disconnect presence."""

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RealtimeConnection:
    """The resolved identity of a live connection, for room scoping + presence."""

    authn: AuthnIdentity
    """Authenticated principal (also stored for inbound command auth)."""

    tenant: UUID | None = None
    """The connection's tenant, used to scope its rooms."""

    expires_at: datetime | None = None
    """When the connection's credential expires — a **timezone-aware** (UTC) instant;
    ``None`` never expires.

    Captured from the verified assertion/token at connect time (``AuthnIdentity``
    itself is principal-only). A sweeper drops the connection once past this, so a
    long-lived socket can't outlive the credential that authenticated it. A naive
    datetime is refused at construction: the sweep isolates per-connection failures,
    so a naive value would not error the connect — it would make every expiry check
    raise inside the sweep and be logged-and-skipped, silently never enforcing."""

    client: ClientIdentity | None = None
    """The device/session this connection is, keying its offline-replay cursor.

    Resolve it from the connect handshake (a client-supplied ``device_id``) and/or
    the token ``sid``; absent one, the cursor falls back to the per-connection sid."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.expires_at is not None and self.expires_at.tzinfo is None:
            raise exc.configuration(
                "RealtimeConnection.expires_at must be a timezone-aware (UTC) datetime; "
                "a naive value cannot be compared against the aware sweep clock. "
                "Resolve it with tzinfo set (e.g. from the token's exp claim in UTC).",
                code="realtime_expiry_naive",
            )

    # ....................... #

    @property
    def principal_room(self) -> str:
        return room_for(Audience.principal(str(self.authn.principal_id)), self.tenant)

    # ....................... #

    @property
    def principal(self) -> str:
        return str(self.authn.principal_id)

    # ....................... #

    def client_key(self, sid: str) -> str:
        """The stable cursor key: the client's ``device_id``/``session_id``, else *sid*."""

        return resolve_client_key(self.client, fallback=sid)

    # ....................... #

    def is_expired(self, now: datetime) -> bool:
        """Whether the connection's credential has expired as of *now*."""

        return self.expires_at is not None and now >= self.expires_at


# ....................... #

ConnectionResolver = Callable[
    [SocketIOConnect],
    "RealtimeConnection | None | Awaitable[RealtimeConnection | None]",
]
"""Resolve a connection's identity at connect time. Return ``None`` for anonymous
(no principal room joined), or raise a client-safe :class:`CoreException`
(e.g. ``exc.authentication``) to refuse the connection."""


# ....................... #

# RealtimePresence / InMemoryRealtimePresence live in the transport-neutral kernel
# (forze.application.integrations.realtime), imported above and re-exported here —
# presence is only honest if every transport reports into the same store.

# ----------------------- #


async def _resolve(
    resolver: ConnectionResolver,
    connect: SocketIOConnect,
) -> RealtimeConnection | None:
    result = resolver(connect)

    return await result if isawaitable(result) else result


# ....................... #


def _bind_connection(
    ctx: ExecutionContext,
    connection: RealtimeConnection,
) -> AbstractContextManager[None]:
    """Bind the connection's authenticated identity **and** tenant for a fresh scope.

    Replay and ack open a *new* execution scope, which has neither the authn nor the tenant
    bound — so the connection's own identity (captured at connect time) must be re-established,
    or the mailbox/cursor operations run unauthenticated (and a tenant-aware store fails
    closed). Both come from the connection, never from the empty fresh ``ctx``.
    """

    tenant = TenantIdentity(tenant_id=connection.tenant) if connection.tenant is not None else None

    return ctx.inv_ctx.bind_identity(authn=connection.authn, tenant=tenant)


# ....................... #


@final
@attrs.define(slots=True)
class _ReplayProgress:
    """How far a connection's replay has contiguously delivered — the ack clamp's input.

    Socket.IO live emits (room fan-out) race the connect-time replay, so a client can
    receive — and ack — a live frame while older mailbox entries are still draining.
    Without this, that ack's cumulative claim would advance the cursor over the
    undelivered middle and the all-device trim would delete it. Node-local by design:
    the replay runs on the node holding the socket, and ``realtime.ack`` arrives on
    that same node.
    """

    floor: HlcTimestamp | None = None
    """The highest position delivered in order so far (the replay-start cursor until
    the first entry drains); ``None`` when nothing is contiguously delivered yet."""

    complete: bool = False
    """The replay drained fully — every retained entry up to the live tail was sent,
    so cumulative acks need no clamp from here on."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _ConnectionLifecycle:
    """The connect / ack / disconnect handlers, as methods so each can be unit-tested in
    isolation (the closures they replaced could not be reached from a test)."""

    sio: AsyncServer
    namespace: str
    resolve: ConnectionResolver
    presence: RealtimePresence | None = None
    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox] | None = None
    cursors_factory: Callable[[ExecutionContext], MailboxCursors] | None = None
    runtime: ExecutionRuntime | None = None

    _replay_progress: dict[str, _ReplayProgress] = attrs.field(factory=dict, init=False)
    """Per-sid replay progress on this node, created at connect, dropped at disconnect."""

    # ....................... #

    @property
    def replay_enabled(self) -> bool:
        return (
            self.mailbox_factory is not None
            and self.cursors_factory is not None
            and self.runtime is not None
        )

    # ....................... #

    async def replay(self, connection: RealtimeConnection, sid: str) -> None:
        """Drain everything past this device's cursor to the freshly-connected socket."""

        if self.mailbox_factory is None or self.cursors_factory is None or self.runtime is None:
            return

        client_key = connection.client_key(sid)
        progress = self._replay_progress.get(sid)
        delivered = 0
        cap: Any = None

        # Stream the backlog page-by-page inside the scope and emit as we go, so peak
        # memory is one page rather than the whole (up to ``cap``) backlog per
        # reconnecting device. The scope stays open during the emits, but the document
        # query ports do not pin a connection between paged reads.
        async with self.runtime.scope():
            ctx = self.runtime.get_context()

            with _bind_connection(ctx, connection):  # connection identity — fresh scope is empty
                mailbox = self.mailbox_factory(ctx)  # ports resolved for this unit of work
                cursors = self.cursors_factory(ctx)
                cap = getattr(mailbox, "cap", None)
                since = await cursors.get(principal=connection.principal, client_key=client_key)

                if progress is not None and since is not None:
                    progress.floor = since  # the already-acked prefix counts as delivered

                async for entry in iter_replay(
                    mailbox, principal=connection.principal, since=since
                ):
                    await self.sio.emit(
                        entry.event,
                        {"id": entry.event_id, "data": entry.payload},
                        to=sid,
                        namespace=self.namespace,
                    )

                    delivered += 1

                    if progress is not None:
                        progress.floor = entry.hlc

        # A replay that filled the mailbox cap may have stopped short of the retained
        # tail (``replay_since`` exits identically drained-vs-capped), so it must NOT
        # lift the clamp: a live frame acked past the undelivered middle would advance
        # the cursor over it and the all-device trim would hard-delete it. The clamp
        # then holds at the replayed floor until the device reconnects and the next
        # replay continues from there. Only a replay that provably drained (below the
        # cap) — and did not raise — lifts it.
        truncated = cap is not None and delivered >= int(cap)

        if progress is not None and not truncated:
            progress.complete = True

    # ....................... #

    async def on_reauth(self, sid: str, data: Any = None) -> dict[str, Any]:
        """``realtime.reauth {token, ...}``: re-verify credentials in place.

        A rotating access token otherwise forces the client to disconnect and reconnect —
        a reconnect storm at scale, and a mailbox replay per device — just to stay
        authenticated past the old token's expiry. This re-runs the **same** connection
        resolver with the fresh auth payload and swaps the stored identity (including a
        new ``expires_at``, so the expiry sweep keeps its hands off).

        **Same principal, same tenant.** Rooms, presence, replay cursors — everything
        keys off the (tenant, principal) pair, so a payload resolving to a different
        principal, a different tenant, or to anonymous is refused: the socket would stay
        in its old tenant-scoped rooms while everything else used the new identity. That
        is a re-login, and a re-login reconnects.
        """

        session = await self.sio.get_session(sid, namespace=self.namespace)
        current: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

        if current is None:
            return build_core_exception_ack(
                exc.authentication("Reauth on an unauthenticated connection — reconnect")
            )

        connect = SocketIOConnect(sid=sid, namespace=self.namespace, environ={}, auth=data)

        try:
            refreshed = await _resolve(self.resolve, connect)

        except CoreException as error:
            # the ack builder logs server-side kinds itself — no second log here
            return build_core_exception_ack(error)

        except Exception as error:
            # the ack builder logs the traceback itself — no second log here
            return build_unhandled_exception_ack(error)

        if (
            refreshed is None
            or refreshed.principal != current.principal
            or refreshed.tenant != current.tenant
        ):
            return build_core_exception_ack(
                exc.authentication(
                    "Reauth must resolve the same principal and tenant — anything else "
                    "is a re-login, which reconnects. Rooms, replay cursors, and presence "
                    "are all scoped by both; swapping identity in place would leave the "
                    "socket in the old identity's rooms."
                )
            )

        session[IDENTITY_SESSION_KEY] = refreshed.authn
        session[CONNECTION_SESSION_KEY] = refreshed
        await self.sio.save_session(sid, session, namespace=self.namespace)

        return {"ok": True}

    # ....................... #

    async def on_ack(self, sid: str, data: Any = None) -> None:
        """``realtime.ack {up_to}``: advance the device cursor, trim the all-device floor."""

        if self.mailbox_factory is None or self.cursors_factory is None or self.runtime is None:
            return

        session = await self.sio.get_session(sid, namespace=self.namespace)
        connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

        raw = (  # pyright: ignore[reportUnknownVariableType]
            data.get("up_to")  # pyright: ignore[reportUnknownMemberType]
            if isinstance(data, Mapping)
            else None
        )
        event_id = (
            str(raw) if raw else None  # pyright: ignore[reportUnknownArgumentType]
        )

        if connection is None or event_id is None:
            return

        # Live room emits race the connect-time replay, so an ack can name a frame
        # delivered ahead of the drain. Clamp it to the replay's contiguous floor —
        # unclamped it would advance the cursor over undelivered entries, and the
        # all-device trim below the new floor would delete them (silent loss).
        progress = self._replay_progress.get(sid)
        delivered_floor: HlcTimestamp | None = None

        if progress is not None and not progress.complete:
            if progress.floor is None:
                return  # nothing contiguously delivered yet — nothing an ack can claim

            delivered_floor = progress.floor

        async with self.runtime.scope():
            ctx = self.runtime.get_context()
            with _bind_connection(ctx, connection):
                await acknowledge_up_to(
                    self.mailbox_factory(ctx),
                    self.cursors_factory(ctx),
                    principal=connection.principal,
                    client_key=connection.client_key(sid),
                    event_id=event_id,
                    delivered_floor=delivered_floor,
                )

    # ....................... #

    async def on_connect(self, sid: str, environ: Mapping[str, Any], auth: Any = None) -> None:
        """Negotiate the protocol, authenticate, auto-join the principal room, replay."""

        connect = SocketIOConnect(sid=sid, namespace=self.namespace, environ=environ, auth=auth)

        try:
            # Handshake first: the connection speaks exactly one protocol version for its
            # lifetime (missing = 1); an unsupported one is refused before any auth work.
            negotiate_realtime_protocol(auth.get("protocol") if isinstance(auth, Mapping) else None)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]

            connection = await _resolve(self.resolve, connect)

        except SocketIOConnectionRefusedError:
            raise

        except CoreException as error:
            if is_server_error_kind(error.kind):
                log_server_error(error, core=error)
                raise SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL) from error

            raise SocketIOConnectionRefusedError(error.summary) from error

        except Exception as error:
            log_server_error(error)
            raise SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL) from error

        if connection is None:
            return  # anonymous: no principal room, may still join topics later

        session = await self.sio.get_session(sid, namespace=self.namespace)
        session[IDENTITY_SESSION_KEY] = connection.authn
        session[CONNECTION_SESSION_KEY] = connection
        await self.sio.save_session(sid, session, namespace=self.namespace)

        if self.replay_enabled:
            # Registered before the room join: from the first live emit onward, acks
            # must be clamped to what the (not yet started) replay has delivered.
            self._replay_progress[sid] = _ReplayProgress()

        try:
            room = connection.principal_room
            await self.sio.enter_room(sid, room, namespace=self.namespace)

            if self.presence is not None:
                await self.presence.joined(room, sid)

            if self.replay_enabled:
                # replay is best-effort: a drain error must not refuse the live connection
                try:
                    await self.replay(connection, sid)

                except Exception as error:
                    log_server_error(error)

        except BaseException:
            # A raise here refuses the connect, and a refused connect never reaches
            # on_disconnect — without this pop, every failed room join or presence
            # write would leak its progress entry forever (sids are never reused).
            self._replay_progress.pop(sid, None)
            raise

    # ....................... #

    async def on_disconnect(self, sid: str) -> None:
        """Update presence on disconnect (Socket.IO drops room membership itself)."""

        self._replay_progress.pop(sid, None)

        if self.presence is None:
            return

        session = await self.sio.get_session(sid, namespace=self.namespace)
        connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

        if connection is not None:
            await self.presence.left(connection.principal_room, sid)


# ....................... #


def attach_realtime_connection(
    sio: AsyncServer,
    *,
    namespace: str = "/",
    resolve: ConnectionResolver,
    presence: RealtimePresence | None = None,
    mailbox_factory: Callable[[ExecutionContext], RealtimeMailbox] | None = None,
    cursors_factory: Callable[[ExecutionContext], MailboxCursors] | None = None,
    runtime: ExecutionRuntime | None = None,
) -> None:
    """Register connect/disconnect handlers that auto-join the principal room.

    The resolved identity is stored under both the inbound identity key (so
    command handlers see it) and a gateway key (for disconnect presence). A
    client-safe :class:`CoreException` from *resolve* refuses the connection;
    a server-side one is logged and refused generically.

    When *mailbox_factory*, *cursors_factory* and *runtime* are all supplied, offline
    replay is enabled: on connect the connection's device is replayed
    everything past its cursor, and a ``realtime.ack {up_to}`` event advances that cursor
    so a device never re-receives what it acked. The factories build their store with
    ports resolved against each unit-of-work ctx (e.g. ``build_realtime_mailbox``); the
    device is keyed by
    :meth:`RealtimeConnection.client_key` (its ``device_id``/``session_id``, else
    the per-connection sid).

    A ``realtime.reauth`` event is always registered: a client whose token rotates sends
    the fresh auth payload and the **same** resolver re-verifies it in place — same
    principal only — swapping the stored identity and ``expires_at`` without a reconnect
    (and without the replay a reconnect costs). Set ``expires_at`` when resolving so the
    expiry sweep has something to enforce between reauths.

    .. important::
        Socket.IO keeps **one** ``connect`` handler per namespace, so this is the
        *single* connect path: it both authenticates (stores the ``AuthnIdentity``)
        and auto-joins. Do **not** also pass an ``identity_resolver`` to
        :class:`ForzeSocketIOAdapter` for the same namespace — its connect handler
        would silently overwrite this one (or vice versa). Resolve identity here.
    """

    # Offline replay needs all three of mailbox_factory / cursors_factory / runtime. Partial
    # wiring would silently disable replay (broken offline delivery, no warning) — fail closed.
    replay_parts = (mailbox_factory, cursors_factory, runtime)

    if sum(p is not None for p in replay_parts) not in (0, len(replay_parts)):
        raise exc.configuration(
            "Offline replay needs all of mailbox_factory, cursors_factory, and runtime "
            "(or none) — partial wiring would silently disable replay"
        )

    lifecycle = _ConnectionLifecycle(
        sio=sio,
        namespace=namespace,
        resolve=resolve,
        presence=presence,
        mailbox_factory=mailbox_factory,
        cursors_factory=cursors_factory,
        runtime=runtime,
    )

    sio.on("connect", handler=lifecycle.on_connect, namespace=namespace)
    sio.on("disconnect", handler=lifecycle.on_disconnect, namespace=namespace)
    sio.on("realtime.reauth", handler=lifecycle.on_reauth, namespace=namespace)

    if lifecycle.replay_enabled:
        sio.on("realtime.ack", handler=lifecycle.on_ack, namespace=namespace)


# ----------------------- #


def _local_connections(sio: AsyncServer, namespace: str) -> list[str]:
    """The sids connected to *namespace* on this node (room ``None`` = all)."""

    return [sid for sid, _eio in sio.manager.get_participants(namespace, None)]


# ....................... #


async def sweep_expired_connections(
    sio: AsyncServer,
    *,
    namespace: str = "/",
    now: datetime | None = None,
) -> int:
    """Disconnect connections whose credential has expired; return how many.

    Identity is bound once at connect, so without this a socket outlives the
    credential that authenticated it. Run it periodically (see
    :func:`~forze_socketio.realtime_identity_expiry_lifecycle_step`). Only this
    node's connections are visible — each node sweeps its own.
    """

    moment = now if now is not None else utcnow()
    dropped = 0

    for sid in _local_connections(sio, namespace):
        # Per-connection isolation: one socket that fails to read or disconnect (already
        # gone, transport hiccup) must not shield every later socket from the sweep —
        # that would extend expired credentials past their lifetime for as long as the
        # early failure persists. CancelledError is a BaseException and still propagates.
        try:
            session = await sio.get_session(sid, namespace=namespace)
            connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

            if connection is not None and connection.is_expired(moment):
                await sio.disconnect(sid, namespace=namespace)
                dropped += 1

        except Exception as error:
            log_server_error(error)

    return dropped


# ....................... #


async def refresh_presence(
    sio: AsyncServer, presence: RealtimePresence, *, namespace: str = "/"
) -> int:
    """Re-assert presence for every connection this node holds; return how many.

    A TTL-backed presence store (e.g. Redis) expires entries so a crashed node's
    rows don't leak — which means live connections must re-assert (heartbeat) or
    they'd wrongly expire too. Run this on an interval shorter than the store's TTL
    (see :func:`~forze_socketio.realtime_presence_heartbeat_lifecycle_step`).
    """

    refreshed = 0

    for sid in _local_connections(sio, namespace):
        # Per-connection isolation: one failed heartbeat must not starve every later
        # connection's — under a TTL store, a persistently-failing early entry would
        # otherwise expire healthy connections and the gateway would skip their live
        # deliveries. CancelledError is a BaseException and still propagates.
        try:
            session = await sio.get_session(sid, namespace=namespace)
            connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

            if connection is not None:
                await presence.joined(connection.principal_room, sid)
                refreshed += 1

        except Exception as error:
            log_server_error(error)

    return refreshed
