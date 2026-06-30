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

from contextlib import AbstractContextManager
from datetime import datetime
from inspect import isawaitable
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Protocol,
    final,
    runtime_checkable,
)
from uuid import UUID

import attrs
from socketio.async_server import AsyncServer
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity, ClientIdentity
from forze.application.contracts.realtime import Audience
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp, utcnow

from .exceptions import GENERIC_INTERNAL_DETAIL, is_server_error_kind, log_server_error
from .gateway import room_for
from .mailbox import MailboxCursors, MailboxEntry, RealtimeMailbox
from .routing import IDENTITY_SESSION_KEY, SocketIOConnect


async def _iter_replay(
    mailbox: RealtimeMailbox,
    *,
    principal: str,
    since: HlcTimestamp | None,
) -> AsyncIterator[MailboxEntry]:
    """Stream a mailbox's backlog, preferring the paged ``replay_since`` when present.

    ``replay_since`` is optional on the :class:`RealtimeMailbox` protocol, so a mailbox
    that only implements the buffered :meth:`~RealtimeMailbox.read_since` still works —
    it just materializes the page at once instead of streaming.
    """

    stream = getattr(mailbox, "replay_since", None)

    if stream is not None:
        async for entry in stream(principal=principal, since=since):
            yield entry

        return

    for entry in await mailbox.read_since(principal=principal, since=since):
        yield entry

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
    """When the connection's credential expires; ``None`` never expires.

    Captured from the verified assertion/token at connect time (``AuthnIdentity``
    itself is principal-only). A sweeper drops the connection once past this, so a
    long-lived socket can't outlive the credential that authenticated it."""

    client: ClientIdentity | None = None
    """The device/session this connection is, keying its offline-replay cursor.

    Resolve it from the connect handshake (a client-supplied ``device_id``) and/or
    the token ``sid``; absent one, the cursor falls back to the per-connection sid."""

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

        key = self.client.key if self.client is not None else None

        return key or sid

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


# ----------------------- #


@runtime_checkable
class RealtimePresence(Protocol):
    """Tracks how many connections occupy a room (e.g. is a principal online)."""

    def joined(self, room: str, sid: str) -> Awaitable[None]: ...  # pragma: no cover

    def left(self, room: str, sid: str) -> Awaitable[None]: ...  # pragma: no cover

    def count(self, room: str) -> Awaitable[int]: ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True)
class InMemoryRealtimePresence(RealtimePresence):
    """Single-node, in-memory presence. For multi-node use a TTL-backed store."""

    _rooms: dict[str, set[str]] = attrs.field(factory=dict, init=False)

    async def joined(self, room: str, sid: str) -> None:
        self._rooms.setdefault(room, set()).add(sid)

    async def left(self, room: str, sid: str) -> None:
        members = self._rooms.get(room)

        if members is not None:
            members.discard(sid)

            if not members:  # drop the empty bucket so churn doesn't leak room keys
                del self._rooms[room]

    async def count(self, room: str) -> int:
        return len(self._rooms.get(room, ()))


# ----------------------- #


async def _resolve(
    resolver: ConnectionResolver, connect: SocketIOConnect
) -> RealtimeConnection | None:
    result = resolver(connect)

    return await result if isawaitable(result) else result


def _bind_connection(
    ctx: ExecutionContext, connection: RealtimeConnection
) -> AbstractContextManager[None]:
    """Bind the connection's authenticated identity **and** tenant for a fresh scope.

    Replay and ack open a *new* execution scope, which has neither the authn nor the tenant
    bound — so the connection's own identity (captured at connect time) must be re-established,
    or the mailbox/cursor operations run unauthenticated (and a tenant-aware store fails
    closed). Both come from the connection, never from the empty fresh ``ctx``.
    """

    tenant = (
        TenantIdentity(tenant_id=connection.tenant)
        if connection.tenant is not None
        else None
    )

    return ctx.inv_ctx.bind_identity(authn=connection.authn, tenant=tenant)


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

        if (
            self.mailbox_factory is None
            or self.cursors_factory is None
            or self.runtime is None
        ):
            return

        client_key = connection.client_key(sid)

        # Stream the backlog page-by-page inside the scope and emit as we go, so peak
        # memory is one page rather than the whole (up to ``cap``) backlog per
        # reconnecting device. The scope stays open during the emits, but the document
        # query ports do not pin a connection between paged reads.
        async with self.runtime.scope():
            ctx = self.runtime.get_context()
            with _bind_connection(
                ctx, connection
            ):  # connection identity — fresh scope is empty
                mailbox = self.mailbox_factory(
                    ctx
                )  # ports resolved for this unit of work
                cursors = self.cursors_factory(ctx)
                since = await cursors.get(
                    principal=connection.principal, client_key=client_key
                )

                async for entry in _iter_replay(
                    mailbox, principal=connection.principal, since=since
                ):
                    await self.sio.emit(
                        entry.event,
                        {"id": entry.event_id, "data": entry.payload},
                        to=sid,
                        namespace=self.namespace,
                    )

    # ....................... #

    async def on_ack(self, sid: str, data: Any = None) -> None:
        """``realtime.ack {up_to}``: advance the device cursor, trim the all-device floor."""

        if (
            self.mailbox_factory is None
            or self.cursors_factory is None
            or self.runtime is None
        ):
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

        async with self.runtime.scope():
            ctx = self.runtime.get_context()
            with _bind_connection(ctx, connection):
                mailbox = self.mailbox_factory(ctx)
                cursors = self.cursors_factory(ctx)
                position = await mailbox.position_of(
                    principal=connection.principal, event_id=event_id
                )

                if position is not None:
                    await cursors.advance(
                        principal=connection.principal,
                        client_key=connection.client_key(sid),
                        up_to=position,
                    )

                    # trim what every known device has now acked (TTL/cap is the backstop)
                    floor = await cursors.min_cursor(principal=connection.principal)

                    if floor is not None:
                        await mailbox.trim(principal=connection.principal, before=floor)

    # ....................... #

    async def on_connect(
        self, sid: str, environ: Mapping[str, Any], auth: Any = None
    ) -> None:
        """Authenticate (store the identity), auto-join the principal room, replay."""

        connect = SocketIOConnect(
            sid=sid, namespace=self.namespace, environ=environ, auth=auth
        )

        try:
            connection = await _resolve(self.resolve, connect)

        except SocketIOConnectionRefusedError:
            raise

        except CoreException as error:
            if is_server_error_kind(error.kind):
                log_server_error(error, core=error)
                raise SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL) from error

            raise SocketIOConnectionRefusedError(error.summary) from error

        except Exception as error:  # noqa: BLE001
            log_server_error(error)
            raise SocketIOConnectionRefusedError(GENERIC_INTERNAL_DETAIL) from error

        if connection is None:
            return  # anonymous: no principal room, may still join topics later

        session = await self.sio.get_session(sid, namespace=self.namespace)
        session[IDENTITY_SESSION_KEY] = connection.authn
        session[CONNECTION_SESSION_KEY] = connection
        await self.sio.save_session(sid, session, namespace=self.namespace)

        room = connection.principal_room
        await self.sio.enter_room(sid, room, namespace=self.namespace)

        if self.presence is not None:
            await self.presence.joined(room, sid)

        if self.replay_enabled:
            # replay is best-effort: a drain error must not refuse the live connection
            try:
                await self.replay(connection, sid)

            except Exception as error:  # noqa: BLE001
                log_server_error(error)

    # ....................... #

    async def on_disconnect(self, sid: str) -> None:
        """Update presence on disconnect (Socket.IO drops room membership itself)."""

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

    if lifecycle.replay_enabled:
        sio.on("realtime.ack", handler=lifecycle.on_ack, namespace=namespace)


# ----------------------- #


def _local_connections(sio: AsyncServer, namespace: str) -> "list[str]":
    """The sids connected to *namespace* on this node (room ``None`` = all)."""

    return [sid for sid, _eio in sio.manager.get_participants(namespace, None)]


# ....................... #


async def sweep_expired_connections(
    sio: AsyncServer, *, namespace: str = "/", now: datetime | None = None
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
        session = await sio.get_session(sid, namespace=namespace)
        connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

        if connection is not None and connection.is_expired(moment):
            await sio.disconnect(sid, namespace=namespace)
            dropped += 1

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
        session = await sio.get_session(sid, namespace=namespace)
        connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

        if connection is not None:
            await presence.joined(connection.principal_room, sid)
            refreshed += 1

    return refreshed
