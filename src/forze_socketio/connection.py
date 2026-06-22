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

from inspect import isawaitable
from typing import Any, Awaitable, Callable, Mapping, Protocol, final, runtime_checkable
from uuid import UUID

import attrs
from socketio.async_server import AsyncServer
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.realtime import Audience
from forze.base.exceptions import CoreException

from .exceptions import GENERIC_INTERNAL_DETAIL, is_server_error_kind, log_server_error
from .gateway import room_for
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

    # ....................... #

    @property
    def principal_room(self) -> str:
        return room_for(Audience.principal(str(self.authn.principal_id)), self.tenant)


# ....................... #

ConnectionResolver = Callable[
    [SocketIOConnect], "RealtimeConnection | None | Awaitable[RealtimeConnection | None]"
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

    async def count(self, room: str) -> int:
        return len(self._rooms.get(room, ()))


# ----------------------- #


async def _resolve(resolver: ConnectionResolver, connect: SocketIOConnect) -> RealtimeConnection | None:
    result = resolver(connect)

    return await result if isawaitable(result) else result


def attach_realtime_connection(
    sio: AsyncServer,
    *,
    namespace: str = "/",
    resolve: ConnectionResolver,
    presence: RealtimePresence | None = None,
) -> None:
    """Register connect/disconnect handlers that auto-join the principal room.

    The resolved identity is stored under both the inbound identity key (so
    command handlers see it) and a gateway key (for disconnect presence). A
    client-safe :class:`CoreException` from *resolve* refuses the connection;
    a server-side one is logged and refused generically.

    .. important::
        Socket.IO keeps **one** ``connect`` handler per namespace, so this is the
        *single* connect path: it both authenticates (stores the ``AuthnIdentity``)
        and auto-joins. Do **not** also pass an ``identity_resolver`` to
        :class:`ForzeSocketIOAdapter` for the same namespace — its connect handler
        would silently overwrite this one (or vice versa). Resolve identity here.
    """

    async def connect_handler(sid: str, environ: Mapping[str, Any], auth: Any = None) -> None:
        connect = SocketIOConnect(sid=sid, namespace=namespace, environ=environ, auth=auth)

        try:
            connection = await _resolve(resolve, connect)

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

        session = await sio.get_session(sid, namespace=namespace)
        session[IDENTITY_SESSION_KEY] = connection.authn
        session[CONNECTION_SESSION_KEY] = connection
        await sio.save_session(sid, session, namespace=namespace)

        room = connection.principal_room
        await sio.enter_room(sid, room, namespace=namespace)

        if presence is not None:
            await presence.joined(room, sid)

    async def disconnect_handler(sid: str) -> None:
        if presence is None:
            return

        session = await sio.get_session(sid, namespace=namespace)
        connection: RealtimeConnection | None = session.get(CONNECTION_SESSION_KEY)

        if connection is not None:
            await presence.left(connection.principal_room, sid)

    sio.on("connect", handler=connect_handler, namespace=namespace)
    sio.on("disconnect", handler=disconnect_handler, namespace=namespace)
