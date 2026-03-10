from typing import Any, final

import attrs
from pydantic import TypeAdapter

from ._compat import require_socketio

require_socketio()

# ....................... #

import socketio as socketio

# ----------------------- #

SocketIOSkipSid = str | list[str] | None
"""Socket.IO recipient exclusion selector passed as ``skip_sid``."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SocketIOServerEvent[Payload]:
    """Typed outbound server event specification."""

    event: str
    """Socket.IO event name emitted to clients."""

    payload_type: Any
    """Validation type used to parse and normalize payload before emitting."""

    namespace: str = "/"
    """Default namespace for this event."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SocketIOEventEmitter:
    """Typed emitter for outbound Socket.IO server events."""

    sio: socketio.AsyncServer
    """Socket.IO async server used for message delivery."""

    # ....................... #

    async def emit[Payload](
        self,
        event: SocketIOServerEvent[Payload],
        payload: Any,
        *,
        namespace: str | None = None,
        to: str | None = None,
        room: str | None = None,
        skip_sid: SocketIOSkipSid = None,
    ) -> None:
        """Validate and emit a server event.

        :param event: Event specification.
        :param payload: Raw payload value.
        :param namespace: Optional namespace override.
        :param to: Optional recipient sid.
        :param room: Optional recipient room.
        :param skip_sid: Optional sid (or list of sids) to exclude.
        """
        adapter = TypeAdapter[Any](event.payload_type)
        validated = adapter.validate_python(payload)
        data = adapter.dump_python(validated, mode="json")

        await self.sio.emit(
            event.event,
            data=data,
            namespace=namespace or event.namespace,
            to=to,
            room=room,
            skip_sid=skip_sid,
        )

    # ....................... #

    def for_namespace(self, namespace: str) -> "SocketIONamespaceEmitter":
        """Create a namespace-scoped emitter view.

        :param namespace: Namespace bound to the returned emitter.
        :returns: Namespace-scoped emitter.
        """
        return SocketIONamespaceEmitter(emitter=self, namespace=namespace)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SocketIONamespaceEmitter:
    """Namespace-scoped wrapper around :class:`SocketIOEventEmitter`."""

    emitter: SocketIOEventEmitter
    """Parent emitter."""

    namespace: str
    """Default namespace for all emissions."""

    # ....................... #

    async def emit[Payload](
        self,
        event: SocketIOServerEvent[Payload],
        payload: Any,
        *,
        to: str | None = None,
        room: str | None = None,
        skip_sid: SocketIOSkipSid = None,
    ) -> None:
        """Emit using this emitter's namespace.

        :param event: Event specification.
        :param payload: Raw payload value.
        :param to: Optional recipient sid.
        :param room: Optional recipient room.
        :param skip_sid: Optional sid (or list of sids) to exclude.
        """
        await self.emitter.emit(
            event,
            payload,
            namespace=self.namespace,
            to=to,
            room=room,
            skip_sid=skip_sid,
        )
