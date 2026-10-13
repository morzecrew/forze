"""Client managers whose room fan-out queues each socket's frame in turn.

python-socketio's :class:`~socketio.async_manager.AsyncManager` encodes a room's frame
once, then starts one ``asyncio`` task per receiving socket and waits on them all.
Queuing a frame on a socket is a ``put`` on its Engine.IO queue, which does not wait, so
the tasks buy nothing; at a few thousand sockets in a room, scheduling them is most of what
the event loop does. The managers here send to each socket in turn instead: the same
packets, in the same order, to the same sockets.

Two differences from the library, both on a socket that is already going away:

- a send that fails (the socket closed between listing and sending) is logged and skipped,
  where the library's task fails unobserved;
- a socket whose ping timed out is closed by the send itself, and that close runs its
  disconnect handler before the next socket is sent to, rather than beside it.

An emit with a callback needs a distinct packet per socket and keeps the library's path.
This relies on python-socketio internals (``get_participants``, ``_send_eio_packet``, the
Engine.IO packet class), which is why forze owns it and pins the versions it supports.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from socketio import async_manager, packet
from socketio.async_manager import AsyncManager
from socketio.async_redis_manager import AsyncRedisManager

from forze.base.logging import Logger

# ----------------------- #

_logger = Logger("socketio.fanout")

_eio_packet: Any = async_manager.eio_packet  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]
"""The Engine.IO packet module python-socketio frames with, taken from it rather than imported
from a package forze does not depend on directly."""


class FanoutManager(AsyncManager):
    """An in-process client manager that fans a room out without a task per socket."""

    async def emit(  # type: ignore[override]
        self,
        event: str,
        data: Any,
        namespace: str,
        room: Any = None,
        skip_sid: Any = None,
        callback: Any = None,
        to: Any = None,
        **kwargs: Any,
    ) -> None:
        if callback is not None:
            await super().emit(
                event,
                data,
                namespace,
                room=room,
                skip_sid=skip_sid,
                callback=callback,
                to=to,
                **kwargs,
            )
            return

        room = to or room

        if namespace not in self.rooms:
            return

        # The library's argument shape: a tuple spreads into several arguments, anything
        # else but None is one.
        args: list[Any] = (
            list(data)  # pyright: ignore[reportUnknownArgumentType]
            if isinstance(data, tuple)
            else ([] if data is None else [data])
        )

        skipped = skip_sid if isinstance(skip_sid, list) else [skip_sid]
        encoded = self.server.packet_class(
            packet.EVENT, namespace=namespace, data=[event, *args]
        ).encode()
        frames = [
            _eio_packet.Packet(_eio_packet.MESSAGE, part)
            for part in (encoded if isinstance(encoded, list) else [encoded])
        ]

        send: Callable[[str, Any], Awaitable[None]] = self.server._send_eio_packet  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]

        # A copy of the room: a send that closes a socket changes the room under us.
        for sid, eio_sid in self.get_participants(namespace, room):
            if sid in skipped:
                continue

            try:
                for frame in frames:
                    await send(eio_sid, frame)

            except Exception:
                _logger.debug("Skipped a socket that went away mid fan-out", sid=sid, exc_info=True)


# ....................... #


class FanoutRedisManager(AsyncRedisManager, FanoutManager):
    """The Redis backplane, delivering each host's share of a room through :class:`FanoutManager`.

    The backplane hands a message to its local delivery with ``super().emit``, which this
    class's method order resolves to :class:`FanoutManager` before the library's manager.
    """
