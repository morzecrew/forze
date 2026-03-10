from ._compat import require_socketio

require_socketio()

# ....................... #

from typing import Any, Optional

import socketio as socketio
from socketio.async_manager import AsyncManager

from forze.base.errors import CoreError

# ----------------------- #


def build_socketio_server(
    *,
    redis_url: str | None = None,
    redis_channel: str = "socketio",
    redis_write_only: bool = False,
    client_manager: Optional[AsyncManager] = None,
    **kwargs: Any,
) -> socketio.AsyncServer:
    """Build an :class:`socketio.AsyncServer` with optional Redis backplane.

    Uses the official Socket.IO Redis manager when ``redis_url`` is provided.

    :param redis_url: Optional Redis DSN for distributed Socket.IO delivery.
    :param redis_channel: Redis pub/sub channel used by the backplane.
    :param redis_write_only: Use write-only manager mode for emit-only workers.
    :param client_manager: Optional prebuilt client manager.
    :param kwargs: Additional arguments passed to :class:`socketio.AsyncServer`.
    :returns: Configured Socket.IO server instance.
    :raises CoreError: If both ``redis_url`` and ``client_manager`` are provided.
    """

    if redis_url is not None and client_manager is not None:
        raise CoreError("Pass either `redis_url` or `client_manager`, not both")

    if redis_url is not None:
        client_manager = socketio.AsyncRedisManager(
            redis_url,
            channel=redis_channel,
            write_only=redis_write_only,
        )

    return socketio.AsyncServer(
        client_manager=client_manager,
        **kwargs,
    )


# ....................... #


def build_socketio_asgi_app(
    server: socketio.AsyncServer,
    *,
    other_asgi_app: Any = None,
    socketio_path: str = "socket.io",
) -> socketio.ASGIApp:
    """Wrap a Socket.IO server into an ASGI app.

    :param server: Socket.IO async server instance.
    :param other_asgi_app: Optional downstream ASGI app (for mixed HTTP/SIO apps).
    :param socketio_path: Socket.IO endpoint path.
    :returns: Socket.IO ASGI application wrapper.
    """
    return socketio.ASGIApp(
        server,
        other_asgi_app=other_asgi_app,
        socketio_path=socketio_path,
    )
