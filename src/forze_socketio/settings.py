"""Settings for the Socket.IO server's Redis backplane.

The only thing :func:`~forze_socketio.build_socketio_server` takes from a deployment: which
Redis carries messages between workers, and how this process uses it.

The URL is a plain ``str`` rather than a Redis settings model because this package cannot
import :mod:`forze_redis` — integration packages do not depend on each other. Build it with
``RedisSettings(...).dsn.get_secret_value()`` when the application has one, which is the
normal case.
"""

from pydantic import BaseModel, SecretStr

# ----------------------- #


class SocketIOSettings(BaseModel):
    """Redis backplane configuration for one Socket.IO server."""

    redis_url: SecretStr | None = None
    """``redis[s]://…`` for the backplane, as a secret because it carries the password —
    unwrap it with ``.get_secret_value()`` for ``build_socketio_server(redis_url=...)``.

    Unset runs the server single-process: messages reach only the clients connected to
    *this* worker, which is correct for a single replica and silently wrong for two."""

    redis_channel: str = "socketio"
    """Pub/sub channel the backplane uses. Two deployments sharing a Redis need different
    channels, or each receives the other's broadcasts."""

    redis_write_only: bool = False
    """Publish without subscribing — for a process that emits events but serves no
    connections, such as a worker pushing progress to clients held by the web tier."""


# ....................... #

__all__ = ["SocketIOSettings"]
