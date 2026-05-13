from collections.abc import Callable
from datetime import timedelta
from typing import Any, final

import attrs

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class RedisConfig:
    """Redis connection pool configuration."""

    max_size: int = 20
    socket_timeout: timedelta | None = timedelta(seconds=5)
    connect_timeout: timedelta | None = None
    health_check_interval: timedelta | None = timedelta(seconds=30)
    socket_keepalive: bool = True
    retry_on_timeout: bool = True
    client_name: str | None = "forze"
    read_retry_attempts: int = 0
    """How many times to retry idempotent reads (``get``, ``mget``, ``exists``) after transient failures."""

    read_retry_base_delay: timedelta = timedelta(milliseconds=50)
    """Base delay before the first read retry; grows exponentially with the attempt index."""

    pubsub_auto_reconnect: bool = False
    """When ``True``, :meth:`RedisClient.subscribe` reconnects the pub/sub connection after transport errors (long-running consumers; opt-in)."""

    pubsub_reconnect_max_delay: timedelta = timedelta(seconds=30)
    """Upper bound for exponential backoff between pub/sub reconnect attempts."""

    on_read_retry: Callable[[str, int], Any] | None = None
    """Optional hook ``(operation_name, attempt_index)`` when a read is about to be retried (``attempt_index`` ≥ 1)."""

    on_pubsub_reconnect: Callable[[], Any] | None = None
    """Optional hook invoked after a pub/sub transport error before resubscribing."""
