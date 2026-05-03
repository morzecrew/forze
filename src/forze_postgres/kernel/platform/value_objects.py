from datetime import timedelta
from typing import final

import attrs

from forze.base.errors import CoreError

from .._logger import logger
from .types import IsolationLevel

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class PostgresTransactionOptions:
    """Options for :meth:`PostgresClient.transaction`."""

    read_only: bool = False
    """If ``True``, transaction is read-only. Omitted means read-write."""

    isolation: IsolationLevel = "read committed"
    """Transaction isolation level. Omitted means default (read committed)."""


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class PostgresConfig:
    """Connection pool configuration for :class:`PostgresClient`."""

    min_size: int = 2
    """Minimum number of connections in the pool."""

    max_size: int = 15
    """Maximum number of connections in the pool."""

    max_lifetime: timedelta = timedelta(hours=1)
    """Connection lifetime before recycling."""

    max_idle: timedelta = timedelta(minutes=30)
    """Idle time before closing a connection."""

    reconnect_timeout: timedelta = timedelta(seconds=10)
    """Timeout when reconnecting after a failure."""

    num_workers: int = 4
    """Number of worker threads for the pool."""

    pool_headroom: int = 2
    """Connections left for other work when deriving :attr:`max_concurrent_queries`."""

    max_concurrent_queries: int | None = None
    """Max parallel checkout-heavy batch operations outside transactions.

    ``None`` means ``max(1, max_size - pool_headroom)``.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.min_size > self.max_size:
            raise CoreError("Minimum size must be less than or equal to maximum size")

        if self.min_size < 0:
            raise CoreError("Minimum size must be greater than 0")

        if self.max_size < 0:
            raise CoreError("Maximum size must be greater than 0")

        if self.num_workers < 0:
            raise CoreError("Number of workers must be greater than 0")

        if self.pool_headroom < 0:
            raise CoreError("pool_headroom must be greater than or equal to 0")

        if self.max_concurrent_queries is not None and self.max_concurrent_queries < 1:
            raise CoreError("max_concurrent_queries must be at least 1 when set")

        if self.min_size > 10:
            logger.warning(
                "Minimum size is greater than 10 (%s), this is not recommended. Consider using a smaller value.",
                self.min_size,
            )

        if self.max_size > 100:
            logger.warning(
                "Maximum size is greater than 100 (%s), this is not recommended. Consider using a smaller value.",
                self.max_size,
            )

        #! add warnings for timeouts
