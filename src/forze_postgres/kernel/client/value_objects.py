from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

from .._logger import logger
from .types import IsolationLevel

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class PostgresTransactionOptions:
    """Options for :meth:`PostgresClient.transaction`."""

    read_only: bool = False
    """If ``True``, transaction is read-only. Omitted means read-write."""

    isolation: IsolationLevel = "read_committed"
    """Transaction isolation level. Omitted means default (read committed)."""


# ....................... #


@final
@attrs.define(frozen=True, slots=True)
class DeadlinePushdownPolicy:
    """Postgres invocation-deadline push-down policy (see ``PostgresConfig.push_invocation_deadline``).

    A client returns this when the push-down is enabled (``None`` means disabled); the
    transaction manager combines it with the remaining deadline budget to set a per-transaction
    ``statement_timeout``."""

    statement_timeout_cap: timedelta | None = None
    """Static ``statement_timeout`` to tighten against — the effective per-tx bound is the min of
    this and the remaining deadline budget. ``None`` for no static cap."""


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

    statement_timeout: timedelta | None = None
    """If set, ``SET statement_timeout`` on each new pool connection (milliseconds)."""

    push_invocation_deadline: bool = True
    """Push a bound invocation deadline down as a per-transaction ``statement_timeout`` backstop.

    When ``True`` (default) and an operation runs under a deadline, the data-plane transaction
    is bounded by ``SET LOCAL statement_timeout`` = the remaining budget (plus a small grace,
    tighten-only against :attr:`statement_timeout`), so the server cancels a query the
    invocation deadline would kill anyway — freeing the connection cleanly instead of leaving
    it stuck behind an asyncio-cancelled-but-server-running query. A kill switch: set ``False``
    to keep deadline enforcement asyncio-only."""

    lock_timeout: timedelta | None = None
    """If set, ``SET lock_timeout`` on each new pool connection (milliseconds)."""

    idle_in_transaction_session_timeout: timedelta | None = None
    """If set, ``SET idle_in_transaction_session_timeout`` on each new pool connection."""

    application_name: str | None = None
    """If set, ``SET application_name`` on each new pool connection."""

    lazy_transaction: bool = True
    """Defer pool checkout + ``BEGIN`` until the first query inside a transaction.

    When ``True`` (the default), opening a root transaction scope holds no pool
    connection until the first statement runs, so CPU-bound or external work
    before the first query no longer parks a connection idle-in-transaction.
    Pre-bound (UoW) and nested (savepoint) paths are unaffected. Set ``False`` to
    restore eager checkout at scope entry.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.min_size > self.max_size:
            raise exc.configuration(
                "Minimum size must be less than or equal to maximum size"
            )

        if self.min_size < 0:
            raise exc.configuration("Minimum size must be greater than 0")

        if self.max_size < 0:
            raise exc.configuration("Maximum size must be greater than 0")

        if self.num_workers < 0:
            raise exc.configuration("Number of workers must be greater than 0")

        if self.pool_headroom < 0:
            raise exc.configuration("pool_headroom must be greater than or equal to 0")

        if self.max_concurrent_queries is not None and self.max_concurrent_queries < 1:
            raise exc.configuration(
                "max_concurrent_queries must be at least 1 when set"
            )

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

        if self.application_name is not None and len(self.application_name) > 63:
            raise exc.configuration(
                "application_name must be at most 63 characters for Postgres"
            )

        if self.max_lifetime.total_seconds() <= 0:
            raise exc.configuration("Max lifetime must be positive")

        if self.max_idle.total_seconds() <= 0:
            raise exc.configuration("Max idle must be positive")

        if self.reconnect_timeout.total_seconds() <= 0:
            raise exc.configuration("Reconnect timeout must be positive")

        if (
            self.statement_timeout is not None
            and self.statement_timeout.total_seconds() <= 0
        ):
            raise exc.configuration("Statement timeout must be positive")

        if self.lock_timeout is not None and self.lock_timeout.total_seconds() <= 0:
            raise exc.configuration("Lock timeout must be positive")

        if (
            self.idle_in_transaction_session_timeout is not None
            and self.idle_in_transaction_session_timeout.total_seconds() <= 0
        ):
            raise exc.configuration(
                "Idle in transaction session timeout must be positive"
            )
