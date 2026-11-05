"""Connection pool snapshots an integration client reports for metrics.

A client that owns a pool of driver connections implements
:class:`SupportsConnectionPoolStats` from its driver's public numbers, or by counting at its
own acquire and release boundary where the driver exposes none, never from a private driver
attribute. ``instrument_connection_pools`` samples it on every metrics collection.

The method is not named ``pool_stats``: routed clients already use that name for their tenant
pool counters (:class:`~forze.application.contracts.tenancy.TenantPoolStats`).
"""

from typing import Protocol, final, runtime_checkable

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConnectionPoolStats:
    """Point-in-time state of one client's connection pool."""

    in_use: int
    """Connections checked out, or being opened, right now."""

    idle: int
    """Connections in the pool not checked out."""

    max_size: int | None
    """The configured cap on the pool's connections, or ``None`` when the pool has none."""

    waiting: int | None = None
    """Callers queued for a connection, or ``None`` when the driver does not expose it."""


# ....................... #


@runtime_checkable
class SupportsConnectionPoolStats(Protocol):
    """Clients able to report their connection pool to ``instrument_connection_pools``."""

    def connection_pool_stats(self) -> ConnectionPoolStats | None:
        """Snapshot the pool, or ``None`` while the client holds no open pool.

        ``in_use`` counts connections checked out, or being opened or reopened by the pool:
        every connection the pool owns that is not idle. Every backend reports that meaning,
        so ``in_use + idle`` is the pool's size. A backend may read the fields without a lock,
        so a sample taken while the pool grows or shrinks can be off by the connections that
        changed; the next collection reads them again.

        Called on every metrics collection, so it reads in-memory state only: no I/O and no
        waiting on a lock.
        """
        ...  # pragma: no cover
