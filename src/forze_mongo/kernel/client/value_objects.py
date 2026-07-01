from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from datetime import timedelta
from typing import final

import attrs
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode  # pyright: ignore[reportPrivateUsage]
from pymongo.write_concern import WriteConcern

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class MongoTransactionOptions:
    """Options for :meth:`MongoClient.transaction`."""

    read_concern: ReadConcern | None = None
    """Read concern for the transaction. Omitted means driver default."""

    write_concern: WriteConcern | None = None
    """Write concern for the transaction. Omitted means driver default."""

    read_preference: _ServerMode | None = None
    """Read preference for the transaction. Omitted means primary."""


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class MongoConfig:
    """Client configuration for :class:`MongoClient`."""

    appname: str = "forze"
    """App name for driver metadata."""

    connect_timeout: timedelta = timedelta(seconds=10)
    """Connection timeout."""

    server_selection_timeout: timedelta = timedelta(seconds=10)
    """Server selection timeout."""

    push_invocation_deadline: bool = True
    """Push a bound invocation deadline down as a per-operation client-side timeout (CSOT).

    When ``True`` (default) and an operation runs under a deadline, each data-plane query is
    wrapped in ``pymongo.timeout(remaining + grace)`` — bounding the server ``maxTimeMS``, the
    socket, and the retry budget — so the server cancels a query the invocation deadline would
    kill anyway, freeing the connection cleanly. A kill switch: set ``False`` to keep deadline
    enforcement asyncio-only."""

    max_pool_size: int = 100
    """Maximum pool size."""

    min_pool_size: int = 2
    """Minimum pool size."""

    lazy_transaction: bool = True
    """Defer the server session + ``startTransaction`` until the first operation.

    When ``True`` (the default), opening a transaction scope acquires no session
    and starts no transaction until the first operation runs, so CPU-bound or
    external work before the first operation does not count against MongoDB's
    ``transactionLifetimeLimitSeconds`` (60s default) window. Set ``False`` to
    restore eager session + ``startTransaction`` at scope entry.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.connect_timeout.total_seconds() <= 0:
            raise exc.configuration("Connect timeout must be positive")

        if self.server_selection_timeout.total_seconds() <= 0:
            raise exc.configuration("Server selection timeout must be positive")
