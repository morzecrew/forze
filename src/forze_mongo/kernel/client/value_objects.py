from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from datetime import timedelta
from typing import final

import attrs
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode  # pyright: ignore[reportPrivateUsage]
from pymongo.write_concern import WriteConcern

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

    max_pool_size: int = 100
    """Maximum pool size."""

    min_pool_size: int = 2
    """Minimum pool size."""
