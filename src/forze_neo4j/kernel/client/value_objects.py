"""Neo4j client configuration value objects."""

from datetime import timedelta
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class Neo4jConfig:
    """Transport-level configuration for a :class:`Neo4jClient`.

    Connection URI and auth are supplied at :meth:`Neo4jClient.initialize` time (so
    credentials are not held on a long-lived frozen config); this holds pool/timeout
    knobs and the default database only.
    """

    database: str | None = None
    """Default Neo4j database for sessions when a call does not override it."""

    max_connection_pool_size: int = 100
    """Maximum number of pooled connections."""

    connection_acquisition_timeout: timedelta = timedelta(seconds=60)
    """How long to wait to acquire a connection from the pool."""

    connection_timeout: timedelta = timedelta(seconds=30)
    """TCP connection establishment timeout."""

    max_transaction_retry_time: timedelta = timedelta(seconds=30)
    """Upper bound on managed-transaction retries."""
