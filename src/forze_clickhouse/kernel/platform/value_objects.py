"""ClickHouse client configuration and query results."""

from datetime import timedelta
from typing import final

import attrs

from forze.base.primitives import JsonDict

# ----------------------- #

DEFAULT_TIMEOUT = timedelta(seconds=60)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ClickHouseConfig:
    """Connection settings for :class:`ClickHouseClient`."""

    host: str = "localhost"
    """ClickHouse server host."""

    port: int = 8123
    """HTTP port (default 8123)."""

    username: str = "default"
    """Database user."""

    password: str = ""
    """Database password."""

    database: str = "default"
    """Default database for queries and inserts."""

    secure: bool = False
    """Use HTTPS when ``True``."""

    timeout: timedelta = attrs.field(default=DEFAULT_TIMEOUT)
    """Default query/insert timeout."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ClickHouseQueryResult:
    """Parsed outcome of a ClickHouse query execution."""

    rows: list[JsonDict]
    """Result rows as plain dictionaries."""

    row_count: int
    """Number of rows returned in this batch."""
