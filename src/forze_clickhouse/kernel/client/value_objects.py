"""ClickHouse client configuration and query results."""

from datetime import timedelta
from typing import final

import attrs
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #

DEFAULT_TIMEOUT = timedelta(seconds=60)
_DEFAULT_CONNECTOR_LIMIT = 100
_DEFAULT_CONNECTOR_LIMIT_PER_HOST = 20
_DEFAULT_KEEPALIVE_TIMEOUT = timedelta(seconds=30)
_DEFAULT_INSERT_BATCH_SIZE = 1000
_MAX_INSERT_ERRORS = 50

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

    password: SecretStr = attrs.field(
        default=SecretStr(""),
        converter=pydantic_secret_converter,
        repr=False,
    )
    """Database password (plain ``str`` input is coerced to :class:`~pydantic.SecretStr`)."""

    database: str = "default"
    """Default database for queries and inserts."""

    secure: bool = False
    """Use HTTPS when ``True``."""

    timeout: timedelta = attrs.field(default=DEFAULT_TIMEOUT)
    """Default query/insert timeout."""

    connector_limit: int = _DEFAULT_CONNECTOR_LIMIT
    """aiohttp connector total connection limit."""

    connector_limit_per_host: int = _DEFAULT_CONNECTOR_LIMIT_PER_HOST
    """aiohttp connector per-host connection limit."""

    keepalive_timeout: timedelta = attrs.field(default=_DEFAULT_KEEPALIVE_TIMEOUT)
    """aiohttp keepalive timeout."""

    read_retry_attempts: int = 0
    """Retry count for idempotent read operations (``run_query``)."""

    read_retry_base_delay: timedelta = attrs.field(default=timedelta(seconds=0.1))
    """Base delay between read retries (exponential backoff)."""

    insert_batch_size: int = _DEFAULT_INSERT_BATCH_SIZE
    """Maximum rows per ``insert_rows`` HTTP request."""

    max_append_rows: int = 10_000
    """Soft cap enforced by analytics adapter ``append`` (raises when exceeded)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")

        if self.keepalive_timeout.total_seconds() <= 0:
            raise exc.configuration("Keepalive timeout must be positive")

        if self.read_retry_base_delay.total_seconds() <= 0:
            raise exc.configuration("Read retry base delay must be positive")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ClickHouseQueryResult:
    """Parsed outcome of a ClickHouse query execution."""

    rows: list[JsonDict]
    """Result rows as plain dictionaries."""

    row_count: int
    """Number of rows returned in this batch."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ClickHouseInsertResult:
    """Outcome of a ClickHouse insert batch."""

    accepted: int
    """Rows accepted."""

    rejected: int = 0
    """Rows rejected (always 0 for ClickHouse insert API failures)."""

    errors: tuple[JsonDict, ...] = attrs.field(factory=tuple)
    """Row-level errors when provided by the engine."""
