"""BigQuery client configuration and query results."""

from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

_DEFAULT_TIMEOUT = timedelta(seconds=60)
_DEFAULT_POLL_INTERVAL = timedelta(milliseconds=250)
_DEFAULT_MAX_POLL_ATTEMPTS = 240
_DEFAULT_INSERT_BATCH_SIZE = 500
_MAX_INSERT_ERRORS = 50

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BigQueryConfig:
    """Optional overrides for :class:`BigQueryClient`."""

    timeout: timedelta = attrs.field(default=_DEFAULT_TIMEOUT)
    """Default HTTP timeout for BigQuery API calls."""

    use_legacy_sql: bool = False
    """When ``True``, use legacy SQL (default Standard SQL)."""

    maximum_bytes_billed: int | None = None
    """Default maximum bytes billed per query."""

    poll_interval: timedelta = attrs.field(default=_DEFAULT_POLL_INTERVAL)
    """Delay between async job status polls."""

    max_poll_attempts: int = _DEFAULT_MAX_POLL_ATTEMPTS
    """Upper bound on job poll iterations (also capped by request timeout)."""

    read_retry_attempts: int = 0
    """Retry count for idempotent read operations."""

    read_retry_base_delay: timedelta = attrs.field(default=timedelta(seconds=0.1))
    """Base delay between read retries (exponential backoff)."""

    insert_batch_size: int = _DEFAULT_INSERT_BATCH_SIZE
    """Maximum rows per streaming insert request."""

    max_append_rows: int = 10_000
    """Soft cap enforced by analytics adapter ``append``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")

        if self.poll_interval.total_seconds() <= 0:
            raise exc.configuration("Poll interval must be positive")

        if self.max_poll_attempts < 1:
            raise exc.configuration("max_poll_attempts must be >= 1")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BigQueryQueryResult:
    """Parsed outcome of a BigQuery query execution."""

    rows: list[JsonDict]
    """Result rows as plain dictionaries."""

    total_rows: int | None = None
    """Total rows in the full result set when reported by BigQuery."""

    page_token: str | None = None
    """Opaque page token for the next page, if any."""

    job_id: str | None = None
    """Job id when results require follow-up paging via jobs API."""

    total_bytes_processed: int | None = None
    """Bytes processed (populated for dry-run queries)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BigQueryInsertResult:
    """Outcome of a BigQuery streaming insert batch."""

    accepted: int
    """Rows accepted."""

    rejected: int = 0
    """Rows rejected."""

    errors: tuple[JsonDict, ...] = attrs.field(factory=tuple)
    """Row-level insert errors (capped)."""
