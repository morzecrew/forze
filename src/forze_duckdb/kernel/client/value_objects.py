"""DuckDB client configuration and query results."""

from __future__ import annotations

from typing import TYPE_CHECKING, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

if TYPE_CHECKING:
    import pyarrow as pa

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DuckDbConfig:
    """Connection and execution configuration for :class:`DuckDbClient`."""

    threads: int | None = None
    """DuckDB internal worker threads (``PRAGMA threads``). ``None`` keeps the engine default."""

    memory_limit: str | None = None
    """DuckDB memory limit (e.g. ``'2GB'``). ``None`` keeps the engine default."""

    max_concurrent_queries: int = 4
    """Size of the dedicated executor: caps queries running off the event loop at once.

    Keeps DuckDB analytics load isolated from other ``asyncio.to_thread`` users and
    bounds CPU oversubscription (DuckDB also parallelizes a single query internally).
    """

    read_only: bool = False
    """Open the database in read-only mode."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_concurrent_queries < 1:
            raise exc.configuration("DuckDbConfig.max_concurrent_queries must be >= 1")

        if self.threads is not None and self.threads < 1:
            raise exc.configuration("DuckDbConfig.threads must be >= 1 when set")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DuckDbQueryResult:
    """Parsed outcome of a DuckDB query execution.

    Holds the native Arrow table and converts to plain dict rows lazily at the
    shaping boundary (:attr:`rows`), so large scans avoid eager Python-side
    materialization until (and only for) the rows actually consumed.
    """

    arrow: pa.Table
    """Result as a native Arrow table (zero-copy from DuckDB)."""

    # ....................... #

    @property
    def rows(self) -> list[JsonDict]:
        """Result rows as plain dictionaries (materialized from Arrow on access)."""

        return self.arrow.to_pylist()
