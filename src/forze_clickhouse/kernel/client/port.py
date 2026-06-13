"""Structural protocol for ClickHouse clients."""

from datetime import timedelta
from typing import Awaitable, Protocol

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .value_objects import ClickHouseInsertResult, ClickHouseQueryResult

# ----------------------- #


class ClickHouseClientPort(Protocol):
    """Operations implemented by :class:`ClickHouseClient`."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def run_query(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[ClickHouseQueryResult]: ...  # pragma: no cover

    def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> Awaitable[list[JsonDict]]:
        """Fetch all rows (single streaming execution, capped by *max_rows*)."""
        ...  # pragma: no cover

    def insert_rows(
        self,
        database: str,
        table: str,
        rows: list[JsonDict],
        *,
        timeout: timedelta | None = None,
    ) -> Awaitable[ClickHouseInsertResult]: ...  # pragma: no cover

    def run_command(
        self,
        command: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover
