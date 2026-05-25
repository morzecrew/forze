"""Structural protocol for ClickHouse clients."""

from typing import Any, AsyncContextManager, Awaitable, Protocol

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .value_objects import ClickHouseQueryResult

# ----------------------- #


class ClickHouseClientPort(Protocol):
    """Operations implemented by :class:`ClickHouseClient`."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def client(self) -> AsyncContextManager[Any]: ...  # pragma: no cover

    def run_query(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        timeout: int | None = None,
    ) -> Awaitable[ClickHouseQueryResult]: ...  # pragma: no cover

    def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> Awaitable[list[JsonDict]]: ...  # pragma: no cover

    def insert_rows(
        self,
        database: str,
        table: str,
        rows: list[JsonDict],
        *,
        timeout: int | None = None,
    ) -> Awaitable[int]: ...  # pragma: no cover
