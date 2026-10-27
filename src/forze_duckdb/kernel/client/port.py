"""Structural protocol for DuckDB clients."""

from collections.abc import AsyncGenerator, Awaitable, Sequence
from datetime import timedelta
from typing import Protocol

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .value_objects import DuckDbQueryResult

# ----------------------- #


class DuckDbClientPort(Protocol):
    """Operations implemented by :class:`DuckDbClient`."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def run_query(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[DuckDbQueryResult]: ...  # pragma: no cover

    def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> Awaitable[list[JsonDict]]: ...  # pragma: no cover

    def run_query_streamed(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[JsonDict]]: ...  # pragma: no cover

    def run_command(
        self,
        command: str,
        params: BaseModel | JsonDict | None = None,
        *,
        timeout: timedelta | None = None,
    ) -> Awaitable[None]: ...  # pragma: no cover


# ....................... #


class DuckDbDynamicReadClientPort(DuckDbClientPort, Protocol):
    """A :class:`DuckDbClientPort` that also runs governed dynamic reads.

    Kept apart so a client implementing only :class:`DuckDbClientPort` keeps satisfying it;
    the dynamic-read adapter is the one consumer that needs :meth:`run_dynamic`.
    """

    def run_dynamic(
        self,
        statement: str,
        params: JsonDict | None = None,
        *,
        row_probe: int,
        timeout: timedelta,
        use: str | None = None,
    ) -> Awaitable[list[JsonDict]]: ...  # pragma: no cover
