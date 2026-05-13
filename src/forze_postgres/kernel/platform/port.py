"""Structural protocol for Postgres clients (single DSN or tenant-routed)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import asyncio
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Literal,
    Protocol,
    Sequence,
    overload,
)

from psycopg import AsyncConnection
from psycopg.abc import Params, QueryNoTemplate

from forze.base.primitives import JsonDict

from .types import RowFactory
from .value_objects import PostgresTransactionOptions

# ----------------------- #


class PostgresClientPort(Protocol):
    """Operations implemented by :class:`PostgresClient` and routed variants.

    **Commits and transactions:** :meth:`execute` and :meth:`execute_many` commit
    automatically when *not* inside a transaction (no context-bound connection).
    :meth:`fetch_one`, :meth:`fetch_all`, and :meth:`fetch_value` default to
    ``commit=False``; pass ``commit=True`` on a fetch only when you intentionally
    want an auto-commit read outside an explicit :meth:`transaction` block.
    """

    async def close(self) -> None:
        """Close pools / release resources."""

        ...  # pragma: no cover

    async def health(self) -> tuple[str, bool]:
        """Connectivity probe."""

        ...  # pragma: no cover

    def is_in_transaction(self) -> bool: ...  # pragma: no cover

    def query_concurrency_limit(self) -> int: ...  # pragma: no cover

    def gather_concurrency_semaphore(self) -> asyncio.Semaphore:
        """Shared semaphore for :func:`~forze_postgres.kernel.db_gather.gather_db_work`."""

        ...  # pragma: no cover

    def require_transaction(self) -> None: ...  # pragma: no cover

    def bound_connection(
        self,
    ) -> AsyncContextManager[AsyncConnection]: ...  # pragma: no cover

    def transaction(
        self,
        *,
        options: PostgresTransactionOptions | None = None,
    ) -> AsyncContextManager[AsyncConnection]: ...  # pragma: no cover

    @overload
    def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: Literal[False] = False,
    ) -> Awaitable[None]: ...

    @overload
    def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: Literal[True],
    ) -> Awaitable[int]: ...

    def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: bool = False,
    ) -> Awaitable[int | None]: ...  # pragma: no cover

    def execute_many(
        self,
        query: QueryNoTemplate,
        params: Sequence[Params],
    ) -> Awaitable[None]: ...  # pragma: no cover

    @overload
    def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> Awaitable[list[JsonDict]]: ...

    @overload
    def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["tuple"],
        commit: bool = False,
    ) -> Awaitable[list[tuple[Any, ...]]]: ...

    def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> Awaitable[list[JsonDict] | list[tuple[Any, ...]]]: ...  # pragma: no cover

    @overload
    def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> Awaitable[JsonDict | None]: ...

    @overload
    def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["tuple"],
        commit: bool = False,
    ) -> Awaitable[tuple[Any, ...] | None]: ...

    def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> Awaitable[JsonDict | tuple[Any, ...] | None]: ...  # pragma: no cover

    def fetch_value(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        default: Any = None,
    ) -> Awaitable[Any]: ...  # pragma: no cover
