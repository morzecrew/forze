"""Structural protocol for Postgres clients (single DSN or tenant-routed)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Sequence
from contextlib import AbstractAsyncContextManager
from typing import (
    Any,
    Literal,
    Protocol,
    overload,
)

from psycopg import AsyncConnection
from psycopg.abc import Params, QueryNoTemplate

from forze.base.primitives import JsonDict

from .types import RowFactory
from .value_objects import DeadlinePushdownPolicy, PostgresTransactionOptions

# ----------------------- #


class PostgresClientPort(Protocol):
    """Operations implemented by :class:`PostgresClient` and routed variants.

    **Commits and transactions:** :meth:`execute` and :meth:`execute_many` commit
    automatically when *not* inside a transaction (no context-bound connection).
    :meth:`fetch_one`, :meth:`fetch_all`, and :meth:`fetch_value` default to
    ``commit=False``; pass ``commit=True`` on a fetch only when you intentionally
    want an auto-commit read outside an explicit :meth:`transaction` block.
    """

    def close(self) -> Awaitable[None]:
        """Close pools / release resources."""

        ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]:
        """Connectivity probe."""

        ...  # pragma: no cover

    def is_in_transaction(self) -> bool: ...  # pragma: no cover

    def query_concurrency_limit(self) -> int: ...  # pragma: no cover

    def deadline_pushdown(self) -> DeadlinePushdownPolicy | None:
        """The invocation-deadline ``statement_timeout`` push-down policy, or ``None`` when
        disabled (see ``PostgresConfig.push_invocation_deadline``)."""

        ...  # pragma: no cover

    async def apply_statement_timeout(self, ms: int) -> None:
        """Set ``statement_timeout`` on the current root transaction, deferring to
        materialization for a not-yet-opened lazy scope (so it forces no early checkout)."""

        ...  # pragma: no cover

    def gather_concurrency_semaphore(self) -> asyncio.Semaphore:
        """Shared semaphore for :func:`~forze_postgres.kernel.client.gather_db_work`."""

        ...  # pragma: no cover

    def require_transaction(self) -> None: ...  # pragma: no cover

    def bound_connection(
        self,
    ) -> AbstractAsyncContextManager[AsyncConnection]: ...  # pragma: no cover

    def transaction(
        self,
        *,
        options: PostgresTransactionOptions | None = None,
    ) -> AbstractAsyncContextManager[AsyncConnection | None]: ...  # pragma: no cover

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

    def fetch_all_batched(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        batch_size: int = 2000,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> AsyncGenerator[list[JsonDict] | list[tuple[Any, ...]]]: ...  # pragma: no cover

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
