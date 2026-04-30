"""Structural protocol for Postgres clients (single DSN or tenant-routed)."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, AsyncContextManager, Literal, Protocol, Sequence, overload

from psycopg import AsyncConnection
from psycopg.abc import Params, QueryNoTemplate

from forze.base.primitives import JsonDict

from .client import PostgresTransactionOptions, RowFactory

# ----------------------- #


class PostgresClientPort(Protocol):
    """Operations implemented by :class:`PostgresClient` and routed variants."""

    async def close(self) -> None:
        """Close pools / release resources."""

        ...  # pragma: no cover

    async def health(self) -> tuple[str, bool]:
        """Connectivity probe."""

        ...  # pragma: no cover

    def is_in_transaction(self) -> bool: ...  # pragma: no cover

    def query_concurrency_limit(self) -> int: ...  # pragma: no cover

    def require_transaction(self) -> None: ...  # pragma: no cover

    def bound_connection(
        self,
    ) -> AsyncContextManager[AsyncConnection]: ...  # pragma: no cover

    def transaction(
        self,
        *,
        options: PostgresTransactionOptions = ...,
    ) -> AsyncContextManager[AsyncConnection]: ...  # pragma: no cover

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: Literal[False] = False,
    ) -> None: ...

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: Literal[True],
    ) -> int: ...

    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: bool = False,
    ) -> int | None: ...  # pragma: no cover

    async def execute_many(
        self, query: QueryNoTemplate, params: Sequence[Params]
    ) -> None: ...  # pragma: no cover

    @overload
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> list[JsonDict]: ...

    @overload
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["tuple"] = "tuple",
        commit: bool = False,
    ) -> list[tuple[Any, ...]]: ...

    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> list[JsonDict] | list[tuple[Any, ...]]: ...  # pragma: no cover

    @overload
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> JsonDict | None: ...

    @overload
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["tuple"] = "tuple",
        commit: bool = False,
    ) -> tuple[Any, ...] | None: ...

    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> JsonDict | tuple[Any, ...] | None: ...  # pragma: no cover

    async def fetch_value(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        default: Any = None,
    ) -> Any: ...  # pragma: no cover
