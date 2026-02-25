"""Postgres platform client: connection pool, transactions, and query API.

Provides an async Postgres client built on psycopg and psycopg_pool with
context-bound transactions (including nested savepoints) and configurable
pooling. Query methods use :meth:`PostgresClient.execute`-style API with
optional dict/tuple row factories.
"""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import (
    Any,
    AsyncIterator,
    Literal,
    Optional,
    Sequence,
    TypedDict,
    final,
    overload,
)

import attrs
from psycopg import AsyncConnection, Column, sql
from psycopg.abc import Params, QueryNoTemplate
from psycopg_pool import AsyncConnectionPool

from forze.base.primitives import JsonDict
from forze.infra.errors import InfrastructureError

from .errors import psycopg_handled

# ----------------------- #

RowFactory = Literal["tuple", "dict"]
"""Row format for :meth:`PostgresClient.fetch_all` and :meth:`PostgresClient.fetch_one`: ``\"dict\"`` for column-keyed dicts, ``\"tuple\"`` for sequences."""

IsolationLevel = Literal["repeatable read", "serializable"]
"""Supported transaction isolation levels."""

# ....................... #


@final
class TransactionOptions(TypedDict, total=False):
    """Options for :meth:`PostgresClient.transaction`."""

    read_only: bool
    """If ``True``, transaction is read-only. Omitted means read-write."""

    isolation: IsolationLevel
    """Transaction isolation level. Omitted means default (read committed)."""


# ....................... #
#! TypedDict instead ? or typed dict adapter with cast to dataclass (attrs)


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class PostgresConfig:
    """Connection pool configuration for :class:`PostgresClient`."""

    min_size: int = 2
    """Minimum number of connections in the pool."""

    max_size: int = 15
    """Maximum number of connections in the pool."""

    max_lifetime: int = 3600
    """Connection lifetime in seconds before recycling."""

    max_idle: int = 1800
    """Idle time in seconds before closing a connection."""

    reconnect_timeout: int = 10
    """Timeout in seconds when reconnecting after a failure."""

    num_workers: int = 4
    """Number of worker threads for the pool."""


# ....................... #


@final
@attrs.define(slots=True)
class PostgresClient:
    """Async Postgres client with connection pooling and context-bound transactions.

    Must be initialized with a DSN via :meth:`initialize` before use. Uses context
    variables to share a single connection per logical request; nested
    :meth:`transaction` blocks reuse the same connection and use savepoints.
    Query methods acquire a connection from the pool or the current transaction
    context and auto-commit when not inside a transaction.
    """

    __pool: Optional[AsyncConnectionPool] = attrs.field(default=None, init=False)
    __ctx_conn: ContextVar[Optional[AsyncConnection]] = attrs.field(
        factory=lambda: ContextVar(
            "pg_conn",
            default=None,
        ),
        init=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("pg_tx_depth", default=0),
        init=False,
    )

    # Connection options
    __acquire_timeout: float = attrs.field(default=0.5, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        dsn: str,
        *,
        config: PostgresConfig = PostgresConfig(),
        acquire_timeout: float = 0.5,
    ) -> None:
        """Creates and opens the connection pool.

        Idempotent; safe to call multiple times. Later calls are no-ops.

        :param dsn: Database connection string.
        :param config: Pool configuration. Defaults to :class:`PostgresConfig`.
        :param acquire_timeout: Timeout in seconds when acquiring a connection.
        """

        if self.__pool is not None:
            return

        self.__pool = AsyncConnectionPool(
            conninfo=dsn,
            open=False,
            min_size=config.min_size,
            max_size=config.max_size,
            max_lifetime=config.max_lifetime,
            max_idle=config.max_idle,
            reconnect_timeout=config.reconnect_timeout,
            num_workers=config.num_workers,
        )

        self.__acquire_timeout = acquire_timeout

        await self.__pool.open()

    # ....................... #

    async def close(self) -> None:
        """Closes the connection pool. No-op if not initialized."""

        if self.__pool is None:
            return

        await self.__pool.close()
        self.__pool = None

    # ....................... #

    def __require_pool(self) -> AsyncConnectionPool:
        """Returns the active pool. Raises :exc:`InfrastructureError` if not initialized."""

        if self.__pool is None:
            raise InfrastructureError("Postgres client is not initialized")

        return self.__pool

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Runs a simple query to check connectivity.

        :returns: A pair ``(message, ok)``. ``ok`` is ``True`` on success.
        """

        try:
            async with self.__acquire_conn() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #
    # Context helpers

    def __current_conn(self) -> Optional[AsyncConnection]:
        """Connection bound to the current context, or ``None``."""

        return self.__ctx_conn.get()

    # ....................... #

    @asynccontextmanager
    async def __acquire_conn(self) -> AsyncIterator[AsyncConnection]:
        """Yields the context-bound connection or a new one from the pool."""

        conn = self.__current_conn()

        if conn is not None:
            yield conn
            return

        async with self.__require_pool().connection(
            timeout=self.__acquire_timeout
        ) as pooled_conn:
            yield pooled_conn

    # ....................... #
    # Transaction API

    def is_in_transaction(self) -> bool:
        """Returns ``True`` if the current context is inside a transaction (including nested)."""

        return self.__ctx_depth.get() > 0 and self.__current_conn() is not None

    def require_transaction(self) -> None:
        """Raises :exc:`InfrastructureError` if the current context is not inside a transaction."""

        if not self.is_in_transaction():
            raise InfrastructureError("Transactional context is required")

    # ....................... #

    @psycopg_handled("postgres.transaction")
    @asynccontextmanager
    async def transaction(
        self,
        *,
        options: TransactionOptions = TransactionOptions(),
    ) -> AsyncIterator[AsyncConnection]:
        """Enters a transaction (or nested savepoint), yielding the connection.

        Nested calls use savepoints; the top-level call uses ``BEGIN`` /
        ``COMMIT`` / ``ROLLBACK``. If a connection is already bound in context
        (e.g. tests or UoW), that connection is used for the top-level
        transaction instead of acquiring a new one.

        :param options: Isolation level and read-only mode. All keys optional.
        :yields: The connection to use for the transaction.
        """

        depth = self.__ctx_depth.get()
        parent_conn = self.__current_conn()

        if depth > 0 and parent_conn is not None:
            sp_name = f"sp_{depth}"
            self.__ctx_depth.set(depth + 1)

            async with parent_conn.cursor() as cur:
                await cur.execute(
                    sql.SQL("SAVEPOINT {}").format(sql.Identifier(sp_name))
                )

            try:
                yield parent_conn

                async with parent_conn.cursor() as cur:
                    await cur.execute(
                        sql.SQL("RELEASE SAVEPOINT {}").format(sql.Identifier(sp_name))
                    )

            except Exception:
                async with parent_conn.cursor() as cur:
                    await cur.execute(
                        sql.SQL("ROLLBACK TO SAVEPOINT {}").format(
                            sql.Identifier(sp_name)
                        )
                    )
                    await cur.execute(
                        sql.SQL("RELEASE SAVEPOINT {}").format(sql.Identifier(sp_name))
                    )

                raise

            finally:
                self.__ctx_depth.set(depth)

            return

        # If a connection is already bound in the context (e.g., tests / UoW),
        # run the top-level transaction on it instead of acquiring a new one.
        if depth == 0 and parent_conn is not None:
            self.__ctx_depth.set(1)

            try:
                async with parent_conn.cursor() as cur:
                    await cur.execute("BEGIN")

                    isolation = options.get("isolation")
                    read_only = options.get("read_only", False)

                    if isolation:
                        await cur.execute(
                            sql.SQL("SET TRANSACTION ISOLATION LEVEL {}").format(
                                sql.Identifier(isolation.upper())
                            )
                        )

                    if read_only:
                        await cur.execute("SET TRANSACTION READ ONLY")

                yield parent_conn
                await parent_conn.commit()

            except Exception:
                await parent_conn.rollback()
                raise

            finally:
                self.__ctx_depth.set(0)

            return

        async with self.__require_pool().connection(
            timeout=self.__acquire_timeout
        ) as conn:
            token_conn = self.__ctx_conn.set(conn)
            token_depth = self.__ctx_depth.set(1)

            try:
                async with conn.cursor() as cur:
                    await cur.execute("BEGIN")

                    isolation = options.get("isolation")
                    read_only = options.get("read_only", False)

                    if isolation:
                        await cur.execute(
                            sql.SQL("SET TRANSACTION ISOLATION LEVEL {}").format(
                                sql.Identifier(isolation.upper())
                            )
                        )

                    if read_only:
                        await cur.execute("SET TRANSACTION READ ONLY")

                yield conn

                await conn.commit()

            except Exception:
                await conn.rollback()
                raise

            finally:
                self.__ctx_depth.reset(token_depth)
                self.__ctx_conn.reset(token_conn)

    # ....................... #
    # Query API

    @staticmethod
    def _rows_to_dicts(
        description: Optional[Sequence[Column]],
        rows: Sequence[Sequence[Any]],
    ) -> list[JsonDict]:
        """Builds a list of column-keyed dicts from cursor description and rows."""

        cols = [d.name for d in (description or [])]

        return [dict(zip(cols, row)) for row in rows]

    # ....................... #

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        return_rowcount: Literal[False] = False,
    ) -> None: ...

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        return_rowcount: Literal[True],
    ) -> int: ...

    @psycopg_handled("postgres.execute")
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        return_rowcount: bool = False,
    ) -> Optional[int]:
        """Executes a statement.

        When not inside a transaction, commits automatically. Optionally
        returns the affected row count.

        :param query: SQL query or statement.
        :param params: Query parameters.
        :param return_rowcount: If ``True``, return :attr:`cursor.rowcount`.
        :returns: Row count when ``return_rowcount`` is ``True``, else ``None``.
        """

        async with self.__acquire_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)

                rowcount = cur.rowcount

            if self.__current_conn() is None:
                await conn.commit()

            if return_rowcount:
                return rowcount

            return None

    # ....................... #

    @psycopg_handled("postgres.execute_many")
    async def execute_many(
        self, query: QueryNoTemplate, params: Sequence[Params]
    ) -> None:
        """Executes the same statement for each parameter set.

        When not inside a transaction, commits automatically after all
        executions.
        """

        async with self.__acquire_conn() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(query, params)

            if self.__current_conn() is None:
                await conn.commit()

    # ....................... #

    @overload
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> list[JsonDict]: ...

    @overload
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        row_factory: Literal["tuple"] = "tuple",
        commit: bool = False,
    ) -> list[tuple[Any, ...]]: ...

    @psycopg_handled("postgres.fetch_all")
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> list[JsonDict] | list[tuple[Any, ...]]:
        """Executes a query and returns all rows.

        Row format follows ``row_factory``. When ``commit`` is ``True`` and
        not inside a transaction, commits after the query.

        :param query: SQL query.
        :param params: Query parameters.
        :param row_factory: ``\"dict\"`` or ``\"tuple\"`` row format.
        :param commit: If ``True``, commit when not in a transaction.
        :returns: List of rows as dicts or tuples.
        """

        async with self.__acquire_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)

                rows = await cur.fetchall()

                if row_factory == "tuple":
                    res = list(rows)

                else:
                    res = self._rows_to_dicts(cur.description, rows)

            if commit and self.__current_conn() is None:
                await conn.commit()

            return res

    # ....................... #

    @overload
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> Optional[JsonDict]: ...

    @overload
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        row_factory: Literal["tuple"] = "tuple",
        commit: bool = False,
    ) -> Optional[tuple[Any, ...]]: ...

    @psycopg_handled("postgres.fetch_one")
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> Optional[JsonDict | tuple[Any, ...]]:
        """Executes a query and returns the first row.

        Returns ``None`` when no rows match. When ``commit`` is ``True`` and
        not inside a transaction, commits after the query.

        :param query: SQL query.
        :param params: Query parameters.
        :param row_factory: ``\"dict\"`` or ``\"tuple\"`` row format.
        :param commit: If ``True``, commit when not in a transaction.
        :returns: First row as dict or tuple, or ``None``.
        """

        async with self.__acquire_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)

                row = await cur.fetchone()

                if row is None:
                    res = None

                elif row_factory == "tuple":
                    res = tuple(row)

                else:
                    res = self._rows_to_dicts(cur.description, [row])[0]

            if commit and self.__current_conn() is None:
                await conn.commit()

            return res

    # ....................... #

    @psycopg_handled("postgres.fetch_value")
    async def fetch_value(
        self,
        query: QueryNoTemplate,
        params: Optional[Params] = None,
        *,
        default: Any = None,
    ) -> Any:
        """Executes a query and returns the first column of the first row.

        Returns ``default`` when no rows match.

        :param query: SQL query.
        :param params: Query parameters.
        :param default: Value returned when the result set is empty.
        :returns: First column value or ``default``.
        """

        async with self.__acquire_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)

                row = await cur.fetchone()

                if not row:
                    return default

                return row[0]
