"""Postgres platform client: connection pool, transactions, and query API.

Provides an async Postgres client built on psycopg and psycopg_pool with
context-bound transactions (including nested savepoints) and configurable
pooling. Query methods use :meth:`PostgresClient.execute`-style API with
optional dict/tuple row factories.
"""

from pydantic import SecretStr

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import asyncio
from collections.abc import AsyncGenerator, Sequence
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
    nullcontext,
)
from contextvars import ContextVar
from datetime import timedelta
from typing import Any, Literal, final, overload

import attrs
from psycopg import AsyncConnection, Column, sql
from psycopg.abc import Params, QueryNoTemplate
from psycopg_pool import AsyncConnectionPool

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, uuid4

from .._logger import logger
from .errors import exc_interceptor
from .helpers import isolation_level_enum
from .port import PostgresClientPort
from .types import RowFactory
from .value_objects import (
    DeadlinePushdownPolicy,
    PostgresConfig,
    PostgresTransactionOptions,
)

# ----------------------- #


def _timeout_ms(t: timedelta) -> int:
    """Convert a timeout to a positive Postgres ``*_timeout`` millisecond value."""

    ms = int(t.total_seconds() * 1000)

    return max(1, ms)


# ....................... #


def _pool_configure_for_config(  # type: ignore[no-untyped-def]
    cfg: PostgresConfig,
):
    """Return an async pool ``configure`` callback, or ``None`` if nothing to set."""

    if not any(
        (
            cfg.statement_timeout,
            cfg.lock_timeout,
            cfg.idle_in_transaction_session_timeout,
            cfg.application_name,
        ),
    ):
        return None

    async def configure(conn: AsyncConnection) -> None:
        async with conn.cursor() as cur:
            if cfg.application_name is not None:
                await cur.execute(
                    sql.SQL("SET application_name = {}").format(
                        sql.Literal(cfg.application_name),
                    ),
                )

            if cfg.statement_timeout is not None:
                await cur.execute(
                    sql.SQL("SET statement_timeout = {}").format(
                        sql.Literal(_timeout_ms(cfg.statement_timeout)),
                    ),
                )

            if cfg.lock_timeout is not None:
                await cur.execute(
                    sql.SQL("SET lock_timeout = {}").format(
                        sql.Literal(_timeout_ms(cfg.lock_timeout)),
                    ),
                )

            if cfg.idle_in_transaction_session_timeout is not None:
                await cur.execute(
                    sql.SQL("SET idle_in_transaction_session_timeout = {}").format(
                        sql.Literal(_timeout_ms(cfg.idle_in_transaction_session_timeout)),
                    ),
                )

    return configure


# ....................... #


async def _pool_reset_transaction_attributes(conn: AsyncConnection) -> None:
    """Pool ``reset`` callback: clear transaction-shaping attributes on check-in.

    Second belt against transaction-option leaks (the first is the ``finally``
    restore in :meth:`PostgresClient.transaction`): even if a code path forgets
    to restore ``read_only`` / ``isolation_level``, no connection ever re-enters
    the pool with them set. psycopg_pool invokes ``reset`` only after bringing
    the connection to IDLE (or discards it), and psycopg's attribute setters are
    pure client-side when the connection is idle — ``_set_read_only_gen`` /
    ``_set_isolation_level_gen`` only update the cached ``BEGIN`` statement, no
    SQL is sent — so this costs no round-trip. The ``is not None`` guards keep
    the common check-in (nothing leaked) entirely allocation- and lock-free.

    Also the second belt for ``autocommit`` (the first is the ``finally``
    restore in :meth:`PostgresClient._statement_conn`): a connection must never
    re-enter the pool in autocommit mode, or later transactional work on it
    would silently skip ``BEGIN``. ``_set_autocommit_gen`` is likewise pure
    client-side on an idle connection, and the truthiness guard keeps the clean
    check-in a no-op.
    """

    if conn.isolation_level is not None:
        await conn.set_isolation_level(None)

    if conn.read_only is not None:
        await conn.set_read_only(None)

    if conn.autocommit:
        await conn.set_autocommit(False)


# ....................... #


@attrs.define(slots=True)
class _PendingTx:
    """Lazy root-transaction state held in a context var until first use.

    The :class:`AsyncExitStack` is entered around the scope body; materialization
    pushes the pooled connection, the ``BEGIN`` (``conn.transaction()``), the
    option-restore callback, and the ``__ctx_conn`` reset onto it. The stack is
    unwound with the real exception info when the scope body exits, so an error
    after materialization rolls back (a bare ``aclose()`` would commit on error).
    """

    options: PostgresTransactionOptions
    stack: AsyncExitStack
    conn: AsyncConnection | None = None
    statement_timeout_ms: int | None = None
    """A deadline-derived ``statement_timeout`` (ms) to set right after ``BEGIN``, carried
    here so applying the backstop never forces this lazy scope to check out early — it is
    applied at materialization, on the first real statement's connection."""
    lock: asyncio.Lock = attrs.field(factory=asyncio.Lock)
    """Serializes materialization so concurrent first statements in one scope open
    a single transaction (not one each)."""


# ....................... #


@final
@attrs.define(slots=True)
class PostgresClient(PostgresClientPort):
    """Async Postgres client with connection pooling and context-bound transactions.

    Must be initialized with a DSN via :meth:`initialize` before use. Uses context
    variables to share a single connection per logical request; nested
    :meth:`transaction` blocks reuse the same connection and use savepoints.
    Query methods acquire a connection from the pool or the current transaction
    context and auto-commit when not inside a transaction.
    """

    __pool: AsyncConnectionPool | None = attrs.field(default=None, init=False)
    __ctx_conn: ContextVar[AsyncConnection | None] = attrs.field(
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
    __ctx_pending: ContextVar["_PendingTx | None"] = attrs.field(
        factory=lambda: ContextVar("pg_tx_pending", default=None),
        init=False,
    )
    """Per-scope lazy-transaction state: set on root scope entry, materialized on
    first query. ``None`` outside a lazy root scope (and always in eager mode)."""

    __lazy_tx: bool = attrs.field(default=False, init=False)
    """Whether root transaction scopes defer pool checkout to the first query."""

    # Connection options
    __acquire_timeout: timedelta = attrs.field(default=timedelta(seconds=5), init=False)
    __max_concurrent_queries: int = attrs.field(default=1, init=False)
    """Cap for parallel operations that each checkout a pool connection."""

    __deadline_pushdown: DeadlinePushdownPolicy | None = attrs.field(default=None, init=False)
    """Invocation-deadline ``statement_timeout`` push-down policy, or ``None`` when disabled
    (set from config in :meth:`initialize`)."""

    __gather_sem: asyncio.Semaphore | None = attrs.field(default=None, init=False)
    """Pool-wide limiter for :func:`~forze_postgres.kernel.client.gather_db_work`."""

    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        dsn: str | SecretStr,
        *,
        config: PostgresConfig = PostgresConfig(),
        acquire_timeout: timedelta = timedelta(seconds=5),
    ) -> None:
        """Creates and opens the connection pool.

        Idempotent; safe to call multiple times. Later calls are no-ops.
        Concurrent calls serialize on an internal lock so only one coroutine
        performs the setup.

        :param dsn: Database connection string.
        :param config: Pool configuration. Defaults to :class:`PostgresConfig`.
        :param acquire_timeout: Timeout when acquiring a connection from the pool.
        """

        async with self.__init_lock:
            if self.__pool is not None:
                return

            if config.max_concurrent_queries is not None:
                self.__max_concurrent_queries = config.max_concurrent_queries

            else:
                self.__max_concurrent_queries = max(1, config.max_size - config.pool_headroom)

            self.__lazy_tx = config.lazy_transaction

            self.__deadline_pushdown = (
                DeadlinePushdownPolicy(statement_timeout_cap=config.statement_timeout)
                if config.push_invocation_deadline
                else None
            )

            configure = _pool_configure_for_config(config)

            if isinstance(dsn, SecretStr):
                dsn = dsn.get_secret_value()

            pool = AsyncConnectionPool(
                conninfo=dsn,
                open=False,
                min_size=config.min_size,
                max_size=config.max_size,
                max_lifetime=config.max_lifetime.total_seconds(),
                max_idle=config.max_idle.total_seconds(),
                reconnect_timeout=config.reconnect_timeout.total_seconds(),
                num_workers=config.num_workers,
                configure=configure,
                reset=_pool_reset_transaction_attributes,
            )

            self.__acquire_timeout = acquire_timeout

            self.__gather_sem = asyncio.Semaphore(self.__max_concurrent_queries)

            # Open before assigning so a failed open doesn't leave the guard
            # satisfied with an unopened pool.
            await pool.open()
            self.__pool = pool  # type: ignore[assignment]
            logger.trace("Postgres pool opened")

    # ....................... #

    async def close(self) -> None:
        """Closes the connection pool. No-op if not initialized."""

        async with self.__init_lock:
            if self.__pool is None:
                return

            await self.__pool.close()
            self.__pool = None
            self.__gather_sem = None
            logger.trace("Postgres pool closed")

    # ....................... #

    def __require_pool(self) -> AsyncConnectionPool:
        """Returns the active pool. Raises :exc:`InfrastructureError` if not initialized."""

        if self.__pool is None:
            raise exc.internal("Postgres client is not initialized")

        return self.__pool

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Runs a simple query to check connectivity.

        :returns: A pair ``(message, ok)``. ``ok`` is ``True`` on success.
        """

        try:
            async with self.__acquire_conn() as conn, conn.cursor() as cur:
                await cur.execute("SELECT 1")
                return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #
    # Context helpers

    def __current_conn(self) -> AsyncConnection | None:
        """Connection bound to the current context, or ``None``.

        Falls through to a materialized lazy scope's connection, which is carried
        on the pending object rather than a context var: it is set during the
        first query (a different context than the ``transaction()`` generator's
        ``__aexit__``), and a context-var token cannot be reset across contexts.
        """

        conn = self.__ctx_conn.get()

        if conn is not None:
            return conn

        pending = self.__ctx_pending.get()

        return pending.conn if pending is not None else None

    # ....................... #

    async def _materialize_pending(self) -> AsyncConnection:
        """Check out the pooled connection and open ``BEGIN`` for a lazy root scope.

        Idempotent: a second call inside the same scope returns the connection
        materialized by the first. Pushes the pool checkout, the option-restore
        callback, the ``BEGIN`` (``conn.transaction()``) and the ``__ctx_conn``
        reset onto the pending scope's exit stack, so they unwind — with the real
        exception, hence rollback on error — when the scope body exits.

        :raises InfrastructureError: if called with no pending root scope.
        """

        pending = self.__ctx_pending.get()

        if pending is None:
            raise exc.internal("No pending transaction to materialize")

        if pending.conn is not None:
            return pending.conn

        # Double-checked lock: concurrent first statements in one scope serialize
        # here so exactly one opens the connection + BEGIN; the rest reuse it.
        async with pending.lock:
            if pending.conn is not None:
                return pending.conn

            conn = await pending.stack.enter_async_context(
                self.__require_pool().connection(timeout=self.__acquire_timeout.total_seconds())
            )

            # Apply read_only / isolation as connection attributes before BEGIN
            # (zero round-trips); restore them as the connection returns to the pool.
            if not self._options_are_default(pending.options):
                await self._apply_transaction_options(conn, pending.options)
                pending.stack.push_async_callback(self._restore_transaction_attributes, conn)

            await pending.stack.enter_async_context(conn.transaction())

            # Apply a carried deadline backstop as the first statement after BEGIN — here,
            # at real checkout, so binding it never forced this lazy scope to open early.
            if pending.statement_timeout_ms is not None:
                await conn.execute(
                    sql.SQL("SET LOCAL statement_timeout = {}").format(
                        sql.Literal(pending.statement_timeout_ms)
                    )
                )

            # The connection is reachable via __current_conn through the pending
            # object — NOT bound to __ctx_conn here: this runs in the first query's
            # context, and the matching reset would land in the generator's
            # __aexit__ context, which a context-var token forbids.
            pending.conn = conn

            return conn

    # ....................... #

    async def apply_statement_timeout(self, ms: int) -> None:
        """Set ``statement_timeout`` on the current root transaction.

        A lazy root scope that has not materialized carries the value on its pending state
        and applies it right after ``BEGIN`` at first checkout, so the backstop never forces
        an early pool checkout; an eager transaction (or an already-materialized lazy scope)
        sets it now on the live connection.
        """

        pending = self.__ctx_pending.get()

        if pending is not None and pending.conn is None:
            pending.statement_timeout_ms = ms
            return

        await self.execute(sql.SQL("SET LOCAL statement_timeout = {}").format(sql.Literal(ms)))

    # ....................... #

    @asynccontextmanager
    async def __acquire_conn(self) -> AsyncGenerator[AsyncConnection]:
        """Yields the context-bound connection or a new one from the pool.

        Inside a lazy root scope that has not yet run a statement, materializes
        the pending transaction so the scan rides the scope's own connection.
        """

        conn = self.__current_conn()

        if conn is not None:
            yield conn
            return

        if self.__ctx_pending.get() is not None:
            yield await self._materialize_pending()
            return

        async with self.__require_pool().connection(
            timeout=self.__acquire_timeout.total_seconds()
        ) as pooled_conn:
            yield pooled_conn

    # ....................... #

    @asynccontextmanager
    async def _statement_conn(self) -> AsyncGenerator[AsyncConnection]:
        """Yields a connection for a single statement, autocommit when out-of-tx.

        With a context-bound connection (transaction or :meth:`bound_connection`)
        it is yielded as-is — its owner controls commit/rollback. Otherwise a
        pooled connection is checked out and switched to **autocommit** for the
        duration of the statement: psycopg then skips the implicit ``BEGIN``
        and the explicit ``COMMIT``, so an out-of-transaction statement costs
        exactly one server statement instead of ``BEGIN``/statement/``COMMIT``.

        ``set_autocommit`` is pure client-side on an idle connection (verified
        in psycopg's ``_set_autocommit_gen``: it only flips a flag after
        ``_check_intrans_gen``, no SQL) and is independent of the transaction
        attribute machinery — ``read_only`` / ``isolation_level`` only ride the
        composed ``BEGIN``, which ``_start_query`` never emits under
        autocommit. The ``finally`` restore is best-effort (a broken connection
        must not mask the statement's own error); the pool ``reset`` callback
        (:func:`_pool_reset_transaction_attributes`) clears a leaked autocommit
        flag on check-in as the second belt.
        """

        conn = self.__current_conn()

        if conn is not None:
            yield conn
            return

        # Inside a lazy root scope, the first statement materializes the pending
        # transaction and rides its connection (transactional, not autocommit).
        if self.__ctx_pending.get() is not None:
            yield await self._materialize_pending()
            return

        async with self.__require_pool().connection(
            timeout=self.__acquire_timeout.total_seconds()
        ) as pooled_conn:
            await pooled_conn.set_autocommit(True)

            try:
                yield pooled_conn

            finally:
                try:
                    await pooled_conn.set_autocommit(False)

                except Exception:
                    logger.warning(
                        "Failed to restore autocommit after out-of-transaction "
                        "statement; the pool reset callback will clear it on "
                        "check-in",
                        exc_info=True,
                    )

    # ....................... #
    # Transaction API

    def is_in_transaction(self) -> bool:
        """Returns ``True`` if the current context is inside a transaction (including nested).

        Depth-based (logical): a lazy root scope that has opened but not yet run a
        statement — so its connection is not materialized — still counts as in a
        transaction, so the next query materializes the pending scope rather than
        running a stray autocommit statement. Equivalent to the old
        ``depth and conn`` test in eager mode, where a non-zero depth always has a
        bound connection.
        """

        return self.__ctx_depth.get() > 0

    # ....................... #

    @asynccontextmanager
    async def detached(self) -> AsyncGenerator[None]:
        """Scope whose statements never join the ambient transaction.

        Statements inside run out of transaction (pooled autocommit connections) even
        when the calling context has a transaction open or pending — for writes that
        must survive the caller's rollback, such as counter allocation. A
        :meth:`transaction` opened inside the scope starts a fresh root on its own
        connection.
        """

        token_conn = self.__ctx_conn.set(None)
        token_depth = self.__ctx_depth.set(0)
        token_pending = self.__ctx_pending.set(None)

        try:
            yield
        finally:
            self.__ctx_conn.reset(token_conn)
            self.__ctx_depth.reset(token_depth)
            self.__ctx_pending.reset(token_pending)

    # ....................... #

    def query_concurrency_limit(self) -> int:
        """Maximum parallel operations that should each acquire a pool connection.

        Ignored when :meth:`is_in_transaction` is true (work is serialized).
        """

        return self.__max_concurrent_queries

    # ....................... #

    def deadline_pushdown(self) -> DeadlinePushdownPolicy | None:
        return self.__deadline_pushdown

    # ....................... #

    def gather_concurrency_semaphore(self) -> asyncio.Semaphore:
        """Semaphore shared by all coroutines using this client for :func:`gather_db_work`."""

        if self.__gather_sem is None:
            raise exc.internal("Postgres client is not initialized")

        return self.__gather_sem

    # ....................... #

    def require_transaction(self) -> None:
        """Raises :exc:`InfrastructureError` if the current context is not inside a transaction."""

        if not self.is_in_transaction():
            raise exc.internal("Transactional context is required")

    # ....................... #

    @exc_interceptor.asynccontextmanager("postgres.bound_connection")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def bound_connection(self) -> AsyncGenerator[AsyncConnection]:
        """Check out one pool connection and bind it as the current context connection.

        While this context is active, query methods and a top-level
        :meth:`transaction` use this connection — the latter runs ``BEGIN`` /
        ``COMMIT`` / ``ROLLBACK`` on it instead of checking out another handle.

        Intended for unit-of-work style composition and tests that must exercise
        the pre-bound connection path of :meth:`transaction`.

        :yields: The checked-out connection.
        :raises InfrastructureError: If a connection is already bound or a
            transaction/savepoint stack is already active.
        """

        if self.__ctx_depth.get() > 0:
            raise exc.internal(
                "Cannot bind a connection while already inside a transaction",
            )

        if self.__current_conn() is not None:
            raise exc.internal("A connection is already bound in this context")

        async with self.__require_pool().connection(
            timeout=self.__acquire_timeout.total_seconds()
        ) as conn:
            token = self.__ctx_conn.set(conn)

            try:
                yield conn

            finally:
                self.__ctx_conn.reset(token)

    # ....................... #

    @staticmethod
    def _options_are_default(options: PostgresTransactionOptions) -> bool:
        """``True`` when *options* request the defaults (read-write, read committed).

        Default root transactions then touch no connection attributes at all:
        psycopg emits a plain ``BEGIN`` and there is nothing to restore.
        """

        return not options.read_only and options.isolation == "read_committed"

    # ....................... #

    @staticmethod
    async def _apply_transaction_options(
        conn: AsyncConnection,
        options: PostgresTransactionOptions,
    ) -> None:
        """Apply *options* as psycopg connection attributes, **before** ``BEGIN``.

        psycopg composes ``read_only`` / ``isolation_level`` into the ``BEGIN``
        statement itself (``BEGIN [ISOLATION LEVEL …] [READ ONLY]``), so the
        options cost zero extra round-trips — unlike a separate
        ``SET TRANSACTION`` statement inside the transaction. The setters are
        pure client-side while the connection is idle (verified in psycopg's
        ``_set_*_gen`` / ``_check_intrans_gen``: no SQL, they only invalidate
        the cached begin statement) and raise if a transaction is in progress.

        The attributes persist across pool check-ins, so every caller **must**
        restore them via :meth:`_restore_transaction_attributes` in a
        ``finally`` — and the pool's ``reset`` callback
        (:func:`_pool_reset_transaction_attributes`) clears them on check-in as
        a second belt. Nested transactions (savepoints) must never call this:
        options are a root-transaction-only contract.
        """

        await conn.set_isolation_level(isolation_level_enum(options.isolation))

        if options.read_only:
            await conn.set_read_only(True)

    # ....................... #

    @staticmethod
    async def _restore_transaction_attributes(conn: AsyncConnection) -> None:
        """Reset transaction-shaping attributes to defaults after a root transaction.

        Client-side only (no SQL) when the connection is idle; with ``None``
        psycopg falls back to a plain ``BEGIN`` for subsequent transactions.
        Best-effort: if the connection is broken or unexpectedly still in a
        transaction the setters raise — swallow it (the original error from the
        transaction body must win) and rely on the pool ``reset`` callback /
        connection discard to prevent any leak.
        """

        try:
            await conn.set_isolation_level(None)
            await conn.set_read_only(None)

        except Exception:
            logger.warning(
                "Failed to restore transaction attributes; "
                "the pool reset callback will clear them on check-in",
                exc_info=True,
            )

    # ....................... #

    @exc_interceptor.asynccontextmanager("postgres.transaction")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def transaction(
        self,
        *,
        options: PostgresTransactionOptions | None = None,
    ) -> AsyncGenerator[AsyncConnection | None]:
        """Enters a transaction (or nested savepoint), yielding the connection.

        Nested calls use savepoints; the top-level call uses psycopg's
        :meth:`~psycopg.AsyncConnection.transaction`. If a connection is already
        bound in context (e.g. tests or UoW), that connection is used for the
        top-level transaction instead of acquiring a new one.

        With ``lazy_transaction`` enabled, a root scope acquires no pool
        connection until the first query (see :meth:`_materialize_pending`); the
        context manager then yields ``None`` until that point, so callers must run
        work through the client's query methods rather than the yielded handle.

        :param options: Isolation level and read-only mode. All keys optional.
        :yields: The connection to use for the transaction, or ``None`` for a lazy
            root scope before its first query.
        """

        depth = self.__ctx_depth.get()
        parent_conn = self.__current_conn()

        options = options if options is not None else PostgresTransactionOptions()

        # A nested scope opened inside a lazy root that has not run a statement
        # yet must materialize the root first — a savepoint needs a real BEGIN.
        if depth > 0 and parent_conn is None and self.__ctx_pending.get() is not None:
            parent_conn = await self._materialize_pending()

        if depth > 0 and parent_conn is not None:
            sp_name = f"fz_sp_{depth}_{uuid4().hex[:12]}"
            token_depth = self.__ctx_depth.set(depth + 1)

            try:
                async with parent_conn.transaction(savepoint_name=sp_name):
                    yield parent_conn

            except Exception:
                logger.exception(
                    "Error in nested transaction, rolling back to savepoint '%s'",
                    sp_name,
                )
                raise

            finally:
                self.__ctx_depth.reset(token_depth)

            return

        # Non-default options are applied as connection attributes before BEGIN
        # (zero extra round-trips) and must be restored in the finally of the
        # SAME top-level path that set them; default options touch nothing.
        apply_options = not self._options_are_default(options)

        # If a connection is already bound in the context (e.g., tests / UoW),
        # run the top-level transaction on it instead of acquiring a new one.
        if depth == 0 and parent_conn is not None:
            token_depth = self.__ctx_depth.set(1)

            try:
                if apply_options:
                    await self._apply_transaction_options(parent_conn, options)

                async with parent_conn.transaction():
                    yield parent_conn

            except Exception:
                logger.exception("Error in top-level transaction, rolling back")
                raise

            finally:
                if apply_options:
                    await self._restore_transaction_attributes(parent_conn)

                self.__ctx_depth.reset(token_depth)

            return

        # Lazy root: register the scope but acquire nothing. The first query (or a
        # nested scope) materializes the connection + BEGIN via the exit stack,
        # which unwinds with the real exception on body exit — so an error after
        # materialization rolls back, and a scope that never queried holds nothing.
        if self.__lazy_tx:
            pending = _PendingTx(options=options, stack=AsyncExitStack())
            token_pending = self.__ctx_pending.set(pending)
            token_depth = self.__ctx_depth.set(1)

            try:
                async with pending.stack:
                    yield None

            except Exception:
                logger.exception("Error in transaction, rolling back")
                raise

            finally:
                self.__ctx_depth.reset(token_depth)
                self.__ctx_pending.reset(token_pending)

            return

        async with self.__require_pool().connection(
            timeout=self.__acquire_timeout.total_seconds()
        ) as conn:
            token_conn = self.__ctx_conn.set(conn)
            token_depth = self.__ctx_depth.set(1)

            try:
                if apply_options:
                    await self._apply_transaction_options(conn, options)

                async with conn.transaction():
                    yield conn

            except Exception:
                logger.exception("Error in transaction, rolling back")
                raise

            finally:
                if apply_options:
                    await self._restore_transaction_attributes(conn)

                self.__ctx_depth.reset(token_depth)
                self.__ctx_conn.reset(token_conn)

    # ....................... #
    # Query API

    @staticmethod
    def _rows_to_dicts(
        description: Sequence[Column] | None,
        rows: Sequence[Sequence[Any]],
    ) -> list[JsonDict]:
        """Builds a list of column-keyed dicts from cursor description and rows."""

        cols = tuple(d.name for d in description) if description else ()

        return [dict(zip(cols, row, strict=False)) for row in rows]

    # ....................... #

    @staticmethod
    def _row_to_dict(
        description: Sequence[Column] | None,
        row: Sequence[Any],
    ) -> JsonDict:
        """Builds a single column-keyed dict from cursor description and a row."""

        cols = tuple(d.name for d in description) if description else ()

        return dict(zip(cols, row, strict=False))

    # ....................... #

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

    @exc_interceptor.coroutine("postgres.execute")  # type: ignore[untyped-decorator]
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: bool = False,
    ) -> int | None:
        """Executes a statement.

        When not inside a transaction, runs in autocommit mode (one server
        statement, no ``BEGIN``/``COMMIT``). Optionally returns the affected
        row count.

        :param query: SQL query or statement.
        :param params: Query parameters.
        :param return_rowcount: If ``True``, return :attr:`cursor.rowcount`.
        :returns: Row count when ``return_rowcount`` is ``True``, else ``None``.
        """

        async with self._statement_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)

                rowcount = cur.rowcount

            if return_rowcount:
                return rowcount

            return None

    # ....................... #

    @exc_interceptor.coroutine("postgres.execute_many")  # type: ignore[untyped-decorator]
    async def execute_many(
        self,
        query: QueryNoTemplate,
        params: Sequence[Params],
    ) -> None:
        """Executes the same statement for each parameter set.

        When not inside a transaction, runs in autocommit mode: each execution
        commits on its own. Wrap the call in :meth:`transaction` when the batch
        must be atomic.
        """

        async with self._statement_conn() as conn, conn.cursor() as cur:
            await cur.executemany(query, params)

    # ....................... #

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

    @exc_interceptor.coroutine("postgres.fetch_all")  # type: ignore[untyped-decorator]
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> list[JsonDict] | list[tuple[Any, ...]]:
        """Executes a query and returns all rows.

        Row format follows ``row_factory``. When not inside a transaction, the
        query runs in autocommit mode; ``commit`` is accepted for API symmetry
        only and has no effect (out-of-transaction statements always commit).

        :param query: SQL query.
        :param params: Query parameters.
        :param row_factory: ``\"dict\"`` or ``\"tuple\"`` row format.
        :param commit: Accepted for API symmetry; no effect.
        :returns: List of rows as dicts or tuples.
        """

        _ = commit

        async with self._statement_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)

                rows = await cur.fetchall()
                res: list[JsonDict] | list[tuple[Any, ...]]

                if row_factory == "tuple":
                    res = list(rows)

                else:
                    res = self._rows_to_dicts(cur.description, rows)

            return res

    # ....................... #

    @exc_interceptor.asyncgenerator("postgres.fetch_all_batched")  # type: ignore[untyped-decorator]
    async def fetch_all_batched(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        batch_size: int = 2000,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> AsyncGenerator[list[JsonDict] | list[tuple[Any, ...]]]:
        """Execute *query* and yield row chunks of at most *batch_size* rows.

        Uses a **server-side (named) cursor** so the database streams rows in
        ``FETCH FORWARD`` batches and the full result set is never buffered
        client-side — client memory stays bounded regardless of result size (a
        plain cursor would have libpq buffer every row before the first chunk).

        Server-side cursors require an open transaction: an existing context
        transaction (see :meth:`transaction`) is reused, otherwise a short-lived
        transaction wraps the scan. ``commit`` is accepted for API symmetry only and
        has **no effect** here — the scan always runs inside (and is committed by) the
        reused or wrapping transaction; a reused context transaction's owner controls
        its own commit/rollback as before.
        """

        if batch_size < 1:
            msg = "batch_size must be >= 1"
            raise ValueError(msg)

        _ = commit

        # A server-side cursor needs a transaction. Reuse an active context
        # transaction (its owner controls commit/rollback); otherwise scope a
        # short-lived one to the scan. The cursor loop is inlined here — and not
        # delegated to a sub-generator — so that on early termination the ``yield``
        # shares a frame with the ``async with`` blocks and the cursor ``CLOSE`` /
        # transaction unwind run promptly, leaving the connection clean.
        async with self.__acquire_conn() as conn:
            tx_cm: AbstractAsyncContextManager[Any] = (
                nullcontext() if self.is_in_transaction() else conn.transaction()
            )

            async with tx_cm:
                name = f"forze_stream_{uuid4().hex}"

                async with conn.cursor(name=name) as cur:
                    await cur.execute(query, params)

                    while True:
                        chunk = await cur.fetchmany(batch_size)

                        if not chunk:
                            break

                        if row_factory == "tuple":
                            yield list(chunk)

                        else:
                            yield self._rows_to_dicts(cur.description, chunk)

    # ....................... #

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

    @exc_interceptor.coroutine("postgres.fetch_one")  # type: ignore[untyped-decorator]
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> JsonDict | tuple[Any, ...] | None:
        """Executes a query and returns the first row.

        Returns ``None`` when no rows match. When not inside a transaction,
        the query runs in autocommit mode; ``commit`` is accepted for API
        symmetry only and has no effect (out-of-transaction statements always
        commit).

        :param query: SQL query.
        :param params: Query parameters.
        :param row_factory: ``\"dict\"`` or ``\"tuple\"`` row format.
        :param commit: Accepted for API symmetry; no effect.
        :returns: First row as dict or tuple, or ``None``.
        """

        _ = commit

        async with self._statement_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)

                row = await cur.fetchone()
                res: JsonDict | tuple[Any, ...] | None

                if row is None:
                    res = None

                elif row_factory == "tuple":
                    res = tuple(row)

                else:
                    res = self._row_to_dict(cur.description, row)

            return res

    # ....................... #

    @exc_interceptor.coroutine("postgres.fetch_value")  # type: ignore[untyped-decorator]
    async def fetch_value(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        default: Any = None,
    ) -> Any:
        """Executes a query and returns the first column of the first row.

        Returns ``default`` when no rows match. When not inside a transaction,
        the query runs in autocommit mode.

        :param query: SQL query.
        :param params: Query parameters.
        :param default: Value returned when the result set is empty.
        :returns: First column value or ``default``.
        """

        async with self._statement_conn() as conn, conn.cursor() as cur:
            await cur.execute(query, params)

            row = await cur.fetchone()

            if not row:
                return default

            return row[0]
