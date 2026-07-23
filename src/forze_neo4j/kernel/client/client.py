"""Async Neo4j client with context-bound explicit transactions."""

from forze_neo4j._compat import require_neo4j

require_neo4j()

# ....................... #

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import final

import attrs
from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession, AsyncTransaction
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .._logger import logger
from .errors import exc_interceptor
from .port import Neo4jClientPort
from .value_objects import Neo4jConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class _TxScope:
    """The explicit transaction bound to the current context — opened lazily.

    A Neo4j transaction belongs to a *session*, and a session is bound to one database for
    its lifetime; a transaction cannot span databases. Under the ``namespace`` tenancy tier
    the target database is per-tenant and is only known when a statement names it, so the
    session is not opened at scope entry — the first statement inside the scope decides which
    database the transaction runs on, and every later statement in it must agree.

    Opening eagerly on the client's static default was the bug: a per-tenant statement's
    ``database=`` was silently dropped and the write landed in the shared default database.
    """

    database: str | None
    """The bound database: the scope's own (pinned at entry) or the first statement's.
    ``None`` means undecided — until :attr:`opened`, when it means the driver default."""

    opened: bool = False
    session: AsyncSession | None = None
    tx: AsyncTransaction | None = None

    lock: asyncio.Lock = attrs.field(factory=asyncio.Lock)
    """Serializes the lazy open: without it two statements under one ``asyncio.gather``
    both see ``opened`` unset, both begin a transaction, and the second overwrites the
    first — whose transaction is never committed and whose session leaks to the server
    timeout."""


# ----------------------- #


@final
@attrs.define(slots=True)
class Neo4jClient(Neo4jClientPort):
    """Async Neo4j client wrapping :class:`neo4j.AsyncDriver`.

    Runs each query auto-committed by default. Opening a :meth:`transaction` scope binds an
    explicit Neo4j transaction on the current context so queries inside it commit or roll
    back as a unit. The framework transaction scope drives this via
    :class:`~forze_neo4j.adapters.Neo4jTxManagerAdapter` when an operation's ``tx_route`` is
    bound to the module's ``tx`` group (otherwise open ``async with client.transaction():``
    explicitly). Graph writes remain **not** co-transactional with the Postgres outbox or any
    other backend — Neo4j is a separate database and there is no cross-database two-phase
    commit, so this only makes a handler's *graph* statements atomic among themselves.
    """

    _driver: AsyncDriver | None = attrs.field(default=None, init=False)
    _config: Neo4jConfig = attrs.field(factory=Neo4jConfig, init=False)
    _tx_var: ContextVar[_TxScope | None] = attrs.field(
        factory=lambda: ContextVar("forze_neo4j_tx", default=None),
        init=False,
        repr=False,
    )
    _init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #

    @property
    def _require_driver(self) -> AsyncDriver:
        if self._driver is None:
            raise exc.internal("Neo4j client is not initialized")

        return self._driver

    # ....................... #

    async def initialize(
        self,
        uri: str | SecretStr,
        *,
        auth: tuple[str, str] | None = None,
        config: Neo4jConfig = Neo4jConfig(),
    ) -> None:
        """Create the driver. Idempotent — a second call is a no-op. Concurrent
        calls serialize on an internal lock so only one coroutine performs the
        setup.
        """

        async with self._init_lock:
            if self._driver is not None:
                return

            resolved = uri.get_secret_value() if isinstance(uri, SecretStr) else uri

            self._config = config
            self._driver = AsyncGraphDatabase.driver(  # pyright: ignore[reportUnknownMemberType]
                resolved,
                auth=auth,
                max_connection_pool_size=config.max_connection_pool_size,
                connection_acquisition_timeout=config.connection_acquisition_timeout.total_seconds(),
                connection_timeout=config.connection_timeout.total_seconds(),
                max_transaction_retry_time=config.max_transaction_retry_time.total_seconds(),
            )
            logger.trace("Neo4j driver connected")

    # ....................... #

    async def close(self) -> None:
        async with self._init_lock:
            if self._driver is not None:
                await self._driver.close()
                self._driver = None
                logger.trace("Neo4j driver closed")

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Verify driver connectivity.

        :returns: A pair ``(message, ok)``. ``ok`` is ``True`` on success. Never raises.
        """

        try:
            await self._require_driver.verify_connectivity()  # pyright: ignore[reportUnknownMemberType]

        except Exception as e:
            logger.debug("Neo4j health check failed", exc_info=True)
            return str(e) or "Neo4j health check failed", False

        return "ok", True

    # ....................... #

    def _database(self, database: str | None) -> str | None:
        return database or self._config.database

    # ....................... #

    async def _tx_for(self, scope: _TxScope, database: str | None) -> AsyncTransaction:
        """The scope's transaction, opened on *database* if this is the first statement.

        The first statement in a scope that did not pin a database decides which one the
        transaction runs on — that is how a per-tenant (``namespace``-tier) database reaches
        an enlisted transaction at all, since the transaction manager opens the scope long
        before any tenant-resolved name exists. Once bound, a statement naming a *different*
        database is refused: a Neo4j transaction lives in one session, and a session in one
        database, so there is no honest way to run it — and silently running it on the bound
        database is how tenant A's writes land in tenant B's (or the shared default's) store.
        A statement that names no database has no opinion and joins whatever is bound.
        """

        target = self._database(database)

        if not scope.opened:
            # The open is serialized: ``opened`` flips only under the lock, so two
            # statements racing here (one asyncio.gather, one scope) resolve to one
            # session/transaction — the loser re-checks and joins the winner's.
            async with scope.lock:
                if not scope.opened:
                    if scope.database is None:
                        scope.database = target

                    elif target is not None and target != scope.database:
                        raise self._tx_database_conflict(target, scope.database)

                    session = self._require_driver.session(  # pyright: ignore[reportUnknownMemberType]
                        database=scope.database
                    )

                    try:
                        tx = await session.begin_transaction()

                    except BaseException:
                        # A half-open scope must not leak the session to the server
                        # timeout; ``opened`` stays False so a retry can re-open.
                        await session.close()
                        raise

                    scope.session = session
                    scope.tx = tx
                    scope.opened = True

        if target is not None and target != scope.database:
            raise self._tx_database_conflict(target, scope.database)

        if scope.tx is None:  # pragma: no cover - opened implies tx
            raise exc.internal("Neo4j transaction scope is open without a transaction")

        return scope.tx

    # ....................... #

    @staticmethod
    def _tx_database_conflict(target: str | None, bound: str | None) -> Exception:
        return exc.configuration(
            f"Graph statement targets Neo4j database {target!r} but the active transaction is "
            f"bound to {bound!r}. A Neo4j transaction cannot span databases: enlist only routes "
            f"that resolve to one database per transaction (a per-tenant 'database' resolver "
            f"resolves per tenant, so this means two routes disagree), or run the statement "
            f"outside the transaction scope.",
            code="neo4j_tx_database_conflict",
        )

    # ....................... #

    @exc_interceptor.coroutine("neo4j.run")  # type: ignore[untyped-decorator]
    async def run(
        self,
        query: str,
        params: JsonDict | None = None,
        *,
        database: str | None = None,
    ) -> list[JsonDict]:
        scope = self._tx_var.get()

        if scope is not None:
            tx = await self._tx_for(scope, database)
            result = await tx.run(
                query,  # pyright: ignore[reportArgumentType]
                parameters=dict(params or {}),
            )
            return list(await result.data())

        eager = await self._require_driver.execute_query(  # pyright: ignore[reportCallIssue, reportUnknownVariableType]
            query,  # pyright: ignore[reportArgumentType]
            parameters_=dict(params or {}),
            database_=self._database(database),
        )

        return [
            record.data()  # pyright: ignore[reportUnknownMemberType]
            for record in eager.records  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        ]

    # ....................... #

    def is_in_transaction(self) -> bool:
        return self._tx_var.get() is not None

    # ....................... #

    @exc_interceptor.asynccontextmanager("neo4j.transaction")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def transaction(
        self,
        *,
        database: str | None = None,
    ) -> AsyncGenerator[None]:
        """Bind an explicit transaction for the duration of the context.

        The underlying session/transaction is opened by the **first statement** inside the
        scope, not at entry: the transaction manager enlists this scope with no database of its
        own, while the database a statement runs on may be resolved per tenant. Passing
        *database* (or configuring a static one) pins the scope up front instead; a statement
        that then names a different database is refused rather than silently redirected. A scope
        with no statements in it opens nothing and commits nothing.
        """

        if self._tx_var.get() is not None:
            # Already inside a transaction — reuse it (nested scope is a no-op).
            yield
            return

        scope = _TxScope(database=self._database(database))
        token = self._tx_var.set(scope)

        try:
            try:
                yield

            except BaseException:
                if scope.tx is not None:
                    await scope.tx.rollback()

                raise

            else:
                if scope.tx is not None:
                    await scope.tx.commit()

        finally:
            self._tx_var.reset(token)

            if scope.session is not None:
                await scope.session.close()
