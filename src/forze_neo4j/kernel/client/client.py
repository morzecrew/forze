"""Async Neo4j client with context-bound explicit transactions."""

from forze_neo4j._compat import require_neo4j

require_neo4j()

# ....................... #

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, final

import attrs
from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncTransaction
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
class Neo4jClient(Neo4jClientPort):
    """Async Neo4j client wrapping :class:`neo4j.AsyncDriver`.

    Runs queries auto-committed by default; when a :meth:`transaction` scope is active
    on the current context, queries route through that explicit transaction so they
    commit or roll back as a unit (used by the Forze transaction scope).
    """

    _driver: AsyncDriver | None = attrs.field(default=None, init=False)
    _config: Neo4jConfig = attrs.field(factory=Neo4jConfig, init=False)
    _tx_var: ContextVar[AsyncTransaction | None] = attrs.field(
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
            logger.debug("Neo4j driver connected")

    # ....................... #

    async def close(self) -> None:
        async with self._init_lock:
            if self._driver is not None:
                await self._driver.close()
                self._driver = None
                logger.debug("Neo4j driver closed")

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Verify driver connectivity.

        :returns: A pair ``(message, ok)``. ``ok`` is ``True`` on success. Never raises.
        """

        try:
            await self._require_driver.verify_connectivity()  # pyright: ignore[reportUnknownMemberType]

        except Exception as e:  # noqa: BLE001 - health must not raise
            logger.debug("Neo4j health check failed", exc_info=True)
            return str(e) or "Neo4j health check failed", False

        return "ok", True

    # ....................... #

    def _database(self, database: str | None) -> str | None:
        return database or self._config.database

    # ....................... #

    @exc_interceptor.coroutine("neo4j.run")  # type: ignore[untyped-decorator]
    async def run(
        self,
        query: str,
        params: JsonDict | None = None,
        *,
        database: str | None = None,
    ) -> list[JsonDict]:
        tx = self._tx_var.get()

        if tx is not None:
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
        """Bind an explicit transaction for the duration of the context."""

        if self._tx_var.get() is not None:
            # Already inside a transaction — reuse it (nested scope is a no-op).
            yield
            return

        async with (
            self._require_driver.session(  # pyright: ignore[reportUnknownMemberType]
                database=self._database(database)
            ) as session
        ):
            tx = await session.begin_transaction()
            token = self._tx_var.set(tx)

            try:
                yield

            except BaseException:
                await tx.rollback()
                raise

            else:
                await tx.commit()

            finally:
                self._tx_var.reset(token)
