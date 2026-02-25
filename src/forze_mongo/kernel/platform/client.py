"""Mongo platform client: connection pool, sessions, transactions, and query API.

Provides an async Mongo client built on PyMongo Async API with context-bound
client sessions and optional transactions. Query methods attach the current
session automatically when inside a transaction.
"""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import Any, AsyncIterator, Mapping, Optional, Sequence, TypedDict, final

import attrs
from bson import ObjectId
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode  # pyright: ignore[reportPrivateUsage]
from pymongo.write_concern import WriteConcern

from forze.base.errors import InfrastructureError
from forze.base.primitives import JsonDict

# ----------------------- #


@final
class MongoTransactionOptions(TypedDict, total=False):
    """Options for :meth:`MongoClient.transaction`."""

    read_concern: ReadConcern
    """Read concern for the transaction. Omitted means driver default."""

    write_concern: WriteConcern
    """Write concern for the transaction. Omitted means driver default."""

    read_preference: _ServerMode
    """Read preference for the transaction. Omitted means primary."""


# ....................... #


@final
@attrs.define(frozen=True, slots=True, kw_only=True)
class MongoConfig:
    """Client configuration for :class:`MongoClient`."""

    appname: str = "forze"
    """App name for driver metadata."""

    connect_timeout: timedelta = timedelta(seconds=10)
    """Connection timeout."""

    server_selection_timeout: timedelta = timedelta(seconds=10)
    """Server selection timeout."""

    max_pool_size: int = 100
    """Maximum pool size."""

    min_pool_size: int = 0
    """Minimum pool size."""


# ....................... #


@final
@attrs.define(slots=True)
class MongoClient:
    """Async Mongo client with context-bound sessions and optional transactions.

    Must be initialized with a URI via :meth:`initialize` before use. Uses
    context variables to share a single client session per logical request.
    Transactions are re-entrant: nested :meth:`transaction` blocks reuse the
    same session and do not start nested transactions (MongoDB does not support
    nested transactions).
    """

    __client: Optional[AsyncMongoClient[JsonDict]] = attrs.field(
        default=None, init=False
    )

    __ctx_session: ContextVar[Optional[AsyncClientSession]] = attrs.field(
        factory=lambda: ContextVar("mongo_session", default=None),
        init=False,
        repr=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("mongo_tx_depth", default=0),
        init=False,
        repr=False,
    )

    __db_name: Optional[str] = attrs.field(default=None, init=False, repr=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        uri: str,
        *,
        db_name: str,
        config: MongoConfig = MongoConfig(),
    ) -> None:
        """Create the client. Idempotent; later calls are no-ops.

        :param uri: Mongo connection string.
        :param db_name: Default database name used by :meth:`db`.
        :param config: Client configuration.
        """

        if self.__client is not None:
            return

        self.__db_name = db_name
        self.__client = AsyncMongoClient(
            uri,
            appname=config.appname,
            connectTimeoutMS=int(config.connect_timeout.total_seconds() * 1e3),
            serverSelectionTimeoutMS=int(
                config.server_selection_timeout.total_seconds() * 1e3
            ),
            maxPoolSize=config.max_pool_size,
            minPoolSize=config.min_pool_size,
            document_class=JsonDict,
        )

        # Optionally force initial server selection early:
        # await self.health()

    # ....................... #

    async def close(self) -> None:
        """Close the underlying client. No-op if not initialized."""

        if self.__client is None:
            return

        await self.__client.close()

        self.__client = None

    # ....................... #

    def __require_client(self) -> AsyncMongoClient[JsonDict]:
        """Return the active client. Raises :exc:`InfrastructureError` if not initialized."""

        if self.__client is None:
            raise InfrastructureError("Mongo client is not initialized")

        return self.__client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Ping the server.

        :returns: A pair ``(message, ok)``. ``ok`` is ``True`` on success.
        """

        try:
            client = self.__require_client()
            await client.admin.command("ping")
            return "ok", True
        except Exception as e:
            return str(e), False

    # ....................... #
    # DB/collection helpers

    def db(self, name: Optional[str] = None) -> AsyncDatabase[JsonDict]:
        """Return an async database handle.

        :param name: Database name. Defaults to the name passed to :meth:`initialize`.
        """

        db_name = name or self.__db_name
        if not db_name:
            raise InfrastructureError("Mongo database name is not configured")

        return self.__require_client().get_database(db_name)

    # ....................... #

    def collection(
        self,
        name: str,
        *,
        db_name: Optional[str] = None,
    ) -> AsyncCollection[JsonDict]:
        """Return an async collection handle bound to :meth:`db`."""

        return self.db(db_name).get_collection(name)

    # ....................... #
    # Context helpers

    def __current_session(self) -> Optional[AsyncClientSession]:
        """Session bound to the current context, or ``None``."""

        return self.__ctx_session.get()

    # ....................... #

    def is_in_transaction(self) -> bool:
        """Return ``True`` if the current context is inside a transaction scope."""

        return self.__ctx_depth.get() > 0 and self.__current_session() is not None

    # ....................... #

    def require_transaction(self) -> None:
        """Raise :exc:`InfrastructureError` if not inside a transaction scope."""

        if not self.is_in_transaction():
            raise InfrastructureError("Transactional context is required")

    # ....................... #

    @asynccontextmanager
    async def __acquire_session(self) -> AsyncIterator[AsyncClientSession]:
        """Yield the context-bound session or a new one."""

        s = self.__current_session()
        if s is not None:
            yield s
            return

        client = self.__require_client()
        session = client.start_session()

        try:
            yield session

        finally:
            await session.end_session()

    # ....................... #
    # Transaction API

    @asynccontextmanager
    async def transaction(
        self,
        *,
        options: MongoTransactionOptions = MongoTransactionOptions(),
    ) -> AsyncIterator[AsyncClientSession]:
        """Enter a transaction scope, yielding the active session.

        MongoDB does not support nested transactions. Nested calls reuse the
        same session/transaction and only the outermost block commits/aborts.

        :param options: Read/write concerns and read preference.
        :yields: The session to attach to operations.
        """

        depth = self.__ctx_depth.get()
        parent = self.__current_session()

        # Nested: just bump depth and reuse session/transaction.
        if depth > 0 and parent is not None:
            self.__ctx_depth.set(depth + 1)

            try:
                yield parent

            finally:
                self.__ctx_depth.set(depth)

            return

        # Top-level: create/bind a session and start a transaction.
        async with self.__acquire_session() as session:
            token_s = self.__ctx_session.set(session)
            token_d = self.__ctx_depth.set(1)

            try:
                async with await session.start_transaction(
                    read_concern=options.get("read_concern"),
                    write_concern=options.get("write_concern"),
                    read_preference=options.get("read_preference"),
                    # max_commit_time_ms=options.get("max_commit_time_ms")
                ):
                    yield session

            finally:
                self.__ctx_depth.reset(token_d)
                self.__ctx_session.reset(token_s)

    # ....................... #
    # Query API (minimal)

    async def find_one(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Optional[Mapping[str, Any]] = None,
        sort: Optional[Sequence[tuple[str, int]]] = None,
    ) -> Optional[JsonDict]:
        """Find a single document.

        Automatically attaches the current session when in a transaction.
        """

        session = self.__current_session()
        doc = await coll.find_one(
            filter,
            projection=projection,
            sort=sort,
            session=session,
        )
        return doc

    # ....................... #

    async def find_many(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Optional[Mapping[str, Any]] = None,
        sort: Optional[Sequence[tuple[str, int]]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
    ) -> list[JsonDict]:
        """Find many documents and return them as a list."""

        session = self.__current_session()
        cur = coll.find(filter, projection=projection, sort=sort, session=session)

        if skip is not None:
            cur = cur.skip(skip)

        if limit is not None:
            cur = cur.limit(limit)

        docs = await cur.to_list(length=limit or 0)
        return docs

    # ....................... #

    async def insert_one(
        self,
        coll: AsyncCollection[Any],
        document: Mapping[str, Any],
    ) -> ObjectId:
        """Insert a single document and return its ``_id``."""

        session = self.__current_session()
        res = await coll.insert_one(document, session=session)
        return res.inserted_id

    # ....................... #

    async def insert_many(
        self,
        coll: AsyncCollection[Any],
        documents: Sequence[Mapping[str, Any]],
        *,
        ordered: bool = True,
    ) -> list[ObjectId]:
        """Insert multiple documents and return inserted ``_id`` values."""

        session = self.__current_session()
        res = await coll.insert_many(list(documents), ordered=ordered, session=session)
        return list(res.inserted_ids)

    # ....................... #

    async def update_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        """Update a single document and return matched count."""

        session = self.__current_session()
        res = await coll.update_one(filter, update, upsert=upsert, session=session)
        return int(res.matched_count)

    # ....................... #

    async def update_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        """Update multiple documents and return matched count."""

        session = self.__current_session()
        res = await coll.update_many(filter, update, upsert=upsert, session=session)
        return int(res.matched_count)

    # ....................... #

    async def delete_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        """Delete a single document and return deleted count."""

        session = self.__current_session()
        res = await coll.delete_one(filter, session=session)
        return int(res.deleted_count)

    # ....................... #

    async def delete_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        """Delete many documents and return deleted count."""

        session = self.__current_session()
        res = await coll.delete_many(filter, session=session)
        return int(res.deleted_count)
