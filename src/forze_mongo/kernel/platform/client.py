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
from typing import Any, AsyncIterator, Mapping, Sequence, final

import attrs
from bson import ObjectId
from pymongo import UpdateOne
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.mongo_client import AsyncMongoClient

from forze.base.errors import InfrastructureError
from forze.base.primitives import JsonDict

from .errors import mongo_handled
from .port import MongoClientPort
from .value_objects import MongoConfig, MongoTransactionOptions

# ----------------------- #


@final
@attrs.define(slots=True)
class MongoClient(MongoClientPort):
    """Async Mongo client with context-bound sessions and optional transactions.

    Must be initialized with a URI via :meth:`initialize` before use. Uses
    context variables to share a single client session per logical request.
    Transactions are re-entrant: nested :meth:`transaction` blocks reuse the
    same session and do not start nested transactions (MongoDB does not support
    nested transactions).
    """

    __client: AsyncMongoClient[JsonDict] | None = attrs.field(default=None, init=False)

    __ctx_session: ContextVar[AsyncClientSession | None] = attrs.field(
        factory=lambda: ContextVar("mongo_session", default=None),
        init=False,
        repr=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("mongo_tx_depth", default=0),
        init=False,
        repr=False,
    )

    __db_name: str | None = attrs.field(default=None, init=False, repr=False)

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

    async def db(self, name: str | None = None) -> AsyncDatabase[JsonDict]:
        """Return an async database handle.

        :param name: Database name. Defaults to the name passed to :meth:`initialize`.
        """

        db_name = name or self.__db_name

        if not db_name:
            raise InfrastructureError("Mongo database name is not configured")

        return self.__require_client().get_database(db_name)

    # ....................... #

    async def collection(
        self,
        name: str,
        *,
        db_name: str | None = None,
    ) -> AsyncCollection[JsonDict]:
        """Return an async collection handle bound to :meth:`db`."""

        d = await self.db(db_name)
        return d.get_collection(name)

    # ....................... #
    # Context helpers

    def __current_session(self) -> AsyncClientSession | None:
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

    @mongo_handled("mongo.transaction")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def transaction(
        self,
        *,
        options: MongoTransactionOptions | None = None,
    ) -> AsyncIterator[AsyncClientSession]:
        """Enter a transaction scope, yielding the active session.

        MongoDB does not support nested transactions. Nested calls reuse the
        same session/transaction and only the outermost block commits/aborts.

        :param options: Read/write concerns and read preference.
        :yields: The session to attach to operations.
        """

        depth = self.__ctx_depth.get()
        parent = self.__current_session()

        options = options if options is not None else MongoTransactionOptions()

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
                    read_concern=options.read_concern,
                    write_concern=options.write_concern,
                    read_preference=options.read_preference,
                    # max_commit_time_ms=options.get("max_commit_time_ms")
                ):
                    yield session

            finally:
                self.__ctx_depth.reset(token_d)
                self.__ctx_session.reset(token_s)

    # ....................... #
    # Query API (minimal)

    @mongo_handled("mongo.find_one")  # type: ignore[untyped-decorator]
    async def find_one(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Mapping[str, Any] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
    ) -> JsonDict | None:
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

    @mongo_handled("mongo.find_many")  # type: ignore[untyped-decorator]
    async def find_many(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Mapping[str, Any] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
        limit: int | None = None,
        skip: int | None = None,
    ) -> list[JsonDict]:
        """Find many documents and return them as a list."""

        session = self.__current_session()
        cur = coll.find(filter, projection=projection, sort=sort, session=session)

        if skip is not None:
            cur = cur.skip(skip)

        if limit is not None:
            cur = cur.limit(limit)

        docs = await cur.to_list(length=limit)
        return docs

    # ....................... #

    @mongo_handled("mongo.aggregate")  # type: ignore[untyped-decorator]
    async def aggregate(
        self,
        coll: AsyncCollection[JsonDict],
        pipeline: Sequence[Mapping[str, Any]],
        *,
        limit: int | None = None,
    ) -> list[JsonDict]:
        """Run an aggregation pipeline and return documents as a list."""

        session = self.__current_session()
        cur = await coll.aggregate(list(pipeline), session=session)
        docs = await cur.to_list(length=limit)
        return list(docs)

    # ....................... #

    @mongo_handled("mongo.insert_one")  # type: ignore[untyped-decorator]
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

    @mongo_handled("mongo.insert_many")  # type: ignore[untyped-decorator]
    async def insert_many(
        self,
        coll: AsyncCollection[Any],
        documents: Sequence[Mapping[str, Any]],
        *,
        ordered: bool = True,
        batch_size: int = 200,
    ) -> list[ObjectId]:
        """Insert multiple documents and return inserted ``_id`` values.

        :param coll: The collection to insert into.
        :param documents: A sequence of documents to insert.
        :param ordered: Whether to execute operations in order.
        :param batch_size: Batch size for the bulk operation.
        :returns: A list of inserted ``_id`` values.
        """

        session = self.__current_session()
        docs = list(documents)
        inserted_ids: list[ObjectId] = []

        for offset in range(0, len(docs), batch_size):
            batch = docs[offset : offset + batch_size]
            res = await coll.insert_many(batch, ordered=ordered, session=session)
            inserted_ids.extend(res.inserted_ids)

        return inserted_ids

    # ....................... #

    @mongo_handled("mongo.bulk_write")  # type: ignore[untyped-decorator]
    async def bulk_write(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[Any],
        *,
        ordered: bool = True,
    ) -> Any:
        """Run ``bulk_write`` on a collection; returns the driver's result object."""

        session = self.__current_session()
        return await coll.bulk_write(list(operations), ordered=ordered, session=session)

    # ....................... #

    @mongo_handled("mongo.update_one_upsert")  # type: ignore[untyped-decorator]
    async def update_one_upsert(
        self,
        coll: AsyncCollection[Any],
        flt: Mapping[str, Any],
        update: Mapping[str, Any],
    ) -> Any:
        """``update_one`` with ``upsert=True``; returns the full driver result (e.g. ``upserted_id``)."""

        session = self.__current_session()
        return await coll.update_one(
            flt,
            update,
            upsert=True,
            session=session,
        )

    # ....................... #

    @mongo_handled("mongo.update_one")  # type: ignore[untyped-decorator]
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

    @mongo_handled("mongo.bulk_update")  # type: ignore[untyped-decorator]
    async def bulk_update(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[tuple[Mapping[str, Any], Mapping[str, Any]]],
        *,
        ordered: bool = True,
        batch_size: int = 200,
    ) -> int:
        """Execute multiple updates in a single bulk operation.

        :param coll: The collection to update.
        :param operations: A sequence of ``(filter, update)`` pairs.
        :param ordered: Whether to execute operations in order.
        :param batch_size: Batch size for the bulk operation.
        :returns: Total matched count.
        """

        if not operations:
            return 0

        requests = [UpdateOne(f, u) for f, u in operations]
        session = self.__current_session()
        matched_count = 0

        for offset in range(0, len(requests), batch_size):
            batch = requests[offset : offset + batch_size]
            res = await coll.bulk_write(batch, ordered=ordered, session=session)
            matched_count += int(res.matched_count)

        return matched_count

    # ....................... #

    @mongo_handled("mongo.update_many")  # type: ignore[untyped-decorator]
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

    @mongo_handled("mongo.delete_one")  # type: ignore[untyped-decorator]
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

    @mongo_handled("mongo.delete_many")  # type: ignore[untyped-decorator]
    async def delete_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        """Delete many documents and return deleted count."""

        session = self.__current_session()
        res = await coll.delete_many(filter, session=session)
        return int(res.deleted_count)

    # ....................... #

    @mongo_handled("mongo.count")  # type: ignore[untyped-decorator]
    async def count(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        """Count documents matching ``filter``."""

        session = self.__current_session()
        res = await coll.count_documents(filter, session=session)
        return int(res)
