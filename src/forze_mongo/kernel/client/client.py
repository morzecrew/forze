"""Mongo platform client: connection pool, sessions, transactions, and query API.

Provides an async Mongo client built on PyMongo Async API with context-bound
client sessions and optional transactions. Query methods attach the current
session automatically when inside a transaction.
"""

from pydantic import SecretStr

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
from contextvars import ContextVar
from typing import (
    Any,
    Concatenate,
    ParamSpec,
    TypeVar,
    final,
)

import attrs
import pymongo
from bson import ObjectId
from pymongo import ReturnDocument, UpdateOne
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.mongo_client import AsyncMongoClient

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, driver_deadline_budget

from .._logger import logger
from .errors import exc_interceptor
from .port import MongoClientPort
from .value_objects import MongoConfig, MongoTransactionOptions

# ----------------------- #

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _deadline_bounded(
    fn: Callable[Concatenate["MongoClient", _P], Awaitable[_R]],
) -> Callable[Concatenate["MongoClient", _P], Awaitable[_R]]:
    """Wrap a coroutine op in ``pymongo.timeout`` for the remaining invocation deadline.

    A loose CSOT backstop (``remaining + grace``): the authoritative :func:`asyncio.timeout`
    at the invocation boundary is tighter and fires first, while this bounds the server
    ``maxTimeMS`` / socket so a stuck query is cancelled and the connection recovers. A no-op
    when the push-down is disabled or no deadline is bound. Not ``functools.wraps``-ed: the
    outer ``exc_interceptor`` relabels the op, so the wrapper's identity is not observed."""

    async def _wrapped(self: "MongoClient", /, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        budget = (
            driver_deadline_budget()
            if self._push_deadline  # pyright: ignore[reportPrivateUsage]
            else None
        )

        if budget is None:
            return await fn(self, *args, **kwargs)

        with pymongo.timeout(budget):
            return await fn(self, *args, **kwargs)

    return _wrapped


# ----------------------- #


@attrs.define(slots=True)
class _PendingMongoTx:
    """Lazy transaction state held in a context var until the first operation.

    The :class:`AsyncExitStack` is entered around the scope body; materialization
    pushes the server session and ``startTransaction`` onto it, so they unwind —
    committing on clean exit and aborting on error — when the body exits (a bare
    ``aclose()`` would commit on error).
    """

    options: MongoTransactionOptions
    stack: AsyncExitStack
    session: AsyncClientSession | None = None
    lock: asyncio.Lock = attrs.field(factory=asyncio.Lock)
    """Serializes materialization so concurrent first operations in one scope start
    a single session + transaction (not one each)."""


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
    __ctx_pending: ContextVar["_PendingMongoTx | None"] = attrs.field(
        factory=lambda: ContextVar("mongo_tx_pending", default=None),
        init=False,
        repr=False,
    )
    """Per-scope lazy-transaction state: set on root scope entry, materialized on
    the first operation. ``None`` outside a lazy root scope (and in eager mode)."""

    __lazy_tx: bool = attrs.field(default=False, init=False)
    """Whether root transaction scopes defer the session + ``startTransaction``."""

    _push_deadline: bool = attrs.field(default=True, init=False)
    """Whether to push a bound invocation deadline down as a per-op CSOT (set from config).

    Single-underscore so the module-level :func:`_deadline_bounded` decorator can read it."""

    __db_name: str | None = attrs.field(default=None, init=False, repr=False)

    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        uri: str | SecretStr,
        *,
        db_name: str,
        config: MongoConfig = MongoConfig(),
    ) -> None:
        """Create the client. Idempotent; later calls are no-ops. Concurrent
        calls serialize on an internal lock so only one coroutine performs the
        setup.

        :param uri: Mongo connection string.
        :param db_name: Default database name used by :meth:`db`.
        :param config: Client configuration.
        """

        async with self.__init_lock:
            if self.__client is not None:
                return

            if isinstance(uri, SecretStr):
                uri = uri.get_secret_value()

            self.__db_name = db_name
            self.__lazy_tx = config.lazy_transaction
            self._push_deadline = config.push_invocation_deadline
            self.__client = AsyncMongoClient(
                uri,
                appname=config.appname,
                connectTimeoutMS=int(config.connect_timeout.total_seconds() * 1e3),
                serverSelectionTimeoutMS=int(config.server_selection_timeout.total_seconds() * 1e3),
                maxPoolSize=config.max_pool_size,
                minPoolSize=config.min_pool_size,
                document_class=JsonDict,
            )
            logger.trace("Mongo client connected", db=db_name)

            # Optionally force initial server selection early:
            # await self.health()

    # ....................... #

    async def close(self) -> None:
        """Close the underlying client. No-op if not initialized."""

        async with self.__init_lock:
            if self.__client is None:
                return

            await self.__client.close()

            self.__client = None
            logger.trace("Mongo client closed")

    # ....................... #

    def __require_client(self) -> AsyncMongoClient[JsonDict]:
        """Return the active client. Raises :exc:`InfrastructureError` if not initialized."""

        if self.__client is None:
            raise exc.internal("Mongo client is not initialized")

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
            raise exc.configuration("Mongo database name is not configured")

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
        """Session bound to the current context, or ``None``.

        Falls through to a materialized lazy scope's session, which is carried on
        the pending object rather than a context var: it is started during the
        first operation (a different context than the ``transaction()``
        generator's ``__aexit__``), and a context-var token cannot be reset across
        contexts.
        """

        session = self.__ctx_session.get()

        if session is not None:
            return session

        pending = self.__ctx_pending.get()

        return pending.session if pending is not None else None

    # ....................... #

    async def _session_for_op(self) -> AsyncClientSession | None:
        """Session to attach to an operation, materializing a lazy scope on first use.

        Returns the bound session if present, materializes the pending lazy
        transaction (session + ``startTransaction``) on its first operation, or
        ``None`` when outside any transaction scope (the operation then runs
        without a session, as before).
        """

        session = self.__current_session()

        if session is not None:
            return session

        if self.__ctx_pending.get() is None:
            return None

        return await self._materialize_pending()

    # ....................... #

    async def _materialize_pending(self) -> AsyncClientSession:
        """Start the session + transaction for a lazy root scope on first use.

        Idempotent within a scope. Pushes the session and ``startTransaction``
        onto the pending scope's exit stack so they unwind — with the real
        exception, hence abort on error — when the scope body exits.

        :raises InfrastructureError: if called with no pending root scope.
        """

        pending = self.__ctx_pending.get()

        if pending is None:
            raise exc.internal("No pending transaction to materialize")

        if pending.session is not None:
            return pending.session

        # Double-checked lock: concurrent first operations in one scope serialize
        # here so exactly one starts the session + transaction; the rest reuse it.
        async with pending.lock:
            if pending.session is not None:
                return pending.session

            session = await pending.stack.enter_async_context(self.__acquire_session())

            await pending.stack.enter_async_context(
                await session.start_transaction(
                    read_concern=pending.options.read_concern,
                    write_concern=pending.options.write_concern,
                    read_preference=pending.options.read_preference,
                )
            )

            # Reachable via __current_session through the pending object — NOT bound
            # to __ctx_session here: this runs in the first operation's context, and
            # the matching reset would land in the generator's __aexit__ context,
            # which a context-var token forbids.
            pending.session = session

            return session

    # ....................... #

    def is_in_transaction(self) -> bool:
        """Return ``True`` if the current context is inside a transaction scope.

        Depth-based (logical): a lazy scope that has opened but not yet run an
        operation — so its session is not materialized — still counts as in a
        transaction, so the next operation materializes it. Equivalent to the old
        ``depth and session`` test in eager mode, where a non-zero depth always
        has a bound session.
        """

        return self.__ctx_depth.get() > 0

    # ....................... #

    def require_transaction(self) -> None:
        """Raise :exc:`InfrastructureError` if not inside a transaction scope."""

        if not self.is_in_transaction():
            raise exc.internal("Transactional context is required")

    # ....................... #

    @asynccontextmanager
    async def __acquire_session(self) -> AsyncGenerator[AsyncClientSession]:
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

    @exc_interceptor.asynccontextmanager("mongo.transaction")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def transaction(
        self,
        *,
        options: MongoTransactionOptions | None = None,
    ) -> AsyncGenerator[AsyncClientSession | None]:
        """Enter a transaction scope, yielding the active session.

        MongoDB does not support nested transactions. Nested calls reuse the
        same session/transaction and only the outermost block commits/aborts.

        With ``lazy_transaction`` enabled, a root scope acquires no session until
        the first operation (see :meth:`_materialize_pending`); the context
        manager then yields ``None`` until that point, so callers must run work
        through the client's operations rather than the yielded handle.

        :param options: Read/write concerns and read preference.
        :yields: The session to attach to operations, or ``None`` for a lazy root
            scope before its first operation.
        """

        depth = self.__ctx_depth.get()
        parent = self.__current_session()

        options = options if options is not None else MongoTransactionOptions()

        # Nested: bump depth and reuse the scope. The session may still be
        # unmaterialized in a lazy scope (``parent`` is ``None``); the first
        # operation in any nesting level materializes it.
        if depth > 0:
            self.__ctx_depth.set(depth + 1)

            try:
                yield parent

            finally:
                self.__ctx_depth.set(depth)

            return

        # Lazy root: register the scope but acquire nothing. The first operation
        # materializes the session + transaction via the exit stack, which unwinds
        # with the real exception on body exit — so an error after materialization
        # aborts, and a scope that never ran an operation holds nothing.
        if self.__lazy_tx:
            pending = _PendingMongoTx(options=options, stack=AsyncExitStack())
            token_p = self.__ctx_pending.set(pending)
            token_d = self.__ctx_depth.set(1)

            try:
                async with pending.stack:
                    yield None

            finally:
                self.__ctx_depth.reset(token_d)
                self.__ctx_pending.reset(token_p)

            return

        # Eager top-level: create/bind a session and start a transaction.
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

    @exc_interceptor.coroutine("mongo.find_one")  # type: ignore[untyped-decorator]
    @_deadline_bounded
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

        session = await self._session_for_op()
        doc = await coll.find_one(
            filter,
            projection=projection,
            sort=sort,
            session=session,
        )
        return doc

    # ....................... #

    @exc_interceptor.coroutine("mongo.find_one_and_update")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def find_one_and_update(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        sort: Sequence[tuple[str, int]] | None = None,
    ) -> JsonDict | None:
        """Atomically update and return the document after modification."""

        session = await self._session_for_op()
        doc = await coll.find_one_and_update(
            filter,
            update,
            sort=sort,
            return_document=ReturnDocument.AFTER,
            session=session,
        )
        return doc

    # ....................... #

    @exc_interceptor.coroutine("mongo.find_many")  # type: ignore[untyped-decorator]
    @_deadline_bounded
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

        session = await self._session_for_op()
        cur = coll.find(filter, projection=projection, sort=sort, session=session)

        if skip is not None:
            cur = cur.skip(skip)

        if limit is not None:
            cur = cur.limit(limit)

        docs = await cur.to_list(length=limit)
        return docs

    # ....................... #

    @exc_interceptor.asyncgenerator("mongo.find_many_streamed")  # type: ignore[untyped-decorator]
    async def find_many_streamed(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Mapping[str, Any] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
        limit: int | None = None,
        skip: int | None = None,
        batch_size: int = 2000,
    ) -> AsyncGenerator[list[JsonDict]]:
        """Stream matching documents in ``batch_size`` batches, never buffering all.

        Iterates the driver cursor (with a matching network ``batch_size``) and yields
        one app-level batch at a time, so peak memory is a single batch regardless of
        how many documents match — unlike :meth:`find_many`, which drains the cursor to
        a list. ``limit`` still caps the total when set.
        """

        if batch_size < 1:
            raise exc.internal("batch_size must be >= 1")

        # CSOT bounds the whole stream (initial find + every getMore) by the remaining
        # invocation deadline; the decorator can't wrap a generator, so scope it here.
        budget = driver_deadline_budget() if self._push_deadline else None

        with pymongo.timeout(budget) if budget is not None else nullcontext():
            session = await self._session_for_op()
            cur = coll.find(filter, projection=projection, sort=sort, session=session)

            if skip is not None:
                cur = cur.skip(skip)

            if limit is not None:
                cur = cur.limit(limit)

            cur = cur.batch_size(batch_size)
            out: list[JsonDict] = []

            async for doc in cur:
                out.append(doc)

                if len(out) >= batch_size:
                    yield out
                    out = []

            if out:
                yield out

    # ....................... #

    @exc_interceptor.coroutine("mongo.aggregate")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def aggregate(
        self,
        coll: AsyncCollection[JsonDict],
        pipeline: Sequence[Mapping[str, Any]],
        *,
        limit: int | None = None,
    ) -> list[JsonDict]:
        """Run an aggregation pipeline and return documents as a list."""

        session = await self._session_for_op()
        cur = await coll.aggregate(list(pipeline), session=session)
        docs = await cur.to_list(length=limit)
        return list(docs)

    # ....................... #

    @exc_interceptor.coroutine("mongo.insert_one")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def insert_one(
        self,
        coll: AsyncCollection[Any],
        document: Mapping[str, Any],
    ) -> ObjectId:
        """Insert a single document and return its ``_id``."""

        session = await self._session_for_op()
        res = await coll.insert_one(document, session=session)
        return res.inserted_id

    # ....................... #

    @exc_interceptor.coroutine("mongo.insert_many")  # type: ignore[untyped-decorator]
    @_deadline_bounded
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

        session = await self._session_for_op()
        docs = list(documents)
        inserted_ids: list[ObjectId] = []

        for offset in range(0, len(docs), batch_size):
            batch = docs[offset : offset + batch_size]
            res = await coll.insert_many(batch, ordered=ordered, session=session)
            inserted_ids.extend(res.inserted_ids)

        return inserted_ids

    # ....................... #

    @exc_interceptor.coroutine("mongo.bulk_write")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def bulk_write(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[Any],
        *,
        ordered: bool = True,
    ) -> Any:
        """Run ``bulk_write`` on a collection; returns the driver's result object."""

        session = await self._session_for_op()
        return await coll.bulk_write(list(operations), ordered=ordered, session=session)

    # ....................... #

    @exc_interceptor.coroutine("mongo.update_one_upsert")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def update_one_upsert(
        self,
        coll: AsyncCollection[Any],
        flt: Mapping[str, Any],
        update: Mapping[str, Any],
    ) -> Any:
        """``update_one`` with ``upsert=True``; returns the full driver result (e.g. ``upserted_id``)."""

        session = await self._session_for_op()
        return await coll.update_one(
            flt,
            update,
            upsert=True,
            session=session,
        )

    # ....................... #

    @exc_interceptor.coroutine("mongo.update_one")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def update_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        """Update a single document and return matched count."""

        session = await self._session_for_op()
        res = await coll.update_one(filter, update, upsert=upsert, session=session)
        return int(res.matched_count)

    # ....................... #

    @exc_interceptor.coroutine("mongo.bulk_update")  # type: ignore[untyped-decorator]
    @_deadline_bounded
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
        session = await self._session_for_op()
        matched_count = 0

        for offset in range(0, len(requests), batch_size):
            batch = requests[offset : offset + batch_size]
            res = await coll.bulk_write(batch, ordered=ordered, session=session)
            matched_count += int(res.matched_count)

        return matched_count

    # ....................... #

    @exc_interceptor.coroutine("mongo.update_many")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def update_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        """Update multiple documents and return matched count."""

        session = await self._session_for_op()
        res = await coll.update_many(filter, update, upsert=upsert, session=session)
        return int(res.matched_count)

    # ....................... #

    @exc_interceptor.coroutine("mongo.delete_one")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def delete_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        """Delete a single document and return deleted count."""

        session = await self._session_for_op()
        res = await coll.delete_one(filter, session=session)

        return int(res.deleted_count)

    # ....................... #

    @exc_interceptor.coroutine("mongo.delete_many")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def delete_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        """Delete many documents and return deleted count.

        Executes as a single server-side bulk delete; no client-side chunking
        is needed (the port and all callers pass only ``filter``).
        """

        session = await self._session_for_op()

        res = await coll.delete_many(filter, session=session)
        return int(res.deleted_count)

    # ....................... #

    @exc_interceptor.coroutine("mongo.count")  # type: ignore[untyped-decorator]
    @_deadline_bounded
    async def count(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        """Count documents matching ``filter``."""

        session = await self._session_for_op()
        res = await coll.count_documents(filter, session=session)
        return int(res)

    # ....................... #

    @exc_interceptor.coroutine("mongo.list_indexes")  # type: ignore[untyped-decorator]
    async def list_indexes(
        self,
        *,
        database: str,
        collection: str,
    ) -> list[JsonDict]:
        """Return raw index specification documents for a collection."""

        coll = await self.collection(collection, db_name=database)
        session = await self._session_for_op()
        cursor = await coll.list_indexes(session=session)
        return [dict(doc) async for doc in cursor]
