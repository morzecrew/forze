"""Firestore async client with context-bound transactions."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncGenerator, Mapping, Sequence, final

import attrs
from google.cloud.firestore_v1 import (
    AsyncClient,
    AsyncCollectionReference,
    AsyncTransaction,
)
from google.cloud.firestore_v1.base_query import BaseFilter

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .errors import exc_interceptor
from .port import FirestoreClientPort

# ----------------------- #
# Per-operation timeouts are deliberately not configured here. The SDK calls
# accept ``timeout=``, but exposing it is not a client-local change: the client
# has no config object, so a timeout would have to thread through
# ``initialize`` → ``FirestoreStartupHook`` → ``firestore_lifecycle_step`` and
# the routed-client credential path, while the transaction lifecycle runs on
# private SDK coroutines that take no deadline. Callers that need a deadline
# bound their operations with ``asyncio.timeout`` today; a config surface can
# be added when a concrete deployment needs per-op deadlines.


def _snapshot_to_dict(snapshot: Any) -> JsonDict:
    data: JsonDict = snapshot.to_dict() or {}
    out = dict(data)
    out["id"] = snapshot.id

    return out


# ----------------------- #
# Private SDK transaction API.
#
# The async Firestore SDK exposes no public begin/commit/rollback on
# ``AsyncTransaction``: its only public transaction entrypoint is the
# ``async_transactional`` decorator, which retries a wrapped coroutine and
# cannot back a context-manager API. The driver-level lifecycle lives in the
# private (but docstring-documented) coroutines ``_begin`` / ``_commit`` /
# ``_rollback``, which we call through the helpers below.
#
# Verified against google-cloud-firestore 2.27.0. A unit test
# (tests/unit/test_forze_firestore/test_firestore_client_tx.py::
# TestPrivateTransactionApiCanary) asserts these attributes still exist on the
# installed SDK so a driver upgrade fails loudly in CI instead of at runtime.

_TX_PRIVATE_API = ("_begin", "_commit", "_rollback")


async def _tx_begin(tx: AsyncTransaction) -> None:
    await tx._begin()  # type: ignore[attr-defined]


async def _tx_commit(tx: AsyncTransaction) -> None:
    await tx._commit()  # type: ignore[attr-defined]


async def _tx_rollback(tx: AsyncTransaction) -> None:
    if tx.in_progress:
        await tx._rollback()  # type: ignore[attr-defined]


@asynccontextmanager
async def _tx_lifecycle(tx: AsyncTransaction) -> AsyncGenerator[AsyncTransaction]:
    """Begin on entry, commit on a clean exit, roll back on any exception.

    Mirrors the eager :meth:`FirestoreClient.transaction` body so a lazily
    materialized scope commits/aborts identically; entered onto the pending
    scope's exit stack, it unwinds with the real exception (commit vs rollback).
    """

    await _tx_begin(tx)

    try:
        yield tx
        await _tx_commit(tx)

    except BaseException:
        await _tx_rollback(tx)
        raise


# ....................... #


@attrs.define(slots=True)
class _PendingFirestoreTx:
    """Lazy transaction state held in a context var until the first operation.

    The :class:`AsyncExitStack` is entered around the scope body; materialization
    pushes the transaction lifecycle (:func:`_tx_lifecycle`) onto it, so it
    unwinds — committing on clean exit, aborting on error — when the body exits.
    """

    stack: AsyncExitStack
    tx: AsyncTransaction | None = None


# ....................... #


@final
@attrs.define(slots=True)
class FirestoreClient(FirestoreClientPort):
    """Firestore client with re-entrant transaction scopes."""

    __client: AsyncClient | None = attrs.field(default=None, init=False)
    __project_id: str | None = attrs.field(default=None, init=False)
    __database_id: str | None = attrs.field(default=None, init=False)

    __ctx_transaction: ContextVar[AsyncTransaction | None] = attrs.field(
        factory=lambda: ContextVar("firestore_transaction", default=None),
        init=False,
        repr=False,
    )
    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("firestore_tx_depth", default=0),
        init=False,
        repr=False,
    )
    __ctx_pending: ContextVar["_PendingFirestoreTx | None"] = attrs.field(
        factory=lambda: ContextVar("firestore_tx_pending", default=None),
        init=False,
        repr=False,
    )
    """Per-scope lazy-transaction state: set on root scope entry, materialized on
    the first operation. ``None`` outside a lazy root scope (and in eager mode)."""

    __lazy_tx: bool = attrs.field(default=True, init=False)
    """Whether root transaction scopes defer ``_begin`` to the first operation."""

    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #

    async def initialize(
        self,
        *,
        project_id: str,
        database: str = "(default)",
        lazy_transaction: bool = True,
    ) -> None:
        """Initialize the client.

        ``lazy_transaction`` (default ``True``) defers ``_begin`` until the first
        operation inside a transaction scope; set ``False`` to begin eagerly at
        scope entry.
        """

        async with self.__init_lock:
            if self.__client is not None:
                return

            self.__project_id = project_id
            self.__database_id = database
            self.__lazy_tx = lazy_transaction
            self.__client = AsyncClient(project=project_id, database=database)

    # ....................... #

    async def close(self) -> None:
        async with self.__init_lock:
            if self.__client is None:
                return

            self.__client.close()  # type: ignore[no-untyped-call]
            self.__client = None

    # ....................... #

    def __require_client(self) -> AsyncClient:
        if self.__client is None:
            raise exc.internal("Firestore client is not initialized")

        return self.__client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        try:
            coll = self.__require_client().collection("_forze_health")

            async for _ in coll.limit(1).stream():
                break

            return "ok", True
        except Exception as e:
            return str(e), False

    # ....................... #

    async def collection(
        self,
        name: str,
        *,
        database: str | None = None,
    ) -> AsyncCollectionReference:
        if database is not None and database != self.__database_id:
            raise exc.configuration(
                "forze_firestore does not support per-call database selection; "
                "configure the client's database_id",
                details={
                    "requested_database": database,
                    "configured_database": self.__database_id,
                },
            )

        return self.__require_client().collection(name)

    # ....................... #

    def __current_transaction(self) -> AsyncTransaction | None:
        """Transaction bound to the current context, or ``None``.

        Falls through to a materialized lazy scope's transaction, which is carried
        on the pending object rather than a context var: it is begun during the
        first operation (a different context than the ``transaction()`` generator's
        ``__aexit__``), and a context-var token cannot be reset across contexts.
        """

        bound = self.__ctx_transaction.get()

        if bound is not None:
            return bound

        pending = self.__ctx_pending.get()

        return pending.tx if pending is not None else None

    async def _materialize_pending(self) -> AsyncTransaction:
        """Create + ``_begin`` the transaction for a lazy scope on first use.

        Idempotent within a scope. Enters :func:`_tx_lifecycle` on the pending
        scope's exit stack so the transaction commits on a clean scope exit and
        rolls back on error.

        :raises InfrastructureError: if called with no pending root scope.
        """

        pending = self.__ctx_pending.get()

        if pending is None:
            raise exc.internal("No pending transaction to materialize")

        if pending.tx is not None:
            return pending.tx

        tx = self.__require_client().transaction()
        # Reachable via __current_transaction through the pending object — NOT
        # bound to __ctx_transaction here: this runs in the first operation's
        # context, and the matching reset would land in the generator's __aexit__
        # context, which a context-var token forbids.
        await pending.stack.enter_async_context(_tx_lifecycle(tx))
        pending.tx = tx

        return tx

    async def _transaction_for_op(self) -> AsyncTransaction | None:
        """Transaction to attach to an operation, materializing a lazy scope on
        first use; ``None`` when outside any transaction scope."""

        current = self.__current_transaction()

        if current is not None:
            return current

        if self.__ctx_pending.get() is None:
            return None

        return await self._materialize_pending()

    def is_in_transaction(self) -> bool:
        # Depth-based (logical): a lazy scope that has opened but not yet run an
        # operation still counts as in a transaction, so the next operation
        # materializes it. Equivalent to the old ``depth and tx`` test in eager
        # mode, where a non-zero depth always has a bound transaction.
        return self.__ctx_depth.get() > 0

    def require_transaction(self) -> None:
        if not self.is_in_transaction():
            raise exc.internal("Transactional context is required")

    # ....................... #

    @exc_interceptor.asynccontextmanager("firestore.transaction")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[AsyncTransaction | None]:
        depth = self.__ctx_depth.get()
        parent = self.__current_transaction()

        # A nested scope opened inside a lazy root that has not run an operation
        # yet must materialize the root first — a re-entrant scope reuses its tx.
        if depth > 0 and parent is None and self.__ctx_pending.get() is not None:
            parent = await self._materialize_pending()

        if depth > 0 and parent is not None:
            self.__ctx_depth.set(depth + 1)

            try:
                yield parent

            finally:
                self.__ctx_depth.set(depth)

            return

        # Lazy root: register the scope but begin nothing. The first operation
        # materializes the transaction via the exit stack, which unwinds with the
        # real exception on body exit — so an error after materialization rolls
        # back, and a scope that never ran an operation begins/commits nothing.
        if self.__lazy_tx:
            pending = _PendingFirestoreTx(stack=AsyncExitStack())
            token_p = self.__ctx_pending.set(pending)
            token_d = self.__ctx_depth.set(1)

            try:
                async with pending.stack:
                    yield None

            finally:
                self.__ctx_depth.reset(token_d)
                self.__ctx_pending.reset(token_p)

            return

        client = self.__require_client()
        tx = client.transaction()
        token_t = self.__ctx_transaction.set(tx)
        token_d = self.__ctx_depth.set(1)

        try:
            await _tx_begin(tx)
            yield tx
            await _tx_commit(tx)

        except BaseException:
            # Roll back on *any* exit path — including asyncio.CancelledError —
            # so a cancelled scope does not leak an in-progress server-side
            # transaction. CancelledError is re-raised unmapped (the exc
            # interceptor bypasses control-flow exceptions). An ABORTED commit
            # (contention) also lands here: the transaction is rolled back and
            # the error is mapped to the CONCURRENCY kind, which plugs into
            # forze's OCC retry machinery (the whole scope is re-executed with
            # a fresh transaction).
            await _tx_rollback(tx)
            raise

        finally:
            self.__ctx_depth.reset(token_d)
            self.__ctx_transaction.reset(token_t)

    # ....................... #

    @exc_interceptor.coroutine("firestore.get_document")  # type: ignore[untyped-decorator]
    async def get_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
    ) -> JsonDict | None:
        tx = await self._transaction_for_op()
        ref = coll.document(doc_id)
        snap = await ref.get(transaction=tx)  # type: ignore[untyped-call]

        if not snap.exists:
            return None

        return _snapshot_to_dict(snap)

    # ....................... #

    @exc_interceptor.coroutine("firestore.set_document")  # type: ignore[untyped-decorator]
    async def set_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
        data: Mapping[str, Any],
        *,
        merge: bool = False,
    ) -> None:
        tx = await self._transaction_for_op()
        ref = coll.document(doc_id)

        if tx is not None:
            tx.set(  # type: ignore[untyped-call]
                ref,
                dict(data),
                merge=merge,
            )

        else:
            await ref.set(  # type: ignore[untyped-call]
                dict(data),
                merge=merge,
            )

    # ....................... #

    @exc_interceptor.coroutine("firestore.delete_document")  # type: ignore[untyped-decorator]
    async def delete_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
    ) -> None:
        tx = await self._transaction_for_op()
        ref = coll.document(doc_id)

        if tx is not None:
            tx.delete(ref)

        else:
            await ref.delete()

    # ....................... #

    async def _build_query(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None,
        order_by: Sequence[tuple[str, str]] | None,
        limit: int | None,
        start_after_id: str | None,
        start_before_id: str | None,
    ) -> Any:
        query: Any = coll
        tx = await self._transaction_for_op()

        if filters is not None:
            query = query.where(filter=filters)

        if order_by:
            for field, direction in order_by:
                query = query.order_by(field, direction=direction)

        if start_after_id is not None:
            snap = await coll.document(start_after_id).get(transaction=tx)  # type: ignore[untyped-call]

            if snap.exists:
                query = query.start_after([snap])

        if start_before_id is not None:
            snap = await coll.document(start_before_id).get(transaction=tx)  # type: ignore[untyped-call]

            if snap.exists:
                query = query.end_before([snap])

        if limit is not None:
            query = query.limit(limit)

        return query

    # ....................... #

    @exc_interceptor.coroutine("firestore.query_stream")  # type: ignore[untyped-decorator]
    async def query_stream(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        limit: int | None = None,
        start_after_id: str | None = None,
        start_before_id: str | None = None,
    ) -> list[JsonDict]:
        tx = await self._transaction_for_op()
        query = await self._build_query(
            coll,
            filters=filters,
            order_by=order_by,
            limit=limit,
            start_after_id=start_after_id,
            start_before_id=start_before_id,
        )
        out: list[JsonDict] = []

        async for snap in query.stream(transaction=tx):
            out.append(_snapshot_to_dict(snap))

        return out

    # ....................... #

    @exc_interceptor.coroutine("firestore.count_documents")  # type: ignore[untyped-decorator]
    async def count_documents(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None = None,
    ) -> int:
        tx = await self._transaction_for_op()
        query = coll

        if filters is not None:
            query = query.where(filter=filters)  # type: ignore[assignment]

        results = await query.count().get(transaction=tx)  # type: ignore[no-untyped-call]
        return int(results[0][0].value)  # type: ignore[arg-type]

    # ....................... #

    @exc_interceptor.coroutine("firestore.insert_many")  # type: ignore[untyped-decorator]
    async def insert_many(
        self,
        coll: AsyncCollectionReference,
        documents: Sequence[tuple[str, Mapping[str, Any]]],
        *,
        batch_size: int = 200,
    ) -> None:
        if not documents:
            return

        tx = await self._transaction_for_op()

        if tx is not None:
            for doc_id, data in documents:
                tx.set(  # type: ignore[untyped-call]
                    coll.document(doc_id),
                    dict(data),
                )

            return

        client = self.__require_client()

        for offset in range(0, len(documents), batch_size):
            chunk = documents[offset : offset + batch_size]
            batch = client.batch()

            for doc_id, data in chunk:
                batch.set(  # type: ignore[untyped-call]
                    coll.document(doc_id),
                    dict(data),
                )

            await batch.commit()
