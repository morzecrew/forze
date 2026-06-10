"""Firestore async client with context-bound transactions."""

import asyncio
from contextlib import asynccontextmanager
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
#! TODO: configurable timeout for operations (ref.get etc)


def _snapshot_to_dict(snapshot: Any) -> JsonDict:
    data: JsonDict = snapshot.to_dict() or {}
    out = dict(data)
    out["id"] = snapshot.id

    return out


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

    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #

    async def initialize(
        self,
        *,
        project_id: str,
        database: str = "(default)",
    ) -> None:
        """Initialize the client"""

        async with self.__init_lock:
            if self.__client is not None:
                return

            self.__project_id = project_id
            self.__database_id = database
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
        _ = database or self.__database_id
        return self.__require_client().collection(name)

    # ....................... #

    def __current_transaction(self) -> AsyncTransaction | None:
        return self.__ctx_transaction.get()

    def is_in_transaction(self) -> bool:
        return self.__ctx_depth.get() > 0 and self.__current_transaction() is not None

    def require_transaction(self) -> None:
        if not self.is_in_transaction():
            raise exc.internal("Transactional context is required")

    # ....................... #

    @exc_interceptor.asynccontextmanager("firestore.transaction")  # type: ignore[untyped-decorator]
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[AsyncTransaction]:
        depth = self.__ctx_depth.get()
        parent = self.__current_transaction()

        if depth > 0 and parent is not None:
            self.__ctx_depth.set(depth + 1)

            try:
                yield parent

            finally:
                self.__ctx_depth.set(depth)

            return

        client = self.__require_client()
        tx = client.transaction()
        token_t = self.__ctx_transaction.set(tx)
        token_d = self.__ctx_depth.set(1)

        try:
            await tx._begin()  # type: ignore[attr-defined]
            yield tx
            await tx._commit()  # type: ignore[attr-defined]

        except Exception:
            if tx.in_progress:
                await tx._rollback()  # type: ignore[attr-defined]

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
        tx = self.__current_transaction()
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
        tx = self.__current_transaction()
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
        tx = self.__current_transaction()
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
        tx = self.__current_transaction()

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
        tx = self.__current_transaction()
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
        query = coll

        if filters is not None:
            query = query.where(filter=filters)  # type: ignore[assignment]

        results = await query.count().get()  # type: ignore[no-untyped-call]
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

        tx = self.__current_transaction()

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
