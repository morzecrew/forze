"""In-memory adapters implementing Forze application contracts.

This module provides mock adapters that are safe for concurrent async usage and
threaded access. Adapters share a :class:`MockState` instance so document and
search contracts observe the same data.
"""

from __future__ import annotations

import asyncio
import mimetypes
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncIterator,
    Literal,
    Mapping,
    Sequence,
    TypeVar,
    cast,
    final,
    overload,
)
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.cache import CachePort
from forze.application.contracts.counter import CounterPort
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.application.contracts.idempotency import IdempotencyPort, IdempotencySnapshot
from forze.application.contracts.pubsub import (
    PubSubCommandPort,
    PubSubMessage,
    PubSubQueryPort,
)
from forze.application.contracts.query import (
    PaginationExpression,
    QueryExpr,
    QueryField,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryOr,
    QuerySortExpression,
)
from forze.application.contracts.queue import (
    QueueCommandPort,
    QueueMessage,
    QueueQueryPort,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchQueryPort,
    SearchSpec,
)
from forze.application.contracts.storage import (
    DownloadedObject,
    StoragePort,
    StoredObject,
)
from forze.application.contracts.stream import (
    StreamCommandPort,
    StreamGroupQueryPort,
    StreamMessage,
    StreamQueryPort,
)
from forze.application.contracts.tx import TxManagerPort, TxScopeKey
from forze.base.errors import ConcurrencyError, ConflictError, CoreError, NotFoundError
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze.base.serialization import (
    pydantic_dump,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.constants import REV_FIELD
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)
M = TypeVar("M", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

_MISSING = object()


# ----------------------- #
# Shared state


@final
@attrs.define(slots=True)
class MockState:
    """Shared in-memory state used by all mock adapters.

    The state uses a process-local :class:`threading.RLock` to protect updates
    across threads and async tasks.
    """

    documents: dict[str, dict[UUID, JsonDict]] = attrs.field(factory=dict)
    counters: dict[tuple[str, str | None], int] = attrs.field(factory=dict)
    cache_kv: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    cache_pointers: dict[str, dict[str, str]] = attrs.field(factory=dict)
    cache_bodies: dict[str, dict[tuple[str, str], Any]] = attrs.field(factory=dict)
    idempotency: dict[
        tuple[str, str, str], tuple[str, str, IdempotencySnapshot | None]
    ] = attrs.field(factory=dict)
    storage: dict[str, dict[str, StoredObject]] = attrs.field(factory=dict)
    storage_bytes: dict[str, dict[str, bytes]] = attrs.field(factory=dict)
    queues: dict[str, dict[str, list[QueueMessage[Any]]]] = attrs.field(factory=dict)
    queue_pending: dict[str, dict[str, dict[str, QueueMessage[Any]]]] = attrs.field(
        factory=dict
    )
    pubsub_logs: dict[str, dict[str, list[PubSubMessage[Any]]]] = attrs.field(
        factory=dict
    )
    streams: dict[str, dict[str, list[StreamMessage[Any]]]] = attrs.field(factory=dict)
    stream_ack: dict[tuple[str, str, str], set[str]] = attrs.field(factory=dict)

    # non-initable
    __lock: threading.RLock = attrs.field(
        factory=threading.RLock, init=False, repr=False
    )
    __seq: int = attrs.field(default=0, init=False, repr=False)

    # ....................... #

    @property
    def lock(self) -> threading.RLock:
        return self.__lock

    # ....................... #

    def next_id(self, prefix: str = "mock") -> str:
        with self.__lock:
            self.__seq += 1
            return f"{prefix}-{self.__seq}"


# ----------------------- #
# Query helpers


def _path_get(obj: Any, path: str) -> Any:
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return _MISSING
            cur = cur[part]  # pyright: ignore[reportUnknownVariableType]
            continue

        return _MISSING

    return cur  # pyright: ignore[reportUnknownVariableType]


def _path_text(obj: Any, path: str) -> str:
    value = _path_get(obj, path)
    if value is _MISSING or value is None:
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return " ".join(
            str(x)  # pyright: ignore[reportUnknownArgumentType]
            for x in value  # pyright: ignore[reportUnknownVariableType]
        )

    return str(value)


def _value_is_empty(value: Any) -> bool:
    if value is None:
        return True

    if isinstance(value, (str, bytes, bytearray, list, tuple, dict, set, frozenset)):
        return len(value) == 0  # pyright: ignore[reportUnknownArgumentType]

    return False


def _coerce_set(value: Any) -> set[Any]:
    if isinstance(value, (list, tuple, set, frozenset)):
        return set(value)  # pyright: ignore[reportUnknownArgumentType]

    return {value}


def _eq(left: Any, right: Any) -> bool:
    if left == right:
        return True

    if isinstance(left, UUID):
        return str(left) == str(right)

    if isinstance(right, UUID):
        return str(left) == str(right)

    return False


def _memb_contains(field_value: Any, values: Sequence[Any]) -> bool:
    if isinstance(field_value, Sequence) and not isinstance(
        field_value, (str, bytes, bytearray)
    ):
        return any(
            _eq(item, candidate)
            for item in field_value  # pyright: ignore[reportUnknownVariableType]
            for candidate in values
        )

    return any(_eq(field_value, candidate) for candidate in values)


def _match_field(doc: JsonDict, field: QueryField) -> bool:
    value = _path_get(doc, field.name)

    match field.op:
        case "$eq":
            if value is _MISSING:
                return False
            return _eq(value, field.value)

        case "$neq":
            if value is _MISSING:
                return True
            return not _eq(value, field.value)

        case "$gt":
            if value is _MISSING:
                return False
            try:
                return value > field.value
            except TypeError:
                return False

        case "$gte":
            if value is _MISSING:
                return False
            try:
                return value >= field.value
            except TypeError:
                return False

        case "$lt":
            if value is _MISSING:
                return False
            try:
                return value < field.value
            except TypeError:
                return False

        case "$lte":
            if value is _MISSING:
                return False
            try:
                return value <= field.value
            except TypeError:
                return False

        case "$null":
            should_be_null = bool(field.value)
            if should_be_null:
                return value is _MISSING or value is None
            return value is not _MISSING and value is not None

        case "$empty":
            should_be_empty = bool(field.value)
            if value is _MISSING:
                return False
            return _value_is_empty(value) is should_be_empty

        case "$in":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return _memb_contains(value, values)

        case "$nin":
            if value is _MISSING:
                return True
            values = cast(Sequence[Any], field.value)
            return not _memb_contains(value, values)

        case "$superset":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return _coerce_set(value).issuperset(values)

        case "$subset":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return _coerce_set(value).issubset(values)

        case "$disjoint":
            if value is _MISSING:
                return True
            values = cast(Sequence[Any], field.value)
            return _coerce_set(value).isdisjoint(values)

        case "$overlaps":
            if value is _MISSING:
                return False
            values = cast(Sequence[Any], field.value)
            return not _coerce_set(value).isdisjoint(values)

    raise ValueError(f"Unknown query operator: {field.op}")


def _match_expr(doc: JsonDict, expr: QueryExpr) -> bool:
    match expr:
        case QueryField():
            return _match_field(doc, expr)

        case QueryOr(items=items):
            return any(_match_expr(doc, item) for item in items)

        case _:
            # QueryAnd and fallback
            items = getattr(  # pyright: ignore[reportUnknownVariableType]
                expr,
                "items",
                tuple(),  # pyright: ignore[reportUnknownArgumentType]
            )
            return all(
                _match_expr(doc, item)  # pyright: ignore[reportUnknownArgumentType]
                for item in items  # pyright: ignore[reportUnknownVariableType]
            )


def _match_filters(doc: JsonDict, filters: QueryFilterExpression | None) -> bool:  # type: ignore[valid-type]
    if filters is None:
        return True

    expr = QueryFilterExpressionParser.parse(filters)
    return _match_expr(doc, expr)


def _project(doc: JsonDict, return_fields: Sequence[str] | None) -> JsonDict:
    if return_fields is None:
        return dict(doc)

    out: JsonDict = {}
    for path in return_fields:
        value = _path_get(doc, path)
        if value is _MISSING:
            continue
        out[path] = value

    return out


def _sort_docs(
    docs: list[JsonDict],
    sorts: QuerySortExpression | None,
) -> list[JsonDict]:
    if not sorts:
        return docs

    out = list(docs)
    for field, direction in reversed(list(sorts.items())):
        reverse = direction == "desc"

        def _sort_key(d: JsonDict, _f: str = field) -> tuple[bool, str]:
            v = _path_get(d, _f)
            return (v is _MISSING, str(v))

        out.sort(key=_sort_key, reverse=reverse)
    return out


# ----------------------- #
# Core adapters


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDocumentAdapter[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](
    DocumentQueryPort[R],
    DocumentCommandPort[R, D, C, U],
):
    """In-memory document adapter with filter/sort/projection support."""

    spec: DocumentSpec[R, D, C, U]
    state: MockState
    namespace: str
    read_model: type[R]
    domain_model: type[D] | None = None

    # ....................... #

    def _store(self) -> dict[UUID, JsonDict]:
        with self.state.lock:
            return self.state.documents.setdefault(self.namespace, {})

    # ....................... #

    def _to_read(self, doc: JsonDict) -> R:
        return pydantic_validate(self.read_model, dict(doc))

    # ....................... #

    def _require_domain_model(self) -> type[D]:
        if self.domain_model is None:
            raise CoreError("Write support requires a domain model")
        return self.domain_model

    # ....................... #

    def _to_domain(self, doc: JsonDict) -> D:
        model = self._require_domain_model()
        return pydantic_validate(model, dict(doc))

    # ....................... #

    def _ensure_exists(self, pk: UUID) -> JsonDict:
        store = self._store()
        if pk not in store:
            raise NotFoundError(f"Document not found: {pk}")
        return store[pk]

    # ....................... #

    def _check_rev(self, current_rev: int, expected_rev: int | None) -> None:
        if expected_rev is None:
            return
        if expected_rev != current_rev:
            raise ConcurrencyError("Revision conflict")

    # ....................... #

    def _to_read_or_projection(
        self,
        doc: JsonDict,
        return_fields: Sequence[str] | None,
    ) -> R | JsonDict:
        if return_fields is not None:
            return _project(doc, return_fields)
        return self._to_read(doc)

    # ....................... #

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> JsonDict: ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R: ...

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Sequence[str] | None = None,
    ) -> R | JsonDict:
        del for_update
        with self.state.lock:
            doc = dict(self._ensure_exists(pk))
        return self._to_read_or_projection(doc, return_fields)

    # ....................... #

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str],
    ) -> Sequence[JsonDict]: ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: None = ...,
    ) -> Sequence[R]: ...

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str] | None = None,
    ) -> Sequence[R] | Sequence[JsonDict]:
        with self.state.lock:
            store = self._store()
            missing = [pk for pk in pks if pk not in store]
            if missing:
                raise NotFoundError(f"Documents not found: {missing}")
            docs = [dict(store[pk]) for pk in pks]

        return [self._to_read_or_projection(doc, return_fields) for doc in docs]  # type: ignore[return-value]

    # ....................... #

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> JsonDict | None: ...

    @overload
    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R | None: ...

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_fields: Sequence[str] | None = None,
    ) -> R | JsonDict | None:
        del for_update

        hits, _ = await self.find_many(  # type: ignore[call-overload]
            filters=filters,
            pagination={"limit": 1},
            return_fields=return_fields,
        )

        if not hits:
            return None

        return hits[0]

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
    ) -> tuple[list[R], int]: ...

    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_fields: Sequence[str] | None = None,
    ) -> tuple[list[R] | list[JsonDict], int]:
        with self.state.lock:
            docs = [dict(doc) for doc in self._store().values()]

        filtered = [doc for doc in docs if _match_filters(doc, filters)]
        total = len(filtered)
        ordered = _sort_docs(filtered, sorts)

        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = pagination.get("offset")

        if offset:
            ordered = ordered[offset:]

        if limit is not None:
            ordered = ordered[:limit]

        out = [self._to_read_or_projection(doc, return_fields) for doc in ordered]

        return out, total  # type: ignore[return-value]

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type, return-value]
        with self.state.lock:
            docs = [dict(doc) for doc in self._store().values()]
        return sum(1 for doc in docs if _match_filters(doc, filters))

    # ....................... #

    @overload
    async def create(self, dto: C, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def create(self, dto: C, *, return_new: Literal[False]) -> None: ...

    async def create(self, dto: C, *, return_new: bool = True) -> R | None:
        domain_model = self._require_domain_model()
        payload = pydantic_dump(dto, exclude={"none": True})
        domain = pydantic_validate(domain_model, payload)
        serialized = pydantic_dump(domain)

        with self.state.lock:
            store = self._store()
            store[domain.id] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

    # ....................... #

    @overload
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not dtos:
            return []
        if return_new:
            return [await self.create(dto, return_new=True) for dto in dtos]
        for dto in dtos:
            await self.create(dto, return_new=False)
        return None

    # ....................... #

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> R: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> tuple[R, JsonDict]: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> None: ...

    @overload
    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> JsonDict: ...

    async def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> R | JsonDict | None | tuple[R, JsonDict]:
        patch = pydantic_dump(dto, exclude={"unset": True})

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)

            updated, diff = current.update(patch)
            if diff:
                updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)

            serialized = pydantic_dump(updated)
            self._store()[pk] = serialized

            if diff:
                write_diff: JsonDict = {**dict(diff), REV_FIELD: updated.rev}
            else:
                write_diff = {}

        if not return_new:
            if return_diff:
                return write_diff

            return None

        read_result = self._to_read(serialized)

        if return_diff:
            return read_result, write_diff

        return read_result

    # ....................... #

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> Sequence[R]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> Sequence[tuple[R, JsonDict]]: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> None: ...

    @overload
    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> Sequence[JsonDict]: ...

    async def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> Sequence[R] | Sequence[JsonDict] | Sequence[tuple[R, JsonDict]] | None:
        if not updates:
            if not return_new:
                return None

            return []

        pks = [u[0] for u in updates]
        if len(set(pks)) != len(pks):
            raise CoreError("Primary keys must be unique")

        if return_new:
            if return_diff:
                return [
                    await self.update(pk, r, dto, return_new=True, return_diff=True)
                    for pk, r, dto in updates
                ]

            return [
                await self.update(pk, r, dto, return_new=True, return_diff=False)
                for pk, r, dto in updates
            ]

        if return_diff:
            return [
                await self.update(pk, r, dto, return_new=False, return_diff=True)
                for pk, r, dto in updates
            ]

        for pk, r, dto in updates:
            await self.update(pk, r, dto, return_new=False)

        return None

    # ....................... #

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def touch(self, pk: UUID, *, return_new: Literal[False]) -> None: ...

    async def touch(self, pk: UUID, *, return_new: bool = True) -> R | None:
        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            updated, _ = current.touch()
            updated = updated.model_copy(update={"rev": current.rev + 1}, deep=True)
            serialized = pydantic_dump(updated)
            self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

    # ....................... #

    @overload
    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not pks:
            return []
        if len(set(pks)) != len(pks):
            raise CoreError("Primary keys must be unique")
        if return_new:
            return [await self.touch(pk, return_new=True) for pk in pks]
        for pk in pks:
            await self.touch(pk, return_new=False)
        return None

    # ....................... #

    async def kill(self, pk: UUID) -> None:
        with self.state.lock:
            _ = self._ensure_exists(pk)
            del self._store()[pk]

    # ....................... #

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        if len(set(pks)) != len(pks):
            raise CoreError("Primary keys must be unique")
        for pk in pks:
            await self.kill(pk)

    # ....................... #

    def _supports_soft_delete(self) -> bool:
        return self.domain_model is not None and issubclass(
            self.domain_model, SoftDeletionMixin
        )

    # ....................... #

    @overload
    async def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def delete(self, pk: UUID, rev: int, *, return_new: bool = True) -> R | None:
        if not self._supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")

        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)
            if cast(Any, current).is_deleted:
                serialized = pydantic_dump(current)
                self._store()[pk] = serialized
            else:
                updated = current.model_copy(
                    update={
                        "is_deleted": True,
                        "last_update_at": utcnow(),
                        "rev": current.rev + 1,
                    },
                    deep=True,
                )
                serialized = pydantic_dump(updated)
                self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

    # ....................... #

    @overload
    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not self._supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")
        if not deletes:
            return []
        if return_new:
            return [await self.delete(pk, r, return_new=True) for pk, r in deletes]
        for pk, r in deletes:
            await self.delete(pk, r, return_new=False)
        return None

    # ....................... #

    @overload
    async def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[True] = True,
    ) -> R: ...

    @overload
    async def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def restore(self, pk: UUID, rev: int, *, return_new: bool = True) -> R | None:
        if not self._supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")
        with self.state.lock:
            current_raw = dict(self._ensure_exists(pk))
            current = self._to_domain(current_raw)
            self._check_rev(current.rev, rev)
            if not cast(Any, current).is_deleted:
                serialized = pydantic_dump(current)
                self._store()[pk] = serialized
            else:
                updated = current.model_copy(
                    update={
                        "is_deleted": False,
                        "last_update_at": utcnow(),
                        "rev": current.rev + 1,
                    },
                    deep=True,
                )
                serialized = pydantic_dump(updated)
                self._store()[pk] = serialized

        if not return_new:
            return None
        return self._to_read(serialized)

    # ....................... #

    @overload
    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        if not self._supports_soft_delete():
            raise CoreError("Soft deletion is not supported for this model")
        if not restores:
            return []
        if return_new:
            return [await self.restore(pk, r, return_new=True) for pk, r in restores]
        for pk, r in restores:
            await self.restore(pk, r, return_new=False)
        return None


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockSearchAdapter[M: BaseModel](SearchQueryPort[M]):
    """In-memory search adapter over documents in :class:`MockState`."""

    state: MockState
    spec: SearchSpec[M]

    # ....................... #

    def _store(self) -> dict[UUID, JsonDict]:
        with self.state.lock:
            return self.state.documents.setdefault(self.spec.name, {})

    # ....................... #

    def _resolve_fields(
        self,
        options: SearchOptions | None,
    ) -> tuple[list[str], dict[str, float] | None]:
        """Return field paths to search and optional per-field weights."""

        opts = options or {}
        allowed = list(self.spec.fields)

        weights_opt = opts.get("weights")
        if weights_opt:
            fields = [f for f in allowed if weights_opt.get(f, 0.0) > 0.0]
            if not fields:
                fields = allowed
            w = {f: float(weights_opt.get(f, 0.0)) for f in fields}
            return fields, w

        fields_opt = opts.get("fields")

        if fields_opt:
            sub = [f for f in fields_opt if f in allowed]
            allowed = sub if sub else allowed

        def_weights = (
            dict(self.spec.default_weights) if self.spec.default_weights else None
        )

        return allowed, def_weights

    # ....................... #

    def _text_score(
        self,
        query: str,
        doc: JsonDict,
        field_paths: Sequence[str],
        mode: str,
    ) -> float:
        q = query.strip().lower()
        if not q:
            return 1.0

        tokens = [x for x in q.split() if x]
        if not tokens:
            return 1.0

        joined = " ".join(_path_text(doc, p).lower() for p in field_paths)
        if not joined.strip():
            return 0.0

        if mode == "exact":
            return 1.0 if q == joined else 0.0

        if mode == "prefix":
            words = joined.split()
            matched = sum(
                1 for token in tokens if any(w.startswith(token) for w in words)
            )
            return matched / len(tokens)

        # fulltext and phrase use token containment for mock behavior.
        matched = sum(1 for token in tokens if token in joined)
        return matched / len(tokens)

    # ....................... #

    def _document_score(
        self,
        query: str,
        doc: JsonDict,
        fields: Sequence[str],
        weights: dict[str, float] | None,
    ) -> float:
        mode = "fulltext"
        if not fields:
            return 0.0
        if weights:
            total_w = sum(weights.values())
            if total_w <= 0.0:
                return 0.0
            acc = 0.0
            for f in fields:
                w = weights.get(f, 0.0)
                if w <= 0.0:
                    continue
                acc += w * self._text_score(query, doc, [f], mode)
            return acc / total_w
        return self._text_score(query, doc, fields, mode)

    # ....................... #

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: None = ...,
    ) -> tuple[list[M], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: type[T],
        return_fields: None = ...,
    ) -> tuple[list[T], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        options: SearchOptions | None = ...,
        return_type: None = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    async def search(
        self,
        query: str,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[T] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> tuple[list[M] | list[T] | list[JsonDict], int]:
        fields, weights = self._resolve_fields(options)

        with self.state.lock:
            docs = [dict(doc) for doc in self._store().values()]

        ranked: list[tuple[float, JsonDict]] = []
        for doc in docs:
            if not _match_filters(doc, filters):
                continue

            score = self._document_score(query, doc, fields, weights)
            if score <= 0.0:
                continue
            ranked.append((score, doc))

        total = len(ranked)
        ranked.sort(key=lambda x: x[0], reverse=True)
        ordered = [doc for _, doc in ranked]

        if sorts:
            ordered = _sort_docs(ordered, sorts)

        pagination = pagination or {}
        limit = pagination.get("limit")
        offset = pagination.get("offset")

        if offset:
            ordered = ordered[offset:]

        if limit is not None:
            ordered = ordered[:limit]

        if return_fields is not None:
            return [_project(doc, return_fields) for doc in ordered], total

        if return_type is not None:
            return pydantic_validate_many(return_type, ordered), total

        allowed = set(self.spec.model_type.model_fields.keys())
        typed_docs = [{k: v for k, v in doc.items() if k in allowed} for doc in ordered]

        return pydantic_validate_many(self.spec.model_type, typed_docs), total


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCounterAdapter(CounterPort):
    """In-memory counter adapter with namespace/suffix partitioning."""

    state: MockState
    namespace: str

    # ....................... #

    def _key(self, suffix: str | None) -> tuple[str, str | None]:
        return self.namespace, suffix

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        with self.state.lock:
            key = self._key(suffix)
            value = self.state.counters.get(key, 0) + by
            self.state.counters[key] = value
            return value

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
    ) -> list[int]:
        if size <= 1:
            raise CoreError("Size must be greater than 1")
        with self.state.lock:
            key = self._key(suffix)
            prev = self.state.counters.get(key, 0)
            curr = prev + size
            self.state.counters[key] = curr
            return list(range(prev + 1, curr + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        with self.state.lock:
            key = self._key(suffix)
            value = self.state.counters.get(key, 0) - by
            self.state.counters[key] = value
            return value

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        with self.state.lock:
            self.state.counters[self._key(suffix)] = value
            return value


# ----------------------- #
# Additional contracts


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCacheAdapter(CachePort):
    """In-memory cache adapter with plain and versioned entries."""

    state: MockState
    namespace: str

    # ....................... #

    def _kv(self) -> dict[str, Any]:
        return self.state.cache_kv.setdefault(self.namespace, {})

    # ....................... #

    def _pointers(self) -> dict[str, str]:
        return self.state.cache_pointers.setdefault(self.namespace, {})

    # ....................... #

    def _bodies(self) -> dict[tuple[str, str], Any]:
        return self.state.cache_bodies.setdefault(self.namespace, {})

    # ....................... #

    async def get(self, key: str) -> Any | None:
        with self.state.lock:
            pointer = self._pointers().get(key)
            if pointer is not None:
                body = self._bodies().get((key, pointer), _MISSING)
                if body is not _MISSING:
                    return body
            return self._kv().get(key)

    # ....................... #

    async def get_many(self, keys: Sequence[str]) -> tuple[dict[str, Any], list[str]]:
        with self.state.lock:
            hits: dict[str, Any] = {}
            for key in keys:
                pointer = self._pointers().get(key)
                if pointer is not None:
                    body = self._bodies().get((key, pointer), _MISSING)
                    if body is not _MISSING:
                        hits[key] = body
                        continue
                if key in self._kv():
                    hits[key] = self._kv()[key]
            misses = [key for key in keys if key not in hits]
            return hits, misses

    # ....................... #

    async def set(self, key: str, value: Any) -> None:
        with self.state.lock:
            self._kv()[key] = value

    # ....................... #

    async def set_versioned(self, key: str, version: str, value: Any) -> None:
        with self.state.lock:
            self._pointers()[key] = version
            self._bodies()[(key, version)] = value

    # ....................... #

    async def set_many(self, key_mapping: dict[str, Any]) -> None:
        with self.state.lock:
            self._kv().update(key_mapping)

    # ....................... #

    async def set_many_versioned(
        self,
        key_version_mapping: Mapping[tuple[str, str], Any],
    ) -> None:
        with self.state.lock:
            for (key, version), value in key_version_mapping.items():
                self._pointers()[key] = version
                self._bodies()[(key, version)] = value

    # ....................... #

    async def delete(self, key: str, *, hard: bool) -> None:
        with self.state.lock:
            self._kv().pop(key, None)
            if hard:
                stale = [k for k in self._bodies() if k[0] == key]
                for item in stale:
                    self._bodies().pop(item, None)
            self._pointers().pop(key, None)

    # ....................... #

    async def delete_many(self, keys: Sequence[str], *, hard: bool) -> None:
        for key in keys:
            await self.delete(key, hard=hard)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockIdempotencyAdapter(IdempotencyPort):
    """In-memory idempotency adapter."""

    state: MockState
    namespace: str

    # ....................... #

    def _key(self, op: str, key: str) -> tuple[str, str, str]:
        return self.namespace, op, key

    # ....................... #

    async def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> IdempotencySnapshot | None:
        if not key:
            return None

        with self.state.lock:
            k = self._key(op, key)
            current = self.state.idempotency.get(k)
            if current is None:
                self.state.idempotency[k] = ("pending", payload_hash, None)
                return None

            status, existing_hash, snapshot = current
            if existing_hash != payload_hash:
                raise ConflictError("Payload hash mismatch")
            if status != "done" or snapshot is None:
                raise ConflictError("Idempotency is in progress")
            return snapshot

    # ....................... #

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None:
        if not key:
            return

        with self.state.lock:
            k = self._key(op, key)
            current = self.state.idempotency.get(k)

            if current is None:
                raise ConflictError("Idempotency commit failed (missing key)")

            _, existing_hash, _ = current

            if existing_hash != payload_hash:
                raise ConflictError("Payload hash mismatch")

            self.state.idempotency[k] = (  # type: ignore[assignment]
                "done",
                payload_hash,
                snapshot,
            )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStorageAdapter(StoragePort):
    """In-memory object storage adapter."""

    state: MockState
    bucket: str

    # ....................... #

    def _objects(self) -> dict[str, StoredObject]:
        return self.state.storage.setdefault(self.bucket, {})

    # ....................... #

    def _payloads(self) -> dict[str, bytes]:
        return self.state.storage_bytes.setdefault(self.bucket, {})

    # ....................... #

    async def upload(
        self,
        filename: str,
        data: bytes,
        description: str | None = None,
        *,
        prefix: str | None = None,
    ) -> StoredObject:
        key = f"{prefix.strip('/') + '/' if prefix else ''}{uuid7()}"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        obj = StoredObject(
            key=key,
            filename=filename,
            description=description,
            content_type=content_type,
            size=len(data),
            created_at=utcnow(),
        )
        with self.state.lock:
            self._objects()[key] = obj
            self._payloads()[key] = bytes(data)
        return obj

    # ....................... #

    async def download(self, key: str) -> DownloadedObject:
        with self.state.lock:
            if key not in self._objects() or key not in self._payloads():
                raise NotFoundError(f"Object not found: {key}")
            obj = self._objects()[key]
            payload = self._payloads()[key]
        return DownloadedObject(
            data=payload,
            content_type=obj["content_type"],
            filename=obj["filename"],
        )

    # ....................... #

    async def delete(self, key: str) -> None:
        with self.state.lock:
            self._objects().pop(key, None)
            self._payloads().pop(key, None)

    # ....................... #

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: str | None = None,
    ) -> tuple[list[StoredObject], int]:
        with self.state.lock:
            rows = list(self._objects().values())
        if prefix:
            rows = [row for row in rows if row["key"].startswith(prefix)]
        total = len(rows)
        return rows[offset : offset + limit], total


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockTxManagerAdapter(TxManagerPort):
    """No-op transaction manager for mock environments."""

    scope_key: TxScopeKey = attrs.field(factory=lambda: TxScopeKey(name="mock"))

    # ....................... #

    def transaction(self):  # type: ignore[no-untyped-def]
        @asynccontextmanager
        async def _noop():  # type: ignore[no-untyped-def]
            yield

        return _noop()


# ----------------------- #
# Message contracts (optional mock coverage)


def _sleep_interval(timeout: timedelta | None) -> float:
    if timeout is None:
        return 0.05
    return max(timeout.total_seconds(), 0.001)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockQueueAdapter[M: BaseModel](QueueQueryPort[M], QueueCommandPort[M]):
    """In-memory queue adapter with ack/nack support."""

    state: MockState
    namespace: str
    model: type[M]

    # ....................... #

    def _queue_store(self) -> dict[str, list[QueueMessage[M]]]:
        return cast(
            dict[str, list[QueueMessage[M]]],
            self.state.queues.setdefault(self.namespace, {}),
        )

    # ....................... #

    def _pending_store(self) -> dict[str, dict[str, QueueMessage[M]]]:
        return cast(
            dict[str, dict[str, QueueMessage[M]]],
            self.state.queue_pending.setdefault(self.namespace, {}),
        )

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
    ) -> str:
        message_id = self.state.next_id("queue")
        message: QueueMessage[M] = {
            "queue": queue,
            "id": message_id,
            "payload": payload,
            "type": type,
            "key": key,
            "enqueued_at": enqueued_at or utcnow(),
        }
        with self.state.lock:
            self._queue_store().setdefault(queue, []).append(message)
        return message_id

    # ....................... #

    async def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
    ) -> list[str]:
        out: list[str] = []
        for payload in payloads:
            out.append(
                await self.enqueue(
                    queue,
                    payload,
                    type=type,
                    key=key,
                    enqueued_at=enqueued_at,
                )
            )
        return out

    # ....................... #

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[QueueMessage[M]]:
        del timeout
        with self.state.lock:
            messages = self._queue_store().setdefault(queue, [])
            pending = self._pending_store().setdefault(queue, {})
            count = limit if limit is not None else len(messages)
            batch = messages[:count]
            del messages[:count]
            for msg in batch:
                pending[msg["id"]] = msg
            return list(batch)

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[QueueMessage[M]]:
        while True:
            batch = await self.receive(queue, limit=1, timeout=timeout)
            if batch:
                yield batch[0]
                continue
            await asyncio.sleep(_sleep_interval(timeout))

    # ....................... #

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        with self.state.lock:
            pending = self._pending_store().setdefault(queue, {})
            acked = 0
            for item_id in ids:
                if item_id in pending:
                    pending.pop(item_id, None)
                    acked += 1
            return acked

    # ....................... #

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        with self.state.lock:
            pending = self._pending_store().setdefault(queue, {})
            queued = self._queue_store().setdefault(queue, [])
            nacked = 0
            for item_id in ids:
                msg = pending.pop(item_id, None)
                if msg is None:
                    continue
                nacked += 1
                if requeue:
                    queued.append(msg)
            return nacked


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockPubSubAdapter[M: BaseModel](PubSubCommandPort[M], PubSubQueryPort[M]):
    """In-memory pub/sub adapter backed by append-only topic logs."""

    state: MockState
    namespace: str
    model: type[M]

    # ....................... #

    def _topic_store(self) -> dict[str, list[PubSubMessage[M]]]:
        return cast(
            dict[str, list[PubSubMessage[M]]],
            self.state.pubsub_logs.setdefault(self.namespace, {}),
        )

    # ....................... #

    async def publish(
        self,
        topic: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
    ) -> None:
        message: PubSubMessage[M] = {
            "topic": topic,
            "payload": payload,
            "type": type,
            "key": key,
            "published_at": published_at or utcnow(),
        }
        with self.state.lock:
            self._topic_store().setdefault(topic, []).append(message)

    # ....................... #

    async def subscribe(
        self,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[PubSubMessage[M]]:
        with self.state.lock:
            cursors = {
                topic: len(self._topic_store().get(topic, [])) for topic in topics
            }

        while True:
            emitted = False
            pending: list[PubSubMessage[M]] = []
            with self.state.lock:
                for topic in topics:
                    log = self._topic_store().setdefault(topic, [])
                    cur = cursors.get(topic, 0)
                    if cur >= len(log):
                        continue
                    pending.extend(log[cur:])
                    emitted = True
                    cursors[topic] = len(log)

            for msg in pending:
                yield msg

            if emitted:
                continue
            await asyncio.sleep(_sleep_interval(timeout))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStreamAdapter[M: BaseModel](StreamQueryPort[M], StreamCommandPort[M]):
    """In-memory stream adapter with monotonic message identifiers."""

    state: MockState
    namespace: str
    model: type[M]

    # ....................... #

    def _stream_store(self) -> dict[str, list[StreamMessage[M]]]:
        return cast(
            dict[str, list[StreamMessage[M]]],
            self.state.streams.setdefault(self.namespace, {}),
        )

    # ....................... #

    def _id_to_int(self, value: str) -> int:
        suffix = value.rsplit("-", 1)[-1]
        try:
            return int(suffix)
        except ValueError:
            return 0

    # ....................... #

    async def append(
        self,
        stream: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
    ) -> str:
        message_id = self.state.next_id("stream")
        message: StreamMessage[M] = {
            "stream": stream,
            "id": message_id,
            "payload": payload,
            "type": type,
            "key": key,
            "timestamp": timestamp or utcnow(),
        }
        with self.state.lock:
            self._stream_store().setdefault(stream, []).append(message)
        return message_id

    # ....................... #

    async def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        del timeout
        out: list[StreamMessage[M]] = []
        with self.state.lock:
            for stream, last_id in stream_mapping.items():
                log = self._stream_store().setdefault(stream, [])
                last_num = self._id_to_int(last_id)
                for msg in log:
                    if self._id_to_int(msg["id"]) > last_num:
                        out.append(msg)
                        if limit is not None and len(out) >= limit:
                            return out
        return out

    # ....................... #

    async def tail(
        self,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[StreamMessage[M]]:
        cursor = dict(stream_mapping)
        while True:
            messages = await self.read(cursor, timeout=timeout)
            for message in messages:
                cursor[message["stream"]] = message["id"]
                yield message
            if not messages:
                await asyncio.sleep(_sleep_interval(timeout))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStreamGroupAdapter[M: BaseModel](StreamGroupQueryPort[M]):
    """In-memory stream group adapter."""

    stream: MockStreamAdapter[M]
    state: MockState
    namespace: str

    # ....................... #

    async def read(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        del consumer
        return await self.stream.read(
            stream_mapping,
            limit=limit,
            timeout=timeout,
        )

    # ....................... #

    async def tail(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[StreamMessage[M]]:
        del group, consumer
        async for item in self.stream.tail(stream_mapping, timeout=timeout):
            yield item

    # ....................... #

    async def ack(self, group: str, stream: str, ids: Sequence[str]) -> int:
        key = (self.namespace, group, stream)
        with self.state.lock:
            ack_set = self.state.stream_ack.setdefault(key, set())
            before = len(ack_set)
            ack_set.update(ids)
            return len(ack_set) - before
