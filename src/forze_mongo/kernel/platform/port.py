"""Structural protocol for Mongo clients (single URI or tenant-routed)."""

from __future__ import annotations

from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Mapping,
    Protocol,
    Sequence,
)

from bson import ObjectId
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from forze.base.primitives import JsonDict

from .value_objects import MongoTransactionOptions

# ----------------------- #


class MongoClientPort(Protocol):
    """Operations implemented by :class:`MongoClient` and routed variants."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def db(
        self, name: str | None = None
    ) -> Awaitable[AsyncDatabase[JsonDict]]: ...  # pragma: no cover

    def collection(
        self,
        name: str,
        *,
        db_name: str | None = None,
    ) -> Awaitable[AsyncCollection[JsonDict]]: ...  # pragma: no cover

    def is_in_transaction(self) -> bool: ...  # pragma: no cover

    def require_transaction(self) -> None: ...  # pragma: no cover

    def transaction(
        self,
        *,
        options: MongoTransactionOptions | None = None,
    ) -> AsyncContextManager[AsyncClientSession]: ...  # pragma: no cover

    def find_one(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Mapping[str, Any] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
    ) -> Awaitable[JsonDict | None]: ...  # pragma: no cover

    def find_many(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Mapping[str, Any] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
        limit: int | None = None,
        skip: int | None = None,
    ) -> Awaitable[list[JsonDict]]: ...  # pragma: no cover

    def aggregate(
        self,
        coll: AsyncCollection[JsonDict],
        pipeline: Sequence[Mapping[str, Any]],
        *,
        limit: int | None = None,
    ) -> Awaitable[list[JsonDict]]: ...  # pragma: no cover

    def insert_one(
        self,
        coll: AsyncCollection[Any],
        document: Mapping[str, Any],
    ) -> Awaitable[ObjectId]: ...  # pragma: no cover

    def insert_many(
        self,
        coll: AsyncCollection[Any],
        documents: Sequence[Mapping[str, Any]],
        *,
        ordered: bool = True,
        batch_size: int = 200,
    ) -> Awaitable[list[ObjectId]]: ...  # pragma: no cover

    def bulk_write(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[Any],
        *,
        ordered: bool = True,
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def update_one_upsert(
        self,
        coll: AsyncCollection[Any],
        flt: Mapping[str, Any],
        update: Mapping[str, Any],
    ) -> Awaitable[Any]: ...  # pragma: no cover

    def update_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> Awaitable[int]: ...  # pragma: no cover

    def bulk_update(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[tuple[Mapping[str, Any], Mapping[str, Any]]],
        *,
        ordered: bool = True,
        batch_size: int = 200,
    ) -> Awaitable[int]: ...  # pragma: no cover

    def update_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> Awaitable[int]: ...  # pragma: no cover

    def delete_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> Awaitable[int]: ...  # pragma: no cover

    def delete_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> Awaitable[int]: ...  # pragma: no cover

    def count(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> Awaitable[int]: ...  # pragma: no cover
