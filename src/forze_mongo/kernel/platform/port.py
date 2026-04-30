"""Structural protocol for Mongo clients (single URI or tenant-routed)."""

from __future__ import annotations

from typing import (
    Any,
    AsyncContextManager,
    Mapping,
    Protocol,
    Sequence,
)

from bson import ObjectId
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from forze.base.primitives import JsonDict

from .client import MongoTransactionOptions

# ----------------------- #


class MongoClientPort(Protocol):
    """Operations implemented by :class:`MongoClient` and routed variants."""

    async def close(self) -> None:
        ...  # pragma: no cover

    async def health(self) -> tuple[str, bool]:
        ...  # pragma: no cover

    async def db(self, name: str | None = None) -> AsyncDatabase[JsonDict]:
        ...  # pragma: no cover

    async def collection(
        self,
        name: str,
        *,
        db_name: str | None = None,
    ) -> AsyncCollection[JsonDict]:
        ...  # pragma: no cover

    def is_in_transaction(self) -> bool:
        ...  # pragma: no cover

    def require_transaction(self) -> None:
        ...  # pragma: no cover

    def transaction(
        self,
        *,
        options: MongoTransactionOptions = ...,
    ) -> AsyncContextManager[AsyncClientSession]:
        ...  # pragma: no cover

    async def find_one(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Mapping[str, Any] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
    ) -> JsonDict | None:
        ...  # pragma: no cover

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
        ...  # pragma: no cover

    async def aggregate(
        self,
        coll: AsyncCollection[JsonDict],
        pipeline: Sequence[Mapping[str, Any]],
        *,
        limit: int | None = None,
    ) -> list[JsonDict]:
        ...  # pragma: no cover

    async def insert_one(
        self,
        coll: AsyncCollection[Any],
        document: Mapping[str, Any],
    ) -> ObjectId:
        ...  # pragma: no cover

    async def insert_many(
        self,
        coll: AsyncCollection[Any],
        documents: Sequence[Mapping[str, Any]],
        *,
        ordered: bool = True,
        batch_size: int = 200,
    ) -> list[ObjectId]:
        ...  # pragma: no cover

    async def bulk_write(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[Any],
        *,
        ordered: bool = True,
    ) -> Any:
        ...  # pragma: no cover

    async def update_one_upsert(
        self,
        coll: AsyncCollection[Any],
        flt: Mapping[str, Any],
        update: Mapping[str, Any],
    ) -> Any:
        ...  # pragma: no cover

    async def update_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        ...  # pragma: no cover

    async def bulk_update(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[tuple[Mapping[str, Any], Mapping[str, Any]]],
        *,
        ordered: bool = True,
        batch_size: int = 200,
    ) -> int:
        ...  # pragma: no cover

    async def update_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        ...  # pragma: no cover

    async def delete_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        ...  # pragma: no cover

    async def delete_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        ...  # pragma: no cover

    async def count(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        ...  # pragma: no cover
