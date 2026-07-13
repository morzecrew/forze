"""Structural protocol for Firestore clients."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import (
    Any,
    Protocol,
)

from google.cloud.firestore_v1.async_collection import AsyncCollectionReference
from google.cloud.firestore_v1.base_query import BaseFilter

from forze.base.primitives import JsonDict

# ----------------------- #


class FirestoreClientPort(Protocol):
    """Operations implemented by :class:`FirestoreClient`."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def collection(
        self,
        name: str,
        *,
        database: str | None = None,
    ) -> Awaitable[AsyncCollectionReference]: ...  # pragma: no cover

    def is_in_transaction(self) -> bool: ...  # pragma: no cover

    def require_transaction(self) -> None: ...  # pragma: no cover

    def transaction(self) -> AbstractAsyncContextManager[Any]: ...  # pragma: no cover

    def get_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
    ) -> Awaitable[JsonDict | None]: ...  # pragma: no cover

    def set_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
        data: Mapping[str, Any],
        *,
        merge: bool = False,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def create_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
        data: Mapping[str, Any],
    ) -> Awaitable[None]: ...  # pragma: no cover

    def delete_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
    ) -> Awaitable[None]: ...  # pragma: no cover

    def query_stream(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        limit: int | None = None,
        start_after_id: str | None = None,
    ) -> Awaitable[list[JsonDict]]: ...  # pragma: no cover

    def query_stream_batched(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        limit: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[list[JsonDict]]: ...  # pragma: no cover

    def count_documents(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None = None,
    ) -> Awaitable[int]: ...  # pragma: no cover

    def insert_many(
        self,
        coll: AsyncCollectionReference,
        documents: Sequence[tuple[str, Mapping[str, Any]]],
        *,
        batch_size: int = 200,
        create_only: bool = False,
    ) -> Awaitable[None]: ...  # pragma: no cover
