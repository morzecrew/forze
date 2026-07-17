"""Firestore client that resolves project/database per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import (
    Any,
    cast,
    final,
)
from uuid import UUID

import attrs
from google.cloud.firestore_v1.async_collection import AsyncCollectionReference
from google.cloud.firestore_v1.base_query import BaseFilter
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.fingerprint import stable_fingerprint

from .client import FirestoreClient
from .port import FirestoreClientPort
from .routing_credentials import FirestoreRoutingCredentials

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedFirestoreClient(
    StructuredSecretRoutedTenantClientBase[FirestoreClient],
    FirestoreClientPort,
):
    """Routes each operation to a lazily created :class:`FirestoreClient` for the current tenant.

    Project and database ids are JSON secrets (see :class:`FirestoreRoutingCredentials`)
    resolved via :func:`~forze.application.contracts.secrets.resolve_structured`.

    Register under :data:`~forze_firestore.execution.deps.FirestoreClientDepKey` and use
    :func:`~forze_firestore.execution.lifecycle.routed_firestore_lifecycle_step`.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    max_cached_tenants: int = 100
    creds_type: type[BaseModel] = attrs.field(
        default=FirestoreRoutingCredentials,
        init=False,
    )
    backend: str = attrs.field(default="Firestore", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed Firestore access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        c = cast(FirestoreRoutingCredentials, creds)

        return stable_fingerprint(c.project_id, c.database)

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: FirestoreRoutingCredentials,
    ) -> FirestoreClient:
        client = FirestoreClient()

        await client.initialize(
            project_id=creds.project_id,
            database=creds.database,
        )

        return client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

    async def collection(
        self,
        name: str,
        *,
        database: str | None = None,
    ) -> AsyncCollectionReference:
        inner = await self._get_client()
        return await inner.collection(name, database=database)

    def is_in_transaction(self) -> bool:
        tid = self.tenant_provider()

        if tid is None:
            return False

        inner = self._peek_client(tid)

        if inner is None:
            return False

        return inner.is_in_transaction()

    def require_transaction(self) -> None:
        tid = self.tenant_provider()

        if tid is None:
            raise exc.internal("Transactional context is required")

        inner = self._peek_client(tid)

        if inner is None:
            raise exc.internal("Transactional context is required")

        inner.require_transaction()

    def transaction(self) -> AbstractAsyncContextManager[Any]:
        @asynccontextmanager
        async def _cm() -> AsyncGenerator[Any]:
            inner = await self._get_client()

            async with inner.transaction() as tx:
                yield tx

        return _cm()

    def detached(self) -> AbstractAsyncContextManager[None]:
        @asynccontextmanager
        async def _cm() -> AsyncGenerator[None]:
            inner = await self._get_client()

            async with inner.detached():
                yield

        return _cm()

    async def get_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
    ) -> JsonDict | None:
        inner = await self._get_client()
        return await inner.get_document(coll, doc_id)

    async def set_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
        data: Mapping[str, Any],
        *,
        merge: bool = False,
    ) -> None:
        inner = await self._get_client()
        await inner.set_document(coll, doc_id, data, merge=merge)

    async def create_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
        data: Mapping[str, Any],
    ) -> None:
        inner = await self._get_client()
        await inner.create_document(coll, doc_id, data)

    async def delete_document(
        self,
        coll: AsyncCollectionReference,
        doc_id: str,
    ) -> None:
        inner = await self._get_client()
        await inner.delete_document(coll, doc_id)

    async def query_stream(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        limit: int | None = None,
        start_after_id: str | None = None,
    ) -> list[JsonDict]:
        inner = await self._get_client()
        return await inner.query_stream(
            coll,
            filters=filters,
            order_by=order_by,
            limit=limit,
            start_after_id=start_after_id,
        )

    async def query_stream_batched(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        limit: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[list[JsonDict]]:
        inner = await self._get_client()

        async for batch in inner.query_stream_batched(
            coll,
            filters=filters,
            order_by=order_by,
            limit=limit,
            fetch_batch_size=fetch_batch_size,
        ):
            yield batch

    async def count_documents(
        self,
        coll: AsyncCollectionReference,
        *,
        filters: BaseFilter | None = None,
    ) -> int:
        inner = await self._get_client()
        return await inner.count_documents(coll, filters=filters)

    async def insert_many(
        self,
        coll: AsyncCollectionReference,
        documents: Sequence[tuple[str, Mapping[str, Any]]],
        *,
        batch_size: int = 200,
        create_only: bool = False,
    ) -> None:
        inner = await self._get_client()
        await inner.insert_many(coll, documents, batch_size=batch_size, create_only=create_only)
