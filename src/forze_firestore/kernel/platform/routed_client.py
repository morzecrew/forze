"""Firestore client that resolves project/database per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from contextlib import asynccontextmanager
from typing import Any, AsyncContextManager, AsyncGenerator, Callable, Mapping, Sequence, final
from uuid import UUID

import attrs
from google.cloud.firestore_v1.async_collection import AsyncCollectionReference
from google.cloud.firestore_v1.base_query import BaseFilter

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy import (
    TenantClientRegistry,
    ensure_structured_fingerprint,
    require_tenant_id,
    resolve_structured_for_tenant,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.fingerprint import stable_fingerprint

from .client import FirestoreClient
from .port import FirestoreClientPort
from .routing_credentials import FirestoreRoutingCredentials

# ----------------------- #


@final
@attrs.define(slots=True)
class RoutedFirestoreClient(FirestoreClientPort):
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

    __pool: TenantClientRegistry[FirestoreClient, str] = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.__pool = TenantClientRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.close(),
            guarded=False,
        )

    # ....................... #

    async def startup(self) -> None:
        await self.__pool.startup()

    # ....................... #

    async def close(self) -> None:
        await self.__pool.close()

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        await self.__pool.evict(tenant_id)

    # ....................... #

    async def _fingerprint_for(self, tenant_id: UUID) -> str:
        creds = await resolve_structured_for_tenant(
            FirestoreRoutingCredentials,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="Firestore",
        )

        return stable_fingerprint(creds.project_id, creds.database)

    # ....................... #

    async def _create_client(self, tid: UUID) -> FirestoreClient:
        creds = await resolve_structured_for_tenant(
            FirestoreRoutingCredentials,
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="Firestore",
        )
        client = FirestoreClient()

        await client.initialize(
            project_id=creds.project_id,
            database=creds.database,
        )

        return client

    # ....................... #

    async def _get_client(self) -> FirestoreClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Firestore access",
        )

        await ensure_structured_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            fingerprint=lambda: self._fingerprint_for(tenant_id),
        )

        return await self.__pool.get(tenant_id)

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

        inner = self.__pool.peek(tid)

        if inner is None:
            return False

        return inner.is_in_transaction()

    def require_transaction(self) -> None:
        tid = self.tenant_provider()

        if tid is None:
            raise exc.internal("Transactional context is required")

        inner = self.__pool.peek(tid)

        if inner is None:
            raise exc.internal("Transactional context is required")

        inner.require_transaction()

    def transaction(self) -> AsyncContextManager[Any]:
        @asynccontextmanager
        async def _cm() -> AsyncGenerator[Any]:
            inner = await self._get_client()

            async with inner.transaction() as tx:
                yield tx

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
        start_before_id: str | None = None,
    ) -> list[JsonDict]:
        inner = await self._get_client()
        return await inner.query_stream(
            coll,
            filters=filters,
            order_by=order_by,
            limit=limit,
            start_after_id=start_after_id,
            start_before_id=start_before_id,
        )

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
    ) -> None:
        inner = await self._get_client()
        await inner.insert_many(coll, documents, batch_size=batch_size)
