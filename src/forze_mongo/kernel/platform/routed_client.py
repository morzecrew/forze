"""Mongo client that resolves a URI per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Callable,
    Mapping,
    Sequence,
    final,
)
from uuid import UUID

import attrs
from bson import ObjectId
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsPort,
    resolve_str_for_tenant,
    secret_ref_for_tenant,
)
from forze.application.contracts.tenancy import require_tenant_id
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.primitives.lru_registry import SimpleLruRegistry

from .client import MongoClient
from .port import MongoClientPort
from .value_objects import MongoConfig, MongoTransactionOptions

# ----------------------- #


@final
@attrs.define(slots=True)
class RoutedMongoClient(MongoClientPort):
    """Routes each call to a lazily created :class:`MongoClient` for the current tenant.

    Connection URIs are loaded via :meth:`AsyncSecretsPort.resolve_str` using
    ``secret_ref_for_tenant``. Default database names come from
    ``database_name_for_tenant``.

    Call :meth:`startup` during application startup (see
    :func:`~forze_mongo.execution.lifecycle.routed_mongo_lifecycle_step`).
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef]
    tenant_provider: Callable[[], UUID | None]
    database_name_for_tenant: Callable[[UUID], str]
    """Logical database name for each tenant (may match the tenant's dedicated cluster)."""

    mongo_config: MongoConfig = attrs.field(factory=MongoConfig)
    max_cached_tenants: int = 100

    _registry: SimpleLruRegistry[UUID, MongoClient] = attrs.field(init=False)
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise exc.internal("max_cached_tenants must be at least 1")

        self._registry = SimpleLruRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.close(),
        )

    # ....................... #

    async def startup(self) -> None:
        """Mark the client as ready (idempotent)."""

        self._started = True

    # ....................... #

    async def close(self) -> None:
        await self._registry.close_all()
        self._started = False

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        """Close and remove the client for one tenant."""

        await self._registry.evict(tenant_id)

    # ....................... #

    async def _create_client(self, tid: UUID) -> MongoClient:
        ref = secret_ref_for_tenant(self.secret_ref_for_tenant, tid)
        uri = await resolve_str_for_tenant(
            self.secrets,
            ref,
            tenant_id=tid,
            backend="Mongo",
        )

        db_name = self.database_name_for_tenant(tid)
        client = MongoClient()

        await client.initialize(
            uri,
            db_name=db_name,
            config=self.mongo_config,
        )

        return client

    # ....................... #

    async def _get_client(self) -> MongoClient:
        if not self._started:
            raise exc.internal("Routed Mongo client is not started")

        return await self._registry.get_or_create(
            require_tenant_id(
                self.tenant_provider,
                message="Tenant ID is required for routed Mongo access",
            ),
        )

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

    # ....................... #

    async def db(self, name: str | None = None) -> AsyncDatabase[JsonDict]:
        inner = await self._get_client()
        return await inner.db(name)

    # ....................... #

    async def collection(
        self,
        name: str,
        *,
        db_name: str | None = None,
    ) -> AsyncCollection[JsonDict]:
        inner = await self._get_client()
        return await inner.collection(name, db_name=db_name)

    # ....................... #

    def is_in_transaction(self) -> bool:
        tid = self.tenant_provider()

        if tid is None:
            return False

        inner = self._registry.peek(tid)

        if inner is None:
            return False

        return inner.is_in_transaction()

    # ....................... #

    def require_transaction(self) -> None:
        tid = self.tenant_provider()

        if tid is None:
            raise exc.internal("Transactional context is required")

        inner = self._registry.peek(tid)

        if inner is None:
            raise exc.internal("Transactional context is required")

        inner.require_transaction()

    # ....................... #

    def transaction(
        self,
        *,
        options: MongoTransactionOptions | None = None,
    ) -> AsyncContextManager[AsyncClientSession]:
        @asynccontextmanager
        async def _cm() -> AsyncGenerator[AsyncClientSession]:
            inner = await self._get_client()

            async with inner.transaction(options=options) as session:
                yield session

        return _cm()

    # ....................... #

    async def find_one(
        self,
        coll: AsyncCollection[JsonDict],
        filter: Mapping[str, Any],
        *,
        projection: Mapping[str, Any] | None = None,
        sort: Sequence[tuple[str, int]] | None = None,
    ) -> JsonDict | None:
        inner = await self._get_client()
        return await inner.find_one(coll, filter, projection=projection, sort=sort)

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
        inner = await self._get_client()
        return await inner.find_many(
            coll,
            filter,
            projection=projection,
            sort=sort,
            limit=limit,
            skip=skip,
        )

    async def aggregate(
        self,
        coll: AsyncCollection[JsonDict],
        pipeline: Sequence[Mapping[str, Any]],
        *,
        limit: int | None = None,
    ) -> list[JsonDict]:
        inner = await self._get_client()
        return await inner.aggregate(coll, pipeline, limit=limit)

    async def insert_one(
        self,
        coll: AsyncCollection[Any],
        document: Mapping[str, Any],
    ) -> ObjectId:
        inner = await self._get_client()
        return await inner.insert_one(coll, document)

    async def insert_many(
        self,
        coll: AsyncCollection[Any],
        documents: Sequence[Mapping[str, Any]],
        *,
        ordered: bool = True,
        batch_size: int = 200,
    ) -> list[ObjectId]:
        inner = await self._get_client()
        return await inner.insert_many(
            coll, documents, ordered=ordered, batch_size=batch_size
        )

    async def bulk_write(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[Any],
        *,
        ordered: bool = True,
    ) -> Any:
        inner = await self._get_client()
        return await inner.bulk_write(coll, operations, ordered=ordered)

    async def update_one_upsert(
        self,
        coll: AsyncCollection[Any],
        flt: Mapping[str, Any],
        update: Mapping[str, Any],
    ) -> Any:
        inner = await self._get_client()
        return await inner.update_one_upsert(coll, flt, update)

    async def update_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        inner = await self._get_client()
        return await inner.update_one(coll, filter, update, upsert=upsert)

    async def bulk_update(
        self,
        coll: AsyncCollection[Any],
        operations: Sequence[tuple[Mapping[str, Any], Mapping[str, Any]]],
        *,
        ordered: bool = True,
        batch_size: int = 200,
    ) -> int:
        inner = await self._get_client()
        return await inner.bulk_update(
            coll, operations, ordered=ordered, batch_size=batch_size
        )

    async def update_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        upsert: bool = False,
    ) -> int:
        inner = await self._get_client()
        return await inner.update_many(coll, filter, update, upsert=upsert)

    async def delete_one(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        inner = await self._get_client()
        return await inner.delete_one(coll, filter)

    async def delete_many(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        inner = await self._get_client()
        return await inner.delete_many(coll, filter)

    async def count(
        self,
        coll: AsyncCollection[Any],
        filter: Mapping[str, Any],
    ) -> int:
        inner = await self._get_client()
        return await inner.count(coll, filter)

    async def list_indexes(
        self,
        *,
        database: str,
        collection: str,
    ) -> list[JsonDict]:
        inner = await self._get_client()
        return await inner.list_indexes(database=database, collection=collection)
