"""Mongo client that resolves a URI per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any, AsyncContextManager, AsyncIterator, Mapping, Sequence, final
from uuid import UUID

import attrs
from bson import ObjectId
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError
from forze.base.primitives import JsonDict

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

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    _clients: OrderedDict[UUID, MongoClient] = attrs.field(
        factory=OrderedDict,
        init=False,
    )
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise CoreError("max_cached_tenants must be at least 1")

    # ....................... #

    async def startup(self) -> None:
        """Mark the client as ready (idempotent)."""

        self._started = True

    # ....................... #

    async def close(self) -> None:
        async with self._lock:
            to_close = list(self._clients.values())
            self._clients.clear()

        for c in to_close:
            await c.close()

        self._started = False

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        """Close and remove the client for one tenant."""

        async with self._lock:
            client = self._clients.pop(tenant_id, None)

        if client is not None:
            await client.close()

    # ....................... #

    def _require_tenant_id(self) -> UUID:
        tid = self.tenant_provider()

        if tid is None:
            raise CoreError(
                "Tenant ID is required for routed Mongo access",
                code="tenant_required",
            )

        return tid

    # ....................... #

    async def _get_client(self) -> MongoClient:
        if not self._started:
            raise InfrastructureError("Routed Mongo client is not started")

        tid = self._require_tenant_id()

        async with self._lock:
            if tid in self._clients:
                client = self._clients[tid]
                self._clients.move_to_end(tid)
                return client

            ref = self.secret_ref_for_tenant(tid)

            try:
                uri = await self.secrets.resolve_str(ref)

            except SecretNotFoundError:
                raise

            except Exception as e:
                raise InfrastructureError(
                    f"Failed to resolve Mongo secret for tenant {tid}: {e}",
                ) from e

            db_name = self.database_name_for_tenant(tid)
            client = MongoClient()
            await client.initialize(
                uri,
                db_name=db_name,
                config=self.mongo_config,
            )
            self._clients[tid] = client
            self._clients.move_to_end(tid)

            while len(self._clients) > self.max_cached_tenants:
                _, old = self._clients.popitem(last=False)
                await old.close()

            return client

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

        inner = self._clients.get(tid)

        if inner is None:
            return False

        return inner.is_in_transaction()

    # ....................... #

    def require_transaction(self) -> None:
        tid = self.tenant_provider()

        if tid is None:
            raise InfrastructureError("Transactional context is required")

        inner = self._clients.get(tid)

        if inner is None:
            raise InfrastructureError("Transactional context is required")

        inner.require_transaction()

    # ....................... #

    def transaction(
        self,
        *,
        options: MongoTransactionOptions | None = None,
    ) -> AsyncContextManager[AsyncClientSession]:
        @asynccontextmanager
        async def _cm() -> AsyncIterator[AsyncClientSession]:
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
