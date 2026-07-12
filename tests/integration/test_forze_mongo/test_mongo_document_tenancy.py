"""Integration tests for tenant-aware Mongo documents against a real MongoDB.

Tenant-aware gateways stamp ``TENANT_ID_FIELD`` on writes and add it to every
filter. These tests exercise the full CRUD surface under two tenants against a
real server: the stamp must be driver-encodable (PyMongo rejects a native
``uuid.UUID`` without an explicit ``uuidRepresentation``), stored in the same
canonical string form filters use, and must actually isolate tenants.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.tenancy import TENANT_ID_FIELD, TenantIdentity
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps


class TenantDoc(Document):
    label: str


class TenantCreate(CreateDocumentCmd):
    label: str


class TenantUpdate(BaseDTO):
    label: str | None = None


class TenantRead(ReadDocument):
    label: str


def _tenant_ctx(mongo_client: MongoClient, db_name: str, collection: str) -> ExecutionContext:
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db_name, collection),
            write=(db_name, collection),
            tenant_aware=True,
        )
    )
    return context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )


def _spec(name: str) -> DocumentSpec[TenantRead, TenantDoc, TenantCreate, TenantUpdate]:
    return DocumentSpec(
        name=name,
        read=TenantRead,
        write={
            "domain": TenantDoc,
            "create_cmd": TenantCreate,
            "update_cmd": TenantUpdate,
        },
    )


@contextmanager
def _as_tenant(ctx: ExecutionContext, tenant_id: UUID) -> Iterator[None]:
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_id)):
        yield


@pytest.mark.asyncio
async def test_tenant_aware_create_and_read_back(mongo_client: MongoClient) -> None:
    """Create under a tenant succeeds at the driver and reads back under the same tenant."""
    collection = f"tenancy_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name
    ctx = _tenant_ctx(mongo_client, db_name, collection)
    spec = _spec("tenant_docs_rw")
    tenant_a = uuid4()

    with _as_tenant(ctx, tenant_a):
        cmd = ctx.document.command(spec)
        query = ctx.document.query(spec)

        created = await cmd.create(TenantCreate(label="alpha"))
        assert created.rev == 1

        loaded = await query.get(created.id)
        assert loaded.label == "alpha"
        assert loaded.id == created.id

        found = await query.find({"$values": {"label": {"$eq": "alpha"}}})
        assert found is not None
        assert found.id == created.id

    # The stored stamp is the canonical string form (what filters also use).
    coll = await mongo_client.collection(collection, db_name=db_name)
    raw = await mongo_client.find_one(coll, {"_id": str(created.id)})
    assert raw is not None
    assert raw[TENANT_ID_FIELD] == str(tenant_a)


@pytest.mark.asyncio
async def test_tenant_filter_isolates_other_tenant(mongo_client: MongoClient) -> None:
    """Documents created under one tenant are invisible to another tenant."""
    collection = f"tenancy_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name
    ctx = _tenant_ctx(mongo_client, db_name, collection)
    spec = _spec("tenant_docs_isolated")
    tenant_a = uuid4()
    tenant_b = uuid4()

    with _as_tenant(ctx, tenant_a):
        cmd = ctx.document.command(spec)
        created = await cmd.create(TenantCreate(label="private"))

    with _as_tenant(ctx, tenant_b):
        query = ctx.document.query(spec)

        assert await query.count() == 0
        assert await query.find({"$values": {"label": {"$eq": "private"}}}) is None

        page = await query.find_page(pagination={"limit": 10})
        assert page.count == 0
        assert page.hits == []

        with pytest.raises(CoreException, match="not found"):
            await query.get(created.id)

    with _as_tenant(ctx, tenant_a):
        query = ctx.document.query(spec)

        assert await query.count() == 1
        same = await query.get(created.id)
        assert same.label == "private"


@pytest.mark.asyncio
async def test_tenant_aware_update_and_delete(mongo_client: MongoClient) -> None:
    """Update and hard-delete work under the owning tenant and fail under another."""
    collection = f"tenancy_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name
    ctx = _tenant_ctx(mongo_client, db_name, collection)
    spec = _spec("tenant_docs_mutation")
    tenant_a = uuid4()
    tenant_b = uuid4()

    with _as_tenant(ctx, tenant_a):
        cmd = ctx.document.command(spec)
        created = await cmd.create(TenantCreate(label="v1"))

    with _as_tenant(ctx, tenant_b):
        cmd = ctx.document.command(spec)

        with pytest.raises(CoreException, match="not found"):
            await cmd.update(created.id, created.rev, TenantUpdate(label="stolen"))

        with pytest.raises(CoreException, match="not found"):
            await cmd.kill(created.id)

    with _as_tenant(ctx, tenant_a):
        cmd = ctx.document.command(spec)
        query = ctx.document.query(spec)

        updated = await cmd.update(created.id, created.rev, TenantUpdate(label="v2"))
        assert updated.label == "v2"
        assert updated.rev == 2

        await cmd.kill(created.id)
        assert await query.count() == 0

    # The failed cross-tenant mutations must not have touched the raw document
    # before the owning tenant's update, nor left anything behind after the kill.
    coll = await mongo_client.collection(collection, db_name=db_name)
    raw = await mongo_client.find_one(coll, {"_id": str(created.id)})
    assert raw is None


@pytest.mark.asyncio
async def test_tenant_aware_bulk_create_and_kill_many(mongo_client: MongoClient) -> None:
    """Bulk create stamps every document; bulk delete stays tenant-scoped."""
    collection = f"tenancy_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name
    ctx = _tenant_ctx(mongo_client, db_name, collection)
    spec = _spec("tenant_docs_bulk")
    tenant_a = uuid4()
    tenant_b = uuid4()

    with _as_tenant(ctx, tenant_a):
        cmd = ctx.document.command(spec)
        created = await cmd.create_many(
            [TenantCreate(label="one"), TenantCreate(label="two")]
        )
        assert len(created) == 2

    coll = await mongo_client.collection(collection, db_name=db_name)
    for doc in created:
        raw = await mongo_client.find_one(coll, {"_id": str(doc.id)})
        assert raw is not None
        assert raw[TENANT_ID_FIELD] == str(tenant_a)

    with _as_tenant(ctx, tenant_b):
        cmd = ctx.document.command(spec)

        with pytest.raises(CoreException, match="tenant scope"):
            await cmd.kill_many([doc.id for doc in created])

    with _as_tenant(ctx, tenant_a):
        cmd = ctx.document.command(spec)
        query = ctx.document.query(spec)

        await cmd.kill_many([doc.id for doc in created])
        assert await query.count() == 0
