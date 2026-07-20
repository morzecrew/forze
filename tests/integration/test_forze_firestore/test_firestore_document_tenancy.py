"""Integration tests for tenant-aware Firestore documents against the emulator.

Tenant-aware gateways stamp ``TENANT_ID_FIELD`` on writes and add it to every
filter. These tests exercise the full CRUD surface under two tenants against the
emulator: the stamp must be driver-encodable (the Firestore SDK rejects a native
``uuid.UUID``), stored in the same canonical string form filters use, must
survive updates (a full-document ``set`` must not strip it), and must actually
isolate tenants.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
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
from forze_firestore.execution.deps import (
    ConfigurableFirestoreDocument,
    FirestoreDocumentConfig,
)
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.utils import doc_write_gw
from forze_firestore.kernel.client import FirestoreClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class TenantDoc(Document):
    label: str


class TenantCreate(CreateDocumentCmd):
    label: str


class TenantUpdate(BaseDTO):
    label: str | None = None


class TenantRead(ReadDocument):
    label: str


_WRITE_TYPES = {
    "domain": TenantDoc,
    "create_cmd": TenantCreate,
    "update_cmd": TenantUpdate,
}


def _tenant_ctx(client: FirestoreClient, collection: str) -> ExecutionContext:
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
            tenant_aware=True,
        )
    )
    return context_from_deps(
        Deps.plain(
            {
                FirestoreClientDepKey: client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )


def _spec(name: str) -> DocumentSpec[TenantRead, TenantDoc, TenantCreate, TenantUpdate]:
    return DocumentSpec(
        name=name,
        read=TenantRead,
        write=_WRITE_TYPES,
    )


@contextmanager
def _as_tenant(ctx: ExecutionContext, tenant_id: UUID) -> Iterator[None]:
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_id)):
        yield


async def test_tenant_aware_create_and_read_back(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """Create under a tenant succeeds at the driver and reads back under the same tenant."""

    collection = f"tenancy_rw_{unique_collection}"
    ctx = _tenant_ctx(firestore_client, collection)
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

        assert await query.count() == 1

    # The stored stamp is the canonical string form (what filters also use).
    coll = await firestore_client.collection(collection)
    raw = await firestore_client.get_document(coll, str(created.id))
    assert raw is not None
    assert raw[TENANT_ID_FIELD] == str(tenant_a)


async def test_tenant_aware_update_preserves_tenant_tag(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """An update must not strip the tenant stamp from the stored document.

    Updates write the merged image back as a full-document ``set``; if that image
    loses ``tenant_id`` the row silently vanishes from every tenant-filtered read
    and the owning tenant can no longer get/kill its own document.
    """

    collection = f"tenancy_up_{unique_collection}"
    ctx = _tenant_ctx(firestore_client, collection)
    spec = _spec("tenant_docs_update")
    tenant_a = uuid4()

    with _as_tenant(ctx, tenant_a):
        cmd = ctx.document.command(spec)
        query = ctx.document.query(spec)

        created = await cmd.create(TenantCreate(label="v1"))
        updated = await cmd.update(created.id, created.rev, TenantUpdate(label="v2"))
        assert updated.label == "v2"

        # Tenant-filtered read-back still sees the just-updated document.
        loaded = await query.get(created.id)
        assert loaded.label == "v2"

        found = await query.find({"$values": {"label": {"$eq": "v2"}}})
        assert found is not None
        assert found.id == created.id

        assert await query.count() == 1

        # The stored stamp survived the update in canonical string form.
        coll = await firestore_client.collection(collection)
        raw = await firestore_client.get_document(coll, str(created.id))
        assert raw is not None
        assert raw[TENANT_ID_FIELD] == str(tenant_a)

        # The owning tenant can still delete its own just-updated document.
        await cmd.kill(created.id)

        with pytest.raises(CoreException, match="not found"):
            await query.get(created.id)


async def test_tenant_filter_isolates_other_tenant(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """Documents created under one tenant are invisible and immutable to another."""

    collection = f"tenancy_iso_{unique_collection}"
    ctx = _tenant_ctx(firestore_client, collection)
    spec = _spec("tenant_docs_isolated")
    tenant_a = uuid4()
    tenant_b = uuid4()

    with _as_tenant(ctx, tenant_a):
        cmd = ctx.document.command(spec)
        created = await cmd.create(TenantCreate(label="private"))

    with _as_tenant(ctx, tenant_b):
        cmd = ctx.document.command(spec)
        query = ctx.document.query(spec)

        assert await query.count() == 0
        assert await query.find({"$values": {"label": {"$eq": "private"}}}) is None

        with pytest.raises(CoreException, match="not found"):
            await query.get(created.id)

        with pytest.raises(CoreException, match="not found"):
            await cmd.update(created.id, created.rev, TenantUpdate(label="stolen"))

        with pytest.raises(CoreException, match="not found"):
            await cmd.kill(created.id)

    with _as_tenant(ctx, tenant_a):
        query = ctx.document.query(spec)

        assert await query.count() == 1
        same = await query.get(created.id)
        assert same.label == "private"


async def test_tenant_aware_history_write_and_read_back(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """History snapshots are tenant-stamped so tenant-filtered history reads find them."""

    collection = f"tenancy_hist_{unique_collection}"
    history = f"{collection}_history"
    ctx = context_from_deps(Deps.plain({FirestoreClientDepKey: firestore_client}))
    tenant_a = uuid4()

    with _as_tenant(ctx, tenant_a):
        write = doc_write_gw(
            ctx,
            write_types=_WRITE_TYPES,
            write_relation=("(default)", collection),
            history_relation=("(default)", history),
            history_enabled=True,
            tenant_aware=True,
        )

        created = await write.create(TenantCreate(label="h1"))
        updated, _diff = await write.update(
            created.id, TenantUpdate(label="h2"), rev=created.rev
        )
        assert updated.rev == created.rev + 1

        assert write.history_gw is not None
        first = await write.history_gw.read(created.id, created.rev)
        assert first.label == "h1"

    # Stored history rows carry the same canonical tenant stamp as live rows.
    coll = await firestore_client.collection(history)
    raw = await firestore_client.get_document(coll, f"{created.id}_{created.rev}")
    assert raw is not None
    assert raw[TENANT_ID_FIELD] == str(tenant_a)
