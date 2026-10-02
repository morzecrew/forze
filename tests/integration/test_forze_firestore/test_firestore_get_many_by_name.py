"""``get_many`` on Firestore finds a document by its name, as ``get`` does.

``get`` reads a document by its name; ``get_many`` used to query the ``id`` field in the body
instead. A document written without that field — by the console, a migration or another
service — was found by ``get`` and reported missing by ``get_many``, so every grant resolution
and tenant listing that touched it failed.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze_firestore.execution.deps import ConfigurableFirestoreDocument, FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from forze_identity.tenancy.application.specs import tenant_spec
from forze_identity.tenancy.domain.models.tenant import CreateTenantCmd
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _ctx(client: FirestoreClient, collection: str, *, tenant_aware: bool = False) -> Any:
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
            tenant_aware=tenant_aware,
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


@contextmanager
def _as_tenant(ctx: Any, tenant_id: UUID) -> Iterator[None]:
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_id)):
        yield


async def test_a_document_without_its_id_in_the_body_is_read_by_name(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    ctx = _ctx(firestore_client, unique_collection)
    made = await ctx.document.command(tenant_spec).create(CreateTenantCmd(tenant_key="t"))
    coll = await firestore_client.collection(unique_collection)
    raw = await firestore_client.get_document(coll, str(made.id))
    assert raw is not None
    # The same document, rewritten without the `id` field in its body.
    await firestore_client.set_document(
        coll, str(made.id), {k: v for k, v in raw.items() if k != "id"}
    )
    query = ctx.document.query(tenant_spec)

    assert (await query.get(made.id)).id == made.id
    assert [row.id for row in await query.get_many([made.id])] == [made.id]


async def test_ids_past_one_batch_come_back_in_the_order_asked_with_repeats(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    ctx = _ctx(firestore_client, unique_collection)
    command = ctx.document.command(tenant_spec)
    ids = [(await command.create(CreateTenantCmd(tenant_key=f"t-{i}"))).id for i in range(35)]
    asked = [*reversed(ids), ids[3]]

    rows = await ctx.document.query(tenant_spec).get_many(asked)

    assert [row.id for row in rows] == asked


async def test_a_missing_id_fails_the_whole_read(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    ctx = _ctx(firestore_client, unique_collection)
    made = await ctx.document.command(tenant_spec).create(CreateTenantCmd(tenant_key="t"))

    with pytest.raises(CoreException) as missing:
        await ctx.document.query(tenant_spec).get_many([made.id, uuid4()])

    assert missing.value.code == "core.not_found"


async def test_another_tenants_document_reads_as_missing(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    ctx = _ctx(firestore_client, unique_collection, tenant_aware=True)
    owner, stranger = uuid4(), uuid4()

    with _as_tenant(ctx, owner):
        made = await ctx.document.command(tenant_spec).create(CreateTenantCmd(tenant_key="t"))
        assert [row.id for row in await ctx.document.query(tenant_spec).get_many([made.id])] == [
            made.id
        ]

    with _as_tenant(ctx, stranger), pytest.raises(CoreException) as hidden:
        await ctx.document.query(tenant_spec).get_many([made.id])

    assert hidden.value.code == "core.not_found"


async def test_a_tenant_aware_read_without_a_tenant_is_refused_before_reading(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    ctx = _ctx(firestore_client, unique_collection, tenant_aware=True)

    # Refused before any read (resolving the collection needs the tenant), even for an id
    # that names nothing.
    with pytest.raises(CoreException) as refused:
        await ctx.document.query(tenant_spec).get_many([uuid4()])

    assert refused.value.code == "tenant_required"
