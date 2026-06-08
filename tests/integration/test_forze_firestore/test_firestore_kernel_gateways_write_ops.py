"""Firestore kernel gateway write/read paths (upsert, touch, aggregates errors)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.utils import doc_write_gw, read_gw
from forze_firestore.kernel.client import FirestoreClient
from tests.support import (
    IntegrationCreateCmd,
    IntegrationDocument,
    IntegrationUpdateCmd,
    make_create_cmd,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_WRITE_TYPES = {
    "domain": IntegrationDocument,
    "create_cmd": IntegrationCreateCmd,
    "update_cmd": IntegrationUpdateCmd,
}


class _NameOnly(BaseModel):
    name: str


def _ctx(client: FirestoreClient) -> ExecutionContext:
    return context_from_deps(Deps.plain({FirestoreClientDepKey: client}))


def _write(ctx: ExecutionContext, collection: str) -> object:
    return doc_write_gw(
        ctx,
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_enabled=False,
        tenant_aware=False,
    )


def _read(ctx: ExecutionContext, collection: str) -> object:
    return read_gw(
        ctx,
        read_type=IntegrationDocument,
        read_relation=("(default)", collection),
        tenant_aware=False,
    )


async def test_write_gateway_upsert_insert_then_update(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """``upsert`` creates when missing, then applies ``update_dto`` when the doc exists."""
    collection = f"gw_upsert_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)
    read = _read(ctx, collection)

    doc_id = UUID("50000000-0000-0000-0000-000000000001")

    created = await write.upsert(
        doc_id,
        make_create_cmd(name="new"),
        IntegrationUpdateCmd(name="should-not-apply-on-insert"),
    )
    assert created.name == "new"
    assert created.rev == 1

    updated = await write.upsert(
        doc_id,
        make_create_cmd(name="ignored"),
        IntegrationUpdateCmd(name="updated"),
    )
    assert updated.name == "updated"
    assert updated.rev == 2

    loaded = await read.get(doc_id)
    assert loaded.name == "updated"


async def test_write_gateway_upsert_many_mixed_batch(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """``upsert_many`` inserts fresh docs and updates existing ones in the same batch."""
    collection = f"gw_upsert_many_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)

    id_a = UUID("51000000-0000-0000-0000-000000000001")
    id_b = UUID("51000000-0000-0000-0000-000000000002")
    await write.create(make_create_cmd(name="existing"), id=id_a)

    results = await write.upsert_many(
        [id_a, id_b],
        [make_create_cmd(name="existing"), make_create_cmd(name="inserted")],
        [
            IntegrationUpdateCmd(name="a-up"),
            IntegrationUpdateCmd(name="ignored-on-insert"),
        ],
    )
    assert len(results) == 2
    by_id = {d.id: d for d in results}
    assert by_id[id_a].name == "a-up"
    assert by_id[id_b].name == "inserted"


async def test_write_gateway_touch_bumps_revision(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_touch_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)

    created = await write.create(make_create_cmd(name="touch-me"))
    touched = await write.touch(created.id)
    assert touched.rev == created.rev + 1
    assert touched.name == created.name

    touched_many = await write.touch_many([created.id])
    assert len(touched_many) == 1
    assert touched_many[0].rev == touched.rev + 1


async def test_write_gateway_touch_many_duplicate_pk_raises(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_touch_dup_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)

    created = await write.create(make_create_cmd(name="dup"))

    with pytest.raises(CoreException, match="unique"):
        await write.touch_many([created.id, created.id])


async def test_read_gateway_get_many_empty(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_empty_many_{unique_collection}"
    ctx = _ctx(firestore_client)
    read = _read(ctx, collection)

    assert await read.get_many([]) == []


async def test_read_gateway_find_with_return_model(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_find_model_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)
    read = _read(ctx, collection)

    await write.create(make_create_cmd(name="model-find"), id=uuid4())

    row = await read.find(
        {"$values": {"name": "model-find"}},
        return_model=_NameOnly,
    )
    assert row is not None
    assert row.name == "model-find"


async def test_read_gateway_aggregates_not_supported(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_aggr_{unique_collection}"
    ctx = _ctx(firestore_client)
    read = _read(ctx, collection)

    with pytest.raises(CoreException, match="aggregates"):
        await read.find_many_aggregates(
            None,
            aggregates={"$count": "id"},
        )

    with pytest.raises(CoreException, match="aggregates"):
        await read.count_aggregates(
            None,
            aggregates={"$count": "id"},
        )


async def test_write_gateway_update_matching_not_supported(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_match_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)

    with pytest.raises(CoreException, match="update_matching"):
        await write.update_matching(
            {"$values": {"name": "x"}},
            IntegrationUpdateCmd(name="y"),
        )
