"""Direct Firestore kernel gateway integration (read/write/history)."""

from __future__ import annotations

import pytest

from forze.application.execution import Deps
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.utils import doc_write_gw, read_gw
from forze_firestore.kernel.client import FirestoreClient
from tests.support import (
    IntegrationCreateCmd,
    IntegrationDocument,
    IntegrationUpdateCmd,
    IsPartialDict,
    IsUUID,
    make_create_cmd,
    make_document,
)
from tests.support.execution_context import (
    context_from_deps,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firestore_read_gateway_find_and_projections(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_rd_{unique_collection}"
    ctx = context_from_deps(Deps.plain({FirestoreClientDepKey: firestore_client}))
    write = doc_write_gw(
        ctx,
        write_types={
            "domain": IntegrationDocument,
            "create_cmd": IntegrationCreateCmd,
            "update_cmd": IntegrationUpdateCmd,
        },
        write_relation=("(default)", collection),
        history_enabled=False,
        tenant_aware=False,
    )
    read = read_gw(
        ctx,
        read_type=IntegrationDocument,
        read_relation=("(default)", collection),
        tenant_aware=False,
    )

    doc = make_document(name="kernel-read")
    created = await write.create(make_create_cmd(name=doc.name), id=doc.id)
    assert created.id == doc.id

    row = await read.find({"$values": {"name": "kernel-read"}}, return_fields=["id", "name"])
    assert row is not None
    assert row == IsPartialDict({"name": "kernel-read", "id": IsUUID})

    many = await read.find_many(
        None,
        limit=10,
        offset=0,
        sorts={"name": "asc"},
        return_fields=["name"],
    )
    assert len(many) >= 1
    assert many[0]["name"] == "kernel-read"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firestore_find_many_chunked_streams_all(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_chunk_{unique_collection}"
    ctx = context_from_deps(Deps.plain({FirestoreClientDepKey: firestore_client}))
    write = doc_write_gw(
        ctx,
        write_types={
            "domain": IntegrationDocument,
            "create_cmd": IntegrationCreateCmd,
            "update_cmd": IntegrationUpdateCmd,
        },
        write_relation=("(default)", collection),
        history_enabled=False,
        tenant_aware=False,
    )
    read = read_gw(
        ctx,
        read_type=IntegrationDocument,
        read_relation=("(default)", collection),
        tenant_aware=False,
    )

    for i in range(5):
        doc = make_document(name=f"chunk-{i:02d}")
        await write.create(make_create_cmd(name=doc.name), id=doc.id)

    # Stream every doc in bounded batches (peak = one batch), ordered by name.
    batches = [
        chunk
        async for chunk in read.find_many_chunked(
            None,
            sorts={"name": "asc"},
            fetch_batch_size=2,
            return_fields=["name"],
        )
    ]

    # 5 docs -> batches of 2, 2, 1 (last short).
    assert [len(chunk) for chunk in batches] == [2, 2, 1]
    names = [row["name"] for chunk in batches for row in chunk]
    assert names == [f"chunk-{i:02d}" for i in range(5)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firestore_write_gateway_update_and_history(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_wr_{unique_collection}"
    history = f"{collection}_history"
    ctx = context_from_deps(Deps.plain({FirestoreClientDepKey: firestore_client}))
    write = doc_write_gw(
        ctx,
        write_types={
            "domain": IntegrationDocument,
            "create_cmd": IntegrationCreateCmd,
            "update_cmd": IntegrationUpdateCmd,
        },
        write_relation=("(default)", collection),
        history_relation=("(default)", history),
        history_enabled=True,
        tenant_aware=False,
    )

    created = await write.create(make_create_cmd(name="v1"))
    updated, _diff = await write.update(
        created.id,
        IntegrationUpdateCmd(name="v2"),
        rev=created.rev,
    )
    assert updated.name == "v2"
    assert updated.rev == created.rev + 1

    hist_gw = write.history_gw
    assert hist_gw is not None
    prior = await hist_gw.read(created.id, rev=1)
    assert prior.name == "v1"
