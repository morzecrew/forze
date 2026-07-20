"""Integration tests for Mongo document batch reads, projections, and bulk soft-delete."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    KeyedUpdate,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.constants import ID_FIELD
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion, UpdateCmdWithSoftDeletion
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps


class BatchDoc(DocWithSoftDeletion):
    name: str
    tag: str = ""


class BatchCreate(CreateDocumentCmd):
    name: str
    tag: str = ""


class BatchUpdate(UpdateCmdWithSoftDeletion):
    name: str | None = None
    tag: str | None = None


class BatchRead(ReadDocument):
    name: str
    tag: str = ""
    is_deleted: bool = False


async def _setup(
    mongo_client: MongoClient,
    *,
    collection: str,
    history_collection: str,
) -> tuple[ExecutionContext, DocumentSpec]:
    db_name = (await mongo_client.db()).name
    spec = DocumentSpec(
        name="batch_ns",
        read=BatchRead,
        write={
            "domain": BatchDoc,
            "create_cmd": BatchCreate,
            "update_cmd": BatchUpdate,
        },
        history_enabled=True,
    )
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db_name, collection),
            write=(db_name, collection),
            history=(db_name, history_collection),
        )
    )
    ctx = context_from_deps(Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            })
    )
    return ctx, spec


@pytest.mark.asyncio
async def test_get_many_and_field_projection(mongo_client: MongoClient) -> None:
    col = f"batch_proj_{uuid4().hex[:8]}"
    hist = f"{col}_history"
    ctx, spec = await _setup(mongo_client, collection=col, history_collection=hist)
    cmd = ctx.document.command(spec)

    a = await cmd.create(BatchCreate(name="one", tag="x"))
    b = await cmd.create(BatchCreate(name="two", tag="y"))

    many = await cmd.get_many([a.id, b.id])
    assert {x.id for x in many} == {a.id, b.id}

    proj = await cmd.project(
        {"$values": {ID_FIELD: a.id}},
        ("name", "tag"),
    )
    assert proj == {"name": "one", "tag": "x"}


@pytest.mark.asyncio
async def test_create_many_delete_many_restore_many(mongo_client: MongoClient) -> None:
    col = f"batch_soft_{uuid4().hex[:8]}"
    hist = f"{col}_history"
    ctx, spec = await _setup(mongo_client, collection=col, history_collection=hist)
    cmd = ctx.document.command(spec)

    created = await cmd.create_many(
        [
            BatchCreate(name="n1", tag="t"),
            BatchCreate(name="n2", tag="t"),
        ]
    )
    assert len(created) == 2

    deleted = await cmd.update_many(
        [
            KeyedUpdate(id=c.id, rev=c.rev, dto=BatchUpdate(is_deleted=True))
            for c in created
        ],
        return_new=True,
    )
    assert deleted is not None
    assert len(deleted) == 2
    assert all(d.is_deleted for d in deleted)

    restored = await cmd.update_many(
        [
            KeyedUpdate(id=d.id, rev=d.rev, dto=BatchUpdate(is_deleted=False))
            for d in deleted
        ],
        return_new=True,
    )
    assert restored is not None
    assert len(restored) == 2
    assert not any(r.is_deleted for r in restored)


@pytest.mark.asyncio
async def test_kill_many_removes_documents(mongo_client: MongoClient) -> None:
    col = f"batch_kill_{uuid4().hex[:8]}"
    hist = f"{col}_history"
    ctx, spec = await _setup(mongo_client, collection=col, history_collection=hist)
    cmd = ctx.document.command(spec)

    a = await cmd.create(BatchCreate(name="a"))
    b = await cmd.create(BatchCreate(name="b"))
    await cmd.kill_many([a.id, b.id])
    assert await cmd.count() == 0


@pytest.mark.asyncio
async def test_get_many_empty_returns_empty(mongo_client: MongoClient) -> None:
    col = f"batch_empty_{uuid4().hex[:8]}"
    hist = f"{col}_history"
    ctx, spec = await _setup(mongo_client, collection=col, history_collection=hist)
    cmd = ctx.document.command(spec)
    assert await cmd.get_many([]) == []
