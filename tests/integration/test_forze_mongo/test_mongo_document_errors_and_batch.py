"""Integration tests for Mongo document errors, empty finds, and multi-row helpers."""

from __future__ import annotations

from forze.base.exceptions import CoreException
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    KeyedUpdate,
)
from forze.application.execution import Deps, ExecutionContext
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion, UpdateCmdWithSoftDeletion
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps import MongoDocumentConfig
from forze_mongo.execution.deps import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps

class _Doc(Document):
    title: str

class _Create(CreateDocumentCmd):
    title: str

class _Update(BaseDTO):
    title: str | None = None

class _Read(ReadDocument):
    title: str

class _SoftDoc(DocWithSoftDeletion):
    title: str

class _SoftUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None

class _SoftRead(ReadDocument):
    title: str
    is_deleted: bool = False

async def _rw_ctx(
    mongo_client: MongoClient,
    collection: str,
    *,
    history_collection: str | None = None,
    history_enabled: bool = False,
) -> tuple[ExecutionContext, DocumentSpec]:
    db = (await mongo_client.db()).name
    cfg = MongoDocumentConfig(
        read=(db, collection),
        write=(db, collection),
        history=(db, history_collection) if history_collection is not None else None,
    )

    spec = DocumentSpec(
        name="mongo_err_ns",
        read=_SoftRead if history_enabled else _Read,
        write={
            "domain": _SoftDoc if history_enabled else _Doc,
            "create_cmd": _Create,
            "update_cmd": _SoftUpdate if history_enabled else _Update,
        },
        history_enabled=history_enabled,
    )
    fac = ConfigurableMongoDocument(config=cfg)
    ctx = context_from_deps(Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            })
    )
    return ctx, spec

@pytest.mark.asyncio
async def test_get_missing_raises_not_found(mongo_client: MongoClient) -> None:
    col = f"m_get_{uuid4().hex[:8]}"
    ctx, spec = await _rw_ctx(mongo_client, col)
    query = ctx.document.query(spec)
    with pytest.raises(CoreException, match="Record not found"):
        await query.get(uuid4())

@pytest.mark.asyncio
async def test_find_missing_returns_none(mongo_client: MongoClient) -> None:
    col = f"m_find_{uuid4().hex[:8]}"
    ctx, spec = await _rw_ctx(mongo_client, col)
    query = ctx.document.query(spec)
    assert await query.find({"$values": {"title": "missing-doc"}}) is None

@pytest.mark.asyncio
async def test_get_many_partial_missing_raises(mongo_client: MongoClient) -> None:
    col = f"m_gm_{uuid4().hex[:8]}"
    ctx, spec = await _rw_ctx(mongo_client, col)
    cmd = ctx.document.command(spec)
    doc = await cmd.create(_Create(title="only"))
    with pytest.raises(CoreException, match="Some records not found"):
        await ctx.document.query(spec).get_many([doc.id, uuid4()])

@pytest.mark.asyncio
async def test_update_stale_rev_raises_conflict(mongo_client: MongoClient) -> None:
    col = f"m_rev_{uuid4().hex[:8]}"
    ctx, spec = await _rw_ctx(mongo_client, col)
    cmd = ctx.document.command(spec)
    doc = await cmd.create(_Create(title="a"))
    await cmd.update(doc.id, doc.rev, _Update(title="b"))
    with pytest.raises(CoreException, match="Revision mismatch"):
        await cmd.update(doc.id, 1, _Update(title="c"))

@pytest.mark.asyncio
async def test_touch_many_bumps_revisions(mongo_client: MongoClient) -> None:
    col = f"m_tm_{uuid4().hex[:8]}"
    ctx, spec = await _rw_ctx(mongo_client, col)
    cmd = ctx.document.command(spec)
    a = await cmd.create(_Create(title="x"))
    b = await cmd.create(_Create(title="y"))
    touched = await cmd.touch_many([a.id, b.id])
    assert len(touched) == 2
    by_id = {t.id: t for t in touched}
    assert by_id[a.id].rev == 2
    assert by_id[b.id].rev == 2

@pytest.mark.asyncio
async def test_update_many_applies_payloads(mongo_client: MongoClient) -> None:
    col = f"m_um_{uuid4().hex[:8]}"
    ctx, spec = await _rw_ctx(mongo_client, col)
    cmd = ctx.document.command(spec)
    a = await cmd.create(_Create(title="a0"))
    b = await cmd.create(_Create(title="b0"))
    rows = await cmd.update_many(
        [
            KeyedUpdate(id=a.id, rev=a.rev, dto=_Update(title="a1")),
            KeyedUpdate(id=b.id, rev=b.rev, dto=_Update(title="b1")),
        ],
    )
    assert rows is not None
    assert {r.title for r in rows} == {"a1", "b1"}

@pytest.mark.asyncio
async def test_soft_deleted_doc_rejects_title_update(mongo_client: MongoClient) -> None:
    col = f"m_soft_{uuid4().hex[:8]}"
    hist = f"{col}_h"
    ctx, spec = await _rw_ctx(
        mongo_client,
        col,
        history_collection=hist,
        history_enabled=True,
    )
    cmd = ctx.document.command(spec)
    doc = await cmd.create(_Create(title="z"))
    deleted = await cmd.update(doc.id, doc.rev, _SoftUpdate(is_deleted=True))
    with pytest.raises(CoreException, match="soft-deleted"):
        await cmd.update(deleted.id, deleted.rev, _SoftUpdate(title="hack"))

@pytest.mark.asyncio
async def test_count_and_find_many_empty_collection(mongo_client: MongoClient) -> None:
    col = f"m_empty_{uuid4().hex[:8]}"
    ctx, spec = await _rw_ctx(mongo_client, col)
    query = ctx.document.query(spec)
    assert await query.count() == 0
    __p = await query.find_page(
        None,
        pagination={"limit": 5, "offset": 0},
    )
    rows = __p.hits
    total = __p.count
    assert rows == [] and total == 0

@pytest.mark.asyncio
async def test_count_with_filter(mongo_client: MongoClient) -> None:
    col = f"m_cnt_{uuid4().hex[:8]}"
    ctx, spec = await _rw_ctx(mongo_client, col)
    cmd = ctx.document.command(spec)
    await cmd.create(_Create(title="apple"))
    await cmd.create(_Create(title="apricot"))
    await cmd.create(_Create(title="banana"))
    query = ctx.document.query(spec)
    n = await query.count({"$values": {"title": {"$in": ["apple", "apricot"]}}})
    assert n == 2
