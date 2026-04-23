"""Integration tests for dot-path filters and sorts on nested BSON documents."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps.deps import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient


class Meta(BaseModel):
    score: int
    tag: str = ""


class RowDoc(Document):
    title: str
    meta: Meta


class RowCreate(CreateDocumentCmd):
    title: str
    meta: Meta


class RowUpdate(BaseDTO):
    title: str | None = None
    meta: Meta | None = None


class RowRead(ReadDocument):
    title: str
    meta: Meta


def _setup(mongo_client: MongoClient, collection: str) -> tuple[ExecutionContext, DocumentSpec]:
    db = mongo_client.db().name
    spec = DocumentSpec(
        name="nested_mongo_ns",
        read=RowRead,
        write={"domain": RowDoc, "create_cmd": RowCreate, "update_cmd": RowUpdate},
    )
    fac = ConfigurableMongoDocument(
        config={"read": (db, collection), "write": (db, collection)}
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )
    return ctx, spec


@pytest.mark.asyncio
async def test_sort_by_dotted_nested_field(mongo_client: MongoClient) -> None:
    col = f"mn_sort_{uuid4().hex[:8]}"
    ctx, spec = _setup(mongo_client, col)
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="c", meta=Meta(score=30)))
    await cmd.create(RowCreate(title="a", meta=Meta(score=10)))
    await cmd.create(RowCreate(title="b", meta=Meta(score=20)))

    __p = await query.find_many(
        None,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "asc"},
        return_count=True,
    )
    rows = __p.hits
    total = __p.count
    assert total == 3
    assert [r.meta.score for r in rows] == [10, 20, 30]


@pytest.mark.asyncio
async def test_filter_nested_numeric_operator(mongo_client: MongoClient) -> None:
    col = f"mn_filt_{uuid4().hex[:8]}"
    ctx, spec = _setup(mongo_client, col)
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="in", meta=Meta(score=3, tag="x")))
    await cmd.create(RowCreate(title="out", meta=Meta(score=300, tag="y")))

    __p = await query.find_many(
        {"$fields": {"meta.score": {"$lt": 10}}},
        pagination={"limit": 10, "offset": 0},
        return_count=True,
    )
    rows = __p.hits
    total = __p.count
    assert total == 1
    assert rows[0].title == "in"


@pytest.mark.asyncio
async def test_and_or_combinators_with_nested_paths(
    mongo_client: MongoClient,
) -> None:
    col = f"mn_log_{uuid4().hex[:8]}"
    ctx, spec = _setup(mongo_client, col)
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="alpha", meta=Meta(score=5, tag="t1")))
    await cmd.create(RowCreate(title="beta", meta=Meta(score=5, tag="t2")))
    await cmd.create(RowCreate(title="alpha", meta=Meta(score=99, tag="t3")))

    and_filt = {
        "$and": [
            {"$fields": {"title": "alpha"}},
            {"$fields": {"meta.score": {"$eq": 5}}},
        ]
    }
    assert await query.count(and_filt) == 1

    await cmd.create(RowCreate(title="gamma", meta=Meta(score=1000, tag="big")))
    or_filt = {
        "$or": [
            {"$fields": {"meta.score": {"$gte": 1000}}},
            {"$fields": {"meta.tag": {"$eq": "t2"}}},
        ]
    }
    __p = await query.find_many(
        or_filt,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "asc"},
        return_count=True,
    )
    rows = __p.hits
    total = __p.count
    assert total == 2
    assert {r.title for r in rows} == {"beta", "gamma"}


@pytest.mark.asyncio
async def test_multi_field_sort_including_nested(mongo_client: MongoClient) -> None:
    """Stable ordering: nested score first, then top-level title."""
    col = f"mn_msort_{uuid4().hex[:8]}"
    ctx, spec = _setup(mongo_client, col)
    cmd = ctx.doc_command(spec)
    query = ctx.doc_query(spec)

    await cmd.create(RowCreate(title="b", meta=Meta(score=10)))
    await cmd.create(RowCreate(title="a", meta=Meta(score=10)))
    await cmd.create(RowCreate(title="z", meta=Meta(score=5)))

    __p = await query.find_many(
        None,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "desc", "title": "asc"},
        return_count=True,
    )
    rows = __p.hits
    total = __p.count
    assert total == 3
    # score 10: title asc → a, b; then score 5: z
    assert [r.title for r in rows] == ["a", "b", "z"]
