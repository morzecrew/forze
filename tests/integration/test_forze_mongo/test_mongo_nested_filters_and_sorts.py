"""Integration tests for dot-path filters and sorts on nested BSON documents."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.scenarios.document_nested_filters import (
    NestedFilterMeta as Meta,
)
from tests.support.scenarios.document_nested_filters import (
    NestedFilterRowCreate as RowCreate,
)
from tests.support.scenarios.document_nested_filters import (
    NestedFilterRowDoc as RowDoc,
)
from tests.support.scenarios.document_nested_filters import (
    NestedFilterRowRead as RowRead,
)
from tests.support.scenarios.document_nested_filters import (
    NestedFilterRowUpdate as RowUpdate,
)
from tests.support.scenarios.document_nested_filters import (
    expected_scores_ascending,
)


async def _setup(
    mongo_client: MongoClient, collection: str
) -> tuple[ExecutionContext, DocumentSpec]:
    db = (await mongo_client.db()).name
    spec = DocumentSpec(
        name="nested_mongo_ns",
        read=RowRead,
        write={"domain": RowDoc, "create_cmd": RowCreate, "update_cmd": RowUpdate},
    )
    fac = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection))
    )
    ctx = context_from_deps(Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            })
    )
    return ctx, spec


@pytest.mark.asyncio
async def test_sort_by_dotted_nested_field(mongo_client: MongoClient) -> None:
    col = f"mn_sort_{uuid4().hex[:8]}"
    ctx, spec = await _setup(mongo_client, col)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="c", meta=Meta(score=30)))
    await cmd.create(RowCreate(title="a", meta=Meta(score=10)))
    await cmd.create(RowCreate(title="b", meta=Meta(score=20)))

    __p = await query.find_page(None,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "asc"},
    )
    rows = __p.hits
    total = __p.count
    assert total == 3
    assert [r.meta.score for r in rows] == expected_scores_ascending()


@pytest.mark.asyncio
async def test_filter_nested_numeric_operator(mongo_client: MongoClient) -> None:
    col = f"mn_filt_{uuid4().hex[:8]}"
    ctx, spec = await _setup(mongo_client, col)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="in", meta=Meta(score=3, tag="x")))
    await cmd.create(RowCreate(title="out", meta=Meta(score=300, tag="y")))

    __p = await query.find_page({"$values": {"meta.score": {"$lt": 10}}},
        pagination={"limit": 10, "offset": 0},
    )
    rows = __p.hits
    total = __p.count
    assert total == 1
    assert rows[0].title == "in"


@pytest.mark.asyncio
async def test_filter_and_sort_nested_decimal_compares_numerically(
    mongo_client: MongoClient,
) -> None:
    """A Decimal filter value reaches Mongo as Decimal128 — 9.5 < 10.5 (a stringified
    decimal would match nothing against stored Decimal128 values)."""

    from decimal import Decimal

    col = f"mn_dec_{uuid4().hex[:8]}"
    ctx, spec = await _setup(mongo_client, col)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="cheap", meta=Meta(score=1, price=Decimal("9.5"))))
    await cmd.create(RowCreate(title="mid", meta=Meta(score=2, price=Decimal("10.5"))))
    await cmd.create(RowCreate(title="dear", meta=Meta(score=3, price=Decimal("100.25"))))

    __p = await query.find_page(
        {"$values": {"meta.price": {"$lt": Decimal("10.5")}}},
        pagination={"limit": 10, "offset": 0},
    )
    assert __p.count == 1
    assert __p.hits[0].title == "cheap"
    assert __p.hits[0].meta.price == Decimal("9.5")

    __p = await query.find_page(
        None,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.price": "asc"},
    )
    assert [r.title for r in __p.hits] == ["cheap", "mid", "dear"]


@pytest.mark.asyncio
async def test_and_or_combinators_with_nested_paths(
    mongo_client: MongoClient,
) -> None:
    col = f"mn_log_{uuid4().hex[:8]}"
    ctx, spec = await _setup(mongo_client, col)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="alpha", meta=Meta(score=5, tag="t1")))
    await cmd.create(RowCreate(title="beta", meta=Meta(score=5, tag="t2")))
    await cmd.create(RowCreate(title="alpha", meta=Meta(score=99, tag="t3")))

    and_filt = {
        "$and": [
            {"$values": {"title": "alpha"}},
            {"$values": {"meta.score": {"$eq": 5}}},
        ]
    }
    assert await query.count(and_filt) == 1

    await cmd.create(RowCreate(title="gamma", meta=Meta(score=1000, tag="big")))
    or_filt = {
        "$or": [
            {"$values": {"meta.score": {"$gte": 1000}}},
            {"$values": {"meta.tag": {"$eq": "t2"}}},
        ]
    }
    __p = await query.find_page(or_filt,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "asc"},
    )
    rows = __p.hits
    total = __p.count
    assert total == 2
    assert {r.title for r in rows} == {"beta", "gamma"}


@pytest.mark.asyncio
async def test_multi_field_sort_including_nested(mongo_client: MongoClient) -> None:
    """Stable ordering: nested score first, then top-level title."""
    col = f"mn_msort_{uuid4().hex[:8]}"
    ctx, spec = await _setup(mongo_client, col)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="b", meta=Meta(score=10)))
    await cmd.create(RowCreate(title="a", meta=Meta(score=10)))
    await cmd.create(RowCreate(title="z", meta=Meta(score=5)))

    __p = await query.find_page(None,
        pagination={"limit": 10, "offset": 0},
        sorts={"meta.score": "desc", "title": "asc"},
    )
    rows = __p.hits
    total = __p.count
    assert total == 3
    # score 10: title asc → a, b; then score 5: z
    assert [r.title for r in rows] == ["a", "b", "z"]
