"""Integration tests for ``$not`` and element quantifiers (``$any``, ``$all``, ``$none``) on Mongo."""

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
from forze_mongo.execution.deps import MongoDocumentConfig
from forze_mongo.execution.deps.deps import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient
from tests.support.execution_context import context_from_deps


async def _ctx(
    mongo_client: MongoClient,
    collection: str,
) -> ExecutionContext:
    db = (await mongo_client.db()).name
    fac = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection)),
    )
    return context_from_deps(Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            })
    )


@pytest.mark.asyncio
async def test_element_any_all_none_on_scalar_array(mongo_client: MongoClient) -> None:
    col = f"m_elem_any_{uuid4().hex[:8]}"
    ctx = await _ctx(mongo_client, col)

    class _Doc(Document):
        title: str
        tags: list[str]

    class _Create(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _Update(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _Read(ReadDocument):
        title: str
        tags: list[str]

    spec = DocumentSpec(
        name="mongo_elem_any_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="a", tags=["urgent", "ops"]))
    await cmd.create(_Create(title="b", tags=["ops"]))
    await cmd.create(_Create(title="c", tags=[]))

    urgent = {"$values": {"tags": {"$any": "urgent"}}}
    assert await query.count(urgent) == 1
    row = await query.find(urgent)
    assert row is not None and row.title == "a"

    all_ops = {"$values": {"tags": {"$all": {"$eq": "ops"}}}}
    assert await query.count(all_ops) == 2

    none_urgent = {"$values": {"tags": {"$none": "urgent"}}}
    assert await query.count(none_urgent) == 2


@pytest.mark.asyncio
async def test_element_any_scalar_ordering(mongo_client: MongoClient) -> None:
    col = f"m_elem_ord_{uuid4().hex[:8]}"
    ctx = await _ctx(mongo_client, col)

    class _Doc(Document):
        title: str
        scores: list[int]

    class _Create(CreateDocumentCmd):
        title: str
        scores: list[int]

    class _Update(BaseDTO):
        title: str | None = None
        scores: list[int] | None = None

    class _Read(ReadDocument):
        title: str
        scores: list[int]

    spec = DocumentSpec(
        name="mongo_elem_ord_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="high", scores=[5, 15]))
    await cmd.create(_Create(title="low", scores=[1, 2]))

    filt = {"$values": {"scores": {"$any": {"$gte": 10}}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "high"


@pytest.mark.asyncio
async def test_not_combinator(mongo_client: MongoClient) -> None:
    col = f"m_elem_not_{uuid4().hex[:8]}"
    ctx = await _ctx(mongo_client, col)

    class _Doc(Document):
        title: str

    class _Create(CreateDocumentCmd):
        title: str

    class _Update(BaseDTO):
        title: str | None = None

    class _Read(ReadDocument):
        title: str

    spec = DocumentSpec(
        name="mongo_elem_not_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="keep"))
    await cmd.create(_Create(title="drop"))

    filt = {"$not": {"$values": {"title": "drop"}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "keep"


@pytest.mark.asyncio
async def test_not_with_nested_or(mongo_client: MongoClient) -> None:
    col = f"m_elem_not_or_{uuid4().hex[:8]}"
    ctx = await _ctx(mongo_client, col)

    class _Doc(Document):
        status: str

    class _Create(CreateDocumentCmd):
        status: str

    class _Update(BaseDTO):
        status: str | None = None

    class _Read(ReadDocument):
        status: str

    spec = DocumentSpec(
        name="mongo_not_or_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(status="active"))
    await cmd.create(_Create(status="archived"))
    await cmd.create(_Create(status="pending"))

    filt = {
        "$not": {
            "$or": [
                {"$values": {"status": "archived"}},
                {"$values": {"status": "pending"}},
            ],
        },
    }
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.status == "active"


@pytest.mark.asyncio
async def test_element_any_and_none_on_object_array(mongo_client: MongoClient) -> None:
    col = f"m_elem_obj_{uuid4().hex[:8]}"
    ctx = await _ctx(mongo_client, col)

    class _Item(BaseModel):
        status: str
        qty: int

    class _Doc(Document):
        title: str
        items: list[_Item]

    class _Create(CreateDocumentCmd):
        title: str
        items: list[_Item]

    class _Update(BaseDTO):
        title: str | None = None
        items: list[_Item] | None = None

    class _Read(ReadDocument):
        title: str
        items: list[_Item]

    spec = DocumentSpec(
        name="mongo_elem_obj_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(
        _Create(
            title="match",
            items=[_Item(status="open", qty=2), _Item(status="closed", qty=1)],
        ),
    )
    await cmd.create(_Create(title="miss", items=[_Item(status="closed", qty=5)]))

    any_filt = {
        "$values": {
            "items": {
                "$any": {
                    "$values": {
                        "status": "open",
                        "qty": {"$gte": 2},
                    },
                },
            },
        },
    }
    assert await query.count(any_filt) == 1
    row = await query.find(any_filt)
    assert row is not None and row.title == "match"

    none_open = {
        "$values": {
            "items": {"$none": {"$values": {"status": "open"}}},
        },
    }
    assert await query.count(none_open) == 1
    assert (await query.find(none_open)).title == "miss"


@pytest.mark.asyncio
async def test_element_quantifiers_combined_with_and(mongo_client: MongoClient) -> None:
    col = f"m_elem_and_{uuid4().hex[:8]}"
    ctx = await _ctx(mongo_client, col)

    class _Doc(Document):
        title: str
        tags: list[str]

    class _Create(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _Update(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _Read(ReadDocument):
        title: str
        tags: list[str]

    spec = DocumentSpec(
        name="mongo_elem_and_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="yes", tags=["api", "urgent"]))
    await cmd.create(_Create(title="no_tag", tags=["api"]))
    await cmd.create(_Create(title="no_title", tags=["urgent"]))

    filt = {
        "$and": [
            {"$values": {"title": "yes"}},
            {"$values": {"tags": {"$any": "urgent"}}},
        ],
    }
    assert await query.count(filt) == 1
    assert (await query.find(filt)).title == "yes"
