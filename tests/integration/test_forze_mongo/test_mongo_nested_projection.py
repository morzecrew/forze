"""Integration tests for dot-path field projection on nested BSON documents.

A dotted projection path fetches its root field and reshapes the requested leaves out of
it, so ``meta.score`` returns the nested ``{"meta": {"score": ...}}`` shape — identical to
the mock oracle and Postgres.
"""

from __future__ import annotations

from decimal import Decimal
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
    NestedArrayItem as Item,
)
from tests.support.scenarios.document_nested_filters import (
    NestedArrayRowCreate as ArrCreate,
)
from tests.support.scenarios.document_nested_filters import (
    NestedArrayRowDoc as ArrDoc,
)
from tests.support.scenarios.document_nested_filters import (
    NestedArrayRowRead as ArrRead,
)
from tests.support.scenarios.document_nested_filters import (
    NestedArrayRowUpdate as ArrUpdate,
)
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


async def _setup(
    mongo_client: MongoClient, collection: str
) -> tuple[ExecutionContext, DocumentSpec]:
    db = (await mongo_client.db()).name
    spec = DocumentSpec(
        name="nested_mongo_proj_ns",
        read=RowRead,
        write={"domain": RowDoc, "create_cmd": RowCreate, "update_cmd": RowUpdate},
    )
    fac = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection))
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )
    return ctx, spec


@pytest.mark.asyncio
async def test_project_nested_leaf_reshapes(mongo_client: MongoClient) -> None:
    col = f"mn_proj_{uuid4().hex[:8]}"
    ctx, spec = await _setup(mongo_client, col)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="a", meta=Meta(score=10, tag="x")))

    out = await query.project({"$values": {"title": "a"}}, ["meta.score"])

    assert out == {"meta": {"score": 10}}


@pytest.mark.asyncio
async def test_project_sibling_leaves_merge_and_mix_with_top(
    mongo_client: MongoClient,
) -> None:
    col = f"mn_proj_{uuid4().hex[:8]}"
    ctx, spec = await _setup(mongo_client, col)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="a", meta=Meta(score=10, tag="x")))

    out = await query.project(
        {"$values": {"title": "a"}}, ["title", "meta.score", "meta.tag"]
    )

    assert out == {"title": "a", "meta": {"score": 10, "tag": "x"}}


@pytest.mark.asyncio
async def test_project_root_subsumes_leaf(mongo_client: MongoClient) -> None:
    col = f"mn_proj_{uuid4().hex[:8]}"
    ctx, spec = await _setup(mongo_client, col)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(RowCreate(title="a", meta=Meta(score=10, tag="x")))

    out = await query.project({"$values": {"title": "a"}}, ["meta", "meta.score"])

    # Root projection returns the whole stored subdocument, including the Decimal
    # default (stored as Decimal128, decoded back to Decimal on read).
    assert out == {"meta": {"score": 10, "tag": "x", "price": Decimal("0")}}


@pytest.mark.asyncio
async def test_project_array_leaf_maps_over_bson_list(mongo_client: MongoClient) -> None:
    col = f"mn_arr_{uuid4().hex[:8]}"
    db = (await mongo_client.db()).name
    fac = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, col), write=(db, col))
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )
    spec = DocumentSpec(
        name="nested_mongo_arr_ns",
        read=ArrRead,
        write={"domain": ArrDoc, "create_cmd": ArrCreate, "update_cmd": ArrUpdate},
    )
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(
        ArrCreate(ref="o1", items=[Item(sku="A", qty=2), Item(sku="B", qty=1)])
    )

    out = await query.project({"$values": {"ref": "o1"}}, ["items.sku", "items.qty"])

    assert out == {"items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}]}
