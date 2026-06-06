"""Integration coverage for read gateway typed / projection / cursor variants."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.contracts.querying import encode_keyset_v1
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.execution.deps.utils import doc_write_gw
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class RvDoc(Document):
    name: str
    category: str = "default"
    price: float = 0.0


class RvCreate(CreateDocumentCmd):
    name: str
    category: str = "default"
    price: float = 0.0


class RvUpdate(BaseDTO):
    name: str | None = None


class RvProj(BaseModel):
    id: UUID
    name: str


class RvAgg(BaseModel):
    category: str
    orders: int
    revenue: float


def _rv_write_types() -> DocumentWriteTypes[RvDoc, RvCreate, RvUpdate]:
    return DocumentWriteTypes(domain=RvDoc, create_cmd=RvCreate, update_cmd=RvUpdate)


@pytest.fixture
def rv_ctx(mongo_client: MongoClient) -> ExecutionContext:
    return context_from_deps(Deps.plain({MongoClientDepKey: mongo_client}))


async def _seed(mongo_client: MongoClient, rv_ctx: ExecutionContext, prefix: str):
    db_name = (await mongo_client.db()).name
    relation = (db_name, f"{prefix}_{uuid4().hex[:8]}")
    write = doc_write_gw(
        rv_ctx,
        write_types=_rv_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    return write, write.read_gw


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_many_empty_returns_empty(
    mongo_client: MongoClient,
    rv_ctx: ExecutionContext,
) -> None:
    """``get_many([])`` short-circuits to an empty list."""

    _, read = await _seed(mongo_client, rv_ctx, "rv_gm")
    assert await read.get_many([]) == []


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_find_many_return_model_and_dispatch(
    mongo_client: MongoClient,
    rv_ctx: ExecutionContext,
) -> None:
    """``find_many`` returns typed models and dispatches to aggregates."""

    write, read = await _seed(mongo_client, rv_ctx, "rv_fm")
    await write.create(RvCreate(name="a", category="books", price=10.0))
    await write.create(RvCreate(name="b", category="books", price=20.0))

    # return_model branch (line 391).
    typed = await read.find_many(
        {"$values": {"category": "books"}},
        limit=10,
        return_model=RvProj,
    )
    assert {t.name for t in typed} == {"a", "b"}
    assert all(isinstance(t, RvProj) for t in typed)

    # aggregates dispatch through find_many (line 365).
    agg_rows = await read.find_many(
        {"$values": {"category": "books"}},
        limit=10,
        aggregates={
            "$groups": {"category": "category"},
            "$computed": {
                "orders": {"$count": None},
                "revenue": {"$sum": "price"},
            },
        },
        return_model=RvAgg,
    )
    assert agg_rows == [RvAgg(category="books", orders=2, revenue=30.0)]


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_find_many_aggregates_no_model_and_empty_global(
    mongo_client: MongoClient,
    rv_ctx: ExecutionContext,
) -> None:
    """Aggregates return raw rows, an empty-global synthetic row, and reject fields."""

    write, read = await _seed(mongo_client, rv_ctx, "rv_agg")
    await write.create(RvCreate(name="a", category="books", price=10.0))

    aggregates = {
        "$groups": {"category": "category"},
        "$computed": {"orders": {"$count": None}, "revenue": {"$sum": "price"}},
    }

    # No return_model -> raw JSON rows (line 438).
    raw = await read.find_many_aggregates(
        {"$values": {"category": "books"}},
        limit=10,
        aggregates=aggregates,
    )
    assert raw and raw[0]["orders"] == 1

    # Empty global aggregate: no groups, no rows -> synthetic zero/None row.
    global_aggregates = {
        "$computed": {"orders": {"$count": None}, "revenue": {"$sum": "price"}},
    }
    empty = await read.find_many_aggregates(
        {"$values": {"category": {"$eq": "nonexistent"}}},
        aggregates=global_aggregates,
    )
    assert empty == [{"orders": 0, "revenue": None}]

    # return_fields with aggregates is rejected (line 415).
    with pytest.raises(CoreException, match="return_fields"):
        await read.find_many_aggregates(
            {"$values": {"category": "books"}},
            aggregates=aggregates,
            return_fields=["category"],
        )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_count_aggregates_empty_paths(
    mongo_client: MongoClient,
    rv_ctx: ExecutionContext,
) -> None:
    """``count_aggregates`` returns 1 for empty-global and 0 for empty-grouped."""

    write, read = await _seed(mongo_client, rv_ctx, "rv_cnt")
    await write.create(RvCreate(name="a", category="books", price=10.0))

    global_aggregates = {
        "$computed": {"orders": {"$count": None}},
    }
    # No groups, no rows -> single implicit global group (line 460).
    assert (
        await read.count_aggregates(
            {"$values": {"category": {"$eq": "nonexistent"}}},
            aggregates=global_aggregates,
        )
        == 1
    )

    grouped = {
        "$groups": {"category": "category"},
        "$computed": {"orders": {"$count": None}},
    }
    # Groups but no matching rows -> 0 (line 463).
    assert (
        await read.count_aggregates(
            {"$values": {"category": {"$eq": "nonexistent"}}},
            aggregates=grouped,
        )
        == 0
    )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["asc", "desc"])
@pytest.mark.parametrize("edge", ["after", "before"])
async def test_find_many_with_cursor_seek_directions(
    mongo_client: MongoClient,
    rv_ctx: ExecutionContext,
    direction: str,
    edge: str,
) -> None:
    """Exercise after/before x asc/desc keyset seek branches."""

    write, read = await _seed(mongo_client, rv_ctx, "rv_cur")
    ids = [
        UUID("20000000-0000-0000-0000-000000000001"),
        UUID("20000000-0000-0000-0000-000000000002"),
        UUID("20000000-0000-0000-0000-000000000003"),
    ]
    for u, label in zip(ids, ("a", "b", "c"), strict=True):
        await write.create(RvCreate(id=u, name=label))

    pivot = str(ids[1])
    tok = encode_keyset_v1(
        sort_keys=[ID_FIELD],
        directions=[direction],
        values=[pivot],
    )
    # Pass filters so seek is combined with a base query ($and branch).
    page = await read.find_many_with_cursor(
        {"$values": {"name": {"$in": ["a", "b", "c"]}}},
        cursor={"limit": 5, edge: tok},
        sorts={ID_FIELD: direction},
    )
    names = {d.name for d in page}
    # Pivot itself is excluded by strict gt/lt seek.
    assert "b" not in names
    assert names  # at least one neighbour returned


@pytest.mark.integration
@pytest.mark.asyncio
async def test_find_many_with_cursor_invalid_token(
    mongo_client: MongoClient,
    rv_ctx: ExecutionContext,
) -> None:
    """A cursor whose direction disagrees with the sort is rejected (line 570)."""

    write, read = await _seed(mongo_client, rv_ctx, "rv_cur_bad")
    await write.create(RvCreate(name="a"))

    # Token says desc but query sorts asc -> "does not match current sort order".
    mismatched = encode_keyset_v1(
        sort_keys=[ID_FIELD],
        directions=["desc"],
        values=[str(uuid4())],
    )
    with pytest.raises(CoreException, match="sort"):
        await read.find_many_with_cursor(
            None,
            cursor={"after": mismatched},
            sorts={ID_FIELD: "asc"},
        )
