"""Integration tests for :class:`~forze_mongo.kernel.gateways.read.MongoReadGateway` and write gateway against MongoDB."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.contracts.query import encode_keyset_v1
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import (
    CoreError,
    InfrastructureError,
    NotFoundError,
    ValidationError,
)
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_mongo.adapters import MongoTxManagerAdapter
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.execution.deps.utils import doc_write_gw
from forze_mongo.kernel.platform import MongoClient


class GwDoc(Document):
    name: str


class GwOrderDoc(Document):
    category: str
    price: float


class GwCreate(CreateDocumentCmd):
    name: str


class GwOrderCreate(CreateDocumentCmd):
    category: str
    price: float


class GwUpdate(BaseDTO):
    name: str | None = None


class GwOrderUpdate(BaseDTO):
    category: str | None = None
    price: float | None = None


class GwProj(BaseModel):
    id: UUID
    name: str


class GwCategoryStats(BaseModel):
    category: str
    orders: int
    revenue: float
    median_price: float
    premium_orders: int
    premium_revenue: float | None


def _gw_write_types() -> DocumentWriteTypes[GwDoc, GwCreate, GwUpdate]:
    return DocumentWriteTypes(
        domain=GwDoc,
        create_cmd=GwCreate,
        update_cmd=GwUpdate,
    )


def _gw_order_write_types() -> DocumentWriteTypes[
    GwOrderDoc,
    GwOrderCreate,
    GwOrderUpdate,
]:
    return DocumentWriteTypes(
        domain=GwOrderDoc,
        create_cmd=GwOrderCreate,
        update_cmd=GwOrderUpdate,
    )


@pytest.fixture
def mongo_gw_ctx(mongo_client: MongoClient) -> ExecutionContext:
    deps = Deps.plain({MongoClientDepKey: mongo_client})
    return ExecutionContext(deps=deps)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_gateways_create_read_projections_and_list(
    mongo_client: MongoClient,
    mongo_gw_ctx: ExecutionContext,
) -> None:
    """Exercise read/write gateways: create, projections, find, bounded list, count."""
    db_name = (await mongo_client.db()).name
    collection = f"mongo_gw_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    ctx = mongo_gw_ctx

    write = doc_write_gw(
        ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    created = await write.create(GwCreate(name="gateway-one"))
    other = await write.create(GwCreate(name="gateway-two"))

    full = await read.get(created.id)
    assert full.name == "gateway-one"

    proj = await read.get(created.id, return_fields=["name"])
    assert proj["name"] == "gateway-one"

    many_proj = await read.get_many(
        [created.id, other.id],
        return_fields=["id", "name"],
    )
    assert {row["name"] for row in many_proj} == {"gateway-one", "gateway-two"}

    one = await read.find(
        {"$fields": {"name": {"$eq": "gateway-one"}}},
        return_fields=["name"],
    )
    assert one is not None
    assert one["name"] == "gateway-one"

    listed = await read.find_many(limit=10)
    assert len(listed) >= 2

    total = await read.count(None)
    assert total >= 2

    updated, _ = await write.update(created.id, GwUpdate(name="patched"))
    assert updated.name == "patched"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_read_gateway_aggregate_expressions(
    mongo_client: MongoClient,
    mongo_gw_ctx: ExecutionContext,
) -> None:
    """Aggregate expressions group, compute values, sort by aliases, and count groups."""
    db_name = (await mongo_client.db()).name
    collection = f"mongo_gw_agg_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    write = doc_write_gw(
        mongo_gw_ctx,
        write_types=_gw_order_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    await write.create(GwOrderCreate(category="books", price=10.0))
    await write.create(GwOrderCreate(category="books", price=20.0))
    await write.create(GwOrderCreate(category="books", price=30.0))
    await write.create(GwOrderCreate(category="hardware", price=50.0))
    await write.create(GwOrderCreate(category="hardware", price=60.0))
    await write.create(GwOrderCreate(category="hardware", price=70.0))
    await write.create(GwOrderCreate(category="software", price=90.0))

    aggregates = {
        "$fields": {"category": "category"},
        "$computed": {
            "orders": {"$count": None},
            "revenue": {"$sum": "price"},
            "median_price": {"$median": "price"},
            "premium_orders": {
                "$count": {"filter": {"$fields": {"price": {"$gte": 20}}}},
            },
            "premium_revenue": {
                "$sum": {
                    "field": "price",
                    "filter": {"$fields": {"price": {"$gte": 20}}},
                },
            },
        },
    }

    rows = await read.find_many_aggregates(
        filters={"$fields": {"category": {"$in": ["books", "hardware"]}}},
        limit=10,
        offset=0,
        sorts={"revenue": "desc"},
        aggregates=aggregates,
        return_model=GwCategoryStats,
    )

    assert rows == [
        GwCategoryStats(
            category="hardware",
            orders=3,
            revenue=180.0,
            median_price=60.0,
            premium_orders=3,
            premium_revenue=180.0,
        ),
        GwCategoryStats(
            category="books",
            orders=3,
            revenue=60.0,
            median_price=20.0,
            premium_orders=2,
            premium_revenue=50.0,
        ),
    ]
    assert await read.count_aggregates(
        {"$fields": {"category": {"$in": ["books", "hardware"]}}},
        aggregates=aggregates,
    ) == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_write_gateway_upsert_insert_then_update_path(
    mongo_client: MongoClient,
    mongo_gw_ctx: ExecutionContext,
) -> None:
    """``upsert`` uses ``$setOnInsert`` on first write and :meth:`update` when the doc exists."""
    db_name = (await mongo_client.db()).name
    collection = f"mongo_gw_up_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    write = doc_write_gw(
        mongo_gw_ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )

    pk = UUID(int=0xABCDEF0123456789ABCDEF0123456789)
    first = await write.upsert(
        GwCreate(id=pk, name="inserted"),
        GwUpdate(name="ignored-on-first-write"),
    )
    assert first.id == pk
    assert first.name == "inserted"
    assert first.rev == 1

    second = await write.upsert(
        GwCreate(id=pk, name="ignored-on-second"),
        GwUpdate(name="merged-on-existing"),
    )
    assert second.id == pk
    assert second.name == "merged-on-existing"
    assert second.rev == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_write_gateway_upsert_many_mixed_batch(
    mongo_client: MongoClient,
    mongo_gw_ctx: ExecutionContext,
) -> None:
    """``upsert_many`` bulk-inserts new docs and applies updates to keys that already exist."""
    db_name = (await mongo_client.db()).name
    collection = f"mongo_gw_um_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    write = doc_write_gw(
        mongo_gw_ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )

    seed = await write.create(GwCreate(name="seed"))
    out = await write.upsert_many(
        [
            (GwCreate(name="new-a"), GwUpdate(name="n/a")),
            (GwCreate(id=seed.id, name="skip"), GwUpdate(name="patched-seed")),
        ],
        batch_size=10,
    )
    assert len(out) == 2
    by_id = {d.id: d for d in out}
    assert by_id[seed.id].name == "patched-seed"
    assert by_id[seed.id].rev >= 2
    other = next(i for i in by_id if i != seed.id)
    assert by_id[other].name == "new-a"
    assert by_id[other].rev == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_read_gateway_for_update_requires_transaction(
    mongo_client_replica: MongoClient,
) -> None:
    """``for_update=True`` uses :meth:`MongoClient.require_transaction` (replica set)."""
    db_name = (await mongo_client_replica.db()).name
    collection = f"mongo_gw_tx_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    ctx = ExecutionContext(deps=Deps.plain({MongoClientDepKey: mongo_client_replica}))

    write = doc_write_gw(
        ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    created = await write.create(GwCreate(name="tx-doc"))

    with pytest.raises(InfrastructureError, match="Transactional context is required"):
        await read.get(created.id, for_update=True)

    tx = MongoTxManagerAdapter(client=mongo_client_replica)
    async with tx.transaction():
        locked = await read.get(created.id, for_update=True)
        assert locked.name == "tx-doc"

        found = await read.find(
            {"$fields": {"name": {"$eq": "tx-doc"}}},
            for_update=True,
        )
        assert found is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_read_gateway_return_model_and_find_many_validation(
    mongo_client: MongoClient,
    mongo_gw_ctx: ExecutionContext,
) -> None:
    """``return_model`` / ``return_fields`` on get, get_many, find, find_many; unbounded list guard."""
    db_name = (await mongo_client.db()).name
    collection = f"mongo_gw_rm_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    write = doc_write_gw(
        mongo_gw_ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    a = await write.create(GwCreate(name="ma"))
    b = await write.create(GwCreate(name="mb"))

    typed = await read.get(a.id, return_model=GwProj)
    assert isinstance(typed, GwProj)
    assert typed.name == "ma"

    gm = await read.get_many([b.id, a.id], return_model=GwProj)
    assert [x.name for x in gm] == ["mb", "ma"]

    fo = await read.find(
        {"$fields": {"name": "mb"}},
        return_model=GwProj,
    )
    assert fo is not None and fo.name == "mb"

    rows = await read.find_many(
        {"$fields": {"name": {"$in": ["ma", "mb"]}}},
        limit=10,
        offset=0,
        sorts={"name": "asc"},
        return_fields=["name"],
    )
    assert [r["name"] for r in rows] == ["ma", "mb"]

    with pytest.raises(ValidationError, match="Filters or limit"):
        await read.find_many(filters=None, limit=None)

    missing = uuid4()
    with pytest.raises(NotFoundError, match="Some records not found"):
        await read.get_many([a.id, missing])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_read_gateway_find_many_with_cursor(
    mongo_client: MongoClient,
    mongo_gw_ctx: ExecutionContext,
) -> None:
    """Keyset on ``_id`` only: ``after`` / ``before``, asc/desc, validation errors."""
    db_name = (await mongo_client.db()).name
    collection = f"mongo_gw_cur_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    write = doc_write_gw(
        mongo_gw_ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    ids = [
        UUID("10000000-0000-0000-0000-000000000001"),
        UUID("10000000-0000-0000-0000-000000000002"),
        UUID("10000000-0000-0000-0000-000000000003"),
    ]
    for u, label in zip(ids, ("c", "b", "a"), strict=True):
        await write.create(GwCreate(id=u, name=label))

    first = await read.find_many_with_cursor(
        None,
        cursor={"limit": 2},
        sorts={ID_FIELD: "asc"},
        return_fields=[ID_FIELD, "name"],
    )
    assert len(first) == 3
    mid = first[1]
    tok = encode_keyset_v1(
        sort_keys=[ID_FIELD],
        directions=["asc"],
        values=[str(mid[ID_FIELD])],
    )
    second = await read.find_many_with_cursor(
        None,
        cursor={"limit": 2, "after": tok},
        sorts={ID_FIELD: "asc"},
    )
    assert len(second) >= 1

    desc_page = await read.find_many_with_cursor(
        None,
        cursor={"limit": 2},
        sorts={ID_FIELD: "desc"},
        return_model=GwProj,
    )
    assert len(desc_page) == 3
    assert desc_page[0].name == "a"

    with pytest.raises(CoreError, match="at most one"):
        await read.find_many_with_cursor(
            None,
            cursor={"after": tok, "before": tok},
        )

    with pytest.raises(CoreError, match="positive"):
        await read.find_many_with_cursor(None, cursor={"limit": 0})

    with pytest.raises(CoreError, match="primary key"):
        await read.find_many_with_cursor(
            None,
            sorts={"name": "asc"},
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_write_gateway_create_ensure_and_batch_validation(
    mongo_client: MongoClient,
    mongo_gw_ctx: ExecutionContext,
) -> None:
    """``create_many``, ``ensure``, ``ensure_many``; ``update_many`` argument validation."""
    db_name = (await mongo_client.db()).name
    collection = f"mongo_gw_w_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    write = doc_write_gw(
        mongo_gw_ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )

    out = await write.create_many(
        [GwCreate(name="c1"), GwCreate(name="c2")],
        batch_size=10,
    )
    assert len(out) == 2
    assert {d.name for d in out} == {"c1", "c2"}

    eid = uuid4()
    e1 = await write.ensure(GwCreate(id=eid, name="ensure-new"))
    assert e1.id == eid
    e2 = await write.ensure(GwCreate(id=eid, name="ignored"))
    assert e2.name == "ensure-new"

    seed = await write.create(GwCreate(name="seed"))
    em = await write.ensure_many(
        [
            GwCreate(name="only-insert"),
            GwCreate(id=seed.id, name="skip"),
        ],
        batch_size=10,
    )
    assert len(em) == 2
    by_id = {d.id: d for d in em}
    assert by_id[seed.id].name == "seed"

    with pytest.raises(ValidationError, match="unique"):
        await write.update_many([out[0].id, out[0].id], [GwUpdate(), GwUpdate()])

    with pytest.raises(CoreError, match="Length mismatch"):
        await write.update_many([out[0].id], [GwUpdate(name="x"), GwUpdate(name="y")])
