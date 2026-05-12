"""Integration tests for Mongo document adapter: cursor edges, projections, and read-through cache."""

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockCacheAdapter, MockState, MockStateDepKey
from forze_mongo.execution.deps.deps import ConfigurableMongoDocument, mongo_txmanager
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient


class _CxDoc(Document):
    sku: str


class _CxCreate(CreateDocumentCmd):
    sku: str


class _CxUpdate(BaseDTO):
    sku: str | None = None


class _CxRead(ReadDocument):
    sku: str


async def _ctx_cached(
    mongo_client: MongoClient,
    collection: str,
) -> tuple[ExecutionContext, DocumentSpec]:
    db = (await mongo_client.db()).name
    cache_spec = CacheSpec(name=f"cache_{collection}")
    spec = DocumentSpec(
        name=f"doc_{collection}",
        read=_CxRead,
        write={
            "domain": _CxDoc,
            "create_cmd": _CxCreate,
            "update_cmd": _CxUpdate,
        },
        cache=cache_spec,
    )
    fac = ConfigurableMongoDocument(
        config={"read": (db, collection), "write": (db, collection)}
    )
    state = MockState()

    def _cache_factory(ctx: ExecutionContext, cspec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.dep(MockStateDepKey), namespace=cspec.name)

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MockStateDepKey: state,
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
                CacheDepKey: _cache_factory,
            }
        )
    )
    return ctx, spec


async def _ctx_cached_tx(
    mongo_client: MongoClient,
    collection: str,
) -> tuple[ExecutionContext, DocumentSpec]:
    """Like :func:`_ctx_cached` but registers ``TxManagerDepKey`` route ``main``."""

    db = (await mongo_client.db()).name
    cache_spec = CacheSpec(name=f"cache_{collection}")
    spec = DocumentSpec(
        name=f"doc_{collection}",
        read=_CxRead,
        write={
            "domain": _CxDoc,
            "create_cmd": _CxCreate,
            "update_cmd": _CxUpdate,
        },
        cache=cache_spec,
    )
    fac = ConfigurableMongoDocument(
        config={"read": (db, collection), "write": (db, collection)}
    )
    state = MockState()

    def _cache_factory(ctx: ExecutionContext, cspec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.dep(MockStateDepKey), namespace=cspec.name)

    plain = Deps.plain(
        {
            MockStateDepKey: state,
            MongoClientDepKey: mongo_client,
            DocumentQueryDepKey: fac,
            DocumentCommandDepKey: fac,
            CacheDepKey: _cache_factory,
        }
    )
    routed = Deps.routed({TxManagerDepKey: {"main": mongo_txmanager}})
    ctx = ExecutionContext(deps=plain.merge(routed))
    return ctx, spec


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_adapter_cursor_prev_next_and_desc(
    mongo_client: MongoClient,
) -> None:
    """Second page with ``after`` exposes ``prev_cursor``; ``id`` desc ordering."""
    col = f"m_cx_{uuid4().hex[:8]}"
    db = (await mongo_client.db()).name
    spec = DocumentSpec(
        name="cursor_extra_ns",
        read=_CxRead,
        write={
            "domain": _CxDoc,
            "create_cmd": _CxCreate,
            "update_cmd": _CxUpdate,
        },
    )
    fac = ConfigurableMongoDocument(config={"read": (db, col), "write": (db, col)})
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    ids = [
        UUID("20000000-0000-0000-0000-000000000001"),
        UUID("20000000-0000-0000-0000-000000000002"),
        UUID("20000000-0000-0000-0000-000000000003"),
        UUID("20000000-0000-0000-0000-000000000004"),
    ]
    for u in ids:
        await cmd.create(_CxCreate(id=u, sku=str(u)[:8]))

    p1 = await q.find_cursor(None, cursor={"limit": 2}, sorts=None)
    assert p1.prev_cursor is None
    assert p1.next_cursor is not None

    p2 = await q.find_cursor(
        None,
        cursor={"limit": 2, "after": p1.next_cursor},
        sorts=None,
    )
    assert p2.prev_cursor is not None
    assert len(p2.hits) == 2

    p_desc = await q.find_cursor(
        None,
        cursor={"limit": 10},
        sorts={ID_FIELD: "desc"},
    )
    assert p_desc.hits[0].id == ids[-1]

    p_before = await q.find_cursor(
        None,
        cursor={"limit": 2, "before": p1.next_cursor},
        sorts=None,
    )
    assert len(p_before.hits) >= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_adapter_find_and_find_many_projections_with_count(
    mongo_client: MongoClient,
) -> None:
    """``project`` / ``project_page`` for field projections with counts."""
    col = f"m_pr_{uuid4().hex[:8]}"
    db = (await mongo_client.db()).name
    spec = DocumentSpec(
        name="proj_ns",
        read=_CxRead,
        write={
            "domain": _CxDoc,
            "create_cmd": _CxCreate,
            "update_cmd": _CxUpdate,
        },
    )
    fac = ConfigurableMongoDocument(config={"read": (db, col), "write": (db, col)})
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    await cmd.create(_CxCreate(sku="apple"))
    await cmd.create(_CxCreate(sku="banana"))

    one = await q.project(
        {"$fields": {"sku": "apple"}},
        ["sku"],
    )
    assert one is not None
    assert one == {"sku": "apple"}

    page = await q.project_page(
        ["id", "sku"],
        {"$fields": {"sku": {"$in": ["apple", "banana"]}}},
        pagination={"limit": 10, "offset": 0},
        sorts={"sku": "asc"},
    )
    assert page.count == 2
    assert {r["sku"] for r in page.hits} == {"apple", "banana"}
    for row in page.hits:
        assert set(row.keys()) <= {"id", "sku"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_adapter_read_through_cache_get_and_get_many(
    mongo_client: MongoClient,
) -> None:
    """DocumentSpec cache: miss populates versioned cache; hit avoids Mongo; update evicts."""
    col = f"m_cc_{uuid4().hex[:8]}"
    ctx, spec = await _ctx_cached(mongo_client, col)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)
    state = ctx.dep(MockStateDepKey)
    assert spec.cache is not None
    bodies = state.cache_bodies.setdefault(spec.cache.name, {})

    doc = await cmd.create(_CxCreate(sku="cached"))

    first = await q.get(doc.id)
    assert first.sku == "cached"
    assert any(k[0] == str(doc.id) for k in bodies)

    second = await q.get(doc.id)
    assert second.id == doc.id

    many = await q.get_many([doc.id])
    assert len(many) == 1
    assert many[0].sku == "cached"

    other = await cmd.create(_CxCreate(sku="other"))
    mixed = await q.get_many([doc.id, other.id])
    assert len(mixed) == 2
    assert {x.sku for x in mixed} == {"cached", "other"}

    await cmd.update(doc.id, doc.rev, _CxUpdate(sku="patched"))
    third = await q.get(doc.id)
    assert third.sku == "patched"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_adapter_cache_tx_commit_deferred_warm(
    mongo_client_replica: MongoClient,
) -> None:
    """After a successful ``ExecutionContext`` transaction, deferred cache warm matches committed DB state."""

    col = f"m_cc_tx_{uuid4().hex[:8]}"
    ctx, spec = await _ctx_cached_tx(mongo_client_replica, col)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    doc = await cmd.create(_CxCreate(sku="baseline"))
    await q.get(doc.id)

    async with ctx.transaction("main"):
        patched = await cmd.update(doc.id, doc.rev, _CxUpdate(sku="committed"))
        assert patched.sku == "committed"
        in_tx = await q.get(doc.id)
        assert in_tx.sku == "committed"

    out = await q.get(doc.id)
    assert out.sku == "committed"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_adapter_cache_tx_rollback_eager_evict_skips_deferred_warm(
    mongo_client_replica: MongoClient,
) -> None:
    """On rollback, eager cache eviction stands; deferred warm is not run; reads match rolled-back DB."""

    col = f"m_rb_tx_{uuid4().hex[:8]}"
    ctx, spec = await _ctx_cached_tx(mongo_client_replica, col)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)
    state = ctx.dep(MockStateDepKey)
    assert spec.cache is not None

    doc = await cmd.create(_CxCreate(sku="baseline"))
    await q.get(doc.id)
    pointers = state.cache_pointers.setdefault(spec.cache.name, {})

    assert str(doc.id) in pointers

    with pytest.raises(RuntimeError, match="intentional rollback"):
        async with ctx.transaction("main"):
            upd = await cmd.update(doc.id, doc.rev, _CxUpdate(sku="lost"))
            assert upd.sku == "lost"
            in_tx = await q.get(doc.id)
            assert in_tx.sku == "lost"
            raise RuntimeError("intentional rollback")

    assert str(doc.id) not in pointers

    restored = await q.get(doc.id)
    assert restored.sku == "baseline"
    assert restored.rev == doc.rev

    assert str(restored.id) in pointers
