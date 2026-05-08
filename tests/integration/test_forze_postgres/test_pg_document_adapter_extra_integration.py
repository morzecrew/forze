"""Integration tests for Postgres document adapter: cursor edges, projections, cache, and mutation branches."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import Page
from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze.domain.constants import ID_FIELD
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockCacheAdapter, MockState, MockStateDepKey
from forze_postgres.execution.deps.deps import (
    ConfigurablePostgresDocument,
    postgres_txmanager,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class _CxDoc(Document):
    sku: str


class _CxCreate(CreateDocumentCmd):
    sku: str


class _CxUpdate(BaseDTO):
    sku: str | None = None


class _CxRead(ReadDocument):
    sku: str


class _SoftDoc(Document, SoftDeletionMixin):
    label: str


class _SoftCreate(CreateDocumentCmd):
    label: str


class _SoftUpdate(BaseDTO):
    label: str | None = None


class _SoftRead(ReadDocument):
    label: str
    is_deleted: bool = False


def _ctx_cached(
    pg_client: PostgresClient,
    table: str,
) -> tuple[ExecutionContext, DocumentSpec]:
    cache_spec = CacheSpec(name=f"cache_{table}")
    spec = DocumentSpec(
        name=f"doc_{table}",
        read=_CxRead,
        write={
            "domain": _CxDoc,
            "create_cmd": _CxCreate,
            "update_cmd": _CxUpdate,
        },
        cache=cache_spec,
    )
    fac = ConfigurablePostgresDocument(
        config={
            "read": ("public", table),
            "write": ("public", table),
            "bookkeeping_strategy": "application",
        }
    )
    state = MockState()

    def _cache_factory(ctx: ExecutionContext, cspec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.dep(MockStateDepKey), namespace=cspec.name)

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MockStateDepKey: state,
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
                CacheDepKey: _cache_factory,
            }
        )
    )
    return ctx, spec


def _ctx_cached_tx(
    pg_client: PostgresClient,
    table: str,
) -> tuple[ExecutionContext, DocumentSpec]:
    """Like :func:`_ctx_cached` but registers ``TxManagerDepKey`` route ``main`` for :meth:`ExecutionContext.transaction`."""

    cache_spec = CacheSpec(name=f"cache_{table}")
    spec = DocumentSpec(
        name=f"doc_{table}",
        read=_CxRead,
        write={
            "domain": _CxDoc,
            "create_cmd": _CxCreate,
            "update_cmd": _CxUpdate,
        },
        cache=cache_spec,
    )
    fac = ConfigurablePostgresDocument(
        config={
            "read": ("public", table),
            "write": ("public", table),
            "bookkeeping_strategy": "application",
        }
    )
    state = MockState()

    def _cache_factory(ctx: ExecutionContext, cspec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.dep(MockStateDepKey), namespace=cspec.name)

    plain = Deps.plain(
        {
            MockStateDepKey: state,
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            DocumentQueryDepKey: fac,
            DocumentCommandDepKey: fac,
            CacheDepKey: _cache_factory,
        }
    )
    routed = Deps.routed({TxManagerDepKey: {"main": postgres_txmanager}})
    ctx = ExecutionContext(deps=plain.merge(routed))
    return ctx, spec


def _ddl_main(t: str) -> str:
    return f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            sku text NOT NULL
        );
        """


def _ddl_soft(t: str) -> str:
    return f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            label text NOT NULL,
            is_deleted boolean NOT NULL DEFAULT false
        );
        """


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_cursor_prev_next_desc_before_and_projection(
    pg_client: PostgresClient,
) -> None:
    """Keyset pages: ``after`` / ``before``, desc order, and ``return_fields`` cursor rows."""
    t = f"pg_cx_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))

    ctx, spec = _ctx_cached(pg_client, t)
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

    p1 = await q.find_many_with_cursor(None, cursor={"limit": 2}, sorts=None)
    assert p1.prev_cursor is None
    assert p1.next_cursor is not None

    p2 = await q.find_many_with_cursor(
        None,
        cursor={"limit": 2, "after": p1.next_cursor},
        sorts=None,
    )
    assert p2.prev_cursor is not None
    assert len(p2.hits) == 2

    p_desc = await q.find_many_with_cursor(
        None,
        cursor={"limit": 10},
        sorts={ID_FIELD: "desc"},
    )
    assert p_desc.hits[0].id == ids[-1]

    p_before = await q.find_many_with_cursor(
        None,
        cursor={"limit": 2, "before": p1.next_cursor},
        sorts=None,
    )
    assert len(p_before.hits) >= 1

    p_proj = await q.find_many_with_cursor(
        None,
        cursor={"limit": 2},
        sorts={"sku": "asc"},
        return_fields=["id", "sku"],
    )
    assert len(p_proj.hits) == 2
    for row in p_proj.hits:
        assert set(row.keys()) == {"id", "sku"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_find_many_count_zero_and_countless_page(
    pg_client: PostgresClient,
) -> None:
    """``return_count`` short-circuit when count is 0; page without total when ``return_count=False``."""
    t = f"pg_fc_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))

    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    await cmd.create(_CxCreate(sku="only"))

    empty = await q.find_many(
        {"$fields": {"sku": "nope"}},
        pagination={"limit": 10, "offset": 0},
        return_count=True,
    )
    assert isinstance(empty, Page)
    assert empty.count == 0
    assert empty.hits == []

    countless = await q.find_many(
        {"$fields": {"sku": "only"}},
        pagination={"limit": 10, "offset": 0},
        return_count=False,
    )
    assert not isinstance(countless, Page)
    assert len(countless.hits) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_find_and_find_many_projections_with_count(
    pg_client: PostgresClient,
) -> None:
    """``find`` / ``find_many`` with ``return_fields`` and ``return_count``."""
    t = f"pg_pr_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))

    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    await cmd.create(_CxCreate(sku="apple"))
    await cmd.create(_CxCreate(sku="banana"))

    one = await q.find(
        {"$fields": {"sku": "apple"}},
        return_fields=["sku"],
    )
    assert one is not None
    assert one == {"sku": "apple"}

    page = await q.find_many(
        {"$fields": {"sku": {"$in": ["apple", "banana"]}}},
        pagination={"limit": 10, "offset": 0},
        sorts={"sku": "asc"},
        return_fields=["id", "sku"],
        return_count=True,
    )
    assert page.count == 2
    assert {r["sku"] for r in page.hits} == {"apple", "banana"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_find_many_with_cursor_requires_sort_fields_in_projection(
    pg_client: PostgresClient,
) -> None:
    t = f"pg_fe_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))

    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    await cmd.create(_CxCreate(sku="x"))

    with pytest.raises(CoreError, match="projection must include"):
        await q.find_many_with_cursor(
            None,
            cursor={"limit": 5},
            sorts={"sku": "asc"},
            return_fields=["sku"],
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_read_through_cache_get_and_get_many(
    pg_client: PostgresClient,
) -> None:
    """DocumentSpec cache: miss populates cache; hit avoids extra DB reads; update evicts via clear."""
    t = f"pg_cc_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))

    ctx, spec = _ctx_cached(pg_client, t)
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
async def test_pg_adapter_mutation_branches_and_empty_batches(
    pg_client: PostgresClient,
) -> None:
    """``return_new=False``, empty batch shortcuts, diffs, and cache clears."""
    t = f"pg_mb_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))

    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)

    assert await cmd.create_many([], return_new=False) is None
    assert await cmd.ensure_many([], return_new=False) is None
    assert await cmd.upsert_many([], return_new=False) is None
    assert await cmd.update_many([], return_new=False) is None
    assert await cmd.touch_many([], return_new=False) is None
    assert await cmd.delete_many([], return_new=False) is None
    assert await cmd.restore_many([], return_new=False) is None
    assert await cmd.kill_many([]) is None

    base = await cmd.create(_CxCreate(sku="base"))
    await cmd.ensure(_CxCreate(id=base.id, sku="base"), return_new=False)
    await cmd.upsert(
        _CxCreate(id=base.id, sku="ignored"),
        _CxUpdate(sku="upserted"),
        return_new=False,
    )

    synced = await cmd.get(base.id)
    assert synced.sku == "upserted"

    pair = await cmd.update(
        synced.id,
        synced.rev,
        _CxUpdate(sku="diffed"),
        return_diff=True,
    )
    assert isinstance(pair, tuple)
    new_doc, diff = pair
    assert new_doc.sku == "diffed"
    assert "sku" in diff

    diff_only = await cmd.update(
        new_doc.id,
        new_doc.rev,
        _CxUpdate(sku="final"),
        return_new=False,
        return_diff=True,
    )
    assert isinstance(diff_only, dict)

    a = await cmd.create(_CxCreate(sku="ma"))
    b = await cmd.create(_CxCreate(sku="mb"))
    diffs = await cmd.update_many(
        [
            (a.id, a.rev, _CxUpdate(sku="ma2")),
            (b.id, b.rev, _CxUpdate(sku="mb2")),
        ],
        return_new=False,
        return_diff=True,
    )
    assert diffs is not None
    assert len(diffs) == 2

    await cmd.touch(a.id, return_new=False)
    await cmd.touch_many([b.id], return_new=False)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_soft_delete_restore_return_new_false(
    pg_client: PostgresClient,
) -> None:
    t = f"pg_sd_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_soft(t))

    cache_spec = CacheSpec(name=f"cache_{t}")
    spec = DocumentSpec(
        name=f"doc_soft_{t}",
        read=_SoftRead,
        write={
            "domain": _SoftDoc,
            "create_cmd": _SoftCreate,
            "update_cmd": _SoftUpdate,
        },
        cache=cache_spec,
    )
    fac = ConfigurablePostgresDocument(
        config={
            "read": ("public", t),
            "write": ("public", t),
            "bookkeeping_strategy": "application",
        }
    )
    state = MockState()

    def _cache_factory(ctx: ExecutionContext, cspec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.dep(MockStateDepKey), namespace=cspec.name)

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MockStateDepKey: state,
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
                CacheDepKey: _cache_factory,
            }
        )
    )
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    doc = await cmd.create(_SoftCreate(label="live"))
    await q.get(doc.id)

    await cmd.delete(doc.id, doc.rev, return_new=False)
    loaded = await q.get(doc.id)
    assert loaded.is_deleted is True

    await cmd.restore(doc.id, loaded.rev, return_new=False)
    again = await q.get(doc.id)
    assert again.is_deleted is False

    assert await cmd.delete_many([], return_new=True) == []
    assert await cmd.restore_many([], return_new=True) == []


# ....................... #
# Deeper :class:`PostgresDocumentAdapter` surface (read projections, aggregates, cache bypass).
# ....................... #


class _SkuGroup(BaseModel):
    cat: str
    n: int


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_get_get_many_return_fields_bypasses_read_cache(
    pg_client: PostgresClient,
) -> None:
    """``return_fields`` forces the read path that skips cache (adapter lines ~211, ~281)."""
    t = f"pg_rf_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))

    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    doc = await cmd.create(_CxCreate(sku="rf"))
    prj = await q.get(doc.id, return_fields=["sku", "rev"])
    assert prj == {"sku": "rf", "rev": 1}
    b = await cmd.create(_CxCreate(sku="rf2"))
    prjs = await q.get_many([doc.id, b.id], return_fields=["id", "sku"])
    assert len(prjs) == 2
    assert {r["sku"] for r in prjs} == {"rf", "rf2"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_find_many_aggregates_with_typed_page(
    pg_client: PostgresClient,
) -> None:
    """``find_many`` with ``aggregates``, ``return_type``, and ``return_count``."""
    t = f"pg_ag_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))

    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    await cmd.create(_CxCreate(sku="g1"))
    await cmd.create(_CxCreate(sku="g1"))
    await cmd.create(_CxCreate(sku="g2"))

    agg = {
        "$fields": {"cat": "sku"},
        "$computed": {"n": {"$count": None}},
    }
    p = await q.find_many(
        None,
        pagination={"limit": 10, "offset": 0},
        sorts={"cat": "asc"},
        aggregates=agg,
        return_type=_SkuGroup,
        return_count=True,
    )
    assert isinstance(p, Page)
    assert p.count == 2
    assert {r.cat for r in p.hits} == {"g1", "g2"}
    assert {r.n for r in p.hits} == {1, 2}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_find_many_rejects_conflicting_args(
    pg_client: PostgresClient,
) -> None:
    t = f"pg_inv_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    ctx, spec = _ctx_cached(pg_client, t)
    q = ctx.doc_query(spec)

    agg = {
        "$fields": {"c": "sku"},
        "$computed": {"n": {"$count": None}},
    }
    with pytest.raises(CoreError, match="Aggregates cannot be combined with return_fields"):
        await q.find_many(
            None,
            aggregates=agg,
            return_fields=["sku"],
        )

    cmd = ctx.doc_command(spec)
    await cmd.create(_CxCreate(sku="rt-single"))
    page = await q.find_many(
        {"$fields": {"sku": "rt-single"}},
        pagination={"limit": 10},
        return_type=_CxRead,
        return_count=False,
    )
    assert len(page.hits) == 1
    assert isinstance(page.hits[0], _CxRead)
    assert page.hits[0].sku == "rt-single"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_count_method(
    pg_client: PostgresClient,
) -> None:
    t = f"pg_cnt_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)
    await cmd.create(_CxCreate(sku="cnt-a"))
    await cmd.create(_CxCreate(sku="cnt-b"))
    n = await q.count({"$fields": {"sku": {"$in": ["cnt-a", "cnt-b"]}}})
    assert n == 2
    assert await q.count({"$fields": {"sku": "missing"}}) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_create_return_new_false(
    pg_client: PostgresClient,
) -> None:
    t = f"pg_cnf_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)
    uid = uuid4()
    out = await cmd.create(_CxCreate(id=uid, sku="no-ret"), return_new=False)
    assert out is None
    loaded = await q.get(uid)
    assert loaded.sku == "no-ret"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_update_many_return_new_with_diffs(
    pg_client: PostgresClient,
) -> None:
    """``update_many`` with ``return_new=True`` and ``return_diff=True`` returns (row, diff) pairs."""
    t = f"pg_ud_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)

    a = await cmd.create(_CxCreate(sku="u1"))
    b = await cmd.create(_CxCreate(sku="u2"))
    pairs = await cmd.update_many(
        [
            (a.id, a.rev, _CxUpdate(sku="u1x")),
            (b.id, b.rev, _CxUpdate(sku="u2x")),
        ],
        return_new=True,
        return_diff=True,
    )
    assert pairs is not None
    assert len(pairs) == 2
    d0, diff0 = pairs[0]
    assert d0.sku in ("u1x", "u2x")
    assert "sku" in diff0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_touch_many_return_new_true(
    pg_client: PostgresClient,
) -> None:
    t = f"pg_tm_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    a = await cmd.create(_CxCreate(sku="t1"))
    b = await cmd.create(_CxCreate(sku="t2"))
    out = await cmd.touch_many([a.id, b.id], return_new=True)
    assert out is not None
    assert len(out) == 2
    revs = {x.rev for x in out}
    assert 2 in revs


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_soft_delete_and_restore_many_return_new_true(
    pg_client: PostgresClient,
) -> None:
    t = f"pg_sdm_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_soft(t))

    cache_spec = CacheSpec(name=f"cache_{t}")
    spec = DocumentSpec(
        name=f"doc_sdm_{t}",
        read=_SoftRead,
        write={
            "domain": _SoftDoc,
            "create_cmd": _SoftCreate,
            "update_cmd": _SoftUpdate,
        },
        cache=cache_spec,
    )
    fac = ConfigurablePostgresDocument(
        config={
            "read": ("public", t),
            "write": ("public", t),
            "bookkeeping_strategy": "application",
        }
    )
    state = MockState()

    def _cache_factory(ctx: ExecutionContext, cspec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.dep(MockStateDepKey), namespace=cspec.name)

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MockStateDepKey: state,
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
                CacheDepKey: _cache_factory,
            }
        )
    )
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)

    d1 = await cmd.create(_SoftCreate(label="a"))
    d2 = await cmd.create(_SoftCreate(label="b"))
    del_rows = await cmd.delete_many(
        [(d1.id, d1.rev), (d2.id, d2.rev)],
        return_new=True,
    )
    assert del_rows is not None
    assert len(del_rows) == 2
    assert {x.is_deleted for x in del_rows} == {True}
    r1, r2 = del_rows[0], del_rows[1]
    rest = await cmd.restore_many(
        [(r1.id, r1.rev), (r2.id, r2.rev)],
        return_new=True,
    )
    assert rest is not None
    assert len(rest) == 2
    assert {x.is_deleted for x in rest} == {False}
    for row in rest:
        ok = await q.get(row.id)
        assert ok.is_deleted is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_find_for_update_in_transaction(
    pg_client: PostgresClient,
) -> None:
    t = f"pg_fuf_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    ctx, spec = _ctx_cached(pg_client, t)
    cmd = ctx.doc_command(spec)
    q = ctx.doc_query(spec)
    await cmd.create(_CxCreate(sku="lockme"))
    async with pg_client.transaction():
        found = await q.find({"$fields": {"sku": "lockme"}}, for_update=True)
        assert found is not None
        assert found.sku == "lockme"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_uses_clamped_batch_size(
    pg_client: PostgresClient,
) -> None:
    """Config ``batch_size`` below minimum is clamped (``eff_batch_size`` in adapter)."""
    t = f"pg_bs_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    spec = DocumentSpec(
        name=f"doc_bs_{t}",
        read=_CxRead,
        write={
            "domain": _CxDoc,
            "create_cmd": _CxCreate,
            "update_cmd": _CxUpdate,
        },
    )
    fac = ConfigurablePostgresDocument(
        config={
            "read": ("public", t),
            "write": ("public", t),
            "bookkeeping_strategy": "application",
            "batch_size": 3,
        }
    )
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        )
    )
    ad = ctx.doc_command(spec)
    assert ad.eff_batch_size == 200
    await ad.create_many(
        [
            _CxCreate(sku="b1"),
            _CxCreate(sku="b2"),
        ],
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_adapter_cache_tx_commit_deferred_warm(
    pg_client: PostgresClient,
) -> None:
    """After a successful ``ExecutionContext`` transaction, deferred cache warm matches committed DB state."""

    t = f"pg_cc_tx_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    ctx, spec = _ctx_cached_tx(pg_client, t)
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
async def test_pg_adapter_cache_tx_rollback_eager_evict_skips_deferred_warm(
    pg_client: PostgresClient,
) -> None:
    """On rollback, eager cache eviction stands; deferred warm is not run; reads match rolled-back DB."""

    t = f"pg_rb_tx_{uuid4().hex[:12]}"
    await pg_client.execute(_ddl_main(t))
    ctx, spec = _ctx_cached_tx(pg_client, t)
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
