"""Integration tests for Postgres document adapter: cursor edges, projections, cache, and mutation branches."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.base import Page
from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze.domain.constants import ID_FIELD
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockCacheAdapter, MockState, MockStateDepKey
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
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
    assert await cmd.touch_many([], return_new=False) == []
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
