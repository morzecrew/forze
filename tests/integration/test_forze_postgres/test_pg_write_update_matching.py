"""Integration coverage for under-exercised :class:`PostgresWriteGateway` paths.

Targets ``write.py`` blocks the existing suite misses:

* ``update_matching`` — filtered bulk update with ``rev`` bump, empty diff, and
  empty match (returns ``(0, [])``).
* multi-batch ``create_many`` / ``ensure_many`` / ``upsert_many`` (small
  ``batch_size`` forces ``>=2`` insert batches and exercises the
  conflict-vs-insert ordering branches).
* single-row ``touch`` and ``kill``.
"""

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import doc_write_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class WmDoc(Document):
    name: str
    category: str


class WmCreate(CreateDocumentCmd):
    name: str
    category: str


class WmUpdate(BaseDTO):
    name: str | None = None
    category: str | None = None


def _write_types() -> DocumentWriteTypes[WmDoc, WmCreate, WmUpdate]:
    return DocumentWriteTypes(
        domain=WmDoc,
        create_cmd=WmCreate,
        update_cmd=WmUpdate,
    )


def _ctx(pg_client: PostgresClient):
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )


async def _make_gw(pg_client: PostgresClient):
    table = f"pg_wm_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            category text NOT NULL
        );
        """
    )
    return doc_write_gw(
        _ctx(pg_client),
        write_types=_write_types(),
        write_relation=("public", table),
        bookkeeping_strategy="application",
        tenant_aware=False,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_matching_bumps_rev_and_returns_rows(
    pg_client: PostgresClient,
) -> None:
    """``update_matching`` updates all matching rows, bumps ``rev``, returns them."""

    write = await _make_gw(pg_client)
    read = write.read_gw

    a = await write.create(WmCreate(name="a", category="x"))
    b = await write.create(WmCreate(name="b", category="x"))
    c = await write.create(WmCreate(name="c", category="y"))

    count, rows = await write.update_matching(
        {"$values": {"category": "x"}},
        WmUpdate(category="patched"),
    )
    assert count == 2
    assert {r.id for r in rows} == {a.id, b.id}
    assert all(r.category == "patched" for r in rows)
    assert all(r.rev == 2 for r in rows)

    # Unmatched row untouched.
    fresh_c = await read.get(c.id)
    assert fresh_c.category == "y"
    assert fresh_c.rev == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_matching_chunks_by_batch_size(
    pg_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A small ``batch_size`` chunks the update; every row is updated exactly once."""

    write = await _make_gw(pg_client)
    read = write.read_gw

    created = [await write.create(WmCreate(name=f"n{i}", category="x")) for i in range(5)]
    other = await write.create(WmCreate(name="z", category="y"))

    # Spy on the per-chunk history write so the chunking is observable: a single
    # unbounded UPDATE would satisfy the outcome assertions below all the same.
    chunk_sizes: list[int] = []
    gw_cls = type(write)
    original_write_history = gw_cls._write_history

    async def _spy(self: object, *doms: object) -> None:
        chunk_sizes.append(len(doms))
        await original_write_history(self, *doms)

    monkeypatch.setattr(gw_cls, "_write_history", _spy)

    count, rows = await write.update_matching(
        {"$values": {"category": "x"}},
        WmUpdate(category="patched"),
        batch_size=2,
    )

    assert count == 5
    assert {r.id for r in rows} == {c.id for c in created}
    assert all(r.category == "patched" for r in rows)
    # rev bumped exactly once — no chunk re-processed an already-updated row.
    assert all(r.rev == 2 for r in rows)
    # 5 matches with batch_size=2 -> 3 chunks of (2, 2, 1).
    assert chunk_sizes == [2, 2, 1]

    fresh = await read.get(other.id)
    assert fresh.category == "y"
    assert fresh.rev == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_matching_empty_match_returns_zero(
    pg_client: PostgresClient,
) -> None:
    """A predicate that matches nothing returns ``(0, [])`` without error."""

    write = await _make_gw(pg_client)
    await write.create(WmCreate(name="only", category="z"))

    count, rows = await write.update_matching(
        {"$values": {"category": "absent"}},
        WmUpdate(category="never"),
    )
    assert count == 0
    assert rows == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_matching_empty_diff_short_circuits(
    pg_client: PostgresClient,
) -> None:
    """An update DTO with no set fields short-circuits to ``(0, [])``."""

    write = await _make_gw(pg_client)
    await write.create(WmCreate(name="a", category="x"))

    count, rows = await write.update_matching(
        {"$values": {"category": "x"}},
        WmUpdate(),
    )
    assert count == 0
    assert rows == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_many_multiple_batches(pg_client: PostgresClient) -> None:
    """A small ``batch_size`` forces ``create_many`` to issue multiple insert batches."""

    write = await _make_gw(pg_client)
    read = write.read_gw

    dtos = [WmCreate(name=f"n{i}", category="c") for i in range(5)]
    out = await write.create_many(dtos, batch_size=2)
    assert len(out) == 5
    assert {d.name for d in out} == {f"n{i}" for i in range(5)}

    assert await read.count(None) == 5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ensure_many_multibatch_insert_and_conflict(
    pg_client: PostgresClient,
) -> None:
    """``ensure_many`` across multiple batches inserts new rows and reuses existing ones."""

    write = await _make_gw(pg_client)

    seed = await write.create(WmCreate(name="seed", category="c"))

    fresh_ids = [uuid4() for _ in range(3)]
    ids = list(fresh_ids)
    payloads = [WmCreate(name=f"new{i}", category="c") for i in range(3)]
    # Interleave the existing PK so a conflict (DO NOTHING) lands inside a batch.
    ids.insert(1, seed.id)
    payloads.insert(1, WmCreate(name="ignored", category="c"))

    out = await write.ensure_many(ids, payloads, batch_size=2)
    assert len(out) == len(ids)
    by_id = {d.id: d for d in out}
    assert by_id[seed.id].name == "seed"  # existing row reused, not overwritten
    assert by_id[seed.id].rev == seed.rev
    for i, fid in enumerate(fresh_ids):
        assert by_id[fid].name == f"new{i}"
        assert by_id[fid].rev == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upsert_many_multibatch_insert_and_update(
    pg_client: PostgresClient,
) -> None:
    """``upsert_many`` across batches inserts new rows and updates conflicting ones."""

    write = await _make_gw(pg_client)

    e1 = await write.create(WmCreate(name="e1", category="old"))
    e2 = await write.create(WmCreate(name="e2", category="old"))

    ids = [uuid4(), e1.id, uuid4(), e2.id]
    creates = [
        WmCreate(name="brand-new-0", category="c"),
        WmCreate(name="ignored", category="c"),
        WmCreate(name="brand-new-1", category="c"),
        WmCreate(name="ignored", category="c"),
    ]
    updates = [
        WmUpdate(name="n/a"),
        WmUpdate(category="patched-1"),
        WmUpdate(name="n/a"),
        WmUpdate(category="patched-2"),
    ]
    out = await write.upsert_many(ids, creates, updates, batch_size=2)
    assert len(out) == 4

    by_id = {d.id: d for d in out}
    assert by_id[e1.id].category == "patched-1"
    assert by_id[e1.id].rev >= 2
    assert by_id[e2.id].category == "patched-2"
    assert by_id[e2.id].rev >= 2

    fresh = [d for d in out if d.id not in (e1.id, e2.id)]
    assert sorted(d.name for d in fresh) == ["brand-new-0", "brand-new-1"]
    assert all(d.rev == 1 for d in fresh)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_touch_single_bumps_rev(pg_client: PostgresClient) -> None:
    """``touch`` bumps ``rev`` without changing payload fields."""

    write = await _make_gw(pg_client)
    a = await write.create(WmCreate(name="a", category="x"))
    touched = await write.touch(a.id)
    assert touched.rev == a.rev + 1
    assert touched.name == "a"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kill_single_removes_row(pg_client: PostgresClient) -> None:
    """``kill`` deletes a single row by primary key."""

    write = await _make_gw(pg_client)
    read = write.read_gw
    a = await write.create(WmCreate(name="a", category="x"))

    await write.kill(a.id)
    assert await read.count(None) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kill_many_multibatch(pg_client: PostgresClient) -> None:
    """``kill_many`` deletes across multiple batches when ``batch_size`` is small."""

    write = await _make_gw(pg_client)
    read = write.read_gw
    docs = await write.create_many(
        [WmCreate(name=f"n{i}", category="c") for i in range(5)],
        batch_size=10,
    )
    await write.kill_many([d.id for d in docs], batch_size=2)
    assert await read.count(None) == 0
