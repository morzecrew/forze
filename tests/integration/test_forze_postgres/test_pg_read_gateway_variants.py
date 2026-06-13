"""Integration coverage for less-exercised :class:`PostgresReadGateway` variants.

Targets ``read.py`` paths missed by the existing suite: row-lock modes
(``nowait`` / ``skip_locked`` / invalid guard), ``find`` with ``return_model``,
``find_many`` aggregate dispatch, and ``find_many_chunked`` (model / fields /
plain decode paths plus multi-chunk batching).
"""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import encode_keyset_v1
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.domain.constants import ID_FIELD
from forze.domain.models import Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import read_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.kernel.gateways.read import _for_update_sql
from tests.support.execution_context import context_from_deps


class RvDoc(Document):
    name: str
    category: str


class RvNameOnly(BaseModel):
    id: UUID
    name: str


def _ctx(pg_client: PostgresClient):
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )


async def _make_table_with_rows(pg_client: PostgresClient, rows: int) -> str:
    table = f"pg_rv_{uuid4().hex[:10]}"
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
    for i in range(rows):
        await pg_client.execute(
            f"""
            INSERT INTO public.{table}
            (id, rev, created_at, last_update_at, name, category)
            VALUES (%(id)s, 1, now(), now(), %(name)s, %(cat)s);
            """,
            {"id": uuid4(), "name": f"n{i:04d}", "cat": "a" if i % 2 == 0 else "b"},
        )
    return table


def test_for_update_sql_modes_and_invalid_guard() -> None:
    """``_for_update_sql`` maps each lock mode; the invalid branch is a defensive guard."""

    assert _for_update_sql(False) is None
    assert "FOR UPDATE" in _for_update_sql(True).as_string(None)
    assert "NOWAIT" in _for_update_sql("nowait").as_string(None)
    assert "SKIP LOCKED" in _for_update_sql("skip_locked").as_string(None)

    with pytest.raises(CoreException, match="Invalid for_update mode"):
        _for_update_sql("bogus")  # type: ignore[arg-type]


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["nowait", "skip_locked"])
async def test_read_gateway_get_for_update_lock_modes(
    pg_client: PostgresClient, mode: str
) -> None:
    """``get(for_update="nowait"|"skip_locked")`` emits the lock clause in a tx."""

    table = await _make_table_with_rows(pg_client, 1)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )
    row = await gw.find_many(None, limit=1)
    pk = row[0].id

    async with pg_client.transaction():
        locked = await gw.get(pk, for_update=mode)  # type: ignore[arg-type]
        assert locked.id == pk


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_find_return_model(pg_client: PostgresClient) -> None:
    """``find(return_model=...)`` decodes the single row into the projection model."""

    table = await _make_table_with_rows(pg_client, 3)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    res = await gw.find({"$values": {"name": "n0000"}}, return_model=RvNameOnly)
    assert isinstance(res, RvNameOnly)
    assert res.name == "n0000"

    miss = await gw.find({"$values": {"name": "does-not-exist"}}, return_model=RvNameOnly)
    assert miss is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_find_many_dispatches_to_aggregates(
    pg_client: PostgresClient,
) -> None:
    """``find_many(aggregates=...)`` delegates to ``find_many_aggregates``."""

    table = await _make_table_with_rows(pg_client, 4)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    rows = await gw.find_many(
        None,
        aggregates={
            "$groups": {"category": "category"},
            "$computed": {"n": {"$count": None}},
        },
        sorts={"category": "asc"},
    )
    by_cat = {r["category"]: int(r["n"]) for r in rows}
    assert by_cat == {"a": 2, "b": 2}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_find_many_chunked_plain_multichunk(
    pg_client: PostgresClient,
) -> None:
    """``find_many_chunked`` yields multiple validated batches of decoded models."""

    table = await _make_table_with_rows(pg_client, 5)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    chunks = [
        chunk
        async for chunk in gw.find_many_chunked(
            None,
            sorts={"name": "asc"},
            fetch_batch_size=2,
        )
    ]
    assert len(chunks) >= 2
    flat = [d for c in chunks for d in c]
    assert len(flat) == 5
    assert all(isinstance(d, RvDoc) for d in flat)
    assert [d.name for d in flat] == [f"n{i:04d}" for i in range(5)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_find_many_chunked_return_model(
    pg_client: PostgresClient,
) -> None:
    """``find_many_chunked(return_model=...)`` decodes each chunk into the model."""

    table = await _make_table_with_rows(pg_client, 3)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    out: list[RvNameOnly] = []
    async for chunk in gw.find_many_chunked(
        None,
        sorts={"name": "asc"},
        fetch_batch_size=2,
        return_model=RvNameOnly,
    ):
        out.extend(chunk)  # type: ignore[arg-type]
    assert all(isinstance(r, RvNameOnly) for r in out)
    assert [r.name for r in out] == ["n0000", "n0001", "n0002"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_find_many_chunked_return_fields(
    pg_client: PostgresClient,
) -> None:
    """``find_many_chunked(return_fields=...)`` yields projected dict chunks."""

    table = await _make_table_with_rows(pg_client, 3)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    rows: list[dict] = []
    async for chunk in gw.find_many_chunked(
        None,
        sorts={"name": "asc"},
        fetch_batch_size=10,
        return_fields=["name"],
    ):
        rows.extend(chunk)  # type: ignore[arg-type]
    assert [r["name"] for r in rows] == ["n0000", "n0001", "n0002"]
    assert all(set(r.keys()) == {"name"} for r in rows)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_find_many_chunked_rejects_model_and_fields(
    pg_client: PostgresClient,
) -> None:
    """``find_many_chunked`` cannot combine ``return_model`` and ``return_fields``."""

    table = await _make_table_with_rows(pg_client, 1)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    with pytest.raises(CoreException, match="cannot be combined"):
        async for _ in gw.find_many_chunked(
            None,
            return_model=RvNameOnly,
            return_fields=["name"],
        ):
            pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_find_many_chunked_with_offset(
    pg_client: PostgresClient,
) -> None:
    """``find_many_chunked`` applies ``offset`` and skips empty driver batches."""

    table = await _make_table_with_rows(pg_client, 5)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    flat = [
        d
        async for chunk in gw.find_many_chunked(
            None,
            limit=10,
            offset=2,
            sorts={"name": "asc"},
            fetch_batch_size=2,
        )
        for d in chunk
    ]
    assert [d.name for d in flat] == ["n0002", "n0003", "n0004"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_cursor_rejects_order_mismatch(
    pg_client: PostgresClient,
) -> None:
    """A cursor token whose direction differs from the active sort order is rejected."""

    table = await _make_table_with_rows(pg_client, 3)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    # Same sort key, but token encodes the opposite direction.
    tok = encode_keyset_v1(
        sort_keys=[ID_FIELD],
        directions=["desc"],
        values=[uuid4()],
    )
    with pytest.raises(CoreException, match="does not match current search sort"):
        await gw.find_many_with_cursor(
            None,
            cursor={"after": tok},
            sorts={ID_FIELD: "asc"},
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_read_gateway_count_with_parsed_and_filters(
    pg_client: PostgresClient,
) -> None:
    """``count`` honours both filter expressions and pre-parsed query exprs."""

    table = await _make_table_with_rows(pg_client, 6)
    gw = read_gw(
        _ctx(pg_client),
        read_type=RvDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    assert await gw.count(None) == 6
    assert await gw.count({"$values": {"category": "a"}}) == 3
