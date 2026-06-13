"""Integration tests for :class:`~forze_postgres.kernel.gateways.read.PostgresReadGateway`."""

from forze.base.exceptions import CoreException, exc
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import encode_keyset_v1
from forze.application.execution import Deps, ExecutionContext
from forze.domain.constants import ID_FIELD
from forze.domain.models import Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import read_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

class RdDoc(Document):
    name: str

class RdNameOnly(BaseModel):
    id: UUID
    name: str

class RdOrder(Document):
    category: str
    price: float

class RdCategoryStats(BaseModel):
    category: str
    orders: int
    revenue: float
    median_price: float
    premium_orders: int
    premium_revenue: float | None

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_get_find_and_projections(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    id_a, id_b = uuid4(), uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO public.{table}
        (id, rev, created_at, last_update_at, name)
        VALUES
        (%(a)s, 1, now(), now(), 'alpha'),
        (%(b)s, 1, now(), now(), 'beta');
        """,
        {"a": id_a, "b": id_b},
    )

    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    alpha_id = id_a

    from tests.support import IsPartialDict, IsUUID, document_partial

    full = await gw.get(alpha_id)
    assert full.model_dump() == document_partial(name="alpha", id=IsUUID)

    one = await gw.find({"$values": {"name": "beta"}}, return_fields=["id", "name"])
    assert one is not None
    assert one == IsPartialDict({"name": "beta", "id": IsUUID})

    many = await gw.find_many(
        None,
        limit=10,
        offset=0,
        sorts={"name": "asc"},
        return_fields=["name"],
    )
    assert [r["name"] for r in many] == ["alpha", "beta"]

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_aggregate_expressions(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_agg_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            category text NOT NULL,
            price double precision NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        INSERT INTO public.{table}
        (id, rev, created_at, last_update_at, category, price)
        VALUES
        (%(a)s, 1, now(), now(), 'books', 10.0),
        (%(b)s, 1, now(), now(), 'books', 20.0),
        (%(c)s, 1, now(), now(), 'books', 30.0),
        (%(d)s, 1, now(), now(), 'hardware', 50.0),
        (%(e)s, 1, now(), now(), 'hardware', 60.0),
        (%(f)s, 1, now(), now(), 'hardware', 70.0),
        (%(g)s, 1, now(), now(), 'software', 90.0);
        """,
        {
            "a": uuid4(),
            "b": uuid4(),
            "c": uuid4(),
            "d": uuid4(),
            "e": uuid4(),
            "f": uuid4(),
            "g": uuid4(),
        },
    )

    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdOrder,
        read_relation=("public", table),
        tenant_aware=False,
    )

    aggregates = {
        "$groups": {"category": "category"},
        "$computed": {
            "orders": {"$count": None},
            "revenue": {"$sum": "price"},
            "median_price": {"$median": "price"},
            "premium_orders": {
                "$count": {"filter": {"$values": {"price": {"$gte": 20}}}},
            },
            "premium_revenue": {
                "$sum": {
                    "field": "price",
                    "filter": {"$values": {"price": {"$gte": 20}}},
                },
            },
        },
    }

    rows = await gw.find_many_aggregates(
        filters={"$values": {"category": {"$in": ["books", "hardware"]}}},
        limit=10,
        offset=0,
        sorts={"revenue": "desc"},
        aggregates=aggregates,
        return_model=RdCategoryStats,
    )

    assert rows == [
        RdCategoryStats(
            category="hardware",
            orders=3,
            revenue=180.0,
            median_price=60.0,
            premium_orders=3,
            premium_revenue=180.0,
        ),
        RdCategoryStats(
            category="books",
            orders=3,
            revenue=60.0,
            median_price=20.0,
            premium_orders=2,
            premium_revenue=50.0,
        ),
    ]
    assert (
        await gw.count_aggregates(
            {"$values": {"category": {"$in": ["books", "hardware"]}}},
            aggregates=aggregates,
        )
        == 2
    )

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_get_many_order_and_not_found(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_m_{uuid4().hex[:10]}"
    id_first = uuid4()
    id_second = uuid4()
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        INSERT INTO public.{table}
        (id, rev, created_at, last_update_at, name)
        VALUES
        (%(a)s, 1, now(), now(), 'x'),
        (%(b)s, 1, now(), now(), 'y');
        """,
        {"a": id_first, "b": id_second},
    )

    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    ordered = await gw.get_many([id_second, id_first])
    assert [d.id for d in ordered] == [id_second, id_first]

    missing = uuid4()
    with pytest.raises(CoreException, match="Some records not found"):
        await gw.get_many([id_first, missing])

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_find_many_with_cursor(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_cur_{uuid4().hex[:10]}"
    ids = [
        UUID("00000000-0000-0000-0000-000000000041"),
        UUID("00000000-0000-0000-0000-000000000042"),
        UUID("00000000-0000-0000-0000-000000000043"),
        UUID("00000000-0000-0000-0000-000000000044"),
    ]
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """,
    )
    for i, u in enumerate(ids):
        await pg_client.execute(
            f"""
            INSERT INTO public.{table}
            (id, rev, created_at, last_update_at, name)
            VALUES (%(id)s, 1, now(), now(), %(name)s);
            """,
            {"id": u, "name": f"n{i}"},
        )

    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    first = await gw.find_many_with_cursor(
        None,
        cursor={"limit": 2},
        sorts={ID_FIELD: "asc"},
    )
    assert len(first) == 3

    last_row = first[1]
    tok = encode_keyset_v1(
        sort_keys=[ID_FIELD],
        directions=["asc"],
        values=[last_row.id],
    )
    second = await gw.find_many_with_cursor(
        None,
        cursor={"limit": 2, "after": tok},
        sorts={ID_FIELD: "asc"},
    )
    assert len(second) >= 1
    assert second[0].id != first[0].id

    before_page = await gw.find_many_with_cursor(
        None,
        cursor={"limit": 2, "before": tok},
        sorts={ID_FIELD: "asc"},
    )
    assert len(before_page) >= 1

    with pytest.raises(CoreException, match="at most one"):
        await gw.find_many_with_cursor(
            None,
            cursor={"after": tok, "before": tok},
            sorts={ID_FIELD: "asc"},
        )

    with pytest.raises(CoreException, match="positive"):
        await gw.find_many_with_cursor(None, cursor={"limit": 0})

    bad = encode_keyset_v1(
        sort_keys=["name"],
        directions=["asc"],
        values=["x"],
    )
    with pytest.raises(CoreException, match="does not match current search sort"):
        await gw.find_many_with_cursor(
            None,
            cursor={"after": bad},
            sorts={ID_FIELD: "asc"},
        )

    dict_rows = await gw.find_many_with_cursor(
        None,
        cursor={"limit": 3},
        sorts={"name": "asc"},
        return_fields=["id", "name"],
    )
    assert dict_rows[0]["name"] == "n0"

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_for_update_requires_transaction(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_fu_{uuid4().hex[:10]}"
    pk = uuid4()
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        INSERT INTO public.{table}
        (id, rev, created_at, last_update_at, name)
        VALUES (%(id)s, 1, now(), now(), 'x');
        """,
        {"id": pk},
    )

    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    with pytest.raises(CoreException, match="Transactional context"):
        await gw.get(pk, for_update=True)

    async with pg_client.transaction():
        locked = await gw.get(pk, for_update=True)
        assert locked.name == "x"

    with pytest.raises(CoreException, match="Transactional context"):
        await gw.find({"$values": {"name": "x"}}, for_update=True)

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_get_many_empty_sequence(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_empty_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )
    assert await gw.get_many([]) == []

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_find_many_aggregates_raw_rows(
    pg_client: PostgresClient,
) -> None:
    """``find_many_aggregates`` without ``return_model`` returns query rows as ``dict``."""
    table = f"pg_rd_ag_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            category text NOT NULL,
            price double precision NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        INSERT INTO public.{table}
        (id, rev, created_at, last_update_at, category, price)
        VALUES
        (%(a)s, 1, now(), now(), 'a', 1.0),
        (%(b)s, 1, now(), now(), 'a', 2.0);
        """,
        {"a": uuid4(), "b": uuid4()},
    )
    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdOrder,
        read_relation=("public", table),
        tenant_aware=False,
    )
    raw = await gw.find_many_aggregates(
        aggregates={
            "$groups": {"c": "category"},
            "$computed": {"n": {"$count": None}},
        },
    )
    assert len(raw) == 1
    assert raw[0]["c"] == "a"
    assert int(raw[0]["n"]) == 2

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_find_many_with_cursor_return_model(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_curm_{uuid4().hex[:10]}"
    id_a, id_b = uuid4(), uuid4()
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        INSERT INTO public.{table}
        (id, rev, created_at, last_update_at, name)
        VALUES
        (%(a)s, 1, now(), now(), 'z'),
        (%(b)s, 1, now(), now(), 'y');
        """,
        {"a": id_a, "b": id_b},
    )
    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )
    rows = await gw.find_many_with_cursor(
        None,
        cursor={"limit": 2},
        sorts={"name": "asc"},
        return_model=RdNameOnly,
    )
    assert len(rows) == 2
    assert isinstance(rows[0], RdNameOnly)
    assert [r.id for r in rows] == [id_b, id_a]
    assert [r.name for r in rows] == ["y", "z"]

@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_aggregates_reject_return_fields(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_agf_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            category text NOT NULL,
            price double precision NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        INSERT INTO public.{table}
        (id, rev, created_at, last_update_at, category, price)
        VALUES (%(a)s, 1, now(), now(), 'c', 1.0);
        """,
        {"a": uuid4()},
    )
    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )
    gw = read_gw(
        ctx,
        read_type=RdOrder,
        read_relation=("public", table),
        tenant_aware=False,
    )
    with pytest.raises(CoreException, match="Aggregates cannot be combined"):
        await gw.find_many_aggregates(
            aggregates={
                "$groups": {"c": "category"},
                "$computed": {"n": {"$count": None}},
            },
            return_fields=["c"],
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgres_read_gateway_get_missing_raises(
    pg_client: PostgresClient,
) -> None:
    table = f"pg_rd_miss_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """,
    )
    ctx = context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            },
        ),
    )
    gw = read_gw(
        ctx,
        read_type=RdDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )
    with pytest.raises(CoreException, match="not found"):
        await gw.get(uuid4())
