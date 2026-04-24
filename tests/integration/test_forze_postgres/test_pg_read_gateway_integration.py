"""Integration tests for :class:`~forze_postgres.kernel.gateways.read.PostgresReadGateway`."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.query import encode_keyset_v1
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError, InfrastructureError, NotFoundError
from forze.domain.models import Document
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import read_gw
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


class RdDoc(Document):
    name: str


class RdNameOnly(BaseModel):
    id: UUID
    name: str


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

    ctx = ExecutionContext(
        deps=Deps.plain(
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

    full = await gw.get(alpha_id)
    assert full.name == "alpha"

    proj = await gw.get(alpha_id, return_fields=["name"])
    assert proj == {"name": "alpha"}

    typed = await gw.get(alpha_id, return_model=RdNameOnly)
    assert typed.name == "alpha"

    one = await gw.find({"$fields": {"name": "beta"}}, return_fields=["id", "name"])
    assert one is not None
    assert one["name"] == "beta"

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

    ctx = ExecutionContext(
        deps=Deps.plain(
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
    with pytest.raises(NotFoundError, match="Some records not found"):
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

    ctx = ExecutionContext(
        deps=Deps.plain(
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
        sorts=None,
    )
    assert len(first) == 3

    last_row = first[1]
    tok = encode_keyset_v1(
        sort_keys=["id"],
        directions=["asc"],
        values=[last_row.id],
    )
    second = await gw.find_many_with_cursor(
        None,
        cursor={"limit": 2, "after": tok},
        sorts=None,
    )
    assert len(second) >= 1
    assert second[0].id != first[0].id

    before_page = await gw.find_many_with_cursor(
        None,
        cursor={"limit": 2, "before": tok},
        sorts=None,
    )
    assert len(before_page) >= 1

    with pytest.raises(CoreError, match="at most one"):
        await gw.find_many_with_cursor(
            None,
            cursor={"after": tok, "before": tok},
        )

    with pytest.raises(CoreError, match="positive"):
        await gw.find_many_with_cursor(None, cursor={"limit": 0})

    bad = encode_keyset_v1(
        sort_keys=["name"],
        directions=["asc"],
        values=["x"],
    )
    with pytest.raises(CoreError, match="sort keys"):
        await gw.find_many_with_cursor(
            None,
            cursor={"after": bad},
            sorts=None,
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

    ctx = ExecutionContext(
        deps=Deps.plain(
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

    with pytest.raises(InfrastructureError, match="Transactional context"):
        await gw.get(pk, for_update=True)

    async with pg_client.transaction():
        locked = await gw.get(pk, for_update=True)
        assert locked.name == "x"

    with pytest.raises(InfrastructureError, match="Transactional context"):
        await gw.find({"$fields": {"name": "x"}}, for_update=True)
