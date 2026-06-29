"""Integration tests: read fields absent from the relation hydrate from defaults.

A read model may declare a field that has no backing column (``lenient_read_fields``
on the ``DocumentSpec``, threaded to the gateway). Such a field is dropped from the
read projection and filled from its model default on read; without leniency the same
read fails because the projection references a missing column.
"""

from uuid import UUID, uuid4

import pytest

from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze.domain.models import ReadDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.utils import read_gw
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class _LenientReadDoc(ReadDocument):
    name: str
    nickname: str = "anon"  # declared on the read model, no column on the relation


async def _make_table_with_row(pg_client: PostgresClient) -> tuple[str, UUID]:
    table = f"pg_lenient_{uuid4().hex[:8]}"
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

    row_id = uuid4()
    now = utcnow()
    await pg_client.execute(
        f"INSERT INTO public.{table} (id, rev, created_at, last_update_at, name) "
        "VALUES (%s, %s, %s, %s, %s)",
        [row_id, 1, now, now, "Ada"],
    )

    return table, row_id


def _ctx(pg_client: PostgresClient):
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lenient_read_field_hydrates_from_default(
    pg_client: PostgresClient,
) -> None:
    table, row_id = await _make_table_with_row(pg_client)
    ctx = _ctx(pg_client)

    read = read_gw(
        ctx,
        read_type=_LenientReadDoc,
        read_relation=("public", table),
        tenant_aware=False,
        lenient_read_fields=frozenset({"nickname"}),
    )

    fetched = await read.get(row_id)

    assert fetched.name == "Ada"
    # No column for ``nickname`` — it comes from the model default, not the row.
    assert fetched.nickname == "anon"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_strict_read_of_missing_column_fails(
    pg_client: PostgresClient,
) -> None:
    table, row_id = await _make_table_with_row(pg_client)
    ctx = _ctx(pg_client)

    # Without leniency the projection selects ``nickname``, which is not a column.
    read = read_gw(
        ctx,
        read_type=_LenientReadDoc,
        read_relation=("public", table),
        tenant_aware=False,
    )

    with pytest.raises(CoreException):
        await read.get(row_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_return_type_projection_drops_lenient_field(
    pg_client: PostgresClient,
) -> None:
    # A read mapped to an explicit return_type that carries the lenient field must
    # drop it from the projection and hydrate it from the default — not reject it.
    table, _ = await _make_table_with_row(pg_client)
    ctx = _ctx(pg_client)

    read = read_gw(
        ctx,
        read_type=_LenientReadDoc,
        read_relation=("public", table),
        tenant_aware=False,
        lenient_read_fields=frozenset({"nickname"}),
    )

    rows = await read.find_many(None, return_model=_LenientReadDoc)

    assert len(rows) == 1
    assert rows[0].name == "Ada"
    assert rows[0].nickname == "anon"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lenient_field_filter_and_sort_rejected_before_sql(
    pg_client: PostgresClient,
) -> None:
    # A filter or sort on a lenient field is rejected as a clean precondition rather
    # than rendering ORDER BY / WHERE against a column that does not exist.
    table, _ = await _make_table_with_row(pg_client)
    ctx = _ctx(pg_client)

    read = read_gw(
        ctx,
        read_type=_LenientReadDoc,
        read_relation=("public", table),
        tenant_aware=False,
        lenient_read_fields=frozenset({"nickname"}),
    )

    with pytest.raises(CoreException) as ei_filter:
        await read.find_many({"$values": {"nickname": "anon"}})
    assert ei_filter.value.code == "field_not_on_read_model"

    with pytest.raises(CoreException) as ei_sort:
        await read.find_many(None, sorts={"nickname": "asc"})
    assert ei_sort.value.code == "field_not_on_read_model"
