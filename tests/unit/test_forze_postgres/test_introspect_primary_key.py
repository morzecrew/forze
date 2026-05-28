"""Unit tests for primary-key and unique-column introspection."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClient


@pytest.mark.asyncio
async def test_get_primary_key_columns_caches_result() -> None:
    client = MagicMock(spec=PostgresClient)
    client.fetch_value = AsyncMock(return_value="r")
    client.fetch_all = AsyncMock(
        return_value=[
            {
                "is_primary": True,
                "columns": ["tenant_id", "id"],
                "has_expr_key": False,
                "is_partial": False,
                "has_indexprs": False,
            },
            {
                "is_primary": False,
                "columns": ["email"],
                "has_expr_key": False,
                "is_partial": False,
                "has_indexprs": False,
            },
        ],
    )

    intro = PostgresIntrospector(client=client)

    pk = await intro.get_primary_key_columns(schema="public", relation="docs")
    assert pk == ("tenant_id", "id")

    # second call uses cache
    assert await intro.get_primary_key_columns(schema="public", relation="docs") == pk
    assert client.fetch_all.await_count == 1


@pytest.mark.asyncio
async def test_constraint_exists_for_columns() -> None:
    client = MagicMock(spec=PostgresClient)
    client.fetch_value = AsyncMock(return_value="r")
    client.fetch_all = AsyncMock(
        return_value=[
            {
                "is_primary": True,
                "columns": ["id"],
                "has_expr_key": False,
                "is_partial": False,
                "has_indexprs": False,
            },
            {
                "is_primary": False,
                "columns": ["email"],
                "has_expr_key": False,
                "is_partial": False,
                "has_indexprs": False,
            },
        ],
    )

    intro = PostgresIntrospector(client=client)

    assert await intro.constraint_exists_for_columns(
        schema="public",
        relation="t",
        columns=("id",),
    )
    assert await intro.constraint_exists_for_columns(
        schema="public",
        relation="t",
        columns=("email",),
    )
    assert not await intro.constraint_exists_for_columns(
        schema="public",
        relation="t",
        columns=("tenant_id", "id"),
    )


@pytest.mark.asyncio
async def test_expression_primary_key_yields_empty_pk() -> None:
    client = MagicMock(spec=PostgresClient)
    client.fetch_value = AsyncMock(return_value="r")
    client.fetch_all = AsyncMock(
        return_value=[
            {
                "is_primary": True,
                "columns": [],
                "has_expr_key": True,
                "is_partial": False,
                "has_indexprs": False,
            },
        ],
    )

    intro = PostgresIntrospector(client=client)

    assert await intro.get_primary_key_columns(schema="public", relation="t") == ()
