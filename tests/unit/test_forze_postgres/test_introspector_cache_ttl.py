"""Unit tests for :class:`PostgresIntrospector` cache TTL."""

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze_postgres.kernel.introspect import PostgresIntrospector

# ----------------------- #


@pytest.mark.asyncio
async def test_relation_cache_expires_with_ttl() -> None:
    client = MagicMock()
    client.fetch_value = AsyncMock(return_value="r")

    intro = PostgresIntrospector(client=client, cache_ttl=timedelta(seconds=1))

    with patch(
        "forze_postgres.kernel.introspect.introspector.monotonic",
        side_effect=[100.0, 100.5, 102.0, 102.0, 103.0],
    ):
        await intro.get_relation(schema="public", relation="t")
        await intro.get_relation(schema="public", relation="t")
        assert client.fetch_value.await_count == 1

        await intro.get_relation(schema="public", relation="t")
        assert client.fetch_value.await_count == 2


@pytest.mark.asyncio
async def test_invalidate_relation_clears_timestamp_map() -> None:
    client = MagicMock()
    client.fetch_value = AsyncMock(return_value="r")

    intro = PostgresIntrospector(client=client, cache_ttl=timedelta(hours=1))
    await intro.get_relation(schema="public", relation="t")
    intro.invalidate_relation(schema="public", relation="t")
    await intro.get_relation(schema="public", relation="t")
    assert client.fetch_value.await_count == 2


@pytest.mark.asyncio
async def test_get_index_info_respects_ttl() -> None:
    client = MagicMock()
    row_index = {
        "amname": "gin",
        "indexdef": "CREATE INDEX x ON t USING gin (v tsvector_ops)",
        "expr": "to_tsvector('english'::regconfig, t.title)",
        "cols": [],
        "has_tsvector_col": True,
    }
    client.fetch_one = AsyncMock(return_value=row_index)

    intro = PostgresIntrospector(client=client, cache_ttl=timedelta(seconds=1))

    with patch(
        "forze_postgres.kernel.introspect.introspector.monotonic",
        side_effect=[5.0, 5.5, 6.5, 6.5],
    ):
        await intro.get_index_info(index="ix", schema="public")
        await intro.get_index_info(index="ix", schema="public")
        assert client.fetch_one.await_count == 1

        await intro.get_index_info(index="ix", schema="public")
        assert client.fetch_one.await_count == 2


@pytest.mark.asyncio
async def test_column_types_ttl() -> None:
    client = MagicMock()
    client.fetch_value = AsyncMock(return_value="r")
    client.fetch_all = AsyncMock(
        return_value=[
            {
                "column_name": "a",
                "is_array": False,
                "array_elem_type": None,
                "full_type": "int4",
                "not_null": True,
            },
        ],
    )

    intro = PostgresIntrospector(client=client, cache_ttl=timedelta(seconds=1))

    with patch(
        "forze_postgres.kernel.introspect.introspector.monotonic",
        side_effect=[1.0, 1.5, 3.0, 3.0, 3.0, 3.0],
    ):
        t1 = await intro.get_column_types(schema="public", relation="t")
        assert "a" in t1
        assert client.fetch_all.await_count == 1

        t3 = await intro.get_column_types(schema="public", relation="t")
        assert "a" in t3
        assert client.fetch_all.await_count == 2
