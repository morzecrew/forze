"""Unit tests for introspector cache partitioning."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.errors import CoreError

from forze_postgres.kernel.introspect import PostgresIntrospector

# ----------------------- #


@pytest.mark.asyncio
async def test_partition_scopes_relation_cache() -> None:
    client = MagicMock()
    client.fetch_value = AsyncMock(side_effect=["r", "v"])

    t_a = iter(["a"])
    intro = PostgresIntrospector(
        client=client,
        cache_partition_key=lambda: next(t_a),
    )

    k1 = await intro.get_relation(schema="public", relation="t1")
    assert k1 == "table"

    t_b = iter(["b"])
    intro_b = PostgresIntrospector(
        client=client,
        cache_partition_key=lambda: next(t_b),
    )

    k2 = await intro_b.get_relation(schema="public", relation="t1")
    assert k2 == "view"

    assert client.fetch_value.await_count == 2


@pytest.mark.asyncio
async def test_partition_none_raises() -> None:
    client = MagicMock()
    client.fetch_value = AsyncMock(return_value="r")

    intro = PostgresIntrospector(
        client=client,
        cache_partition_key=lambda: None,
    )

    with pytest.raises(CoreError, match="partition"):
        await intro.get_relation(schema="public", relation="t1")


@pytest.mark.asyncio
async def test_no_partition_shared_cache_key_shape() -> None:
    client = MagicMock()
    client.fetch_value = AsyncMock(return_value="r")

    intro = PostgresIntrospector(client=client)
    await intro.get_relation(schema="public", relation="t1")
    await intro.get_relation(schema="public", relation="t1")
    assert client.fetch_value.await_count == 1
