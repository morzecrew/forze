"""Unit tests for :class:`PostgresIntrospector` cache TTL."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.primitives import CacheLane
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector

# ----------------------- #

_INTROSPECTOR_LANES = (
    "_PostgresIntrospector__relation_lane",
    "_PostgresIntrospector__column_lane",
    "_PostgresIntrospector__trigger_lane",
    "_PostgresIntrospector__pk_lane",
    "_PostgresIntrospector__unique_sets_lane",
    "_PostgresIntrospector__index_lane",
)


def _set_lane_clocks(intro: PostgresIntrospector, clock: Callable[[], float]) -> None:
    for attr in _INTROSPECTOR_LANES:
        lane: CacheLane[object, object] = getattr(intro, attr)
        lane.clock = clock


@contextmanager
def _lane_clock(*times: float) -> Iterator[Callable[[], float]]:
    """Drive :class:`~forze.base.primitives.lanes.CacheLane` TTL on an introspector."""

    it = iter(times)

    def fake_monotonic() -> float:
        return next(it)

    yield fake_monotonic


# ----------------------- #


@pytest.mark.asyncio
async def test_relation_cache_expires_with_ttl() -> None:
    client = MagicMock()
    client.fetch_value = AsyncMock(return_value="r")

    intro = PostgresIntrospector(client=client, cache_ttl=timedelta(seconds=1))

    with _lane_clock(100.0, 100.5, 102.0, 102.0) as clock:
        _set_lane_clocks(intro, clock)
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

    with _lane_clock(5.0, 5.5, 6.5, 6.5) as clock:
        _set_lane_clocks(intro, clock)
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

    with _lane_clock(1.0, 1.5, 3.0, 3.0, 3.0, 3.0) as clock:
        _set_lane_clocks(intro, clock)
        t1 = await intro.get_column_types(schema="public", relation="t")
        assert "a" in t1
        assert client.fetch_all.await_count == 1

        t3 = await intro.get_column_types(schema="public", relation="t")
        assert "a" in t3
        assert client.fetch_all.await_count == 2
