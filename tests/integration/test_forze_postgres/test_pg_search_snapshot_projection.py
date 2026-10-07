"""A projected read on a snapshot-enabled spec writes and replays its snapshot.

The snapshot pool keys every hit by its whole record, so the windows that fill it read the
full read model whatever the request projects; only the page returned is projected.
"""

from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze_postgres.kernel.client.client import PostgresClient
from forze_redis.kernel.client import RedisClient
from tests.integration.test_forze_postgres.test_pg_search_snapshot_order import _TITLES, _ports

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Title(BaseModel):
    id: UUID
    title: str


@pytest.mark.parametrize(
    ("engine", "query"),
    [("fts", ""), ("fts", "python"), ("pgroonga", ""), ("pgroonga", "python"), ("hub", "python")],
)
async def test_a_projected_read_writes_and_replays_its_snapshot(
    pg_client: PostgresClient, redis_client: RedisClient, engine: str, query: str
) -> None:
    port, _ = await _ports(pg_client, redis_client, engine)

    projected = await port.project_search_page(
        ["title"], query, None, {"limit": 3}, snapshot={"mode": True}
    )
    assert list(projected.hits) == [{"title": t} for t in _TITLES]
    assert projected.snapshot is not None

    handle = {"id": projected.snapshot.id, "fingerprint": projected.snapshot.fingerprint}
    replayed = await port.project_search_page(["title"], query, None, {"limit": 3}, snapshot=handle)
    assert list(replayed.hits) == [{"title": t} for t in _TITLES]

    selected = await port.select_search_page(
        _Title, query, None, {"limit": 3}, snapshot={"mode": True}
    )
    assert [hit.title for hit in selected.hits] == _TITLES
    assert selected.snapshot is not None

    # The whole records are in the pool, so a full read replays from the same snapshot.
    full = await port.search_page(query, None, {"limit": 3}, snapshot=handle)
    assert [hit.title for hit in full.hits] == _TITLES


@pytest.mark.parametrize(
    ("engine", "query"),
    [("fts", ""), ("fts", "python"), ("pgroonga", ""), ("pgroonga", "python"), ("hub", "python")],
)
async def test_a_zero_limit_snapshot_page_is_empty(
    pg_client: PostgresClient, redis_client: RedisClient, engine: str, query: str
) -> None:
    """A page of no hits is empty whether it writes the snapshot or replays it, and the
    snapshot it writes still holds the whole pool."""

    port, _ = await _ports(pg_client, redis_client, engine)

    written = await port.search_page(query, None, {"limit": 0}, snapshot={"mode": True})
    assert written.hits == []
    assert written.snapshot is not None

    handle = {"id": written.snapshot.id, "fingerprint": written.snapshot.fingerprint}
    replayed = await port.search_page(query, None, {"limit": 0}, snapshot=handle)
    assert replayed.hits == []

    whole = await port.search_page(query, None, {"limit": 3}, snapshot=handle)
    assert [hit.title for hit in whole.hits] == _TITLES
