"""A result snapshot replays only for the order it was taken in.

The snapshot is keyed on a fingerprint of the request, which named the request's sorts but
not the order they resolve to. A request with no sort is ordered by the spec's
``default_sort``, so after that default changed, a snapshot taken under the old one replayed
the old order for as long as it lived.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    HubSearchSpec,
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.execution import Deps
from forze_postgres.execution.deps import (
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresSearch,
)
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresSearchConfig,
)
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_redis.execution.deps import ConfigurableRedisSearchResultSnapshot
from forze_redis.execution.deps.configs import RedisSearchResultSnapshotConfig
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.client import RedisClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_TITLES = ["alpha", "beta", "gamma"]
_SNAPSHOT = SearchResultSnapshotSpec(name="snap", enabled=True, ttl=timedelta(minutes=5))
_FTS = FtsEngine(groups={"A": ("title",), "B": ("content",)})


class _Row(BaseModel):
    id: UUID
    title: str
    content: str


class _Leg(BaseModel):
    title: str
    content: str


async def _ports(
    pg_client: PostgresClient, redis_client: RedisClient, engine: str
) -> tuple[Any, Any]:
    """Two ports over the same rows and snapshot store, one ordered ``title`` ascending by
    default and one descending, under the same spec name."""

    table = f"snap_order_{uuid4().hex[:10]}"
    index = f"idx_{table}"
    using = (
        "USING pgroonga ((ARRAY[title, content]))"
        if engine == "pgroonga"
        else "USING gin (to_tsvector('english', title || ' ' || content))"
    )

    await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")
    await pg_client.execute(
        f"CREATE TABLE {table} (id uuid PRIMARY KEY, title text NOT NULL, content text NOT NULL);"
        f"CREATE INDEX {index} ON {table} {using};"
    )

    for title in _TITLES:
        await pg_client.execute(
            f"INSERT INTO {table} (id, title, content) VALUES (%(id)s, %(t)s, 'python')",
            {"id": uuid4(), "t": title},
        )

    ctx = context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config=RedisSearchResultSnapshotConfig(namespace=f"it:{table}"),
                ),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        index=("public", index),
                        read=("public", table),
                        engine=_FTS if engine == "fts" else "pgroonga",
                    )
                ),
            }
        )
    )

    def port(direction: str) -> Any:
        default = {"title": direction}

        if engine != "hub":
            return ctx.search.query(
                SearchSpec(
                    name="rows",
                    model_type=_Row,
                    fields=["title", "content"],
                    default_sort=default,
                    snapshot=_SNAPSHOT,
                )
            )

        leg = SearchSpec(name="leg", model_type=_Leg, fields=["title", "content"])
        hub = ConfigurablePostgresHubSearch(
            config=PostgresHubSearchConfig(
                hub=("public", table),
                members={
                    "leg": PostgresHubSearchMemberConfig(
                        index=("public", index), read=("public", table), hub_fk="id", engine=_FTS
                    )
                },
            )
        )

        return hub(
            ctx,
            HubSearchSpec(
                name="rows",
                model_type=_Row,
                members=(leg,),
                default_sort=default,
                snapshot=_SNAPSHOT,
            ),
        )

    return port("asc"), port("desc")


@pytest.mark.parametrize(
    ("engine", "query"),
    [("fts", ""), ("fts", "python"), ("pgroonga", ""), ("pgroonga", "python"), ("hub", "python")],
)
async def test_a_changed_default_sort_does_not_replay_the_old_order(
    pg_client: PostgresClient, redis_client: RedisClient, engine: str, query: str
) -> None:
    ascending, descending = await _ports(pg_client, redis_client, engine)

    taken = await ascending.search_page(query, None, {"limit": 3}, snapshot={"mode": True})

    assert [hit.title for hit in taken.hits] == _TITLES
    assert taken.snapshot is not None

    handle = {"id": taken.snapshot.id, "fingerprint": taken.snapshot.fingerprint}
    replayed = await ascending.search_page(query, None, {"limit": 3}, snapshot=handle)
    reordered = await descending.search_page(query, None, {"limit": 3}, snapshot=handle)

    assert [hit.title for hit in replayed.hits] == _TITLES
    assert [hit.title for hit in reordered.hits] == list(reversed(_TITLES))
