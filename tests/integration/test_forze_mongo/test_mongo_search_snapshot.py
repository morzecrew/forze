"""Mongo text search with Redis result-ID snapshot materialization."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.execution import Deps
from forze_mongo.adapters.search import MongoTextSearchAdapter
from forze_mongo.execution.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.configs import MongoSearchConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from forze_redis.execution.deps import ConfigurableRedisSearchResultSnapshot
from forze_redis.execution.deps.configs import RedisSearchResultSnapshotConfig
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.client import RedisClient
from tests.support.execution_context import context_from_deps


class SnapRow(BaseModel):
    id: UUID
    title: str


@pytest.mark.asyncio
async def test_mongo_text_result_snapshot_reread(
    mongo_client: MongoClient,
    redis_client: RedisClient,
) -> None:
    db_name = (await mongo_client.db()).name
    collection = f"search_snap_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.create_index([("title", "text")])

    rid = uuid4()
    await coll.insert_one(
        {
            "_id": str(rid),
            "id": str(rid),
            "title": "snapshot mongo search",
        }
    )

    ns = f"it:mongo:rss:{uuid4().hex[:10]}"
    ctx = context_from_deps(Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config=RedisSearchResultSnapshotConfig(namespace=ns),
                ),
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(
                        read=(db_name, collection),
                        engine="text",
                    )
                ),
            }
        )
    )

    spec = SearchSpec(
        name="snap_ns",
        model_type=SnapRow,
        fields=("title",),
        snapshot=SearchResultSnapshotSpec(
            name="snap_ns",
            enabled=True,
            ttl=timedelta(minutes=5),
        ),
    )
    adapter = ctx.search.query(spec)
    assert isinstance(adapter, MongoTextSearchAdapter)

    first = await adapter.search_page(
        "snapshot",
        pagination={"limit": 10, "offset": 0},
        snapshot={"mode": True},
    )
    assert first.count == 1
    assert first.snapshot is not None

    second = await adapter.search_page(
        "snapshot",
        pagination={"limit": 10, "offset": 0},
        snapshot={"id": first.snapshot.id},
    )
    assert second.count == 1
    assert len(second.hits) == 1


@pytest.mark.asyncio
async def test_a_snapshot_replays_only_for_the_order_it_was_taken_in(
    mongo_client: MongoClient,
    redis_client: RedisClient,
) -> None:
    """A request that sorts differently, or whose default sort changed, runs live."""

    db_name = (await mongo_client.db()).name
    collection = f"search_snap_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.create_index([("title", "text")])
    titles = ["alpha snap", "beta snap", "gamma snap"]

    for title in titles:
        rid = str(uuid4())
        await coll.insert_one({"_id": rid, "id": rid, "title": title})

    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config=RedisSearchResultSnapshotConfig(namespace=f"it:{collection}"),
                ),
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(read=(db_name, collection), engine="text")
                ),
            }
        )
    )

    def port(direction: str):
        return ctx.search.query(
            SearchSpec(
                name="snap_order",
                model_type=SnapRow,
                fields=("title",),
                default_sort={"title": direction},
                snapshot=SearchResultSnapshotSpec(
                    name="snap_order", enabled=True, ttl=timedelta(minutes=5)
                ),
            )
        )

    ascending = port("asc")
    taken = await ascending.search_page("snap", None, {"limit": 3}, snapshot={"mode": True})

    assert [hit.title for hit in taken.hits] == titles
    assert taken.snapshot is not None

    handle = {"id": taken.snapshot.id, "fingerprint": taken.snapshot.fingerprint}
    resorted = await ascending.search_page(
        "snap", None, {"limit": 3}, {"title": "desc"}, snapshot=handle
    )
    redefaulted = await port("desc").search_page("snap", None, {"limit": 3}, snapshot=handle)

    assert [hit.title for hit in resorted.hits] == list(reversed(titles))
    assert [hit.title for hit in redefaulted.hits] == list(reversed(titles))
