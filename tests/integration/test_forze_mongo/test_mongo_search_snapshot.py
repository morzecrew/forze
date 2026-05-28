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
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.adapters.search import MongoTextSearchAdapter
from forze_mongo.execution.deps.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient
from forze_redis.execution.deps.deps import ConfigurableRedisSearchResultSnapshot
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.platform.client import RedisClient


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
    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                RedisClientDepKey: redis_client,
                SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                    config={"namespace": ns},
                ),
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config={
                        "read": (db_name, collection),
                        "engine": "text",
                    }
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
