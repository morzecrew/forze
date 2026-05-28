"""Cursor pagination integration tests for Mongo search adapters."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CursorPage
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.execution.deps.configs import MongoSearchConfig
from forze_mongo.execution.deps.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient

from mongo_search_helpers import wait_search_ready


def _search_ctx(
    mongo_client: MongoClient,
    *,
    db_name: str,
    collection: str,
) -> ExecutionContext:
    return ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(
                        read=(db_name, collection),
                        engine="text",
                    )
                ),
            }
        )
    )


class CursorArticle(BaseModel):
    id: UUID
    title: str


@pytest.mark.asyncio
async def test_mongo_text_search_cursor(mongo_client: MongoClient) -> None:
    db_name = (await mongo_client.db()).name
    collection = f"search_cur_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.create_index([("title", "text")])

    ids = [uuid4(), uuid4(), uuid4()]
    await coll.insert_many(
        [
            {"_id": str(i), "id": str(i), "title": f"item {n}"}
            for n, i in enumerate(ids)
        ]
    )

    ctx = _search_ctx(mongo_client, db_name=db_name, collection=collection)
    spec = SearchSpec(name="articles", model_type=CursorArticle, fields=("title",))
    adapter = ctx.search.query(spec)

    first: CursorPage[CursorArticle] = await adapter.search_cursor(
        "item",
        cursor={"limit": 2},
        sorts={"title": "asc"},
    )

    assert len(first.hits) == 2
    assert first.has_more
    assert first.next_cursor is not None

    second = await adapter.search_cursor(
        "item",
        cursor={"after": first.next_cursor, "limit": 2},
        sorts={"title": "asc"},
    )

    assert len(second.hits) >= 1


@pytest.mark.mongo_atlas_search
@pytest.mark.asyncio
async def test_mongo_atlas_search_cursor(mongo_atlas_client: MongoClient) -> None:
    db_name = (await mongo_atlas_client.db()).name
    collection = f"search_atlas_cur_{uuid4().hex[:10]}"
    index_name = "default"
    coll = await mongo_atlas_client.collection(collection, db_name=db_name)

    for n in range(3):
        i = uuid4()
        await coll.insert_one(
            {"_id": str(i), "id": str(i), "title": f"cursor item {n}"}
        )

    await coll.create_search_index(
        {
            "name": index_name,
            "definition": {
                "mappings": {
                    "dynamic": False,
                    "fields": {"title": {"type": "string"}},
                }
            },
        }
    )
    await wait_search_ready(mongo_atlas_client, coll, index_name=index_name)

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_atlas_client,
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(
                        read=(db_name, collection),
                        engine="atlas",
                        index_name=index_name,
                    )
                ),
            }
        )
    )
    spec = SearchSpec(name="cur", model_type=CursorArticle, fields=("title",))
    adapter = ctx.search.query(spec)

    page = await adapter.search_cursor("cursor", cursor={"limit": 2})
    assert len(page.hits) == 2
