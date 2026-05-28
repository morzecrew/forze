"""Integration tests for :class:`~forze_mongo.adapters.search.MongoAtlasSearchAdapter`."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.adapters.search import MongoAtlasSearchAdapter
from forze_mongo.execution.deps.configs import MongoSearchConfig
from forze_mongo.execution.deps.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient

from mongo_search_helpers import wait_search_ready

# ----------------------- #


class AtlasArticle(BaseModel):
    id: UUID
    title: str
    body: str


def _atlas_ctx(
    mongo_client: MongoClient,
    *,
    db_name: str,
    collection: str,
    index_name: str,
) -> ExecutionContext:
    return ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_client,
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


@pytest.mark.mongo_atlas_search
@pytest.mark.asyncio
async def test_mongo_atlas_search_ranks(mongo_atlas_client: MongoClient) -> None:
    db_name = (await mongo_atlas_client.db()).name
    collection = f"search_atlas_{uuid4().hex[:10]}"
    index_name = "default"
    coll = await mongo_atlas_client.collection(collection, db_name=db_name)

    await coll.insert_many(
        [
            {
                "_id": str(uuid4()),
                "id": str(uuid4()),
                "title": "Atlas Search on Mongo",
                "body": "Uses the search aggregation stage",
            },
            {
                "_id": str(uuid4()),
                "id": str(uuid4()),
                "title": "Cooking",
                "body": "Recipes without search jargon",
            },
        ]
    )

    await coll.create_search_index(
        {
            "name": index_name,
            "definition": {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "body": {"type": "string"},
                    },
                }
            },
        }
    )

    await wait_search_ready(mongo_atlas_client, coll, index_name=index_name)

    ctx = _atlas_ctx(
        mongo_atlas_client,
        db_name=db_name,
        collection=collection,
        index_name=index_name,
    )
    spec = SearchSpec(
        name="atlas_articles",
        model_type=AtlasArticle,
        fields=("title", "body"),
    )
    adapter = ctx.search.query(spec)

    assert isinstance(adapter, MongoAtlasSearchAdapter)

    page = await adapter.search_page("mongo")
    assert page.count >= 1
    assert page.hits[0].title == "Atlas Search on Mongo"
