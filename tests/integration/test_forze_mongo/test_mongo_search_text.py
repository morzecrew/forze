"""Integration tests for :class:`~forze_mongo.adapters.search.MongoTextSearchAdapter`."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.adapters.search import MongoTextSearchAdapter
from forze_mongo.execution.deps.configs import MongoSearchConfig
from forze_mongo.execution.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps


class SearchArticle(BaseModel):
    id: UUID
    title: str
    body: str


def _search_ctx(
    mongo_client: MongoClient,
    *,
    db_name: str,
    collection: str,
) -> ExecutionContext:
    return context_from_deps(Deps.plain(
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


@pytest.mark.asyncio
async def test_mongo_text_search_ranks_and_paginates(mongo_client: MongoClient) -> None:
    db_name = (await mongo_client.db()).name
    collection = f"search_text_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)

    await coll.create_index([("title", "text"), ("body", "text")])

    docs = [
        {
            "_id": str(uuid4()),
            "id": str(uuid4()),
            "title": "MongoDB text search",
            "body": "Full text on a compound index",
        },
        {
            "_id": str(uuid4()),
            "id": str(uuid4()),
            "title": "Cooking",
            "body": "Recipes without database jargon",
        },
    ]
    await coll.insert_many(docs)

    ctx = _search_ctx(mongo_client, db_name=db_name, collection=collection)
    spec = SearchSpec(
        name="articles",
        model_type=SearchArticle,
        fields=("title", "body"),
    )
    adapter = ctx.search.query(spec)

    assert isinstance(adapter, MongoTextSearchAdapter)

    page = await adapter.search_page("mongodb search")
    assert page.count == 1
    assert len(page.hits) == 1
    assert page.hits[0].title == "MongoDB text search"
    # Per-hit textScore is surfaced, index-aligned with hits.
    assert page.scores is not None
    assert len(page.scores) == len(page.hits)
    assert page.scores[0] > 0.0

    empty = await adapter.search_page("zzznotfound")
    assert empty.count == 0
    assert empty.hits == []


@pytest.mark.asyncio
async def test_mongo_text_search_stream_exports_in_bounded_chunks(
    mongo_client: MongoClient,
) -> None:
    """search_stream loops the Mongo keyset cursor, yielding bounded chunks."""
    db_name = (await mongo_client.db()).name
    collection = f"search_stream_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.create_index([("title", "text"), ("body", "text")])

    # Distinct relevance per doc (varying term frequency) so the keyset advances cleanly —
    # a realistic export set rather than fully-tied scores (which the max_pages guard bounds).
    await coll.insert_many(
        [
            {
                "_id": str(uuid4()),
                "id": str(uuid4()),
                "title": f"search doc {i}",
                "body": " ".join(["search"] * (i + 1)),
            }
            for i in range(7)
        ]
    )

    ctx = _search_ctx(mongo_client, db_name=db_name, collection=collection)
    spec = SearchSpec(name="stream_articles", model_type=SearchArticle, fields=("title", "body"))
    adapter = ctx.search.query(spec)
    assert adapter.search_capabilities.supports_stream is True

    chunks = [chunk async for chunk in adapter.search_stream("search", chunk_size=3)]

    assert [len(c) for c in chunks] == [3, 3, 1]
    ids = {h.id for chunk in chunks for h in chunk}
    assert len(ids) == 7
