"""Integration tests for :class:`~forze_mongo.adapters.search.MongoTextSearchAdapter`."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.contracts.tenancy import TENANT_ID_FIELD, TenantIdentity
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.adapters.search import MongoTextSearchAdapter
from forze_mongo.execution.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.configs import MongoSearchConfig
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
    tenant_aware: bool = False,
) -> ExecutionContext:
    return context_from_deps(Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(
                        read=(db_name, collection),
                        engine="text",
                        tenant_aware=tenant_aware,
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

    # Fully-tied textScore (identical body) exercises the keyset id tiebreak: with Forze's
    # storage invariant (``_id == id``) the cursor still advances one _id at a time and
    # terminates. Documents must be stored the canonical way (``_id`` equals the domain id).
    await coll.insert_many(
        [
            {"_id": (doc_id := str(uuid4())), "id": doc_id, "title": "search doc", "body": "full text search"}
            for _ in range(7)
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


@pytest.mark.asyncio
async def test_mongo_text_search_tenant_aware_scopes_hits(mongo_client: MongoClient) -> None:
    """Tenant-aware ``$text`` search: the tenant prefilter must ride in the same first
    ``$match`` as ``$text`` (Mongo rejects a pipeline where ``$text`` is not in the
    first stage), and results must be scoped to the active tenant."""

    db_name = (await mongo_client.db()).name
    collection = f"search_text_tenant_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.create_index([("title", "text"), ("body", "text")])

    tenant_a = uuid4()
    tenant_b = uuid4()

    def _doc(tenant: object, title: str) -> dict:
        doc_id = str(uuid4())
        return {
            "_id": doc_id,
            "id": doc_id,
            "title": title,
            "body": "shared searchable corpus",
            TENANT_ID_FIELD: str(tenant),
        }

    await coll.insert_many(
        [
            _doc(tenant_a, "alpha corpus entry"),
            _doc(tenant_a, "another corpus entry"),
            _doc(tenant_b, "corpus entry for the other tenant"),
        ]
    )

    ctx = _search_ctx(mongo_client, db_name=db_name, collection=collection, tenant_aware=True)
    spec = SearchSpec(name="tenant_articles", model_type=SearchArticle, fields=("title", "body"))

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_a)):
        adapter = ctx.search.query(spec)
        assert isinstance(adapter, MongoTextSearchAdapter)

        page = await adapter.search_page("corpus")
        assert page.count == 2
        assert len(page.hits) == 2

        # Cursor path builds from the same first-stage pipeline.
        cursor_page = await adapter.search_cursor("corpus", cursor={"limit": 10})
        assert len(cursor_page.hits) == 2

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_b)):
        adapter_b = ctx.search.query(spec)
        page_b = await adapter_b.search_page("corpus")
        assert page_b.count == 1
        assert page_b.hits[0].title == "corpus entry for the other tenant"


@pytest.mark.asyncio
async def test_mongo_text_search_with_caller_prefilter(mongo_client: MongoClient) -> None:
    """A caller-supplied filter combined with a ``$text`` query must produce a valid
    single first ``$match`` and narrow the hits."""

    db_name = (await mongo_client.db()).name
    collection = f"search_text_filter_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.create_index([("title", "text"), ("body", "text")])

    def _doc(title: str, body: str) -> dict:
        doc_id = str(uuid4())
        return {"_id": doc_id, "id": doc_id, "title": title, "body": body}

    await coll.insert_many(
        [
            _doc("alpha guide", "full text search guide"),
            _doc("beta guide", "full text search guide"),
            _doc("alpha appendix", "unrelated content"),
        ]
    )

    ctx = _search_ctx(mongo_client, db_name=db_name, collection=collection)
    spec = SearchSpec(name="filtered_articles", model_type=SearchArticle, fields=("title", "body"))
    adapter = ctx.search.query(spec)

    page = await adapter.search_page(
        "guide",
        filters={"$values": {"title": {"$eq": "alpha guide"}}},
    )
    assert page.count == 1
    assert page.hits[0].title == "alpha guide"

    cursor_page = await adapter.search_cursor(
        "guide",
        filters={"$values": {"title": {"$eq": "alpha guide"}}},
        cursor={"limit": 10},
    )
    assert [h.title for h in cursor_page.hits] == ["alpha guide"]
