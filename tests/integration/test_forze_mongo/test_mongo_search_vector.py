"""Integration tests for :class:`~forze_mongo.adapters.search.MongoVectorSearchAdapter`."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_mock import MockHashEmbeddingsProvider
from forze_mongo.adapters.search import MongoVectorSearchAdapter
from forze_mongo.execution.deps.configs import MongoSearchConfig
from forze_mongo.execution.deps.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient

from mongo_search_helpers import wait_vector_index

# ----------------------- #


class VecDoc(BaseModel):
    id: UUID
    label: str


def _embeddings_factory(
    _ctx: ExecutionContext,
    spec: EmbeddingsSpec,
) -> MockHashEmbeddingsProvider:
    return MockHashEmbeddingsProvider(dimensions=spec.dimensions)


@pytest.mark.mongo_atlas_search
@pytest.mark.asyncio
async def test_mongo_vector_search_knn(mongo_atlas_client: MongoClient) -> None:
    db_name = (await mongo_atlas_client.db()).name
    collection = f"search_vec_{uuid4().hex[:10]}"
    index_name = "vec_default"
    path = "embedding"
    coll = await mongo_atlas_client.collection(collection, db_name=db_name)

    prov = MockHashEmbeddingsProvider(dimensions=3)
    v_alpha = await prov.embed_one("alpha")
    v_beta = await prov.embed_one("beta")

    a_id, b_id = uuid4(), uuid4()

    await coll.insert_many(
        [
            {
                "_id": str(a_id),
                "id": str(a_id),
                "label": "alpha",
                "embedding": v_alpha,
            },
            {
                "_id": str(b_id),
                "id": str(b_id),
                "label": "beta",
                "embedding": v_beta,
            },
        ]
    )

    await coll.create_search_index(
        {
            "name": index_name,
            "type": "vectorSearch",
            "definition": {
                "fields": [
                    {
                        "type": "vector",
                        "path": path,
                        "numDimensions": 3,
                        "similarity": "euclidean",
                    }
                ]
            },
        }
    )

    await wait_vector_index(mongo_atlas_client, coll, index_name=index_name, path=path)

    ctx = ExecutionContext(
        deps=Deps.plain(
            {
                MongoClientDepKey: mongo_atlas_client,
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(
                        read=(db_name, collection),
                        engine="vector",
                        vector_path=path,
                        index_name=index_name,
                        embeddings_name="vec_test",
                        embedding_dimensions=3,
                    )
                ),
                EmbeddingsProviderDepKey: _embeddings_factory,
            }
        )
    )

    spec = SearchSpec(name="vec_ns", model_type=VecDoc, fields=("label",))
    adapter = ctx.search.query(spec)

    assert isinstance(adapter, MongoVectorSearchAdapter)

    page = await adapter.search_page("alpha")
    assert page.count >= 1
    assert page.hits[0].label == "alpha"
