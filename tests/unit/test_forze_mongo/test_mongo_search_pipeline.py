"""Unit tests for Mongo search aggregation pipeline builders."""

from forze_mongo.adapters.search._pipeline import (
    build_atlas_ranked_pipeline,
    build_count_pipeline,
    build_text_ranked_pipeline,
    thin_ranked_pipeline,
)
from forze_mongo.adapters.search.constants import MONGO_RANK_FIELD
from forze.application.contracts.search import SearchSpec
from pydantic import BaseModel


class _Read(BaseModel):
    id: str
    title: str
    body: str = ""


def test_text_pipeline_includes_text_match_and_score() -> None:
    spec = SearchSpec(name="n", model_type=_Read, fields=("title", "body"))
    pipeline = build_text_ranked_pipeline(
        pre_filter={"status": "active"},
        terms=("hello",),
        combine="any",
        user_sorts=None,
    )

    assert pipeline[0] == {"$match": {"status": "active"}}
    assert pipeline[1] == {"$match": {"$text": {"$search": "hello"}}}
    assert pipeline[2] == {"$addFields": {MONGO_RANK_FIELD: {"$meta": "textScore"}}}


def test_text_empty_query_browse_mode() -> None:
    pipeline = build_text_ranked_pipeline(
        pre_filter={},
        terms=(),
        combine="any",
        user_sorts=[("title", 1)],
    )

    assert pipeline[-1]["$sort"]["title"] == 1
    assert {"$addFields": {MONGO_RANK_FIELD: 1}} in pipeline


def test_atlas_pipeline_starts_with_search_stage() -> None:
    spec = SearchSpec(name="n", model_type=_Read, fields=("title",))
    pipeline = build_atlas_ranked_pipeline(
        pre_filter={},
        terms=("mongo",),
        combine="any",
        index_name="default",
        spec=spec,
        field_map={},
        options=None,
        user_sorts=None,
    )

    assert "$search" in pipeline[0]
    assert pipeline[0]["$search"]["index"] == "default"


def test_count_pipeline_strips_rank_and_sort() -> None:
    pipeline = build_text_ranked_pipeline(
        pre_filter={"status": "active"},
        terms=("hello",),
        combine="any",
        user_sorts=[("title", 1)],
    )

    count = build_count_pipeline(pipeline)

    # Matchers survive; the rank $addFields and $sort (pure ordering) are dropped.
    assert {"$match": {"status": "active"}} in count
    assert {"$match": {"$text": {"$search": "hello"}}} in count
    assert all("$sort" not in stage for stage in count)
    assert all("$addFields" not in stage for stage in count)
    assert count[-1] == {"$count": "total"}


def test_thin_ranked_pipeline_projects_only_sort_keys_before_sort() -> None:
    pipeline = build_text_ranked_pipeline(
        pre_filter={},
        terms=("hello",),
        combine="any",
        user_sorts=[("title", 1)],
    )

    thin = thin_ranked_pipeline(pipeline)
    assert thin is not None

    sort_index = next(i for i, stage in enumerate(thin) if "$sort" in stage)
    projected = thin[sort_index - 1]["$project"]

    # Only the sort keys (rank + user sort + _id) carry into sort/skip/limit.
    assert set(projected) == {MONGO_RANK_FIELD, "title", "_id"}
    # The $sort itself is unchanged and still follows the thin projection.
    assert thin[sort_index] == pipeline[-1]


def test_thin_ranked_pipeline_is_none_without_sort() -> None:
    # A bare $vectorSearch (no $sort) cannot be thinned — caller keeps full fetch.
    unsorted = [{"$vectorSearch": {}}, {"$addFields": {MONGO_RANK_FIELD: 1}}]

    assert thin_ranked_pipeline(unsorted) is None


# ----------------------- #
# `max_candidates` -> Mongo `$vectorSearch.numCandidates`


from unittest.mock import MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.search import SearchQueryDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_mock import MockHashEmbeddingsProvider
from forze_mongo.adapters.search import MongoVectorSearchAdapter
from forze_mongo.execution.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.configs import MongoSearchConfig, MongoVectorEngine
from forze_mongo.execution.deps.keys import MongoClientDepKey
from tests.support.execution_context import context_from_deps


class _VecDoc(BaseModel):
    id: UUID
    label: str


def _embeddings_factory(
    _ctx: ExecutionContext, spec: EmbeddingsSpec
) -> MockHashEmbeddingsProvider:
    return MockHashEmbeddingsProvider(dimensions=spec.dimensions)


def _vector_adapter() -> MongoVectorSearchAdapter[_VecDoc]:
    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: MagicMock(),
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(
                        read=("db", "coll"),
                        engine=MongoVectorEngine(
                            index_name="vec_idx",
                            vector_path="emb",
                            embeddings_name="vt",
                            dimensions=3,
                        ),
                    )
                ),
                EmbeddingsProviderDepKey: _embeddings_factory,
            }
        )
    )
    adapter = ctx.search.query(SearchSpec(name="vn", model_type=_VecDoc, fields=("label",)))
    assert isinstance(adapter, MongoVectorSearchAdapter)
    return adapter


@pytest.mark.asyncio
async def test_mongo_vector_max_candidates_overrides_num_candidates() -> None:
    adapter = _vector_adapter()

    default = await adapter._ranked_pipeline(
        terms=("alpha",), combine="any", pre_filter={}, sorts=None, options=None
    )
    override = await adapter._ranked_pipeline(
        terms=("alpha",),
        combine="any",
        pre_filter={},
        sorts=None,
        options={"max_candidates": 7},
    )

    assert default[0]["$vectorSearch"]["numCandidates"] == 100  # configured default
    assert override[0]["$vectorSearch"]["numCandidates"] == 7  # per-request override


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_options", [{"facets": ["label"]}, {"highlight": True}])
async def test_mongo_search_fails_closed_on_facets_or_highlights(
    bad_options: dict,
) -> None:
    """Mongo search produces no facets/highlights, so requesting them fails closed
    (``query_feature_unsupported``) rather than silently dropping the sidecars."""

    adapter = _vector_adapter()

    with pytest.raises(CoreException, match="not yet supported"):
        await adapter.search_page("alpha", options=bad_options)

    with pytest.raises(CoreException, match="not yet supported"):
        await adapter.search_cursor("alpha", options=bad_options)
