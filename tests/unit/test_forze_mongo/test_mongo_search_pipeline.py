"""Unit tests for Mongo search aggregation pipeline builders."""

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_mongo.adapters.search._pipeline import (
    build_atlas_ranked_pipeline,
    build_count_pipeline,
    build_text_ranked_pipeline,
    thin_ranked_pipeline,
)
from forze_mongo.adapters.search.constants import MONGO_RANK_FIELD


class _Read(BaseModel):
    id: str
    title: str
    body: str = ""


def test_text_pipeline_merges_prefilter_into_first_text_match() -> None:
    """``$text`` is only valid in the FIRST ``$match`` stage, so the prefilter
    (tenant scope / caller filters) must share that stage rather than precede it."""

    pipeline = build_text_ranked_pipeline(
        pre_filter={"status": "active"},
        terms=("hello",),
        combine="any",
        user_sorts=None,
    )

    assert pipeline[0] == {"$match": {"$text": {"$search": "hello"}, "status": "active"}}
    assert pipeline[1] == {"$addFields": {MONGO_RANK_FIELD: {"$meta": "textScore"}}}
    # No other $match carries $text or the prefilter.
    assert all("$match" not in stage for stage in pipeline[1:])


def test_text_pipeline_without_prefilter_still_leads_with_text_match() -> None:
    pipeline = build_text_ranked_pipeline(
        pre_filter={},
        terms=("hello",),
        combine="any",
        user_sorts=None,
    )

    assert pipeline[0] == {"$match": {"$text": {"$search": "hello"}}}
    assert pipeline[1] == {"$addFields": {MONGO_RANK_FIELD: {"$meta": "textScore"}}}


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

    # The single first-stage matcher ($text + prefilter) survives; the rank
    # $addFields and $sort (pure ordering) are dropped.
    assert count[0] == {"$match": {"$text": {"$search": "hello"}, "status": "active"}}
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


from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.embeddings import EmbeddingsProviderDepKey, EmbeddingsSpec
from forze.application.contracts.search import SearchQueryDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_mock import MockHashEmbeddingsProvider
from forze_mongo.adapters.search import MongoVectorSearchAdapter
from forze_mongo.adapters.search import _simple_base as simple_base
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


@pytest.mark.asyncio
async def test_mongo_vector_ranks_a_query_vector_without_the_embedder() -> None:
    adapter = _vector_adapter()
    qvec = [0.5, 0.25, 0.125]

    pipeline = await adapter._ranked_pipeline(
        terms=(), combine="any", pre_filter={}, sorts=None, options={"query_vector": qvec}
    )

    assert pipeline[0]["$vectorSearch"]["queryVector"] == qvec
    assert adapter._ranks((), {"query_vector": qvec})
    assert not adapter._ranks((), None)


@pytest.mark.asyncio
async def test_mongo_vector_refuses_a_query_vector_of_the_wrong_length() -> None:
    adapter = _vector_adapter()

    with pytest.raises(CoreException) as caught:
        await adapter.search("", options={"query_vector": [0.1]})

    assert caught.value.kind.value == "validation"


@pytest.mark.asyncio
async def test_mongo_vector_refuses_a_query_vector_alongside_a_query() -> None:
    adapter = _vector_adapter()

    with pytest.raises(CoreException) as caught:
        await adapter.search("alpha", options={"query_vector": [0.1, 0.2, 0.3]})

    assert caught.value.code == "search_query_vector_with_query"


def _answering(adapter: MongoVectorSearchAdapter[_VecDoc], doc_id: UUID) -> AsyncMock:
    # One document: the ranked row the aggregate returns, and the row a thin page hydrates.
    doc = {"_id": str(doc_id), "id": str(doc_id), "label": "a"}
    aggregate = AsyncMock(return_value=[{**doc, MONGO_RANK_FIELD: 0.9}])
    adapter.client.aggregate = aggregate  # type: ignore[method-assign]
    adapter.client.find_many = AsyncMock(return_value=[doc])  # type: ignore[method-assign]
    adapter.client.collection = AsyncMock(return_value=MagicMock())  # type: ignore[method-assign]
    return aggregate


@pytest.mark.asyncio
async def test_mongo_vector_searches_by_a_query_vector_through_the_port() -> None:
    adapter = _vector_adapter()
    doc_id = UUID("00000000-0000-4000-8000-000000000001")
    aggregate = _answering(adapter, doc_id)
    qvec = [0.5, 0.25, 0.125]

    page = await adapter.search("", options={"query_vector": qvec})
    cursor_page = await adapter.search_cursor(
        "", cursor={"limit": 5}, options={"query_vector": qvec}
    )

    assert [hit.id for hit in page.hits] == [doc_id]
    assert page.scores == [0.9]
    assert [hit.id for hit in cursor_page.hits] == [doc_id]
    assert cursor_page.scores == [0.9]

    for call in aggregate.call_args_list:
        assert call[0][1][0]["$vectorSearch"]["queryVector"] == qvec


@pytest.mark.asyncio
async def test_mongo_vector_browses_an_explicit_none_query_vector() -> None:
    adapter = _vector_adapter()
    aggregate = _answering(adapter, UUID("00000000-0000-4000-8000-000000000002"))

    page = await adapter.search("", options={"query_vector": None})  # type: ignore[typeddict-item]

    assert page.scores is None
    assert "$vectorSearch" not in aggregate.call_args[0][1][0]


@pytest.mark.asyncio
async def test_mongo_vector_fingerprints_tell_query_vectors_apart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = AsyncMock()
    monkeypatch.setattr(simple_base, "execute_mongo_ranked_offset_search", run)
    adapter = _vector_adapter()

    for qvec in ([0.1, 0.2, 0.3], [0.3, 0.2, 0.1]):
        await adapter._offset_search_impl("", options={"query_vector": qvec}, return_count=False)

    one, other = (call.kwargs["fingerprint_extras"] for call in run.call_args_list)

    assert one["query_vector"] == [0.1, 0.2, 0.3]
    assert one != other
