"""Unit tests for Mongo search aggregation pipeline builders."""

from forze_mongo.adapters.search._pipeline import (
    build_atlas_ranked_pipeline,
    build_text_ranked_pipeline,
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
