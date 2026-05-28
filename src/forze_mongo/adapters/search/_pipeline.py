"""Aggregation pipeline builders for Mongo search engines."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from forze.application.contracts.search import (
    PhraseCombine,
    SearchOptions,
    calculate_effective_field_weights,
)
from forze.application.contracts.search.specs import SearchSpec
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD

from ._query_build import build_text_search_string
from .constants import MONGO_RANK_FIELD

# ----------------------- #


def _sort_dict(
    *,
    ranked: bool,
    user_sorts: list[tuple[str, int]] | None,
    rank_field: str = MONGO_RANK_FIELD,
) -> JsonDict:
    out: JsonDict = {}

    if ranked:
        out[rank_field] = -1

    if user_sorts:
        for field, direction in user_sorts:
            key = "_id" if field == ID_FIELD else field
            out[key] = direction

    if ID_FIELD not in out and "_id" not in out:
        out["_id"] = 1

    return out


def _pre_match_stages(pre_filter: JsonDict) -> list[JsonDict]:
    if not pre_filter:
        return []

    return [{"$match": pre_filter}]


def build_browse_pipeline(
    *,
    pre_filter: JsonDict,
    user_sorts: list[tuple[str, int]] | None,
    rank_field: str = MONGO_RANK_FIELD,
) -> list[JsonDict]:
    """Filter-only browse pipeline (empty text query)."""

    stages: list[JsonDict] = [*_pre_match_stages(pre_filter)]

    stages.append({"$addFields": {rank_field: 1}})
    stages.append(
        {
            "$sort": _sort_dict(
                ranked=False, user_sorts=user_sorts, rank_field=rank_field
            )
        }
    )

    return stages


def build_text_ranked_pipeline(
    *,
    pre_filter: JsonDict,
    terms: tuple[str, ...],
    combine: PhraseCombine,
    user_sorts: list[tuple[str, int]] | None,
    rank_field: str = MONGO_RANK_FIELD,
) -> list[JsonDict]:
    """Ranked pipeline using a compound ``$text`` index."""

    search_str = build_text_search_string(terms, combine=combine)
    stages: list[JsonDict] = [*_pre_match_stages(pre_filter)]

    if search_str:
        stages.append({"$match": {"$text": {"$search": search_str}}})
        stages.append(
            {
                "$addFields": {
                    rank_field: {"$meta": "textScore"},
                }
            }
        )
    else:
        return build_browse_pipeline(
            pre_filter=pre_filter,
            user_sorts=user_sorts,
            rank_field=rank_field,
        )

    stages.append(
        {"$sort": _sort_dict(ranked=True, user_sorts=user_sorts, rank_field=rank_field)}
    )

    return stages


def build_atlas_ranked_pipeline(
    *,
    pre_filter: JsonDict,
    terms: tuple[str, ...],
    combine: PhraseCombine,
    index_name: str,
    spec: SearchSpec[Any],
    field_map: Mapping[str, str],
    options: SearchOptions | None,
    user_sorts: list[tuple[str, int]] | None,
    rank_field: str = MONGO_RANK_FIELD,
) -> list[JsonDict]:
    """Ranked pipeline with a leading ``$search`` stage (Atlas Search)."""

    if not terms:
        return build_browse_pipeline(
            pre_filter=pre_filter,
            user_sorts=user_sorts,
            rank_field=rank_field,
        )

    weights = calculate_effective_field_weights(spec, options)
    active = [f for f in spec.fields if weights.get(f, 0.0) > 0.0] or list(spec.fields)

    should: list[JsonDict] = []

    for term in terms:
        for field in active:
            path = field_map.get(field, field)
            w = float(weights.get(field, 1.0))
            clause: JsonDict = {
                "text": {
                    "query": term,
                    "path": path,
                }
            }

            if w != 1.0:
                clause["text"]["score"] = {"boost": {"value": w}}

            should.append(clause)

    minimum_should_match = 1 if combine == "any" else len(terms) * len(active)

    search_stage: JsonDict = {
        "$search": {
            "index": index_name,
            "compound": {
                "should": should,
                "minimumShouldMatch": minimum_should_match,
            },
        }
    }

    stages: list[JsonDict] = [search_stage]

    if pre_filter:
        stages.append({"$match": pre_filter})

    stages.append({"$addFields": {rank_field: {"$meta": "searchScore"}}})
    stages.append(
        {"$sort": _sort_dict(ranked=True, user_sorts=user_sorts, rank_field=rank_field)}
    )

    return stages


def build_vector_ranked_pipeline(
    *,
    pre_filter: JsonDict,
    query_vector: Sequence[float],
    index_name: str,
    vector_path: str,
    num_candidates: int,
    limit: int,
    rank_field: str = MONGO_RANK_FIELD,
    user_sorts: list[tuple[str, int]] | None = None,
) -> list[JsonDict]:
    """Ranked pipeline using ``$vectorSearch`` (Atlas Vector Search)."""

    effective_limit = min(limit, num_candidates)

    vs: JsonDict = {
        "index": index_name,
        "path": vector_path,
        "queryVector": list(query_vector),
        "numCandidates": num_candidates,
        "limit": effective_limit,
    }

    if pre_filter:
        vs["filter"] = pre_filter

    stages: list[JsonDict] = [
        {"$vectorSearch": vs},
        {"$addFields": {rank_field: {"$meta": "vectorSearchScore"}}},
    ]

    if user_sorts:
        stages.append(
            {
                "$sort": _sort_dict(
                    ranked=True,
                    user_sorts=user_sorts,
                    rank_field=rank_field,
                )
            }
        )

    return stages


def append_pagination_stages(
    pipeline: list[JsonDict],
    *,
    offset: int,
    limit: int | None,
    rank_field: str = MONGO_RANK_FIELD,
    strip_rank: bool = True,
) -> list[JsonDict]:
    """Append ``$skip`` / ``$limit`` and optionally strip the internal rank field."""

    out = list(pipeline)

    if offset:
        out.append({"$skip": offset})

    if limit is not None:
        out.append({"$limit": limit})

    if strip_rank:
        out.append({"$project": {rank_field: 0}})

    return out


def build_count_pipeline(pipeline: list[JsonDict]) -> list[JsonDict]:
    """Wrap a ranked pipeline in ``$count`` for total hits."""

    return [*pipeline, {"$count": "total"}]
