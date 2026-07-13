"""Aggregation pipeline builders for Mongo search engines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

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


# ....................... #


def _pre_match_stages(pre_filter: JsonDict) -> list[JsonDict]:
    return [{"$match": pre_filter}] if pre_filter else []


# ....................... #


def build_browse_pipeline(
    *,
    pre_filter: JsonDict,
    user_sorts: list[tuple[str, int]] | None,
    rank_field: str = MONGO_RANK_FIELD,
) -> list[JsonDict]:
    """Filter-only browse pipeline (empty text query)."""

    return [
        *_pre_match_stages(pre_filter),
        {"$addFields": {rank_field: 1}},
        {"$sort": _sort_dict(ranked=False, user_sorts=user_sorts, rank_field=rank_field)},
    ]


# ....................... #


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
        stages.extend(
            (
                {"$match": {"$text": {"$search": search_str}}},
                {
                    "$addFields": {
                        rank_field: {"$meta": "textScore"},
                    }
                },
            )
        )
    else:
        return build_browse_pipeline(
            pre_filter=pre_filter,
            user_sorts=user_sorts,
            rank_field=rank_field,
        )

    stages.append({"$sort": _sort_dict(ranked=True, user_sorts=user_sorts, rank_field=rank_field)})

    return stages


# ....................... #


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
    stages.append({"$sort": _sort_dict(ranked=True, user_sorts=user_sorts, rank_field=rank_field)})

    return stages


# ....................... #


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


# ....................... #


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


# ....................... #


def _is_rank_only_stage(stage: JsonDict, rank_field: str) -> bool:
    """A ``$sort`` or the internal rank ``$addFields`` — pure ordering work."""

    if "$sort" in stage:
        return True

    added = stage.get("$addFields")

    return isinstance(added, Mapping) and set(
        added.keys()  # pyright: ignore[reportUnknownArgumentType]
    ) == {rank_field}


# ....................... #


def thin_ranked_pipeline(
    pipeline: list[JsonDict],
) -> list[JsonDict] | None:
    """Insert a thin ``$project`` (only the sort keys) just before the ``$sort``.

    Late materialization for ranked search: the server then sorts / skips / limits
    lightweight ``{_id, sort-key, rank}`` documents instead of the full heavy
    documents, so a large match set no longer pushes the whole result through the
    100 MB in-memory sort (which otherwise spills to disk). The caller paginates
    this thin pipeline and hydrates only the page's full documents by ``_id``.

    Returns ``None`` when there is no ``$sort`` to thin (e.g. a bare
    ``$vectorSearch`` ordered by the index), so the caller keeps the plain
    full-document fetch.
    """

    for index, stage in enumerate(pipeline):
        sort = stage.get("$sort")

        if sort is not None:
            projection: JsonDict = dict.fromkeys(sort, 1)
            projection["_id"] = 1

            return [*pipeline[:index], {"$project": projection}, *pipeline[index:]]

    return None


# ....................... #


def build_count_pipeline(
    pipeline: list[JsonDict],
    *,
    rank_field: str = MONGO_RANK_FIELD,
) -> list[JsonDict]:
    """Wrap a ranked pipeline in ``$count`` for total hits.

    Counting needs only the match stages, so the rank ``$addFields`` and the
    ``$sort`` are dropped first: ordering the full matched set just to count it is
    pure server-side work (and can spill the in-memory sort on a large match). The
    matcher itself (``$match`` / ``$search`` / ``$vectorSearch``) is kept.
    """

    counted = [stage for stage in pipeline if not _is_rank_only_stage(stage, rank_field)]

    return [*counted, {"$count": "total"}]
