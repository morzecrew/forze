"""Offset pagination execution for Mongo ranked search."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

# pyright: reportPrivateUsage=false
import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
)
from forze.application.contracts.search import (
    SearchResultSnapshotOptions,
    SearchSpec,
    normalize_search_queries,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.application.integrations.search.offset_executor import (
    OffsetFetchWindow,
    OffsetRowsResult,
    execute_simple_offset_search_with_snapshot,
)
from forze.base.primitives import JsonDict
from forze_mongo.kernel.client.port import MongoClientPort

from ._pipeline import (
    append_pagination_stages,
    build_count_pipeline,
    thin_ranked_pipeline,
)
from .base import MongoSearchGateway

# ----------------------- #


@attrs.define(slots=True)
class _MongoOffsetHooks:
    gw: MongoSearchGateway[Any]
    client: MongoClientPort
    ranked_pipeline: list[JsonDict]
    pagination_dict: dict[str, Any]
    return_count: bool
    is_ranked: bool = True

    _coll: Any = attrs.field(default=None, init=False)

    async def _collection(self) -> Any:
        if self._coll is None:
            self._coll = await self.gw.coll()

        return self._coll

    async def fetch_count(self) -> int | None:
        if not self.return_count:
            return None

        coll = await self._collection()
        count_rows = await self.client.aggregate(
            coll,
            build_count_pipeline(self.ranked_pipeline),
            limit=1,
        )

        return int(count_rows[0]["total"]) if count_rows else 0

    async def fetch_rows(
        self,
        window: OffsetFetchWindow,
        *,
        want_snap: bool,
    ) -> OffsetRowsResult:
        _ = want_snap
        coll = await self._collection()
        offset = window.fetch_offset
        limit = int(window.fetch_limit) if window.fetch_limit is not None else None
        rank_field = self.gw.rank_field

        thin = thin_ranked_pipeline(self.ranked_pipeline)

        if thin is None:
            # No $sort to thin (e.g. a bare $vectorSearch ordered by the index):
            # keep the plain full-document fetch. Keep the rank column so the per-hit
            # score can be surfaced, then strip it before normalizing to the read model.
            data_pipeline = append_pagination_stages(
                self.ranked_pipeline,
                offset=offset,
                limit=limit,
                strip_rank=not self.is_ranked,
            )
            rows = await self.client.aggregate(coll, data_pipeline, limit=None)
            scores = self._take_scores(rows, rank_field)

            return OffsetRowsResult(rows=self._normalize(rows), scores=scores)

        # Late materialization: rank/sort/skip/limit lightweight {_id, sort-key}
        # docs, then hydrate only this window's full documents by _id — the
        # server-side sort never runs over the heavy documents.
        thin_paged = append_pagination_stages(
            thin,
            offset=offset,
            limit=limit,
            strip_rank=False,
        )
        thin_rows = await self.client.aggregate(coll, thin_paged, limit=None)
        ordered_ids = [r["_id"] for r in thin_rows]

        if not ordered_ids:
            return OffsetRowsResult(rows=[])

        full = await self.client.find_many(coll, {"_id": {"$in": ordered_ids}})
        by_id = {doc["_id"]: doc for doc in full}
        hydrated = [by_id[_id] for _id in ordered_ids if _id in by_id]

        # The heavy documents were fetched by ``find_many`` (no rank); re-align the thin
        # scan's rank to the hydrated order by ``_id`` (same ``_id in by_id`` filter as
        # ``hydrated``, so the two stay index-aligned).
        thin_scores = None
        if self.is_ranked:
            rank_by_id = {r["_id"]: r.get(rank_field, 0.0) for r in thin_rows}
            thin_scores = [float(rank_by_id[_id]) for _id in ordered_ids if _id in by_id]

        return OffsetRowsResult(rows=self._normalize(hydrated), scores=thin_scores)

    def _take_scores(self, rows: list[JsonDict], rank_field: str) -> list[float] | None:
        """Pop the rank column off each row into an index-aligned score list."""

        if not self.is_ranked:
            for row in rows:
                row.pop(rank_field, None)
            return None

        scores = [float(row.get(rank_field, 0.0)) for row in rows]

        for row in rows:
            row.pop(rank_field, None)

        return scores

    def _normalize(self, rows: list[JsonDict]) -> list[JsonDict]:
        return [
            self.gw._from_storage_doc(r)  # pyright: ignore[reportPrivateUsage]
            for r in rows
        ]


# ....................... #


async def execute_mongo_ranked_offset_search[M: BaseModel](
    gw: MongoSearchGateway[M],
    *,
    client: MongoClientPort,
    ranked_pipeline: list[JsonDict],
    query: str | Sequence[str],
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    spec: SearchSpec[Any],
    variant: str,
    fingerprint_extras: dict[str, object] | None,
    pagination: PaginationExpression | None,
    snapshot: SearchResultSnapshotOptions | None,
    return_count: bool,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    result_snapshot: SearchResultSnapshot | None,
) -> Any:
    """Run count (optional), aggregation fetch, and snapshot materialization."""

    pagination_dict: dict[str, Any] = dict(pagination or {})

    return await execute_simple_offset_search_with_snapshot(
        query=query,
        filters=filters,
        sorts=None,
        fingerprint_sorts=None,
        spec=spec,
        variant=variant,
        fingerprint_extras=fingerprint_extras,
        pagination=pagination,
        snapshot=snapshot,
        return_count=return_count,
        return_type=return_type,
        return_fields=return_fields,
        model_type=gw.model_type,
        codec=gw.spec.resolved_read_codec,
        result_snapshot=result_snapshot,
        hooks=_MongoOffsetHooks(
            gw=gw,
            client=client,
            ranked_pipeline=ranked_pipeline,
            pagination_dict=pagination_dict,
            return_count=return_count,
            is_ranked=bool(normalize_search_queries(query)),
        ),
    )
