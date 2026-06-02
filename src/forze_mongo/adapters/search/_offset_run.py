"""Offset pagination execution for Mongo ranked search."""

from __future__ import annotations

from typing import Any, Sequence

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
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.application.integrations.search.offset_executor import (
    OffsetFetchWindow,
    OffsetRowsResult,
    execute_simple_offset_search_with_snapshot,
    offset_from_dict,
)
from forze.base.primitives import JsonDict
from forze_mongo.kernel.client.port import MongoClientPort

from ._pipeline import append_pagination_stages, build_count_pipeline
from .base import MongoSearchGateway

# ----------------------- #


@attrs.define(slots=True)
class _MongoOffsetHooks:
    gw: MongoSearchGateway[Any]
    client: MongoClientPort
    ranked_pipeline: list[JsonDict]
    pagination_dict: dict[str, Any]
    return_count: bool

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
        coll = await self._collection()
        offset = (
            window.fetch_offset
            if want_snap
            else offset_from_dict(self.pagination_dict)
        )
        limit = (
            window.fetch_limit
            if want_snap
            else self.pagination_dict.get("limit")
        )

        data_pipeline = append_pagination_stages(
            self.ranked_pipeline,
            offset=offset,
            limit=int(limit) if limit is not None else None,
        )
        rows = await self.client.aggregate(coll, data_pipeline, limit=None)
        normalized = [
            self.gw._from_storage_doc(r)  # pyright: ignore[reportPrivateUsage]
            for r in rows
        ]

        return OffsetRowsResult(rows=normalized)


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
        ),
    )
