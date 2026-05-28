"""Base class for single-index Mongo search adapters."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    effective_phrase_combine,
    normalize_search_queries,
    search_options_for_simple_adapter,
)
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze_mongo.kernel.platform.port import MongoClientPort

from ._cursor_run import execute_mongo_ranked_cursor_search
from ._offset_run import execute_mongo_ranked_offset_search
from .base import MongoSearchGateway
from ._port import MongoSearchPortMixin

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoSimpleSearchAdapter[M: BaseModel](
    MongoSearchGateway[M],
    MongoSearchPortMixin[M],
    SearchQueryPort[M],
):
    """Shared offset/cursor execution for Mongo simple (single-index) search."""

    client: MongoClientPort
    """Mongo client for aggregation queries."""

    snapshot_coord: SearchResultSnapshotCoordinator | None = attrs.field(default=None)
    """Optional result-ID snapshot coordinator."""

    search_variant: str = "mongo"
    """Fingerprint variant label for snapshots."""

    # ....................... #

    async def _ranked_pipeline(
        self,
        *,
        terms: tuple[str, ...],
        combine: str,
        pre_filter: dict[str, Any],
        sorts: QuerySortExpression | None,
        options: SearchOptions | None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    # ....................... #

    def _user_sorts(
        self,
        sorts: QuerySortExpression | None,
    ) -> list[tuple[str, int]] | None:
        return self.render_sorts(sorts)

    # ....................... #

    async def _offset_search_impl(  # type: ignore[override]
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
        return_count: bool = False,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        options = search_options_for_simple_adapter(options)
        terms = tuple(normalize_search_queries(query))
        combine = effective_phrase_combine(options)
        pre_filter = self.render_filters(filters)
        pipeline = await self._ranked_pipeline(
            terms=terms,
            combine=combine,
            pre_filter=pre_filter,
            sorts=sorts,
            options=options,
        )

        return await execute_mongo_ranked_offset_search(
            self,
            client=self.client,
            ranked_pipeline=pipeline,
            query=query,
            filters=filters,
            spec=self.spec,
            variant=self.search_variant,
            fingerprint_extras={"phrase_combine": str(combine)},
            pagination=pagination,
            snapshot=snapshot,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            snapshot_coord=self.snapshot_coord,
        )

    # ....................... #

    async def _cursor_search_impl(  # type: ignore[override]
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        options = search_options_for_simple_adapter(options)
        terms = tuple(normalize_search_queries(query))
        combine = effective_phrase_combine(options)
        pre_filter = self.render_filters(filters)
        pipeline = await self._ranked_pipeline(
            terms=terms,
            combine=combine,
            pre_filter=pre_filter,
            sorts=sorts,
            options=options,
        )

        return await execute_mongo_ranked_cursor_search(
            self,
            client=self.client,
            ranked_pipeline=pipeline,
            terms=terms,
            query=query,
            filters=filters,
            sorts=sorts,
            cursor=cursor,
            return_type=return_type,
            return_fields=return_fields,
        )
