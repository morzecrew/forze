"""Single-index Meilisearch search adapter."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import (
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import (
    SearchOptions,
    SearchQueryPort,
    SearchResultSnapshotOptions,
    search_options_for_simple_adapter,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze_meilisearch.adapters.search._offset_run import (
    execute_meilisearch_offset_search,
)
from forze_meilisearch.adapters.search._port import MeilisearchSearchPortMixin
from forze_meilisearch.adapters.search.base import MeilisearchSearchGateway
from forze_meilisearch.kernel.client.port import MeilisearchClientPort

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchSimpleSearchAdapter[M: BaseModel](
    MeilisearchSearchGateway[M],
    MeilisearchSearchPortMixin[M],
    SearchQueryPort[M],
):
    """Offset search against one Meilisearch index."""

    client: MeilisearchClientPort
    result_snapshot: SearchResultSnapshot | None = attrs.field(default=None)
    search_variant: str = "meilisearch"

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
        combine = (options or {}).get("phrase_combine", "any")

        return await execute_meilisearch_offset_search(
            self,
            client=self.client,
            query=query,
            filters=filters,
            spec=self.spec,
            variant=self.search_variant,
            fingerprint_extras={"phrase_combine": str(combine)},
            pagination=pagination,
            snapshot=snapshot,
            options=options,
            sorts=sorts,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            result_snapshot=self.result_snapshot,
        )
