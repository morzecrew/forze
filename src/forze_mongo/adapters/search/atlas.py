"""Mongo Atlas Search adapter (``$search`` stage)."""

from __future__ import annotations

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import QuerySortExpression
from forze.application.contracts.search import SearchOptions

from ._pipeline import build_atlas_ranked_pipeline
from ._simple_base import MongoSimpleSearchAdapter

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoAtlasSearchAdapter[M: BaseModel](MongoSimpleSearchAdapter[M]):
    """Full-text search using Atlas Search (``$search`` aggregation stage)."""

    index_name: str
    """Atlas Search index name (``$search`` stage)."""

    search_variant: str = attrs.field(default="mongo_atlas", init=False)

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
        return build_atlas_ranked_pipeline(
            pre_filter=pre_filter,
            terms=terms,
            combine=combine,  # type: ignore[arg-type]
            index_name=self.index_name,
            spec=self.spec,
            field_map=self.field_map,
            options=options,
            user_sorts=self._user_sorts(sorts),
            rank_field=self.rank_field,
        )
