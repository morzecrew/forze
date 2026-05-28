"""Mongo ``$text`` search adapter."""

from __future__ import annotations

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import QuerySortExpression
from forze.application.contracts.search import SearchOptions

from ._pipeline import build_text_ranked_pipeline
from ._simple_base import MongoSimpleSearchAdapter

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoTextSearchAdapter[M: BaseModel](MongoSimpleSearchAdapter[M]):
    """Full-text search using a MongoDB compound text index."""

    search_variant: str = attrs.field(default="mongo_text", init=False)

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
        _ = options

        return build_text_ranked_pipeline(
            pre_filter=pre_filter,
            terms=terms,
            combine=combine,  # type: ignore[arg-type]
            user_sorts=self._user_sorts(sorts),
            rank_field=self.rank_field,
        )
