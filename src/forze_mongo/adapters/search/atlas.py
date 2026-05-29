"""Mongo Atlas Search adapter (``$search`` stage)."""

from __future__ import annotations

from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import QuerySortExpression
from forze.application.contracts.resolution import NamedResourceSpec
from forze_mongo.kernel.relation import resolve_mongo_named_resource
from forze.application.contracts.search import SearchOptions

from ._pipeline import build_atlas_ranked_pipeline
from ._simple_base import MongoSimpleSearchAdapter

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoAtlasSearchAdapter[M: BaseModel](MongoSimpleSearchAdapter[M]):
    """Full-text search using Atlas Search (``$search`` aggregation stage)."""

    index_name: NamedResourceSpec
    """Atlas Search index name (``$search`` stage)."""

    _index_name_resolved: str | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    search_variant: str = attrs.field(default="mongo_atlas", init=False)

    # ....................... #

    async def _resolved_index_name(self) -> str:
        if self._index_name_resolved is not None:
            return self._index_name_resolved

        tenant_id: UUID | None = None

        if self.tenant_provider is not None:
            tenant = self.tenant_provider()

            if tenant is not None:
                tenant_id = tenant.tenant_id

        resolved = await resolve_mongo_named_resource(self.index_name, tenant_id)
        object.__setattr__(self, "_index_name_resolved", resolved)

        return resolved

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
            index_name=await self._resolved_index_name(),
            spec=self.spec,
            field_map=self.field_map,
            options=options,
            user_sorts=self._user_sorts(sorts),
            rank_field=self.rank_field,
        )
