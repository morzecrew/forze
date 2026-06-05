"""Mongo Atlas Search adapter (``$search`` stage)."""

from __future__ import annotations

from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import QuerySortExpression
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    is_static_named_resource,
)
from forze.base.primitives import OnceCell
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

    _index_name_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    search_variant: str = attrs.field(default="mongo_atlas", init=False)

    # ....................... #

    async def _resolved_index_name(self) -> str:
        async def _factory() -> str:
            tenant_id: UUID | None = None

            if self.tenant_provider is not None:
                tenant = self.tenant_provider()

                if tenant is not None:
                    tenant_id = tenant.tenant_id

            return await resolve_mongo_named_resource(self.index_name, tenant_id)

        # Only memoize tenant-independent (static) index names; a dynamic resolver
        # depends on the bound tenant and the adapter may be shared across tenants.
        return await self._index_name_cell.resolve(
            _factory,
            cache=is_static_named_resource(self.index_name),
        )

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
