"""Base gateway for Meilisearch search adapters."""

from __future__ import annotations

from typing import Any, Callable, Sequence
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
)
from forze.application.contracts.resolution import is_static_named_resource
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.application.contracts.tenancy.mixins import TenancyMixin
from forze.base.exceptions import exc
from forze.base.serialization import pydantic_dump
from forze.domain.constants import ID_FIELD
from forze_meilisearch.adapters.search._filter_render import MeilisearchFilterRenderer
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig
from forze_meilisearch.kernel.relation import resolve_meilisearch_index_uid

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchSearchGateway[M: BaseModel](TenancyMixin):
    """Shared index mapping and filter rendering for Meilisearch search."""

    spec: SearchSpec[M]
    """Logical search specification."""

    config: MeilisearchSearchConfig
    """Physical Meilisearch mapping."""

    tenant_provider: Callable[[], Any] | None = attrs.field(default=None)
    tenant_aware: bool = attrs.field(default=False)

    filter_parser: QueryFilterExpressionParser = attrs.field(
        factory=lambda: QueryFilterExpressionParser(limits=QueryFilterLimits()),
        init=False,
    )

    _index_uid_resolved: str | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        if tenant is None:
            if self.tenant_aware:
                raise exc.internal("Tenant ID is required for Meilisearch search")

            return None

        return tenant.tenant_id

    # ....................... #

    async def _resolved_index_uid(self) -> str:
        if self._index_uid_resolved is not None:
            return self._index_uid_resolved

        resolved = await resolve_meilisearch_index_uid(
            self.config.index_uid,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_index_uid_resolved", resolved)

        return resolved

    # ....................... #

    @property
    def index_uid(self) -> str:
        """Best-effort sync access when config ``index_uid`` is static."""

        spec = self.config.index_uid

        if is_static_named_resource(spec):
            return spec

        if self._index_uid_resolved is not None:
            return self._index_uid_resolved

        raise exc.internal(
            "index_uid is only available for static index UIDs; await _resolved_index_uid()",
        )

    @property
    def primary_key(self) -> str:
        return self.config.primary_key

    @property
    def field_map(self) -> dict[str, str]:
        return dict(self.config.field_map or {})

    @property
    def filter_renderer(self) -> MeilisearchFilterRenderer:
        return MeilisearchFilterRenderer(field_map=self.field_map)

    # ....................... #

    def physical_path(self, field: str) -> str:
        return self.field_map.get(field, field)

    def physical_paths(self, fields: Sequence[str]) -> list[str]:
        return [self.physical_path(f) for f in fields]

    # ....................... #

    def build_filter(
        self,
        filters: QueryFilterExpression | None,
    ) -> str | None:
        from forze_meilisearch.adapters.search._search_params import (
            merge_filter_strings,
        )

        base = self.filter_renderer.render_filters(filters)
        tenant = self._tenant_filter()
        return merge_filter_strings(base, tenant)

    # ....................... #

    def _tenant_filter(self) -> str | None:
        if not self.tenant_aware:
            return None

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is None:
            return None

        attr = self.physical_path(TENANT_ID_FIELD)
        return f'{attr} = "{tenant_id}"'

    # ....................... #

    def to_index_document(self, model: M) -> dict[str, Any]:
        data = pydantic_dump(model)
        out: dict[str, Any] = {}

        for key, value in data.items():
            phys = self.physical_path(key)
            out[phys] = value

        pk = self.primary_key
        pk_val = out.get(pk, data.get(ID_FIELD, data.get("id")))

        if pk_val is not None:
            out[pk] = pk_val

        return out

    def from_hit(self, hit: dict[str, Any]) -> dict[str, Any]:
        inv = {v: k for k, v in self.field_map.items()}
        out: dict[str, Any] = {}

        for key, value in hit.items():
            if key.startswith("_"):
                continue

            logical = inv.get(key, key)
            out[logical] = value

        return out
