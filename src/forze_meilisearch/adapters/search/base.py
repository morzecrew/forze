"""Base gateway for Meilisearch search adapters."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
)
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.application.contracts.tenancy.mixins import TenancyMixin
from forze.base.serialization import pydantic_dump
from forze.domain.constants import ID_FIELD
from forze_meilisearch.adapters.search._filter_render import MeilisearchFilterRenderer
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig

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

    # ....................... #

    @property
    def index_uid(self) -> str:
        return self.config["index_uid"]

    @property
    def primary_key(self) -> str:
        return str(self.config.get("primary_key", ID_FIELD))

    @property
    def field_map(self) -> dict[str, str]:
        return dict(self.config.get("field_map") or {})

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
        from forze_meilisearch.adapters.search._search_params import merge_filter_strings

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
