"""Base gateway for Mongo search adapters."""

from typing import Any, Callable, Mapping, Sequence

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_mongo.kernel.gateways.base import MongoGateway

from .constants import MONGO_RANK_FIELD

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoSearchGateway[M: BaseModel](MongoGateway[M]):
    """Shared collection access and field mapping for Mongo search."""

    spec: SearchSpec[M]
    """Logical search specification."""

    field_map: Mapping[str, str] = attrs.field(factory=dict[str, str])
    """Maps :class:`SearchSpec` field names to BSON paths."""

    tenant_provider: Callable[[], Any] | None = attrs.field(default=None)
    """Optional tenant id provider when :attr:`tenant_aware` is enabled."""

    tenant_aware: bool = attrs.field(default=False)
    """When ``True``, scope queries to the active tenant."""

    # ....................... #

    def physical_path(self, field: str) -> str:
        """Resolve a logical search field to its BSON path."""

        return self.field_map.get(field, field)

    def physical_paths(self, fields: Sequence[str]) -> list[str]:
        """Resolve multiple logical fields to BSON paths."""

        return [self.physical_path(f) for f in fields]

    @property
    def rank_field(self) -> str:
        """Synthetic score column name used in aggregation pipelines."""

        return MONGO_RANK_FIELD
