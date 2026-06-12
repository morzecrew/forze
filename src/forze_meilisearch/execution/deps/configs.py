"""Meilisearch dependency configuration types."""

from typing import TYPE_CHECKING, Any, Literal, Mapping, Sequence

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

if TYPE_CHECKING:
    from forze.application.contracts.search import FederatedSearchSpec

# ----------------------- #

MeilisearchFederatedMerge = Literal["federation", "rrf"]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchSearchConfig(TenantAwareIntegrationConfig):
    """Physical Meilisearch mapping for one :class:`~forze.application.contracts.search.SearchSpec`."""

    index_uid: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """Meilisearch index UID (static or tenant-scoped resolver)."""

    primary_key: str = "id"
    """Document primary key attribute."""

    field_map: Mapping[str, str] | None = None
    """Maps logical :class:`SearchSpec` field names to index attribute names."""

    searchable_attributes: Sequence[str] | None = None
    """Override searchable attributes for ensure_index."""

    filterable_attributes: Sequence[str] | None = None
    """Override filterable attributes for ensure_index."""

    sortable_attributes: Sequence[str] | None = None
    """Override sortable attributes for ensure_index."""

    ranking_rules: Sequence[str] | None = None
    """Optional Meilisearch ranking rules."""

    wait_for_tasks: bool = True
    """When True, await Meilisearch task completion after writes."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if is_static_named_resource(self.index_uid) and not self.index_uid:
            raise exc.configuration("Meilisearch search config requires index_uid.")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchFederatedSearchConfig(TenantAwareIntegrationConfig):
    """Configuration for federated Meilisearch search."""

    members: StrKeyMapping[MeilisearchSearchConfig] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Per-member index configuration (keys are :class:`SearchSpec` names)."""

    merge: MeilisearchFederatedMerge = "federation"
    """``federation`` uses Meilisearch multi-search; ``rrf`` uses coordinator RRF."""

    rrf_k: int = 60
    """RRF smoothing constant when ``merge`` is ``rrf``."""

    rrf_per_leg_limit: int = 5000
    """Max hits per leg when ``merge`` is ``rrf``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.members) < 2:
            raise exc.configuration(
                "Federated Meilisearch search requires at least two member configurations.",
            )

        if self.merge not in ("federation", "rrf"):
            raise exc.configuration(
                f"Meilisearch federated merge {self.merge!r} must be 'federation' or 'rrf'.",
            )

    # ....................... #

    def validate_against_spec(self, spec: "FederatedSearchSpec[Any]") -> None:
        from forze.application.contracts.search import HubSearchSpec

        for member in spec.members:
            if isinstance(member, HubSearchSpec):
                raise exc.configuration(
                    f"Federated Meilisearch search does not support hub member {member.name!r}.",
                )

            if member.name not in self.members:
                raise exc.configuration(
                    f"Federated member {member.name!r} missing from MeilisearchFederatedSearchConfig.members.",
                )

            pass
