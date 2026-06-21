"""Meilisearch dependency configuration types."""

from typing import TYPE_CHECKING, Any, Literal, Mapping, Sequence

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
)
from forze.application.contracts.search import Rrf
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

if TYPE_CHECKING:
    from forze.application.contracts.search import FederatedSearchSpec

# ----------------------- #

MeilisearchFederatedMerge = Literal["federation", "rrf"]
"""Merge discriminator string (the resolved kind of :attr:`MeilisearchFederatedSearchConfig.merge`)."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchFederation:
    """Native Meilisearch multi-search federation (no coordinator-side fusion)."""


MeilisearchMerge = MeilisearchFederation | Rrf
"""Merge strategy: native ``federation`` or coordinator-side :class:`Rrf`."""


def _coerce_merge(value: "MeilisearchMerge | MeilisearchFederatedMerge") -> MeilisearchMerge:
    """Normalize the ``merge=`` argument; bare strings are accepted as a shorthand."""

    if isinstance(value, (MeilisearchFederation, Rrf)):
        return value

    if value == "federation":
        return MeilisearchFederation()

    if value == "rrf":
        return Rrf()

    raise exc.configuration(
        f"Meilisearch federated merge {value!r} must be 'federation' or 'rrf'.",
    )


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

    merge_spec: MeilisearchMerge = attrs.field(
        alias="merge",
        factory=MeilisearchFederation,
        converter=_coerce_merge,  # type: ignore[misc]
    )
    """Merge strategy: :class:`MeilisearchFederation` (native multi-search, the default) or
    :class:`Rrf` (coordinator-side fusion). ``"federation"`` / ``"rrf"`` are accepted as
    shorthands. Read :attr:`merge` for the resolved discriminator string."""

    # ....................... #

    @property
    def merge(self) -> MeilisearchFederatedMerge:
        """Resolved merge discriminator string (``federation`` / ``rrf``)."""

        return "rrf" if isinstance(self.merge_spec, Rrf) else "federation"

    @property
    def rrf_k(self) -> int:
        return self.merge_spec.k if isinstance(self.merge_spec, Rrf) else 60

    @property
    def rrf_per_leg_limit(self) -> int:
        return self.merge_spec.per_leg_limit if isinstance(self.merge_spec, Rrf) else 5000

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if len(self.members) < 2:
            raise exc.configuration(
                "Federated Meilisearch search requires at least two member configurations.",
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
