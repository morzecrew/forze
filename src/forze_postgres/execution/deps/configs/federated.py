"""Postgres federated search execution configs and validation."""

import attrs

from forze.application.contracts.search import Rrf
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

from .hub import PostgresHubSearchConfig
from .search import PostgresSearchConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFederatedSearchLegSearch:
    """Federated member wired as a single-index Postgres search."""

    search: PostgresSearchConfig


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFederatedSearchLegHub:
    """Federated member wired as an embedded Postgres hub search."""

    hub: PostgresHubSearchConfig


# ....................... #

PostgresFederatedSearchLeg = PostgresFederatedSearchLegSearch | PostgresFederatedSearchLegHub

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFederatedSearchConfig(TenantAwareIntegrationConfig):
    """Postgres configuration for :class:`PostgresFederatedSearchAdapter`."""

    members: StrKeyMapping[PostgresFederatedSearchLeg] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Federated member name to search or embedded hub config."""

    rrf: Rrf = attrs.field(factory=Rrf)
    """Reciprocal Rank Fusion settings (smoothing constant + per-leg fetch cap)."""

    # ....................... #

    @property
    def rrf_k(self) -> int:
        return self.rrf.k

    @property
    def rrf_per_leg_limit(self) -> int:
        return self.rrf.per_leg_limit

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.validate()

    # ....................... #

    def validate(self) -> None:
        """Validate federated member count and nested configs."""

        if len(self.members) < 2:
            raise exc.internal("Federated search requires at least two member configurations.")

        for leg in self.members.values():
            if isinstance(leg, PostgresFederatedSearchLegHub):
                leg.hub.validate()
