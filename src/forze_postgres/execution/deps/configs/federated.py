"""Postgres federated search execution configs and validation."""

from collections.abc import Mapping

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ._mapping import frozen_mapping
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

PostgresFederatedSearchLeg = (
    PostgresFederatedSearchLegSearch | PostgresFederatedSearchLegHub
)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresFederatedSearchConfig(TenantAwareIntegrationConfig):
    """Postgres configuration for :class:`PostgresFederatedSearchAdapter`."""

    members: Mapping[StrKey, PostgresFederatedSearchLeg] = attrs.field(
        converter=frozen_mapping,
    )
    """Federated member name to search or embedded hub config."""

    rrf_k: int = 60
    """RRF smoothing constant."""

    rrf_per_leg_limit: int = 5000
    """Max hits fetched per member for merging."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.validate()

    # ....................... #

    def validate(self) -> None:
        """Validate federated member count and nested configs."""

        if len(self.members) < 2:
            raise exc.internal(
                "Federated search requires at least two member configurations."
            )

        for leg in self.members.values():
            if isinstance(leg, PostgresFederatedSearchLegHub):
                leg.hub.validate()
