"""Validate Postgres tenant isolation wiring (client vs per-route tenant_aware)."""

from typing import Literal, Sequence

import attrs

from forze.application.contracts.tenancy import (
    TenancyRouteSpec,
    TenantIsolationMode,
    derive_tenant_isolation_mode,
    validate_routed_client_tenancy_wiring,
)

from forze_postgres.kernel._logger import logger

# ----------------------- #

PostgresTenantIsolationMode = TenantIsolationMode

PostgresTenancyRouteKind = Literal[
    "document",
    "search",
    "hub_search",
    "federated_search",
    "analytics",
]

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresTenancyRouteSpec:
    """One registered Postgres route and its row-level tenant flag."""

    name: str
    tenant_aware: bool
    kind: PostgresTenancyRouteKind

    def to_contract(self) -> TenancyRouteSpec:
        return TenancyRouteSpec(
            name=self.name,
            tenant_aware=self.tenant_aware,
            kind=self.kind,
        )


# ....................... #


def derive_postgres_tenant_isolation_mode(
    *,
    client_is_routed: bool,
    routes: Sequence[PostgresTenancyRouteSpec],
    has_relation_resolvers: bool = False,
) -> PostgresTenantIsolationMode:
    """Return the effective isolation mode implied by client and route flags."""

    return derive_tenant_isolation_mode(
        client_is_routed=client_is_routed,
        routes=[r.to_contract() for r in routes],
        has_relation_resolvers=has_relation_resolvers,
    )


# ....................... #


def validate_postgres_tenancy_wiring(
    *,
    client_is_routed: bool,
    introspector_cache_partition_key_set: bool,
    routes: Sequence[PostgresTenancyRouteSpec],
    required_isolation: PostgresTenantIsolationMode | None = None,
    has_namespace_routing: bool = False,
) -> None:
    """Fail or warn when Postgres client routing and ``tenant_aware`` flags disagree.

    When ``required_isolation`` is declared, also fail closed if the derived isolation
    tier is weaker than the requirement. ``has_namespace_routing`` marks a per-tenant
    schema (e.g. an analytics ``query_schema`` resolver), which derives the ``schema`` tier.
    Postgres can reach every tier (up to ``database`` via a routed client).
    """

    validate_routed_client_tenancy_wiring(
        integration="Postgres",
        client_is_routed=client_is_routed,
        partition_key_set=introspector_cache_partition_key_set,
        routes=[r.to_contract() for r in routes],
        partition_key_detail=(
            "Set PostgresDepsModule.introspector_cache_partition_key to the same tenant "
            "identity used for routing."
        ),
        validation_failed_code="postgres_tenancy_validation_failed",
        required_isolation=required_isolation,
        has_namespace_routing=has_namespace_routing,
        max_supported_isolation="database",
        log_warning=logger.warning,
    )
