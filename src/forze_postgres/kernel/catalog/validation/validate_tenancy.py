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
    "outbox",
    "inbox",
]

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresTenancyRouteSpec:
    """One registered Postgres route and its tagged-tier tenant flag."""

    name: str
    tenant_aware: bool
    kind: PostgresTenancyRouteKind
    has_namespace_routing: bool = False
    """Whether this route resolves a per-tenant schema (a dynamic relation / ``query_schema``)."""

    def to_contract(self) -> TenancyRouteSpec:
        return TenancyRouteSpec(
            name=self.name,
            tenant_aware=self.tenant_aware,
            kind=self.kind,
            has_namespace_routing=self.has_namespace_routing,
        )


# ....................... #


def derive_postgres_tenant_isolation_mode(
    *,
    client_is_routed: bool,
    routes: Sequence[PostgresTenancyRouteSpec],
    has_namespace_routing: bool = False,
) -> PostgresTenantIsolationMode:
    """Return the effective isolation mode implied by client and route flags."""

    return derive_tenant_isolation_mode(
        client_is_routed=client_is_routed,
        routes=[r.to_contract() for r in routes],
        has_namespace_routing=has_namespace_routing,
    )


# ....................... #


def validate_postgres_tenancy_wiring(
    *,
    client_is_routed: bool,
    introspector_cache_partition_key_set: bool,
    routes: Sequence[PostgresTenancyRouteSpec],
    required_isolation: PostgresTenantIsolationMode | None = None,
) -> None:
    """Fail or warn when Postgres client routing and ``tenant_aware`` flags disagree.

    When ``required_isolation`` is declared, the floor is enforced per route (each route's
    ``tenant_aware`` / ``has_namespace_routing`` — the latter a per-tenant schema, e.g. an
    analytics ``query_schema`` resolver). Postgres can reach every tier (up to ``dedicated``
    via a routed client).
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
        # Postgres reaches a routed per-tenant client / credentials.
        max_supported_isolation="dedicated",
        log_warning=logger.warning,
    )
