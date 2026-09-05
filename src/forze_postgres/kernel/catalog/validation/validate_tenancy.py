"""Validate Postgres tenant isolation wiring (client vs per-route tenant_aware)."""

from collections.abc import Sequence
from typing import Literal

import attrs

from forze.application.contracts.tenancy import (
    StatementOrigin,
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
    "procedures",
    "dynamic_read",
    "outbox",
    "inbox",
    "counter",
    "rotating_credentials",
    "durable_step",
    "durable_run",
    "durable_schedule",
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

    origin: StatementOrigin = "structured"
    """What kind of process authored the statements this route executes.

    Carried here rather than left to the contract's default because Postgres is the one
    integration that does not go through ``validate_module_tenancy``: it converts to the
    contract type in :meth:`to_contract`, so a field this spec cannot express is not a
    missing feature but a **silent** one — every route would take ``structured``, whose
    floor is ``none``, and the origin check would pass over a plane it was written for.
    """

    def to_contract(self) -> TenancyRouteSpec:
        """Convert to the contract route spec.

        Every field is forwarded by hand, so a new contract field has to be added in two
        places. A dropped one substitutes the contract default rather than failing, which
        for anything gating a floor is the permissive end —
        ``test_the_converter_forwards_every_contract_field`` is the ratchet that catches it.
        """

        return TenancyRouteSpec(
            name=self.name,
            tenant_aware=self.tenant_aware,
            kind=self.kind,
            has_namespace_routing=self.has_namespace_routing,
            origin=self.origin,
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
