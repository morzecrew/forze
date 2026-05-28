"""Validate Postgres tenant isolation wiring (client vs per-route tenant_aware)."""

from collections.abc import Sequence
from typing import Literal

import attrs

from forze.base.exceptions import exc

from forze_postgres.kernel._logger import logger

# ----------------------- #

PostgresTenantIsolationMode = Literal["none", "row", "database"]
"""Derived isolation mode for error messages and docs (not configured directly)."""

PostgresTenancyRouteKind = Literal[
    "document",
    "search",
    "hub_search",
    "federated_search",
]

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresTenancyRouteSpec:
    """One registered Postgres route and its row-level tenant flag."""

    name: str
    """Route name (document or search spec key)."""

    tenant_aware: bool
    """Whether the route applies row-level ``tenant_id`` filtering."""

    kind: PostgresTenancyRouteKind
    """Resource kind for log messages."""


# ....................... #


def derive_postgres_tenant_isolation_mode(
    *,
    client_is_routed: bool,
    routes: Sequence[PostgresTenancyRouteSpec],
) -> PostgresTenantIsolationMode:
    """Return the effective isolation mode implied by client and route flags."""

    if client_is_routed:
        return "database"

    if any(r.tenant_aware for r in routes):
        return "row"

    return "none"


# ....................... #


def validate_postgres_tenancy_wiring(
    *,
    client_is_routed: bool,
    introspector_cache_partition_key_set: bool,
    routes: Sequence[PostgresTenancyRouteSpec],
) -> None:
    """Fail or warn when Postgres client routing and ``tenant_aware`` flags disagree."""

    if client_is_routed and not introspector_cache_partition_key_set:
        raise exc.internal(
            "Postgres tenancy validation failed: RoutedPostgresClient requires "
            "PostgresDepsModule.introspector_cache_partition_key so catalog caches "
            "are partitioned by tenant.",
            code="postgres_tenancy_validation_failed",
            details={"client_is_routed": True},
        )

    if not client_is_routed:
        return

    tenant_aware_routes = [r for r in routes if r.tenant_aware]

    if not tenant_aware_routes:
        return

    for route in tenant_aware_routes:
        logger.warning(
            "Postgres tenancy for %s route %r: RoutedPostgresClient already scopes "
            "connections per tenant; tenant_aware=True adds redundant row-level "
            "filtering (defense-in-depth is acceptable).",
            route.kind,
            route.name,
        )
