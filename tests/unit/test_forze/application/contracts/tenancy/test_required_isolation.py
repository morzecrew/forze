"""Unit tests for the declared-minimum tenant isolation policy."""

from __future__ import annotations

import pytest

from forze.application.contracts.tenancy.wiring import (
    TenancyRouteSpec,
    isolation_satisfies,
    validate_required_isolation,
    validate_routed_client_tenancy_wiring,
)
from forze.base.exceptions import CoreException

# ----------------------- #


def test_isolation_rank_total_order() -> None:
    # weakest -> strongest: none < tagged < namespace < dedicated
    assert isolation_satisfies(derived="dedicated", required="namespace")
    assert isolation_satisfies(derived="namespace", required="tagged")
    assert isolation_satisfies(derived="tagged", required="none")
    assert isolation_satisfies(derived="namespace", required="namespace")  # equal satisfies

    assert not isolation_satisfies(derived="tagged", required="namespace")
    assert not isolation_satisfies(derived="namespace", required="dedicated")
    assert not isolation_satisfies(derived="none", required="tagged")


def test_isolation_ceiling_matrix_is_consistent() -> None:
    from forze.application.contracts.tenancy import INTEGRATION_ISOLATION_CEILINGS
    from forze.application.contracts.tenancy.wiring import _ISOLATION_RANK

    # Every declared ceiling is a valid tier, and the in-process backends are capped below
    # the networked ones (a regression guard on the matrix).
    assert set(INTEGRATION_ISOLATION_CEILINGS.values()) <= set(_ISOLATION_RANK)
    assert INTEGRATION_ISOLATION_CEILINGS["neo4j"] == "tagged"
    assert INTEGRATION_ISOLATION_CEILINGS["duckdb"] == "tagged"
    assert INTEGRATION_ISOLATION_CEILINGS["postgres"] == "dedicated"
    assert not isolation_satisfies(
        derived=INTEGRATION_ISOLATION_CEILINGS["duckdb"], required="dedicated"
    )


def test_max_supported_capability_ceiling() -> None:
    from forze.application.contracts.tenancy.wiring import validate_required_isolation

    # An integration capped at "tagged" cannot meet a "dedicated" floor — capability mismatch.
    with pytest.raises(CoreException, match="x_failed") as ei:
        validate_required_isolation(
            integration="X",
            derived="tagged",
            required="dedicated",
            code="x_failed",
            max_supported="tagged",
        )

    assert ei.value.details["max_supported_isolation"] == "tagged"

    # Within the ceiling, a satisfied floor passes.
    validate_required_isolation(
        integration="X",
        derived="namespace",
        required="namespace",
        code="x_failed",
        max_supported="dedicated",
    )


def test_required_isolation_none_is_opt_out() -> None:
    # No declared floor -> never raises regardless of derived mode.
    validate_required_isolation(
        integration="X",
        derived="none",
        required=None,
        code="x_failed",
    )


def test_required_isolation_satisfied_passes() -> None:
    validate_required_isolation(
        integration="X",
        derived="dedicated",
        required="dedicated",
        code="x_failed",
    )


def test_required_isolation_weaker_fails_closed() -> None:
    with pytest.raises(CoreException, match="x_failed") as ei:
        validate_required_isolation(
            integration="X",
            derived="tagged",
            required="dedicated",
            code="x_failed",
        )

    assert ei.value.details["required_isolation"] == "dedicated"
    assert ei.value.details["derived_isolation"] == "tagged"


def test_routed_wiring_enforces_declared_floor() -> None:
    # Shared client + tenant-aware route derives "tagged"; a "dedicated" floor refuses it.
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        validate_routed_client_tenancy_wiring(
            integration="Postgres",
            client_is_routed=False,
            partition_key_set=False,
            routes=[TenancyRouteSpec(name="doc", tenant_aware=True, kind="document")],
            partition_key_detail="",
            validation_failed_code="postgres_tenancy_validation_failed",
            required_isolation="dedicated",
        )


def test_module_tenancy_dynamic_relation_resolver_derives_namespace() -> None:
    # A dynamic (callable) RelationSpec collection scopes per-tenant → namespace tier; a
    # `namespace` floor is satisfied without a routed client. (`mongo` ceiling = dedicated.)
    from forze.application.contracts.tenancy import (
        TenancyRouteGroup,
        validate_module_tenancy,
    )

    validate_module_tenancy(
        integration="Mongo",
        client_is_routed=False,
        groups=[
            TenancyRouteGroup(
                kind="document",
                configs={"d": object()},
                tenant_aware=lambda _c: False,
                namespace_resolver=lambda _c: (lambda tid: (f"t_{tid}", "users")),
            )
        ],
        required_isolation="namespace",
        validation_failed_code="mongo_tenancy_validation_failed",
    )


def test_module_tenancy_static_relation_is_only_tagged() -> None:
    # A static (tuple) relation is NOT namespace routing — derives tagged (or none).
    from forze.application.contracts.tenancy import (
        TenancyRouteGroup,
        validate_module_tenancy,
    )

    with pytest.raises(CoreException, match="mongo_tenancy_validation_failed"):
        validate_module_tenancy(
            integration="Mongo",
            client_is_routed=False,
            groups=[
                TenancyRouteGroup(
                    kind="document",
                    configs={"d": object()},
                    tenant_aware=lambda _c: True,
                    namespace_resolver=lambda _c: ("public", "users"),
                )
            ],
            required_isolation="namespace",  # tagged < namespace → fails
            validation_failed_code="mongo_tenancy_validation_failed",
        )


def test_module_tenancy_ceiling_comes_from_matrix() -> None:
    # DuckDB's ceiling (tagged, from INTEGRATION_ISOLATION_CEILINGS) makes a dedicated floor a
    # capability mismatch — no per-module literal involved.
    from forze.application.contracts.tenancy import (
        TenancyRouteGroup,
        validate_module_tenancy,
    )

    with pytest.raises(CoreException, match="duckdb_analytics_tenancy_validation_failed"):
        validate_module_tenancy(
            integration="DuckDB",
            client_is_routed=False,
            groups=[
                TenancyRouteGroup(
                    kind="analytics",
                    configs={"a": object()},
                    tenant_aware=lambda _c: True,
                )
            ],
            required_isolation="dedicated",
            validation_failed_code="duckdb_analytics_tenancy_validation_failed",
        )


def test_routed_wiring_floor_met_by_routed_client() -> None:
    # Routed client derives "dedicated" and satisfies the floor.
    validate_routed_client_tenancy_wiring(
        integration="Postgres",
        client_is_routed=True,
        partition_key_set=True,
        routes=[],
        partition_key_detail="",
        validation_failed_code="postgres_tenancy_validation_failed",
        required_isolation="dedicated",
    )
