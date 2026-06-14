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
    # weakest -> strongest: none < relation < row < schema < database
    assert isolation_satisfies(derived="database", required="schema")
    assert isolation_satisfies(derived="schema", required="row")
    assert isolation_satisfies(derived="row", required="relation")
    assert isolation_satisfies(derived="relation", required="none")
    assert isolation_satisfies(derived="schema", required="schema")  # equal satisfies

    assert not isolation_satisfies(derived="row", required="schema")
    assert not isolation_satisfies(derived="schema", required="database")
    assert not isolation_satisfies(derived="relation", required="row")
    assert not isolation_satisfies(derived="none", required="relation")


def test_isolation_ceiling_matrix_is_consistent() -> None:
    from forze.application.contracts.tenancy import INTEGRATION_ISOLATION_CEILINGS
    from forze.application.contracts.tenancy.wiring import _ISOLATION_RANK

    # Every declared ceiling is a valid tier, and the in-process backends are capped below
    # the networked ones (a regression guard on the matrix).
    assert set(INTEGRATION_ISOLATION_CEILINGS.values()) <= set(_ISOLATION_RANK)
    assert INTEGRATION_ISOLATION_CEILINGS["neo4j"] == "row"
    assert INTEGRATION_ISOLATION_CEILINGS["duckdb"] == "row"
    assert INTEGRATION_ISOLATION_CEILINGS["postgres"] == "database"
    assert not isolation_satisfies(
        derived=INTEGRATION_ISOLATION_CEILINGS["duckdb"], required="database"
    )


def test_max_supported_capability_ceiling() -> None:
    from forze.application.contracts.tenancy.wiring import validate_required_isolation

    # An integration capped at "row" cannot meet a "database" floor — capability mismatch.
    with pytest.raises(CoreException, match="x_failed") as ei:
        validate_required_isolation(
            integration="X",
            derived="row",
            required="database",
            code="x_failed",
            max_supported="row",
        )

    assert ei.value.details["max_supported_isolation"] == "row"

    # Within the ceiling, a satisfied floor passes.
    validate_required_isolation(
        integration="X",
        derived="schema",
        required="schema",
        code="x_failed",
        max_supported="database",
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
        derived="database",
        required="database",
        code="x_failed",
    )


def test_required_isolation_weaker_fails_closed() -> None:
    with pytest.raises(CoreException, match="x_failed") as ei:
        validate_required_isolation(
            integration="X",
            derived="row",
            required="database",
            code="x_failed",
        )

    assert ei.value.details["required_isolation"] == "database"
    assert ei.value.details["derived_isolation"] == "row"


def test_routed_wiring_enforces_declared_floor() -> None:
    # Shared client + tenant-aware route derives "row"; a "database" floor refuses it.
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        validate_routed_client_tenancy_wiring(
            integration="Postgres",
            client_is_routed=False,
            partition_key_set=False,
            routes=[TenancyRouteSpec(name="doc", tenant_aware=True, kind="document")],
            partition_key_detail="",
            validation_failed_code="postgres_tenancy_validation_failed",
            required_isolation="database",
        )


def test_routed_wiring_floor_met_by_routed_client() -> None:
    # Routed client derives "database" and satisfies the floor.
    validate_routed_client_tenancy_wiring(
        integration="Postgres",
        client_is_routed=True,
        partition_key_set=True,
        routes=[],
        partition_key_detail="",
        validation_failed_code="postgres_tenancy_validation_failed",
        required_isolation="database",
    )
