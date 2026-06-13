"""Unit tests for Postgres tenancy wiring validation."""

import pytest

from forze.base.exceptions import CoreException

from forze_postgres.kernel.catalog.validation.validate_tenancy import (
    PostgresTenancyRouteSpec,
    derive_postgres_tenant_isolation_mode,
    validate_postgres_tenancy_wiring,
)


def test_derive_mode_none() -> None:
    assert (
        derive_postgres_tenant_isolation_mode(
            client_is_routed=False,
            routes=[],
        )
        == "none"
    )


def test_derive_mode_row() -> None:
    assert (
        derive_postgres_tenant_isolation_mode(
            client_is_routed=False,
            routes=[
                PostgresTenancyRouteSpec(
                    name="doc",
                    tenant_aware=True,
                    kind="document",
                ),
            ],
        )
        == "row"
    )


def test_derive_mode_relation() -> None:
    assert (
        derive_postgres_tenant_isolation_mode(
            client_is_routed=False,
            routes=[],
            has_relation_resolvers=True,
        )
        == "relation"
    )


def test_derive_mode_database() -> None:
    assert (
        derive_postgres_tenant_isolation_mode(
            client_is_routed=True,
            routes=[
                PostgresTenancyRouteSpec(
                    name="doc",
                    tenant_aware=True,
                    kind="document",
                ),
            ],
        )
        == "database"
    )


def test_routed_without_partition_key_fails() -> None:
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        validate_postgres_tenancy_wiring(
            client_is_routed=True,
            introspector_cache_partition_key_set=False,
            routes=[],
        )


def test_shared_client_passes() -> None:
    validate_postgres_tenancy_wiring(
        client_is_routed=False,
        introspector_cache_partition_key_set=False,
        routes=[
            PostgresTenancyRouteSpec(
                name="doc",
                tenant_aware=True,
                kind="document",
            ),
        ],
    )


def test_routed_with_partition_key_and_tenant_aware_warns_only() -> None:
    validate_postgres_tenancy_wiring(
        client_is_routed=True,
        introspector_cache_partition_key_set=True,
        routes=[
            PostgresTenancyRouteSpec(
                name="projects",
                tenant_aware=True,
                kind="document",
            ),
        ],
    )


def test_required_database_isolation_rejects_row_wiring() -> None:
    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        validate_postgres_tenancy_wiring(
            client_is_routed=False,
            introspector_cache_partition_key_set=False,
            routes=[
                PostgresTenancyRouteSpec(
                    name="doc",
                    tenant_aware=True,
                    kind="document",
                ),
            ],
            required_isolation="database",
        )


def test_required_database_isolation_satisfied_by_routed_client() -> None:
    validate_postgres_tenancy_wiring(
        client_is_routed=True,
        introspector_cache_partition_key_set=True,
        routes=[],
        required_isolation="database",
    )
