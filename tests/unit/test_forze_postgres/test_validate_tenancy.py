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


def test_derive_mode_tagged() -> None:
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
        == "tagged"
    )


def test_derive_mode_namespace() -> None:
    # A per-tenant namespace resolver derives the namespace tier (relation rung removed).
    assert (
        derive_postgres_tenant_isolation_mode(
            client_is_routed=False,
            routes=[],
            has_namespace_routing=True,
        )
        == "namespace"
    )


def test_derive_mode_dedicated() -> None:
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
        == "dedicated"
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


def test_required_dedicated_isolation_rejects_tagged_wiring() -> None:
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
            required_isolation="dedicated",
        )


def test_required_dedicated_isolation_satisfied_by_routed_client() -> None:
    validate_postgres_tenancy_wiring(
        client_is_routed=True,
        introspector_cache_partition_key_set=True,
        routes=[],
        required_isolation="dedicated",
    )
