"""Tenant-global Redis routes — fleets, lifecycle locks, outbox wakes — sit outside the floor."""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_redis import (
    RedisCacheConfig,
    RedisDepsModule,
    RedisDistributedLockConfig,
    RedisFleetConfig,
    RedisPubSubConfig,
)
from forze_redis.kernel.client import RedisClient, RoutedRedisClient

# ----------------------- #

LOCK = RedisDistributedLockConfig(namespace="app", tenant_global=True)
WAKE = RedisPubSubConfig(tenant_global=True)
FLEET = RedisFleetConfig(namespace="app")


def _floored(**routes: Any) -> RedisDepsModule:
    return RedisDepsModule(client=RedisClient(), required_tenant_isolation="tagged", **routes)


@pytest.mark.parametrize(
    "routes",
    [
        pytest.param({"fleets": {"gateways": FLEET}}, id="fleet"),
        pytest.param({"dlocks": {"lifecycle": LOCK}}, id="lifecycle-lock"),
        pytest.param({"pubsub": {"outbox-wake": WAKE}}, id="outbox-wake"),
        pytest.param(
            {
                "dlocks": {"lifecycle": LOCK},
                "pubsub": {"outbox-wake": WAKE},
                "fleets": {"g": FLEET},
            },
            id="all-three",
        ),
    ],
)
def test_a_floored_module_with_only_tenant_global_routes_is_accepted(
    routes: dict[str, Any],
) -> None:
    _floored(**routes)


def test_a_tenant_global_lock_sits_beside_a_tenant_aware_cache() -> None:
    _floored(
        dlocks={"lifecycle": LOCK},
        caches={"orders": RedisCacheConfig(namespace="app", tenant_aware=True)},
    )


def test_a_tenant_global_lock_does_not_cover_an_unscoped_cache() -> None:
    with pytest.raises(CoreException) as caught:
        _floored(dlocks={"lifecycle": LOCK}, caches={"orders": RedisCacheConfig(namespace="app")})

    assert caught.value.code == "redis_tenancy_validation_failed"
    assert "cache route 'orders'" in caught.value.summary


def test_an_unmarked_plain_lock_is_still_held_to_the_floor() -> None:
    with pytest.raises(CoreException) as caught:
        _floored(dlocks={"lifecycle": RedisDistributedLockConfig(namespace="app")})

    assert caught.value.code == "redis_tenancy_validation_failed"


@pytest.mark.parametrize(
    "routes",
    [
        pytest.param(
            {
                "dlocks": {
                    "m": RedisDistributedLockConfig(
                        namespace="app", tenant_global=True, tenant_aware=True
                    )
                }
            },
            id="tenant-aware-lock",
        ),
        pytest.param(
            {
                "dlocks": {
                    "m": RedisDistributedLockConfig(
                        namespace=lambda tenant_id: f"app:{tenant_id}", tenant_global=True
                    )
                }
            },
            id="per-tenant-lock-namespace",
        ),
        pytest.param(
            {"pubsub": {"w": RedisPubSubConfig(tenant_global=True, tenant_aware=True)}},
            id="tenant-aware-wake",
        ),
    ],
)
def test_a_tenant_scoped_route_cannot_be_tenant_global(routes: dict[str, Any]) -> None:
    with pytest.raises(CoreException) as caught:
        RedisDepsModule(client=RedisClient(), **routes)

    assert "is tenant-global" in caught.value.summary


def test_a_tenant_global_route_on_a_routed_client_is_refused() -> None:
    routed = RoutedRedisClient(
        secrets=object(),  # type: ignore[arg-type]
        secret_ref_for_tenant=lambda tenant_id: SecretRef(path=f"tenants/{tenant_id}"),
        tenant_provider=lambda: None,
    )

    with pytest.raises(CoreException) as caught:
        RedisDepsModule(client=routed, dlocks={"lifecycle": LOCK})

    assert "is tenant-global" in caught.value.summary
