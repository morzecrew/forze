"""A fleet route is the deployment's, never a tenant's."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze_redis import (
    RedisCacheConfig,
    RedisDepsModule,
    RedisFleetConfig,
    RedisUniversalConfig,
)
from forze_redis.kernel.client import RedisClient

# ----------------------- #


class TestFleetRoutes:
    @pytest.mark.parametrize(
        "kwargs",
        [{"tenant_aware": True}, {"namespace": lambda tenant_id: f"app:{tenant_id}"}],
    )
    def test_a_tenant_scoped_fleet_config_is_refused(self, kwargs: dict[str, object]) -> None:
        with pytest.raises(CoreException) as caught:
            RedisFleetConfig(**{"namespace": "app", **kwargs})  # type: ignore[arg-type]

        assert caught.value.code == "fleet.tenant_scoped_route"

    def test_a_tenant_aware_universal_config_is_refused_as_a_fleet(self) -> None:
        with pytest.raises(CoreException) as caught:
            RedisDepsModule(
                client=RedisClient(),
                fleets={"gateways": RedisUniversalConfig(namespace="app", tenant_aware=True)},
            )

        assert caught.value.code == "fleet.tenant_scoped_route"

    def test_a_fleet_route_does_not_lower_the_tenant_floor(self) -> None:
        RedisDepsModule(
            client=RedisClient(),
            caches={"orders": RedisCacheConfig(namespace="app", tenant_aware=True)},
            fleets={"gateways": RedisFleetConfig(namespace="app")},
            required_tenant_isolation="tagged",
        )
