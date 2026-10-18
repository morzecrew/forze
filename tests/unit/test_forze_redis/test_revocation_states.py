"""Redis revocation states: a tenant-global route, and a reply it cannot read is an error."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import RevocationStatesDepKey
from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException, ExceptionKind
from forze_redis import RedisDepsModule, RedisRevocationStatesConfig, RedisUniversalConfig
from forze_redis.adapters.revocation_states import RedisRevocationStatesAdapter
from forze_redis.kernel.client import RedisClient, RoutedRedisClient

# ----------------------- #


class TestRevocationStatesRoutes:
    @pytest.mark.parametrize(
        "kwargs",
        [{"tenant_aware": True}, {"namespace": lambda tenant_id: f"t:{tenant_id}"}],
    )
    def test_a_tenant_scoped_config_is_refused(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as caught:
            RedisRevocationStatesConfig(**{"namespace": "authn", **kwargs})

        assert caught.value.code == "revocation_states.tenant_scoped_route"

    def test_a_tenant_aware_universal_config_is_refused_as_states(self) -> None:
        with pytest.raises(CoreException) as caught:
            RedisDepsModule(
                client=MagicMock(spec=RedisClient),
                revocation_states={"r": RedisUniversalConfig(namespace="a", tenant_aware=True)},  # type: ignore[dict-item]
            )

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    def test_the_route_is_registered(self) -> None:
        deps = RedisDepsModule(
            client=MagicMock(spec=RedisClient),
            revocation_states={"authn-revocations": RedisRevocationStatesConfig(namespace="a")},
        )()

        assert deps.exists(RevocationStatesDepKey, route="authn-revocations")

    def test_a_routed_client_is_refused(self) -> None:
        routed = RoutedRedisClient(
            secrets=object(),  # type: ignore[arg-type]
            secret_ref_for_tenant=lambda tenant_id: SecretRef(path=f"tenants/{tenant_id}"),
            tenant_provider=lambda: None,
        )

        with pytest.raises(CoreException) as caught:
            RedisDepsModule(
                client=routed,
                revocation_states={"r": RedisRevocationStatesConfig(namespace="a")},
            )

        assert caught.value.code == "revocation_states.routed_client"


class TestRedisRevocationStatesAdapter:
    def _adapter(self, reply: str = "") -> tuple[RedisRevocationStatesAdapter, Any]:
        client = MagicMock(spec=RedisClient)
        client.run_script = AsyncMock(return_value=reply)
        client.mset = AsyncMock(return_value=True)

        return RedisRevocationStatesAdapter(client=client, namespace="authn"), client

    async def test_an_unreadable_reply_is_an_infrastructure_error(self) -> None:
        adapter, _ = self._adapter("garbage")

        with pytest.raises(CoreException) as caught:
            await adapter.read(uuid4(), "s:x")

        assert caught.value.kind is ExceptionKind.INFRASTRUCTURE

    async def test_an_unreadable_api_key_reply_is_an_infrastructure_error(self) -> None:
        adapter, _ = self._adapter("garbage")

        with pytest.raises(CoreException) as caught:
            await adapter.read_api_key("digest")

        assert caught.value.kind is ExceptionKind.INFRASTRUCTURE

    async def test_nothing_to_fill_or_revoke_sends_nothing(self) -> None:
        adapter, client = self._adapter()

        await adapter.fill(uuid4(), {}, expire_at_ms=1)
        await adapter.revoke(uuid4(), [], ttl=timedelta(seconds=1))

        client.run_script.assert_not_awaited()
        client.mset.assert_not_awaited()
