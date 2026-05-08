"""Unit tests for :class:`~forze_redis.kernel.platform.RoutedRedisClient`."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import CoreError, InfrastructureError

from forze_redis.kernel.platform import RoutedRedisClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, dsns: dict[UUID, str]) -> None:
        self.dsns = dsns

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, dsn in self.dsns.items():
            if ref.path == f"tenants/{tid}/redis":
                return dsn
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/redis" for tid in self.dsns)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/redis")


@pytest.mark.asyncio
async def test_routed_redis_requires_startup() -> None:
    secrets = _MemSecrets({_T1: "redis://localhost:6379/0"})
    tenant: UUID | None = None

    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(InfrastructureError, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_redis_eviction() -> None:
    secrets = _MemSecrets(
        {
            _T1: "redis://localhost:6379/0",
            _T2: "redis://localhost:6380/0",
        }
    )
    cur: UUID | None = None

    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=1,
    )
    await routed.startup()

    instances: list[MagicMock] = []

    def _make_client() -> MagicMock:
        inst = MagicMock()
        inst.initialize = AsyncMock()
        inst.close = AsyncMock()
        inst.health = AsyncMock(return_value=("ok", True))
        instances.append(inst)
        return inst

    with patch(
        "forze_redis.kernel.platform.routed_client.RedisClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()
        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1


def test_routed_redis_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: "redis://localhost:6379/0"})
    with pytest.raises(CoreError, match="max_cached_tenants"):
        RoutedRedisClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            max_cached_tenants=0,
        )


@pytest.mark.asyncio
async def test_routed_redis_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: "redis://localhost:6379/0"})
    routed = RoutedRedisClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreError, match="Tenant ID"):
        await routed.health()
