"""Unit tests for :class:`~forze_rabbitmq.kernel.platform.RoutedRabbitMQClient`."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import CoreError, InfrastructureError

from forze_rabbitmq.kernel.platform import RoutedRabbitMQClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, dsns: dict[UUID, str]) -> None:
        self.dsns = dsns

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, dsn in self.dsns.items():
            if ref.path == f"tenants/{tid}/rabbitmq":
                return dsn
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/rabbitmq" for tid in self.dsns)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/rabbitmq")


@pytest.mark.asyncio
async def test_routed_rabbitmq_requires_startup() -> None:
    secrets = _MemSecrets({_T1: "amqp://guest:guest@localhost/"})
    tenant: UUID | None = None

    routed = RoutedRabbitMQClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(InfrastructureError, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_rabbitmq_eviction() -> None:
    secrets = _MemSecrets(
        {
            _T1: "amqp://a/",
            _T2: "amqp://b/",
        }
    )
    cur: UUID | None = None

    routed = RoutedRabbitMQClient(
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
        "forze_rabbitmq.kernel.platform.routed_client.RabbitMQClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()
        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1


def test_routed_rabbitmq_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: "amqp://guest:guest@localhost/"})
    with pytest.raises(CoreError, match="max_cached_tenants"):
        RoutedRabbitMQClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            max_cached_tenants=0,
        )


@pytest.mark.asyncio
async def test_routed_rabbitmq_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: "amqp://localhost/"})
    routed = RoutedRabbitMQClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreError, match="Tenant ID"):
        await routed.health()
