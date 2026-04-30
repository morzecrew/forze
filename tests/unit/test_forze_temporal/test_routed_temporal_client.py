"""Unit tests for :class:`~forze_temporal.kernel.platform.RoutedTemporalClient`."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

pytest.importorskip("temporalio")

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import CoreError, InfrastructureError

from forze_temporal.kernel.platform import RoutedTemporalClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, hosts: dict[UUID, str]) -> None:
        self.hosts = hosts

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, host in self.hosts.items():
            if ref.path == f"tenants/{tid}/temporal":
                return host
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/temporal" for tid in self.hosts)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/temporal")


@pytest.mark.asyncio
async def test_routed_temporal_requires_startup() -> None:
    secrets = _MemSecrets({_T1: "localhost:7233"})
    tenant: UUID | None = None

    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(InfrastructureError, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_temporal_eviction() -> None:
    secrets = _MemSecrets(
        {
            _T1: "host-a:7233",
            _T2: "host-b:7233",
        }
    )
    cur: UUID | None = None

    routed = RoutedTemporalClient(
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
        "forze_temporal.kernel.platform.routed_client.TemporalClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()
        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1


@pytest.mark.asyncio
async def test_routed_temporal_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: "localhost:7233"})
    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreError, match="Tenant ID"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_temporal_get_workflow_handle_requires_cache() -> None:
    secrets = _MemSecrets({_T1: "localhost:7233"})
    routed = RoutedTemporalClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: _T1,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(InfrastructureError, match="No Temporal client"):
        routed.get_workflow_handle("wf-1")
