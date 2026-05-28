"""Unit tests for :class:`~forze_clickhouse.kernel.platform.RoutedClickHouseClient`."""

import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_clickhouse.kernel.platform import RoutedClickHouseClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, payloads: dict[UUID, dict[str, object]]) -> None:
        self.payloads = payloads

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, payload in self.payloads.items():
            if ref.path == f"tenants/{tid}/clickhouse":
                return json.dumps(payload)
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/clickhouse" for tid in self.payloads)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/clickhouse")


def _creds(host: str = "localhost") -> dict[str, object]:
    return {
        "host": host,
        "port": 8123,
        "username": "default",
        "password": "",
        "database": "default",
        "secure": False,
    }


@pytest.mark.asyncio
async def test_routed_clickhouse_requires_startup() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    tenant: UUID | None = None

    routed = RoutedClickHouseClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(CoreException, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_clickhouse_eviction() -> None:
    secrets = _MemSecrets({_T1: _creds("host-a"), _T2: _creds("host-b")})
    cur: UUID | None = None

    routed = RoutedClickHouseClient(
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
        "forze_clickhouse.kernel.platform.routed_client.ClickHouseClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()
        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1


def test_routed_clickhouse_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    with pytest.raises(CoreException, match="max_cached_tenants"):
        RoutedClickHouseClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            max_cached_tenants=0,
        )


@pytest.mark.asyncio
async def test_routed_clickhouse_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    routed = RoutedClickHouseClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreException, match="Tenant ID"):
        await routed.health()
