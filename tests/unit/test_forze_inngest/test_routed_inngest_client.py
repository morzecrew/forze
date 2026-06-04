"""Unit tests for :class:`~forze_inngest.kernel.client.RoutedInngestClient`."""

import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_inngest.kernel.client import RoutedInngestClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, payloads: dict[UUID, dict[str, str]]) -> None:
        self.payloads = payloads

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, payload in self.payloads.items():
            if ref.path == f"tenants/{tid}/inngest":
                return json.dumps(payload)
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/inngest" for tid in self.payloads)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/inngest")


def _creds(app_id: str = "app-a") -> dict[str, str]:
    return {"app_id": app_id, "event_key": "ek"}


@pytest.mark.asyncio
async def test_routed_inngest_requires_startup() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    tenant: UUID | None = None

    routed = RoutedInngestClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(CoreException, match="not started"):
        _ = routed.native


@pytest.mark.asyncio
async def test_routed_inngest_eviction() -> None:
    secrets = _MemSecrets({_T1: _creds("app-1"), _T2: _creds("app-2")})
    cur: UUID | None = None

    routed = RoutedInngestClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=1,
    )
    await routed.startup()

    instances: list[MagicMock] = []

    def _make_client(**_kwargs: object) -> MagicMock:
        inst = MagicMock()
        inst.send = AsyncMock(return_value=["evt-1"])
        inst.native = MagicMock()
        inst.close = AsyncMock()
        instances.append(inst)
        return inst

    with patch(
        "forze_inngest.kernel.client.routed_client.InngestClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.send([])
        cur = _T2
        await routed.send([])
        assert len(instances) == 2

    await routed.close()


def test_routed_inngest_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    with pytest.raises(CoreException, match="max_entries"):
        RoutedInngestClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            max_cached_tenants=0,
        )


@pytest.mark.asyncio
async def test_routed_inngest_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    routed = RoutedInngestClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreException, match="Tenant ID"):
        await routed.send([])
