"""Unit tests for :class:`~forze_meilisearch.kernel.client.RoutedMeilisearchClient`."""

import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_meilisearch.kernel.client import RoutedMeilisearchClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, payloads: dict[UUID, dict[str, str]]) -> None:
        self.payloads = payloads

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, payload in self.payloads.items():
            if ref.path == f"tenants/{tid}/meilisearch":
                return json.dumps(payload)
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/meilisearch" for tid in self.payloads)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/meilisearch")


def _creds(url: str = "http://localhost:7700") -> dict[str, str]:
    return {"url": url, "api_key": "masterKey"}


@pytest.mark.asyncio
async def test_routed_meilisearch_requires_startup() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    tenant: UUID | None = None

    routed = RoutedMeilisearchClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(CoreException, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_meilisearch_eviction() -> None:
    secrets = _MemSecrets({_T1: _creds("http://a"), _T2: _creds("http://b")})
    cur: UUID | None = None

    routed = RoutedMeilisearchClient(
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
        inst.health = AsyncMock(return_value=True)
        instances.append(inst)
        return inst

    with patch(
        "forze_meilisearch.kernel.client.routed_client.MeilisearchClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()
        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1


def test_routed_meilisearch_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    with pytest.raises(CoreException, match="max_entries"):
        RoutedMeilisearchClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            max_cached_tenants=0,
        )


@pytest.mark.asyncio
async def test_routed_meilisearch_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    routed = RoutedMeilisearchClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreException, match="Tenant ID"):
        await routed.health()
