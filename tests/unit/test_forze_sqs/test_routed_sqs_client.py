"""Unit tests for :class:`~forze_sqs.kernel.platform.RoutedSQSClient`."""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import CoreError, InfrastructureError

from forze_sqs.kernel.platform import RoutedSQSClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, payloads: dict[UUID, dict[str, str]]) -> None:
        self.payloads = payloads

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, payload in self.payloads.items():
            if ref.path == f"tenants/{tid}/sqs":
                return json.dumps(payload)
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/sqs" for tid in self.payloads)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/sqs")


def _creds(endpoint: str = "http://localhost:4566") -> dict[str, str]:
    return {
        "endpoint": endpoint,
        "region_name": "us-east-1",
        "access_key_id": "k",
        "secret_access_key": "s",
    }


@pytest.mark.asyncio
async def test_routed_sqs_requires_startup() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    tenant: UUID | None = None

    routed = RoutedSQSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(InfrastructureError, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_sqs_eviction() -> None:
    secrets = _MemSecrets({_T1: _creds("http://a"), _T2: _creds("http://b")})
    cur: UUID | None = None

    routed = RoutedSQSClient(
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

        @asynccontextmanager
        async def _cm() -> object:
            yield object()

        inst.client = MagicMock(side_effect=lambda: _cm())
        instances.append(inst)
        return inst

    with patch(
        "forze_sqs.kernel.platform.routed_client.SQSClient",
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
async def test_routed_sqs_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    routed = RoutedSQSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreError, match="Tenant ID"):
        await routed.health()
