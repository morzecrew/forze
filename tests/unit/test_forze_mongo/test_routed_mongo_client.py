"""Unit tests for :class:`~forze_mongo.kernel.platform.RoutedMongoClient`."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import CoreError, InfrastructureError

from forze_mongo.kernel.platform import RoutedMongoClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, uris: dict[UUID, str]) -> None:
        self.uris = uris

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, uri in self.uris.items():
            if ref.path == f"tenants/{tid}/uri":
                return uri
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/uri" for tid in self.uris)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/uri")


@pytest.mark.asyncio
async def test_routed_mongo_requires_startup() -> None:
    secrets = _MemSecrets({_T1: "mongodb://localhost:27017"})
    tenant: UUID | None = None

    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        database_name_for_tenant=lambda _tid: "app",
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(InfrastructureError, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_mongo_eviction() -> None:
    secrets = _MemSecrets(
        {
            _T1: "mongodb://localhost:27017",
            _T2: "mongodb://localhost:27018",
        }
    )
    cur: UUID | None = None

    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        database_name_for_tenant=lambda _tid: "app",
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
        "forze_mongo.kernel.platform.routed_client.MongoClient",
        side_effect=_make_client,
    ):
        cur = _T1
        await routed.health()
        cur = _T2
        await routed.health()
        assert instances[0].close.await_count == 1

    await routed.close()
    assert instances[1].close.await_count == 1


def test_routed_mongo_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: "mongodb://localhost:27017"})
    with pytest.raises(CoreError, match="max_cached_tenants"):
        RoutedMongoClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            database_name_for_tenant=lambda _tid: "app",
            max_cached_tenants=0,
        )


@pytest.mark.asyncio
async def test_routed_mongo_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: "mongodb://localhost:27017"})
    routed = RoutedMongoClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        database_name_for_tenant=lambda _tid: "app",
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreError, match="Tenant ID"):
        await routed.health()
