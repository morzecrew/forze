"""Unit tests for :class:`~forze_gcs.kernel.client.RoutedGCSClient`."""

import json
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze.base.primitives.gcp_service_file import materialize_service_account_json
from forze_gcs.kernel.client import GCSClient, RoutedGCSClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


class _MemSecrets:
    def __init__(self, payloads: dict[UUID, dict[str, str]]) -> None:
        self.payloads = payloads

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, payload in self.payloads.items():
            if ref.path == f"tenants/{tid}/gcs":
                return json.dumps(payload)
        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/gcs" for tid in self.payloads)


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/gcs")


def _creds(project_id: str = "proj-a") -> dict[str, str]:
    return {"project_id": project_id, "service_file": "/tmp/key.json"}


_SA_JSON = '{"type":"service_account","project_id":"p"}'


@pytest.mark.asyncio
async def test_routed_gcs_requires_startup() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    tenant: UUID | None = None

    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: tenant,
        max_cached_tenants=2,
    )

    tenant = _T1
    with pytest.raises(CoreException, match="not started"):
        await routed.health()


@pytest.mark.asyncio
async def test_routed_gcs_eviction() -> None:
    secrets = _MemSecrets({_T1: _creds("p1"), _T2: _creds("p2")})
    cur: UUID | None = None

    routed = RoutedGCSClient(
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
        "forze_gcs.kernel.client.routed_client.GCSClient",
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
async def test_routed_gcs_inline_json_temp_file_removed_on_eviction() -> None:
    secrets = _MemSecrets(
        {
            _T1: {"project_id": "p1", "service_account_json": _SA_JSON},
            _T2: {"project_id": "p2", "service_account_json": _SA_JSON},
        }
    )
    cur: UUID | None = None
    temp_paths: list[str] = []

    def _track_materialize(raw: str, *, prefix: str) -> tuple[str, bool]:
        path, owned = materialize_service_account_json(raw, prefix=prefix)
        temp_paths.append(path)
        return path, owned

    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=1,
    )
    await routed.startup()

    fake_storage = MagicMock()
    fake_storage.close = AsyncMock()

    with (
        patch(
            "forze_gcs.kernel.client.routed_client.materialize_service_account_json",
            side_effect=_track_materialize,
        ),
        patch(
            "forze_gcs.kernel.client.routed_client.GCSClient",
            side_effect=GCSClient,
        ),
        patch(
            "forze_gcs.kernel.client.client.Storage",
            return_value=fake_storage,
        ),
    ):
        cur = _T1
        await routed.health()
        assert temp_paths
        assert Path(temp_paths[0]).exists()

        cur = _T2
        await routed.health()
        assert not Path(temp_paths[0]).exists()

    await routed.close()
    for path in temp_paths:
        assert not Path(path).exists()


@pytest.mark.asyncio
async def test_routed_gcs_external_service_file_not_deleted(tmp_path: Path) -> None:
    key_file = tmp_path / "key.json"
    key_file.write_text(_SA_JSON, encoding="utf-8")
    secrets = _MemSecrets({_T1: {"project_id": "p1", "service_file": str(key_file)}})
    cur: UUID | None = _T1

    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: cur,
        max_cached_tenants=2,
    )
    await routed.startup()

    fake_storage = MagicMock()
    fake_storage.close = AsyncMock()

    with (
        patch("forze_gcs.kernel.client.routed_client.GCSClient", side_effect=GCSClient),
        patch("forze_gcs.kernel.client.client.Storage", return_value=fake_storage),
    ):
        await routed.health()
        await routed.close()

    assert key_file.exists()


def test_routed_gcs_rejects_zero_max_cached_tenants() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    with pytest.raises(CoreException, match="max_entries"):
        RoutedGCSClient(
            secrets=secrets,
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: _T1,
            max_cached_tenants=0,
        )


@pytest.mark.asyncio
async def test_routed_gcs_requires_tenant() -> None:
    secrets = _MemSecrets({_T1: _creds()})
    routed = RoutedGCSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=lambda: None,
        max_cached_tenants=4,
    )
    await routed.startup()
    with pytest.raises(CoreException, match="Tenant ID"):
        await routed.health()
