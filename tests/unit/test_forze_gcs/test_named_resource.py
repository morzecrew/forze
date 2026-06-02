"""Unit tests for GCS NamedResourceSpec bucket resolution."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from forze.application.contracts.storage import UploadedObject
from forze.application.contracts.tenancy import TenantIdentity
from forze_gcs.adapters.storage import GCSStorageAdapter
from forze_gcs.execution.deps import GCSDepsModule
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.kernel.relation import resolve_gcs_bucket


@pytest.mark.asyncio
async def test_resolve_static_bucket() -> None:
    assert await resolve_gcs_bucket("my-bucket", None) == "my-bucket"


@pytest.mark.asyncio
async def test_resolve_callable_bucket() -> None:
    tid = uuid4()

    def resolver(tenant_id: object) -> str:
        assert tenant_id == tid
        return f"tenant-{tid.hex[:8]}"

    assert await resolve_gcs_bucket(resolver, tid) == f"tenant-{tid.hex[:8]}"


@pytest.mark.asyncio
async def test_storage_adapter_resolves_dynamic_bucket() -> None:
    tid = uuid4()
    client = MagicMock()
    client.client.return_value.__aenter__ = AsyncMock()
    client.client.return_value.__aexit__ = AsyncMock()
    client.ensure_bucket = AsyncMock()
    client.upload_bytes = AsyncMock()

    adapter = GCSStorageAdapter(
        client=client,
        bucket_spec=lambda t: f"bucket-{t}" if t else "shared",
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    await adapter.upload(UploadedObject(filename="f.txt", data=b"x", prefix=None))

    client.ensure_bucket.assert_awaited_once_with(f"bucket-{tid}")
    assert client.upload_bytes.await_args.kwargs["bucket"] == f"bucket-{tid}"


def test_gcs_deps_module_warns_dynamic_bucket_with_tenant_aware() -> None:
    def _resolver(_tenant_id: object) -> str:
        return "tenant-bucket"

    with patch(
        "forze_gcs.execution.deps.module.warn_integration_routes",
    ) as mock_warn:
        GCSDepsModule(
            client=MagicMock(),
            storages={
                "files": GCSStorageConfig(bucket=_resolver, tenant_aware=True),
            },
        )

    mock_warn.assert_called_once()
    kwargs = mock_warn.call_args.kwargs
    assert kwargs["integration"] == "GCS"
    assert kwargs["routes"] is not None
    assert kwargs["routes"]["files"].bucket is _resolver
