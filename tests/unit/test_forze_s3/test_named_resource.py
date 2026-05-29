"""Unit tests for S3 NamedResourceSpec bucket resolution."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.storage import UploadedObject
from forze.application.contracts.tenancy import TenantIdentity
from forze_s3.adapters.storage import S3StorageAdapter
from forze_s3.execution.deps import S3DepsModule
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.kernel.relation import resolve_s3_bucket


@pytest.mark.asyncio
async def test_resolve_static_bucket() -> None:
    assert await resolve_s3_bucket("my-bucket", None) == "my-bucket"


@pytest.mark.asyncio
async def test_resolve_callable_bucket() -> None:
    tid = uuid4()

    def resolver(tenant_id: object) -> str:
        assert tenant_id == tid
        return f"tenant-{tid.hex[:8]}"

    assert await resolve_s3_bucket(resolver, tid) == f"tenant-{tid.hex[:8]}"


@pytest.mark.asyncio
async def test_storage_adapter_resolves_dynamic_bucket() -> None:
    tid = uuid4()
    client = MagicMock()
    client.client.return_value.__aenter__ = AsyncMock()
    client.client.return_value.__aexit__ = AsyncMock()
    client.ensure_bucket = AsyncMock()
    client.upload_bytes = AsyncMock()

    adapter = S3StorageAdapter(
        client=client,
        bucket_spec=lambda t: f"bucket-{t}" if t else "shared",
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    await adapter.upload(UploadedObject(filename="f.txt", data=b"x", prefix=None))

    client.ensure_bucket.assert_awaited_once_with(f"bucket-{tid}")
    client.upload_bytes.assert_awaited_once()
    assert client.upload_bytes.await_args.kwargs["bucket"] == f"bucket-{tid}"


def test_s3_deps_module_warns_dynamic_bucket_with_tenant_aware() -> None:
    from unittest.mock import patch

    def _resolver(_tenant_id: object) -> str:
        return "tenant-bucket"

    with patch(
        "forze_s3.execution.deps.module.warn_dynamic_relation_with_tenant_aware",
    ) as mock_warn:
        S3DepsModule(
            client=MagicMock(),
            storages={
                "files": S3StorageConfig(bucket=_resolver, tenant_aware=True),
            },
        )

    mock_warn.assert_called_once()
    kwargs = mock_warn.call_args.kwargs
    assert kwargs["integration"] == "S3"
    assert kwargs["named_fields"] == [("bucket", _resolver)]
