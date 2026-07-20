from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.storage import UploadedObject
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_s3.adapters.storage import S3StorageAdapter

_TENANT = UUID("12345678-1234-5678-1234-567812345678")
_OWN_KEY = f"tenant_{_TENANT}/docs/object.txt"
_CROSS_TENANT_KEY = "tenant_00000000-0000-0000-0000-0000deadbeef/docs/object.txt"


@pytest.fixture
def storage_adapter_with_tenant() -> S3StorageAdapter:
    client = MagicMock()
    mock_uuid = UUID("00000000-0000-0000-0000-000000000000")
    return S3StorageAdapter(
        client=client,
        bucket_spec="test-bucket",
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=_TENANT),
        key_generator=lambda: str(mock_uuid),
    )


@pytest.mark.asyncio
async def test_upload_with_tenant(storage_adapter_with_tenant: S3StorageAdapter) -> None:
    storage_adapter_with_tenant.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter_with_tenant.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter_with_tenant.client.ensure_bucket = AsyncMock()
    storage_adapter_with_tenant.client.upload_bytes = AsyncMock()

    result = await storage_adapter_with_tenant.upload(
        UploadedObject(filename="file.txt", data=b"data", prefix="docs"),
    )
    assert (
        result.key
        == "tenant_12345678-1234-5678-1234-567812345678/docs/00000000-0000-0000-0000-000000000000"
    )


@pytest.mark.asyncio
async def test_list_with_tenant(storage_adapter_with_tenant: S3StorageAdapter) -> None:
    storage_adapter_with_tenant.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter_with_tenant.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter_with_tenant.client.ensure_bucket = AsyncMock()
    storage_adapter_with_tenant.client.list_objects = AsyncMock(return_value=([], 0))

    await storage_adapter_with_tenant.list(10, 0, prefix="docs")

    storage_adapter_with_tenant.client.list_objects.assert_called_once_with(
        bucket="test-bucket",
        prefix="tenant_12345678-1234-5678-1234-567812345678/docs",
        limit=10,
        offset=0,
        include_tags=False,
    )


# ....................... #
# Tenant isolation on the read/mutate paths (caller-supplied keys)


def _wire_client_cm(adapter: S3StorageAdapter) -> None:
    adapter.client.client.return_value.__aenter__ = AsyncMock()
    adapter.client.client.return_value.__aexit__ = AsyncMock()


@pytest.mark.asyncio
async def test_download_rejects_cross_tenant_key(
    storage_adapter_with_tenant: S3StorageAdapter,
) -> None:
    storage_adapter_with_tenant.client.download_bytes = AsyncMock()

    with pytest.raises(CoreException) as ei:
        await storage_adapter_with_tenant.download(_CROSS_TENANT_KEY)

    assert ei.value.code == "core.storage.key_outside_tenant"
    storage_adapter_with_tenant.client.download_bytes.assert_not_called()


@pytest.mark.asyncio
async def test_delete_rejects_cross_tenant_key(
    storage_adapter_with_tenant: S3StorageAdapter,
) -> None:
    storage_adapter_with_tenant.client.delete_object = AsyncMock()

    with pytest.raises(CoreException) as ei:
        await storage_adapter_with_tenant.delete(_CROSS_TENANT_KEY)

    assert ei.value.code == "core.storage.key_outside_tenant"
    storage_adapter_with_tenant.client.delete_object.assert_not_called()


@pytest.mark.asyncio
async def test_presign_download_rejects_cross_tenant_key(
    storage_adapter_with_tenant: S3StorageAdapter,
) -> None:
    from datetime import timedelta

    with pytest.raises(CoreException) as ei:
        await storage_adapter_with_tenant.presign_download(
            _CROSS_TENANT_KEY, expires_in=timedelta(minutes=5)
        )

    assert ei.value.code == "core.storage.key_outside_tenant"


@pytest.mark.asyncio
async def test_copy_rejects_cross_tenant_source(
    storage_adapter_with_tenant: S3StorageAdapter,
) -> None:
    with pytest.raises(CoreException) as ei:
        await storage_adapter_with_tenant.copy(_CROSS_TENANT_KEY, _OWN_KEY)

    assert ei.value.code == "core.storage.key_outside_tenant"


@pytest.mark.asyncio
async def test_put_object_tags_rejects_cross_tenant_key(
    storage_adapter_with_tenant: S3StorageAdapter,
) -> None:
    with pytest.raises(CoreException) as ei:
        await storage_adapter_with_tenant.put_object_tags(
            _CROSS_TENANT_KEY, {"k": "v"}
        )

    assert ei.value.code == "core.storage.key_outside_tenant"


@pytest.mark.asyncio
async def test_download_allows_own_tenant_key(
    storage_adapter_with_tenant: S3StorageAdapter,
) -> None:
    """A key inside the active tenant's own prefix (as minted by upload) still works."""

    _wire_client_cm(storage_adapter_with_tenant)
    body = MagicMock()
    body.data = b"hello"
    body.metadata = {}
    body.content_type = "text/plain"
    storage_adapter_with_tenant.client.download_bytes = AsyncMock(return_value=body)

    result = await storage_adapter_with_tenant.download(_OWN_KEY)

    assert result.data == b"hello"
    storage_adapter_with_tenant.client.download_bytes.assert_awaited_once()
