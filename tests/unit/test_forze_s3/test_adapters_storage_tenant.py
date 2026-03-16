from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze_s3.adapters.storage import S3StorageAdapter


class MockTenantContext:
    def get(self):
        return UUID("12345678-1234-5678-1234-567812345678")


@pytest.fixture
def storage_adapter_with_tenant():
    client = MagicMock()
    return S3StorageAdapter(
        client=client, bucket="test-bucket", tenant_context=MockTenantContext()
    )


@pytest.mark.asyncio
async def test_upload_with_tenant(storage_adapter_with_tenant, monkeypatch):
    mock_uuid = UUID("00000000-0000-0000-0000-000000000000")
    monkeypatch.setattr("forze_s3.adapters.storage.uuid7", lambda: mock_uuid)

    storage_adapter_with_tenant.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter_with_tenant.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter_with_tenant.client.ensure_bucket = AsyncMock()
    storage_adapter_with_tenant.client.upload_bytes = AsyncMock()

    result = await storage_adapter_with_tenant.upload(
        "file.txt", b"data", prefix="docs"
    )
    assert (
        result["key"]
        == "12345678-1234-5678-1234-567812345678/docs/00000000-0000-0000-0000-000000000000"
    )


@pytest.mark.asyncio
async def test_list_with_tenant(storage_adapter_with_tenant):
    storage_adapter_with_tenant.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter_with_tenant.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter_with_tenant.client.ensure_bucket = AsyncMock()
    storage_adapter_with_tenant.client.list_objects = AsyncMock(return_value=([], 0))

    await storage_adapter_with_tenant.list(10, 0, prefix="docs")

    storage_adapter_with_tenant.client.list_objects.assert_called_once_with(
        bucket="test-bucket",
        prefix="12345678-1234-5678-1234-567812345678/docs",
        limit=10,
        offset=0,
    )
