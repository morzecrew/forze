from forze.base.exceptions import CoreException
from unittest.mock import AsyncMock, MagicMock


import pytest

from forze.application.contracts.storage import UploadedObject
from forze_gcs.adapters.storage import GCSStorageAdapter, _object_metadata_from_gcs_custom

@pytest.fixture
def storage_adapter():
    client = MagicMock()
    return GCSStorageAdapter(client=client, bucket_spec="test-bucket")

def test_validate_prefix_valid(storage_adapter: GCSStorageAdapter) -> None:
    storage_adapter._validate_prefix(None)
    storage_adapter._validate_prefix("")
    storage_adapter._validate_prefix("validPrefix")
    storage_adapter._validate_prefix("prefix/with/slash")

def test_validate_prefix_invalid(storage_adapter: GCSStorageAdapter) -> None:
    with pytest.raises(CoreException, match="Invalid GCS prefix"):
        storage_adapter._validate_prefix("prefix with space")

@pytest.mark.asyncio
async def test_upload_invalid_prefix_raises(storage_adapter: GCSStorageAdapter) -> None:
    with pytest.raises(CoreException):
        await storage_adapter.upload(
            UploadedObject(filename="file.txt", data=b"data", prefix="invalid prefix"),
        )

@pytest.mark.asyncio
async def test_list_invalid_prefix_raises(storage_adapter: GCSStorageAdapter) -> None:
    with pytest.raises(CoreException):
        await storage_adapter.list(10, 0, prefix="invalid prefix")

@pytest.mark.asyncio
async def test_upload_valid_prefix_passes_validation(
    storage_adapter: GCSStorageAdapter,
) -> None:
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.upload_bytes = AsyncMock()

    await storage_adapter.upload(
        UploadedObject(filename="file.txt", data=b"data", prefix="valid/prefix"),
    )

def test_object_metadata_from_gcs_custom_coerces_string_size() -> None:
    meta = _object_metadata_from_gcs_custom(
        {
            "filename": "Zm9v.txt",
            "size": "42",
            "created_at": "2025-01-15T12:00:00+00:00",
        },
    )
    assert meta.size == 42
