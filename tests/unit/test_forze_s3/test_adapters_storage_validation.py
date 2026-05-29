from forze.base.exceptions import CoreException
from unittest.mock import AsyncMock, MagicMock


import pytest

from forze.application.contracts.storage import UploadedObject
from forze_s3.adapters.storage import S3StorageAdapter, _object_metadata_from_s3_user

@pytest.fixture
def storage_adapter():
    client = MagicMock()
    return S3StorageAdapter(client=client, bucket_spec="test-bucket")

def test_validate_prefix_valid(storage_adapter):
    # Should not raise any exception
    storage_adapter._validate_prefix(None)
    storage_adapter._validate_prefix("")
    storage_adapter._validate_prefix("validPrefix")
    storage_adapter._validate_prefix("prefix/with/slash")
    storage_adapter._validate_prefix("prefix-with-dash_and.dot")
    storage_adapter._validate_prefix("!-_.*'()/")

def test_validate_prefix_invalid(storage_adapter):
    invalid_prefixes = [
        "prefix with space",
        "prefix@invalid",
        "prefix#invalid",
        "prefix$invalid",
        "prefix%invalid",
        "prefix^invalid",
        "prefix&invalid",
        "prefix+invalid",
        "prefix=invalid",
        "prefix[invalid",
        "prefix]invalid",
        "prefix{invalid",
        "prefix}invalid",
        "prefix|invalid",
        "prefix\\invalid",
        'prefix"invalid',
        "prefix<invalid",
        "prefix>invalid",
        "prefix?invalid",
        "prefix`invalid",
    ]
    for prefix in invalid_prefixes:
        with pytest.raises(CoreException) as excinfo:
            storage_adapter._validate_prefix(prefix)
        assert f"Invalid S3 prefix: {prefix}" in str(excinfo.value)

@pytest.mark.asyncio
async def test_upload_invalid_prefix_raises(storage_adapter):
    with pytest.raises(CoreException):
        await storage_adapter.upload(
            UploadedObject(filename="file.txt", data=b"data", prefix="invalid prefix"),
        )

@pytest.mark.asyncio
async def test_list_invalid_prefix_raises(storage_adapter):
    with pytest.raises(CoreException):
        await storage_adapter.list(10, 0, prefix="invalid prefix")

@pytest.mark.asyncio
async def test_upload_valid_prefix_passes_validation(storage_adapter):
    # Mocking dependencies to ensure it proceeds past validation
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.upload_bytes = AsyncMock()

    # Should not raise ValidationError
    await storage_adapter.upload(
        UploadedObject(filename="file.txt", data=b"data", prefix="valid/prefix"),
    )

@pytest.mark.asyncio
async def test_list_valid_prefix_passes_validation(storage_adapter):
    # Mocking dependencies to ensure it proceeds past validation
    storage_adapter.client.client.return_value.__aenter__ = AsyncMock()
    storage_adapter.client.client.return_value.__aexit__ = AsyncMock()
    storage_adapter.client.ensure_bucket = AsyncMock()
    storage_adapter.client.list_objects = AsyncMock(return_value=([], 0))

    # Should not raise ValidationError
    await storage_adapter.list(10, 0, prefix="valid/prefix")

def test_object_metadata_from_s3_user_coerces_string_size() -> None:
    meta = _object_metadata_from_s3_user(
        {
            "filename": "Zm9v.txt",
            "size": "42",
            "created_at": "2025-01-15T12:00:00+00:00",
        },
    )
    assert meta.size == 42
    assert meta.filename == "Zm9v.txt"

def test_object_metadata_from_s3_user_accepts_zulu_timestamp() -> None:
    meta = _object_metadata_from_s3_user(
        {
            "filename": "x",
            "size": "1",
            "created_at": "2025-01-15T12:00:00Z",
        },
    )
    assert meta.created_at.isoformat().startswith("2025-01-15T12:00:00")
